// Raft coordinates leadership only. Application configuration lives in D1;
// runtime reports and their replication tail never enter the durable Raft log.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	raft "go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

type member struct {
	ID    uint64 `json:"id"`
	URL   string `json:"url"`
	Label string `json:"label"`
}
type mark struct {
	Index uint64 `json:"index"`
	Term  uint64 `json:"term"`
}

// Only protocol metadata is stored: term/vote, membership, committed boundary,
// and the term/index of pending no-ops. No commands or runtime rlog entries.
type durable struct {
	Hard     pb.HardState        `json:"hard"`
	Snapshot pb.SnapshotMetadata `json:"snapshot"`
	Tail     []mark              `json:"tail"`
}
type statement struct {
	SQL    string `json:"sql"`
	Params []any  `json:"params"`
}
type d1 struct {
	url, key, email string
	http            *http.Client
}

func (d *d1) query(batch ...statement) ([]map[string]any, error) {
	b, _ := json.Marshal(map[string]any{"batch": batch})
	req, e := http.NewRequest("POST", d.url, bytes.NewReader(b))
	if e != nil {
		return nil, e
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Auth-Key", d.key)
	req.Header.Set("X-Auth-Email", d.email)
	res, e := d.http.Do(req)
	if e != nil {
		return nil, errors.New("D1 transport failed")
	}
	defer res.Body.Close()
	var out struct {
		Success bool `json:"success"`
		Result  []struct {
			Success bool             `json:"success"`
			Results []map[string]any `json:"results"`
		} `json:"result"`
	}
	if json.NewDecoder(io.LimitReader(res.Body, 4<<20)).Decode(&out) != nil || res.StatusCode != 200 || !out.Success {
		return nil, fmt.Errorf("D1 metadata request failed (HTTP %d)", res.StatusCode)
	}
	var rows []map[string]any
	for _, r := range out.Result {
		if !r.Success {
			return nil, errors.New("D1 metadata statement failed")
		}
		rows = append(rows, r.Results...)
	}
	return rows, nil
}
func randomID() string {
	b := make([]byte, 16)
	if _, e := rand.Read(b); e != nil {
		panic(e)
	}
	return hex.EncodeToString(b)
}
func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
func number(k string, def int) int {
	v := env(k, strconv.Itoa(def))
	n, e := strconv.Atoi(v)
	if e != nil {
		panic("invalid " + k)
	}
	return n
}

var errIdentityBusy = errors.New("member identity is still owned by another process")

type ownerStore struct {
	db            *d1
	id            uint64
	cluster, boot string
	rev           int64
	deadline      time.Time
	wallDeadline  int64
}

func (s *ownerStore) live() bool {
	return time.Now().Before(s.deadline) && time.Now().UnixMilli() < s.wallDeadline
}
func (s *ownerStore) setDeadline(start time.Time) {
	s.deadline = start.Add(85 * time.Second)
	s.wallDeadline = start.UnixMilli() + 85000
}
func (s *ownerStore) claim(initial durable) (durable, error) {
	b, _ := json.Marshal(initial)
	start := time.Now()
	rows, e := s.db.query(statement{"INSERT INTO cp_raft_state(node_id,cluster_id,owner,revision,lease_until,state) SELECT ?,?,?,1,unixepoch()+90,? WHERE EXISTS(SELECT 1 FROM cp_raft_state WHERE node_id=? AND cluster_id=?) OR EXISTS(SELECT 1 FROM cp_cluster WHERE id=1 AND cluster_id=? AND active=0) ON CONFLICT(node_id) DO UPDATE SET owner=excluded.owner,revision=cp_raft_state.revision+1,lease_until=excluded.lease_until WHERE cp_raft_state.cluster_id=excluded.cluster_id AND cp_raft_state.lease_until<unixepoch() RETURNING revision,state", []any{s.id, s.cluster, s.boot, string(b), s.id, s.cluster, s.cluster}})
	if e != nil {
		return durable{}, e
	}
	if len(rows) != 1 {
		owned, err := s.db.query(statement{"SELECT node_id FROM cp_raft_state WHERE node_id=? AND cluster_id=? AND lease_until>=unixepoch()", []any{s.id, s.cluster}})
		if err != nil {
			return durable{}, err
		}
		if len(owned) == 1 {
			return durable{}, errIdentityBusy
		}
		return durable{}, errors.New("member election state is missing; refusing to reset an active voter")
	}
	s.rev = int64(rows[0]["revision"].(float64))
	s.setDeadline(start)
	var saved durable
	if e = json.Unmarshal([]byte(rows[0]["state"].(string)), &saved); e != nil {
		return saved, errors.New("invalid persisted Raft metadata")
	}
	return saved, nil
}
func (s *ownerStore) save(state *durable) error {
	if !s.live() {
		return errors.New("member identity lease expired")
	}
	start := time.Now()
	sql := "UPDATE cp_raft_state SET revision=revision+1,lease_until=unixepoch()+90"
	params := []any{}
	if state != nil {
		b, e := json.Marshal(state)
		if e != nil {
			return e
		}
		sql += ",state=?"
		params = append(params, string(b))
	}
	sql += " WHERE node_id=? AND cluster_id=? AND owner=? AND revision=? AND lease_until>=unixepoch() RETURNING revision"
	params = append(params, s.id, s.cluster, s.boot, s.rev)
	rows, e := s.db.query(statement{sql, params})
	if e != nil {
		return e
	}
	if len(rows) != 1 {
		return errors.New("member identity changed; refusing to vote")
	}
	s.rev++
	s.setDeadline(start)
	return nil
}
func (s *ownerStore) release() {
	_, _ = s.db.query(statement{"UPDATE cp_raft_state SET lease_until=0 WHERE node_id=? AND cluster_id=? AND owner=?", []any{s.id, s.cluster, s.boot}})
}

type view struct {
	ID            uint64 `json:"id"`
	Leader        uint64 `json:"leader"`
	Term          uint64 `json:"term"`
	Role          string `json:"role"`
	Applied       uint64 `json:"applied"`
	Boot          string `json:"boot"`
	Members       int    `json:"members"`
	Quorum        int    `json:"quorum"`
	IdentityValid bool   `json:"identityValid"`
}
type verification struct {
	ctx   context.Context
	reply chan view
	id    string
}
type delivery struct {
	to           uint64
	snapshot, ok bool
}

type core struct {
	id        uint64
	members   []member
	peers     map[uint64]string
	secret    string
	owner     *ownerStore
	rn        *raft.RawNode
	mem       *raft.MemoryStorage
	hard      pb.HardState
	applied   uint64
	conf      pb.ConfState
	incoming  chan pb.Message
	verify    chan verification
	transfer  chan uint64
	delivered chan delivery
	pending   map[string]verification
	mu        sync.RWMutex
	status    view
	http      *http.Client
}

func (c *core) publish() {
	st := c.rn.BasicStatus()
	c.mu.Lock()
	c.status = view{c.id, st.Lead, st.Term, st.RaftState.String(), c.applied, c.owner.boot, len(c.members), len(c.members)/2 + 1, c.owner.live()}
	c.mu.Unlock()
}
func (c *core) snapshot() view { c.mu.RLock(); defer c.mu.RUnlock(); return c.status }
func (c *core) durable() (durable, error) {
	snap, e := c.mem.Snapshot()
	if e != nil {
		return durable{}, e
	}
	lo, _ := c.mem.FirstIndex()
	hi, _ := c.mem.LastIndex()
	out := durable{Hard: c.hard, Snapshot: snap.Metadata, Tail: []mark{}}
	if hi >= lo {
		entries, e := c.mem.Entries(lo, hi+1, math.MaxUint64)
		if e != nil {
			return out, e
		}
		for _, x := range entries {
			if x.Type != pb.EntryNormal || len(x.Data) != 0 {
				return out, errors.New("application commands are not permitted in the leadership protocol")
			}
			out.Tail = append(out.Tail, mark{x.Index, x.Term})
		}
	}
	return out, nil
}
func (c *core) send(m pb.Message) {
	address := c.peers[m.To]
	if address == "" {
		return
	}
	b, e := m.Marshal()
	if e != nil {
		return
	}
	go func() {
		req, e := http.NewRequest("POST", strings.TrimRight(address, "/")+"/_cp/raft", bytes.NewReader(b))
		if e != nil {
			return
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set("X-CP-Token", c.secret)
		req.Header.Set("X-CP-From", strconv.FormatUint(c.id, 10))
		res, e := c.http.Do(req)
		ok := e == nil && res.StatusCode >= 200 && res.StatusCode < 300
		if e == nil {
			io.Copy(io.Discard, io.LimitReader(res.Body, 4096))
			res.Body.Close()
		}
		select {
		case c.delivered <- delivery{m.To, m.Type == pb.MsgSnap, ok}:
		default:
		}
	}()
}
func (c *core) ready() error {
	for c.rn.HasReady() {
		rd := c.rn.Ready()
		changed := false
		if !raft.IsEmptySnap(rd.Snapshot) {
			if e := c.mem.ApplySnapshot(rd.Snapshot); e != nil {
				return e
			}
			c.conf = rd.Snapshot.Metadata.ConfState
			c.applied = rd.Snapshot.Metadata.Index
			changed = true
		}
		if len(rd.Entries) > 0 {
			for _, e := range rd.Entries {
				if e.Type != pb.EntryNormal || len(e.Data) > 0 {
					return errors.New("unexpected application entry in leadership-only Raft")
				}
			}
			if e := c.mem.Append(rd.Entries); e != nil {
				return e
			}
			changed = true
		}
		if !raft.IsEmptyHardState(rd.HardState) {
			c.hard = rd.HardState
			if e := c.mem.SetHardState(c.hard); e != nil {
				return e
			}
			changed = true
		}
		// Persist before sending any vote, append acknowledgment or heartbeat which
		// depends on new term/log metadata. Losing a D1 response terminates this voter.
		if changed {
			state, e := c.durable()
			if e != nil {
				return e
			}
			if e = c.owner.save(&state); e != nil {
				return e
			}
		}
		if !c.owner.live() {
			return errors.New("member identity expired before reply")
		}
		for _, m := range rd.Messages {
			c.send(m)
		}
		for _, entry := range rd.CommittedEntries {
			c.applied = entry.Index
		}
		// No runtime FSM is persisted. All committed entries are empty leadership
		// barriers, so their complete state is the index/term and fixed membership.
		if len(rd.CommittedEntries) > 0 {
			if _, e := c.mem.CreateSnapshot(c.applied, &c.conf, nil); e != nil {
				return e
			}
			if e := c.mem.Compact(c.applied); e != nil && !errors.Is(e, raft.ErrCompacted) {
				return e
			}
			state, e := c.durable()
			if e != nil {
				return e
			}
			if e = c.owner.save(&state); e != nil {
				return e
			}
		}
		c.publish()
		for _, read := range rd.ReadStates {
			p, ok := c.pending[string(read.RequestCtx)]
			if !ok {
				continue
			}
			delete(c.pending, string(read.RequestCtx))
			v := c.snapshot()
			if v.Leader == c.id && v.Role == "StateLeader" && c.applied >= read.Index && c.owner.live() {
				select {
				case p.reply <- v:
				default:
				}
			}
		}
		c.rn.Advance(rd)
	}
	return nil
}
func (c *core) run(ctx context.Context) error {
	tick := time.NewTicker(time.Duration(number("CP_RAFT_TICK_MS", 500)) * time.Millisecond)
	defer tick.Stop()
	renew := time.NewTicker(30 * time.Second)
	defer renew.Stop()
	if e := c.ready(); e != nil {
		return e
	}
	c.publish()
	for {
		select {
		case <-ctx.Done():
			c.owner.release()
			return nil
		case <-tick.C:
			if !c.owner.live() {
				return errors.New("member identity lease expired")
			}
			c.rn.Tick()
			for id, p := range c.pending {
				if p.ctx.Err() != nil {
					delete(c.pending, id)
				}
			}
		case <-renew.C:
			if e := c.owner.save(nil); e != nil {
				return e
			}
		case m := <-c.incoming:
			if !c.owner.live() {
				return errors.New("member identity lease expired")
			}
			if e := c.rn.Step(m); e != nil && !errors.Is(e, raft.ErrStepPeerNotFound) {
				return e
			}
		case sent := <-c.delivered:
			if !sent.ok {
				c.rn.ReportUnreachable(sent.to)
			}
			if sent.snapshot {
				status := raft.SnapshotFailure
				if sent.ok {
					status = raft.SnapshotFinish
				}
				c.rn.ReportSnapshot(sent.to, status)
			}
		case target := <-c.transfer:
			if c.rn.BasicStatus().Lead == c.id {
				if target == 0 {
					for id, progress := range c.rn.Status().Progress {
						if id != c.id && progress.RecentActive && progress.Match >= c.applied {
							target = id
							break
						}
					}
				}
				if target != 0 && target != c.id && c.peers[target] != "" {
					c.rn.TransferLeader(target)
				}
			}
		case p := <-c.verify:
			v := c.snapshot()
			if v.Leader != c.id || v.Role != "StateLeader" || !c.owner.live() {
				close(p.reply)
			} else {
				c.pending[p.id] = p
				c.rn.ReadIndex([]byte(p.id))
			}
		}
		if e := c.ready(); e != nil {
			return e
		}
		c.publish()
	}
}
func (c *core) handler(w http.ResponseWriter, r *http.Request) {
	if subtle.ConstantTimeCompare([]byte(r.Header.Get("X-CP-Token")), []byte(c.secret)) != 1 {
		http.Error(w, "unauthorized", 401)
		return
	}
	switch r.URL.Path {
	case "/status":
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(c.snapshot())
	case "/transfer":
		if r.Method != "POST" {
			http.Error(w, "POST required", 405)
			return
		}
		target, _ := strconv.ParseUint(r.URL.Query().Get("to"), 10, 64)
		select {
		case c.transfer <- target:
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte("{}"))
		case <-r.Context().Done():
		}
	case "/verify":
		ctx, cancel := context.WithTimeout(r.Context(), 4*time.Second)
		defer cancel()
		p := verification{ctx, make(chan view, 1), randomID()}
		select {
		case c.verify <- p:
		case <-ctx.Done():
			http.Error(w, "quorum unavailable", 503)
			return
		}
		select {
		case v, ok := <-p.reply:
			if !ok {
				http.Error(w, "not leader", 409)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(v)
		case <-ctx.Done():
			http.Error(w, "quorum unavailable", 503)
		}
	case "/message", "/_cp/raft":
		b, e := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		var m pb.Message
		if e != nil || m.Unmarshal(b) != nil || m.To != c.id || c.peers[m.From] == "" {
			http.Error(w, "invalid message", 400)
			return
		}
		select {
		case c.incoming <- m:
			w.WriteHeader(204)
		case <-r.Context().Done():
		case <-time.After(3 * time.Second):
			http.Error(w, "busy", 503)
		}
	default:
		http.NotFound(w, r)
	}
}
func main() {
	if e := start(); e != nil {
		if errors.Is(e, errIdentityBusy) {
			log.Print(e)
			os.Exit(75)
		}
		log.Fatal(e)
	}
}
func start() error {
	var members []member
	if e := json.Unmarshal([]byte(os.Getenv("CP_MEMBERS")), &members); e != nil {
		return errors.New("invalid CP_MEMBERS")
	}
	n := len(members)
	if n < 3 || n%2 == 0 {
		return errors.New("replicated controllers require an odd member count of at least 3")
	}
	id := uint64(number("CP_NODE_ID", 0))
	peers := map[uint64]string{}
	ids := []uint64{}
	for _, m := range members {
		if m.ID == 0 || peers[m.ID] != "" || m.URL == "" {
			return errors.New("invalid member identities")
		}
		peers[m.ID] = m.URL
		ids = append(ids, m.ID)
	}
	if peers[id] == "" {
		return errors.New("CP_NODE_ID is not a cluster member")
	}
	var overrides map[string]string
	if raw := os.Getenv("CP_PEER_OVERRIDES"); raw != "" {
		if json.Unmarshal([]byte(raw), &overrides) != nil {
			return errors.New("invalid CP_PEER_OVERRIDES")
		}
		for k, v := range overrides {
			x, e := strconv.ParseUint(k, 10, 64)
			if e != nil || peers[x] == "" {
				return errors.New("unknown peer override")
			}
			peers[x] = v
		}
	}
	secret := os.Getenv("CP_CLUSTER_TOKEN")
	cluster := os.Getenv("CP_CLUSTER_ID")
	if len(secret) < 32 || cluster == "" {
		return errors.New("cluster identity and token are required")
	}
	account := env("D1_ACCOUNT_ID", os.Getenv("ACCOUNT_ID"))
	database := os.Getenv("D1_DATABASE_ID")
	key := env("D1_API_KEY", os.Getenv("CF_API_KEY"))
	email := env("D1_API_EMAIL", os.Getenv("CF_API_EMAIL"))
	if account == "" || database == "" || key == "" || email == "" {
		return errors.New("D1 credentials are required")
	}
	db := &d1{env("D1_API_BASE", "https://api.cloudflare.com/client/v4") + "/accounts/" + account + "/d1/database/" + database + "/query", key, email, &http.Client{Timeout: 15 * time.Second}}
	own := &ownerStore{db: db, id: id, cluster: cluster, boot: randomID()}
	initial := durable{Hard: pb.HardState{Term: 1, Commit: 1}, Snapshot: pb.SnapshotMetadata{Index: 1, Term: 1, ConfState: pb.ConfState{Voters: ids}}, Tail: []mark{}}
	saved, e := own.claim(initial)
	if e != nil {
		return e
	}
	savedIDs := append([]uint64(nil), saved.Snapshot.ConfState.Voters...)
	configuredIDs := append([]uint64(nil), ids...)
	slices.Sort(savedIDs)
	slices.Sort(configuredIDs)
	if !slices.Equal(savedIDs, configuredIDs) || len(saved.Snapshot.ConfState.VotersOutgoing) > 0 {
		return errors.New("persisted membership differs; coordinated reconfiguration required")
	}
	mem := raft.NewMemoryStorage()
	if e = mem.ApplySnapshot(pb.Snapshot{Metadata: saved.Snapshot}); e != nil {
		return e
	}
	if e = mem.SetHardState(saved.Hard); e != nil {
		return e
	}
	entries := []pb.Entry{}
	for _, x := range saved.Tail {
		entries = append(entries, pb.Entry{Index: x.Index, Term: x.Term, Type: pb.EntryNormal})
	}
	if e = mem.Append(entries); e != nil {
		return e
	}
	rn, e := raft.NewRawNode(&raft.Config{ID: id, ElectionTick: number("CP_RAFT_ELECTION_TICKS", 20), HeartbeatTick: number("CP_RAFT_HEARTBEAT_TICKS", 2), Storage: mem, Applied: saved.Snapshot.Index, MaxSizePerMsg: 64 << 10, MaxInflightMsgs: 16, CheckQuorum: true, PreVote: true, ReadOnlyOption: raft.ReadOnlySafe})
	if e != nil {
		return e
	}
	c := &core{id: id, members: members, peers: peers, secret: secret, owner: own, rn: rn, mem: mem, hard: saved.Hard, applied: saved.Snapshot.Index, conf: saved.Snapshot.ConfState, incoming: make(chan pb.Message, 256), verify: make(chan verification, 128), transfer: make(chan uint64, 4), delivered: make(chan delivery, 512), pending: map[string]verification{}, http: &http.Client{Timeout: 4 * time.Second}}
	c.publish()
	server := &http.Server{Addr: "127.0.0.1:" + env("CP_RAFT_PORT", "8931"), Handler: http.HandlerFunc(c.handler), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if e := server.ListenAndServe(); e != nil && e != http.ErrServerClosed {
			log.Fatal("Raft HTTP server failed")
		}
	}()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return c.run(ctx)
}
