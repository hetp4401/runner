// Volatile Raft membership and leadership. A process has a fresh, unique Raft
// identity on every start. It never restarts a forgotten voter: a surviving
// quorum must add it as a learner and replace its previous incarnation through
// joint consensus. Losing the quorum's memory requires explicit bootstrap.
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
type identity struct {
	Node uint64 `json:"node"`
	ID   uint64 `json:"id"`
	Boot string `json:"boot"`
}
type registry struct {
	Generation string              `json:"generation"`
	Slots      map[uint64]uint64   `json:"slots"`
	Nodes      map[uint64]identity `json:"nodes"`
}
type manifest struct {
	Generation string     `json:"generation"`
	Term       uint64     `json:"term"`
	Members    []identity `json:"members"`
}
type change struct {
	Kind     string   `json:"kind"`
	Member   identity `json:"member"`
	Previous uint64   `json:"previous"`
}
type view struct {
	ID            uint64   `json:"id"`
	Node          uint64   `json:"node"`
	Leader        uint64   `json:"leader"`
	LeaderNode    uint64   `json:"leaderNode"`
	Term          uint64   `json:"term"`
	Role          string   `json:"role"`
	Applied       uint64   `json:"applied"`
	Boot          string   `json:"boot"`
	Members       int      `json:"members"`
	Quorum        int      `json:"quorum"`
	IdentityValid bool     `json:"identityValid"`
	Phase         string   `json:"phase"`
	Generation    string   `json:"generation"`
	Registry      registry `json:"registry"`
	Voters        []uint64 `json:"voters"`
	Outgoing      []uint64 `json:"outgoing"`
	Learners      []uint64 `json:"learners"`
}
type verification struct {
	ctx   context.Context
	reply chan view
	id    string
}
type controlRequest struct {
	kind  string
	data  []byte
	reply chan controlReply
}
type controlReply struct {
	value any
	err   error
}
type delivery struct {
	to, sequence uint64
	snapshot, ok bool
}
type envelope struct {
	Sequence uint64 `json:"sequence"`
	Data     []byte `json:"data"`
}
type adopted struct{ state registry }

type core struct {
	self                 identity
	members              []member
	peers                map[uint64]string
	cluster, secret      string
	rn                   *raft.RawNode
	mem                  *raft.MemoryStorage
	state                registry
	conf                 pb.ConfState
	applied              uint64
	retired, confPending bool
	incoming             chan pb.Message
	verify               chan verification
	commands             chan controlRequest
	adopt                chan adopted
	transfer             chan uint64
	delivered            chan delivery
	pending              map[string]verification
	learnerSeen          map[uint64]time.Time
	mu                   sync.RWMutex
	status               view
	mailMu               sync.Mutex
	mail                 map[uint64][]envelope
	sequence             uint64
	http                 *http.Client
}

func randomID() string {
	b := make([]byte, 16)
	if _, e := rand.Read(b); e != nil {
		panic(e)
	}
	return hex.EncodeToString(b)
}
func randomNode() uint64 {
	b := make([]byte, 6)
	if _, e := rand.Read(b); e != nil {
		panic(e)
	}
	var n uint64
	for _, v := range b {
		n = n<<8 | uint64(v)
	}
	if n == 0 {
		return randomNode()
	}
	return n
}
func env(k, fallback string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return fallback
}
func number(k string, fallback int) int {
	v, e := strconv.Atoi(env(k, strconv.Itoa(fallback)))
	if e != nil {
		panic("invalid " + k)
	}
	return v
}
func emptyRegistry() registry {
	return registry{Slots: map[uint64]uint64{}, Nodes: map[uint64]identity{}}
}
func cloneRegistry(s registry) registry {
	out := emptyRegistry()
	out.Generation = s.Generation
	for k, v := range s.Slots {
		out.Slots[k] = v
	}
	for k, v := range s.Nodes {
		out.Nodes[k] = v
	}
	return out
}
func contains(list []uint64, id uint64) bool { return slices.Contains(list, id) }
func voter(conf pb.ConfState, id uint64) bool {
	return contains(conf.Voters, id) || contains(conf.VotersOutgoing, id)
}
func known(conf pb.ConfState, id uint64) bool {
	return voter(conf, id) || contains(conf.Learners, id) || contains(conf.LearnersNext, id)
}
func (c *core) publish() {
	v := view{ID: c.self.ID, Node: c.self.Node, Boot: c.self.Boot, Members: len(c.members), Quorum: len(c.members)/2 + 1,
		Phase: "uninitialized", Generation: c.state.Generation, Registry: cloneRegistry(c.state), Role: "Uninitialized"}
	if c.rn != nil {
		st := c.rn.BasicStatus()
		v.LeaderNode = st.Lead
		v.Leader = c.state.Nodes[st.Lead].ID
		v.Term = st.Term
		v.Role = st.RaftState.String()
		v.Applied = c.applied
		v.Voters = append([]uint64{}, c.conf.Voters...)
		v.Outgoing = append([]uint64{}, c.conf.VotersOutgoing...)
		v.Learners = append([]uint64{}, c.conf.Learners...)
		v.IdentityValid = voter(c.conf, c.self.Node) && c.state.Slots[c.self.ID] == c.self.Node && !c.retired
		v.Phase = "joining"
		if v.IdentityValid {
			v.Phase = "voter"
		}
		if c.retired {
			v.Phase = "retired"
		}
	}
	c.mu.Lock()
	c.status = v
	c.mu.Unlock()
}
func (c *core) snapshot() view { c.mu.RLock(); defer c.mu.RUnlock(); return c.status }
func (c *core) initialize(state registry, seed *manifest) error {
	if c.rn != nil {
		return errors.New("process already has cluster state")
	}
	c.state = cloneRegistry(state)
	c.mem = raft.NewMemoryStorage()
	if seed != nil {
		ids := []uint64{}
		for _, m := range seed.Members {
			ids = append(ids, m.Node)
		}
		slices.Sort(ids)
		c.conf = pb.ConfState{Voters: ids}
		c.applied = 1
		data, _ := json.Marshal(c.state)
		if e := c.mem.ApplySnapshot(pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 1, Term: seed.Term, ConfState: c.conf}, Data: data}); e != nil {
			return e
		}
		if e := c.mem.SetHardState(pb.HardState{Term: seed.Term, Commit: 1}); e != nil {
			return e
		}
	}
	rn, e := raft.NewRawNode(&raft.Config{ID: c.self.Node, ElectionTick: number("CP_RAFT_ELECTION_TICKS", 20), HeartbeatTick: number("CP_RAFT_HEARTBEAT_TICKS", 2),
		Storage: c.mem, Applied: c.applied, MaxSizePerMsg: 64 << 10, MaxInflightMsgs: 16, CheckQuorum: true, PreVote: true, ReadOnlyOption: raft.ReadOnlySafe, StepDownOnRemoval: true})
	if e != nil {
		return e
	}
	c.rn = rn
	return nil
}
func (c *core) bootstrap(raw []byte) error {
	var m manifest
	if json.Unmarshal(raw, &m) != nil || m.Generation == "" || m.Term < 1 || m.Term > 1<<52 || len(m.Members) != len(c.members) {
		return errors.New("invalid bootstrap manifest")
	}
	if c.state.Generation != "" {
		if c.state.Generation == m.Generation {
			return nil
		}
		return errors.New("refusing to overwrite an initialized cluster")
	}
	state := emptyRegistry()
	state.Generation = m.Generation
	for _, r := range m.Members {
		if r.Node == 0 || r.Node > 1<<52 || r.Boot == "" || c.peers[r.ID] == "" || state.Slots[r.ID] != 0 || state.Nodes[r.Node].Node != 0 {
			return errors.New("invalid bootstrap identities")
		}
		if r.ID == c.self.ID && r != c.self {
			return errors.New("bootstrap targets another incarnation")
		}
		state.Nodes[r.Node] = r
		state.Slots[r.ID] = r.Node
	}
	if state.Slots[c.self.ID] != c.self.Node {
		return errors.New("bootstrap omits this incarnation")
	}
	return c.initialize(state, &m)
}
func (c *core) propose(cc pb.ConfChangeV2, data change) error {
	if c.confPending || len(c.conf.VotersOutgoing) > 0 {
		return errors.New("membership change is in progress")
	}
	cc.Context, _ = json.Marshal(data)
	if e := c.rn.ProposeConfChange(cc); e != nil {
		return e
	}
	c.confPending = true
	return nil
}
func (c *core) join(raw []byte) (registry, error) {
	if c.rn == nil || c.rn.BasicStatus().Lead != c.self.Node || c.retired {
		return registry{}, errors.New("not the current leader")
	}
	var r identity
	if json.Unmarshal(raw, &r) != nil || r.Node == 0 || r.Node > 1<<52 || len(r.Boot) < 16 || c.peers[r.ID] == "" {
		return registry{}, errors.New("invalid joining incarnation")
	}
	if before, ok := c.state.Nodes[r.Node]; ok {
		if before != r {
			return registry{}, errors.New("Raft identity collision")
		}
		return cloneRegistry(c.state), nil
	}
	if len(c.conf.Learners) > 0 || len(c.conf.LearnersNext) > 0 {
		return registry{}, errors.New("another replacement is joining")
	}
	cc := pb.ConfChangeV2{Changes: []pb.ConfChangeSingle{{Type: pb.ConfChangeAddLearnerNode, NodeID: r.Node}}}
	if e := c.propose(cc, change{Kind: "learner", Member: r, Previous: c.state.Slots[r.ID]}); e != nil {
		return registry{}, e
	}
	// The response allows transport before the membership entry commits. It
	// grants no vote: only ApplyConfChange can make the process a voter.
	result := cloneRegistry(c.state)
	result.Nodes[r.Node] = r
	return result, nil
}
func (c *core) promote() {
	if c.rn == nil || c.retired || c.confPending || len(c.conf.VotersOutgoing) > 0 {
		return
	}
	status := c.rn.Status()
	if status.Lead != c.self.Node {
		return
	}
	for node := range c.learnerSeen {
		if !contains(c.conf.Learners, node) {
			delete(c.learnerSeen, node)
		}
	}
	for _, node := range c.conf.Learners {
		r := c.state.Nodes[node]
		old := c.state.Slots[r.ID]
		p, ok := status.Progress[node]
		if c.learnerSeen[node].IsZero() || (ok && p.RecentActive) {
			c.learnerSeen[node] = time.Now()
		}
		if time.Since(c.learnerSeen[node]) > 90*time.Second {
			// A failed staging process must not block later replacements. This
			// removes only a non-voting learner; the configured quorum is intact.
			cc := pb.ConfChangeV2{Changes: []pb.ConfChangeSingle{{Type: pb.ConfChangeRemoveNode, NodeID: node}}}
			_ = c.propose(cc, change{Kind: "discard", Member: r})
			return
		}
		if !ok || !p.RecentActive || p.Match < c.applied || r.ID == 0 || old == 0 || old == node {
			continue
		}
		if old == c.self.Node {
			for id, peer := range status.Progress {
				if id != old && !peer.IsLearner && peer.RecentActive && peer.Match >= c.applied {
					c.rn.TransferLeader(id)
					return
				}
			}
			return
		}
		// Both old and new voter majorities must accept the handoff. Only after
		// leaving joint consensus does the new incarnation satisfy app readiness.
		cc := pb.ConfChangeV2{Transition: pb.ConfChangeTransitionJointImplicit, Changes: []pb.ConfChangeSingle{
			{Type: pb.ConfChangeRemoveNode, NodeID: old}, {Type: pb.ConfChangeAddNode, NodeID: node}}}
		_ = c.propose(cc, change{Kind: "replace", Member: r, Previous: old})
		return
	}
}
func (c *core) apply(entry pb.Entry) error {
	switch entry.Type {
	case pb.EntryNormal:
		if len(entry.Data) > 0 {
			return errors.New("application payloads are not allowed in leadership Raft")
		}
	case pb.EntryConfChangeV2:
		var cc pb.ConfChangeV2
		if e := cc.Unmarshal(entry.Data); e != nil {
			return e
		}
		if len(cc.Context) > 0 {
			var data change
			if e := json.Unmarshal(cc.Context, &data); e != nil {
				return e
			}
			if c.peers[data.Member.ID] == "" || data.Member.Node == 0 {
				return errors.New("invalid committed membership")
			}
			c.state.Nodes[data.Member.Node] = data.Member
			if data.Kind == "replace" {
				c.state.Slots[data.Member.ID] = data.Member.Node
			} else if data.Kind != "learner" && data.Kind != "discard" {
				return errors.New("unknown membership operation")
			}
		}
		c.conf = *c.rn.ApplyConfChange(cc)
		c.confPending = false
		if len(c.conf.VotersOutgoing) == 0 {
			for id := range c.state.Nodes {
				if !known(c.conf, id) {
					delete(c.state.Nodes, id)
					c.mailMu.Lock()
					delete(c.mail, id)
					c.mailMu.Unlock()
				}
			}
			if !known(c.conf, c.self.Node) {
				c.retired = true
			}
		}
	default:
		return errors.New("unexpected Raft entry type")
	}
	c.applied = entry.Index
	return nil
}
func (c *core) ready() error {
	if c.rn == nil {
		return nil
	}
	for c.rn.HasReady() {
		rd := c.rn.Ready()
		if !raft.IsEmptySnap(rd.Snapshot) {
			var state registry
			if json.Unmarshal(rd.Snapshot.Data, &state) != nil || state.Generation != c.state.Generation {
				return errors.New("snapshot generation mismatch")
			}
			if e := c.mem.ApplySnapshot(rd.Snapshot); e != nil {
				return e
			}
			c.state = state
			c.conf = rd.Snapshot.Metadata.ConfState
			c.applied = rd.Snapshot.Metadata.Index
			if !known(c.conf, c.self.Node) {
				c.retired = true
			}
		}
		if e := c.mem.Append(rd.Entries); e != nil {
			return e
		}
		if !raft.IsEmptyHardState(rd.HardState) {
			if e := c.mem.SetHardState(rd.HardState); e != nil {
				return e
			}
		}
		// Volatile writes precede replies. A crash destroys this identity forever;
		// its next process cannot reuse these votes or bootstrap itself.
		for _, m := range rd.Messages {
			c.send(m)
		}
		for _, entry := range rd.CommittedEntries {
			if e := c.apply(entry); e != nil {
				return e
			}
		}
		if len(rd.CommittedEntries) > 0 {
			data, _ := json.Marshal(c.state)
			if _, e := c.mem.CreateSnapshot(c.applied, &c.conf, data); e != nil {
				return e
			}
			if e := c.mem.Compact(c.applied); e != nil {
				return e
			}
		}
		c.publish()
		for _, state := range rd.ReadStates {
			id := string(state.RequestCtx)
			p, ok := c.pending[id]
			if !ok {
				continue
			}
			delete(c.pending, id)
			v := c.snapshot()
			if v.IdentityValid && v.LeaderNode == c.self.Node && c.applied >= state.Index && p.ctx.Err() == nil {
				p.reply <- v
			} else {
				close(p.reply)
			}
		}
		c.rn.Advance(rd)
	}
	return nil
}
func (c *core) send(m pb.Message) {
	record, ok := c.state.Nodes[m.To]
	if !ok {
		return
	}
	address := c.peers[record.ID]
	if address == "" {
		return
	}
	b, e := m.Marshal()
	if e != nil {
		return
	}
	c.mailMu.Lock()
	c.sequence++
	sequence := c.sequence
	queue := append(c.mail[m.To], envelope{sequence, b})
	if len(queue) > 256 {
		queue = queue[len(queue)-256:]
	}
	c.mail[m.To] = queue
	c.mailMu.Unlock()
	generation := c.state.Generation
	go func() {
		req, e := http.NewRequest("POST", strings.TrimRight(address, "/")+"/_cp/raft", bytes.NewReader(b))
		if e != nil {
			return
		}
		c.headers(req, generation)
		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set("X-CP-From", strconv.FormatUint(c.self.ID, 10))
		req.Header.Set("X-CP-Boot", c.self.Boot)
		res, e := c.http.Do(req)
		ok := e == nil && res.StatusCode >= 200 && res.StatusCode < 300
		if e == nil {
			io.Copy(io.Discard, io.LimitReader(res.Body, 4096))
			res.Body.Close()
		}
		select {
		case c.delivered <- delivery{m.To, sequence, m.Type == pb.MsgSnap, ok}:
		default:
		}
	}()
}
func (c *core) headers(req *http.Request, generation string) {
	req.Header.Set("X-CP-Token", c.secret)
	req.Header.Set("X-CP-From", strconv.FormatUint(c.self.ID, 10))
	req.Header.Set("X-CP-Cluster", c.cluster)
	req.Header.Set("X-CP-Generation", generation)
	req.Header.Set("Content-Type", "application/json")
}
func (c *core) request(ctx context.Context, address, path string, body any, generation string, result any) error {
	method := "GET"
	var data []byte
	if body != nil {
		method = "POST"
		data, _ = json.Marshal(body)
	}
	req, e := http.NewRequestWithContext(ctx, method, strings.TrimRight(address, "/")+path, bytes.NewReader(data))
	if e != nil {
		return e
	}
	c.headers(req, generation)
	res, e := c.http.Do(req)
	if e != nil {
		return e
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return fmt.Errorf("peer HTTP %d", res.StatusCode)
	}
	return json.NewDecoder(io.LimitReader(res.Body, 1<<20)).Decode(result)
}

// A joining process can reach its peers before its stable app hostname moves.
// Outbound pulls transport normal Raft messages during that ordinary app move.
// They confer neither a vote nor leadership.
func (c *core) network(ctx context.Context) {
	timer := time.NewTicker(500 * time.Millisecond)
	defer timer.Stop()
	lastJoin := time.Time{}
	canonical := false
	lastCanonical := time.Time{}
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		own := c.snapshot()
		if own.Phase == "retired" {
			continue
		}
		if own.Generation == "" || own.Phase == "joining" {
			if time.Since(lastJoin) > 2*time.Second {
				lastJoin = time.Now()
				for _, peer := range c.members {
					call, cancel := context.WithTimeout(ctx, 1200*time.Millisecond)
					var remote view
					e := c.request(call, c.peers[peer.ID], "/_cp/raft-status", nil, "", &remote)
					cancel()
					if e != nil || !remote.IdentityValid || remote.LeaderNode != remote.Node || remote.Generation == "" {
						continue
					}
					call, cancel = context.WithTimeout(ctx, 2*time.Second)
					var state registry
					e = c.request(call, c.peers[peer.ID], "/_cp/join", c.self, remote.Generation, &state)
					cancel()
					if e == nil && state.Generation != "" {
						select {
						case c.adopt <- adopted{state}:
						case <-ctx.Done():
							return
						}
						break
					}
				}
			}
		}
		own = c.snapshot()
		if own.Generation == "" {
			continue
		}
		if time.Since(lastCanonical) > 3*time.Second {
			lastCanonical = time.Now()
			call, cancel := context.WithTimeout(ctx, time.Second)
			var visible view
			e := c.request(call, c.peers[c.self.ID], "/_cp/raft-status", nil, "", &visible)
			cancel()
			canonical = e == nil && visible.Node == c.self.Node
		}
		if canonical && own.IdentityValid {
			continue
		}
		var wait sync.WaitGroup
		for _, peer := range c.members {
			wait.Add(1)
			go func(peer member) {
				defer wait.Done()
				call, cancel := context.WithTimeout(ctx, 1200*time.Millisecond)
				defer cancel()
				var messages []envelope
				path := "/_cp/pull?node=" + strconv.FormatUint(c.self.Node, 10) + "&boot=" + c.self.Boot
				if c.request(call, c.peers[peer.ID], path, nil, own.Generation, &messages) != nil {
					return
				}
				for _, item := range messages {
					var m pb.Message
					if m.Unmarshal(item.Data) != nil || m.To != c.self.Node {
						continue
					}
					select {
					case c.incoming <- m:
					case <-ctx.Done():
						return
					}
				}
			}(peer)
		}
		wait.Wait()
	}
}
func (c *core) run(ctx context.Context) error {
	tick := time.NewTicker(time.Duration(number("CP_RAFT_TICK_MS", 500)) * time.Millisecond)
	defer tick.Stop()
	c.publish()
	go c.network(ctx)
	for {
		select {
		case <-ctx.Done():
			return nil
		case request := <-c.commands:
			var value any
			var err error
			switch request.kind {
			case "bootstrap":
				err = c.bootstrap(request.data)
				value = map[string]bool{"ok": err == nil}
			case "join":
				value, err = c.join(request.data)
			}
			request.reply <- controlReply{value, err}
		case adopted := <-c.adopt:
			if c.rn == nil {
				if e := c.initialize(adopted.state, nil); e != nil {
					return e
				}
			}
		case <-tick.C:
			if c.rn != nil && !c.retired {
				c.rn.Tick()
				c.promote()
			}
			for id, p := range c.pending {
				if p.ctx.Err() != nil {
					delete(c.pending, id)
				}
			}
		case m := <-c.incoming:
			if c.rn != nil && !c.retired {
				if e := c.rn.Step(m); e != nil && !errors.Is(e, raft.ErrStepPeerNotFound) {
					return e
				}
			}
		case sent := <-c.delivered:
			if sent.ok {
				c.mailMu.Lock()
				queue := c.mail[sent.to]
				for i, item := range queue {
					if item.Sequence == sent.sequence {
						queue = append(queue[:i], queue[i+1:]...)
						break
					}
				}
				if len(queue) == 0 {
					delete(c.mail, sent.to)
				} else {
					c.mail[sent.to] = queue
				}
				c.mailMu.Unlock()
			}
			if c.rn != nil {
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
			}
		case target := <-c.transfer:
			if c.rn != nil && c.rn.BasicStatus().Lead == c.self.Node {
				if target > 0 {
					target = c.state.Slots[target]
				} else {
					for id, p := range c.rn.Status().Progress {
						if id != c.self.Node && !p.IsLearner && p.RecentActive && p.Match >= c.applied {
							target = id
							break
						}
					}
				}
				if target > 0 && target != c.self.Node {
					c.rn.TransferLeader(target)
				}
			}
		case p := <-c.verify:
			v := c.snapshot()
			if c.rn == nil || !v.IdentityValid || v.LeaderNode != c.self.Node || v.Role != "StateLeader" {
				close(p.reply)
			} else {
				c.pending[p.id] = p
				c.rn.ReadIndex([]byte(p.id))
			}
		}
		if e := c.ready(); e != nil {
			return e
		}
		if c.rn != nil && c.rn.BasicStatus().RaftState != raft.StateLeader {
			// A proposal may be overwritten after losing leadership. The new
			// leader's Raft log, rather than this local hint, serializes changes.
			c.confPending = false
		}
		c.publish()
	}
}
func jsonReply(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(value)
}
func (c *core) handler(w http.ResponseWriter, r *http.Request) {
	if subtle.ConstantTimeCompare([]byte(r.Header.Get("X-CP-Token")), []byte(c.secret)) != 1 {
		http.Error(w, "unauthorized", 401)
		return
	}
	if cluster := r.Header.Get("X-CP-Cluster"); cluster != "" && cluster != c.cluster {
		http.Error(w, "wrong cluster", 409)
		return
	}
	current := c.snapshot()
	switch r.URL.Path {
	case "/status":
		jsonReply(w, current)
	case "/bootstrap", "/join":
		if r.Method != "POST" {
			http.Error(w, "POST required", 405)
			return
		}
		raw, e := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if e != nil {
			http.Error(w, "invalid request", 400)
			return
		}
		request := controlRequest{strings.TrimPrefix(r.URL.Path, "/"), raw, make(chan controlReply, 1)}
		select {
		case c.commands <- request:
		case <-r.Context().Done():
			return
		}
		select {
		case result := <-request.reply:
			if result.err != nil {
				http.Error(w, result.err.Error(), 409)
			} else {
				jsonReply(w, result.value)
			}
		case <-r.Context().Done():
		}
	case "/pull":
		node, _ := strconv.ParseUint(r.URL.Query().Get("node"), 10, 64)
		record, ok := current.Registry.Nodes[node]
		if !ok || record.Boot != r.URL.Query().Get("boot") || r.Header.Get("X-CP-Generation") != current.Generation {
			http.Error(w, "unknown incarnation", 409)
			return
		}
		c.mailMu.Lock()
		queue := c.mail[node]
		n := len(queue)
		if n > 64 {
			n = 64
		}
		out := append([]envelope{}, queue[:n]...)
		c.mail[node] = queue[n:]
		c.mailMu.Unlock()
		jsonReply(w, out)
	case "/transfer":
		if r.Method != "POST" {
			http.Error(w, "POST required", 405)
			return
		}
		target, _ := strconv.ParseUint(r.URL.Query().Get("to"), 10, 64)
		select {
		case c.transfer <- target:
			jsonReply(w, map[string]bool{"ok": true})
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
			} else {
				jsonReply(w, v)
			}
		case <-ctx.Done():
			http.Error(w, "quorum unavailable", 503)
		}
	case "/message", "/_cp/raft":
		raw, e := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		var m pb.Message
		if e != nil || m.Unmarshal(raw) != nil || m.To != c.self.Node || current.Generation == "" || r.Header.Get("X-CP-Generation") != current.Generation {
			http.Error(w, "wrong incarnation or generation", 409)
			return
		}
		record, ok := current.Registry.Nodes[m.From]
		if !ok || record.Boot != r.Header.Get("X-CP-Boot") {
			http.Error(w, "unknown sender incarnation", 409)
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
func start() error {
	var members []member
	if json.Unmarshal([]byte(os.Getenv("CP_MEMBERS")), &members) != nil || len(members) < 3 || len(members)%2 == 0 {
		return errors.New("an odd CP_MEMBERS set of at least three is required")
	}
	id := uint64(number("CP_NODE_ID", 0))
	peers := map[uint64]string{}
	for _, m := range members {
		if m.ID == 0 || m.URL == "" || peers[m.ID] != "" {
			return errors.New("invalid members")
		}
		peers[m.ID] = m.URL
	}
	if peers[id] == "" {
		return errors.New("CP_NODE_ID is not configured")
	}
	if raw := os.Getenv("CP_PEER_OVERRIDES"); raw != "" {
		var overrides map[string]string
		if json.Unmarshal([]byte(raw), &overrides) != nil {
			return errors.New("invalid peer overrides")
		}
		for k, v := range overrides {
			n, e := strconv.ParseUint(k, 10, 64)
			if e != nil || peers[n] == "" {
				return errors.New("unknown peer override")
			}
			peers[n] = v
		}
	}
	secret, cluster := os.Getenv("CP_CLUSTER_TOKEN"), os.Getenv("CP_CLUSTER_ID")
	if len(secret) < 32 || cluster == "" {
		return errors.New("cluster identity and secret are required")
	}
	c := &core{self: identity{randomNode(), id, randomID()}, members: members, peers: peers, secret: secret, cluster: cluster, state: emptyRegistry(),
		incoming: make(chan pb.Message, 512), verify: make(chan verification, 128), commands: make(chan controlRequest, 16), adopt: make(chan adopted, 8), transfer: make(chan uint64, 4),
		delivered: make(chan delivery, 1024), pending: map[string]verification{}, learnerSeen: map[uint64]time.Time{}, mail: map[uint64][]envelope{}, http: &http.Client{Timeout: 4 * time.Second}}
	c.publish()
	server := &http.Server{Addr: "127.0.0.1:" + env("CP_RAFT_PORT", "8931"), Handler: http.HandlerFunc(c.handler), ReadHeaderTimeout: 5 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		if e := server.ListenAndServe(); e != nil && e != http.ErrServerClosed {
			log.Fatal("Raft endpoint unavailable")
		}
	}()
	defer server.Close()
	return c.run(ctx)
}
func main() {
	if e := start(); e != nil {
		log.Fatal(e)
	}
}
