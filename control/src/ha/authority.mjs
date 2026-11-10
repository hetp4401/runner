// Raft chooses the leader. This row fences configuration writes and identifies
// plans across terms; possession of the row alone never grants leadership.
export class Authority {
  constructor(config, database, verify) {
    this.config = config;
    this.database = database;
    this.verify = verify;
    this.current = null;
    this.deadline = 0;
    this.wallDeadline = 0;
  }
  clear() { this.current = null; this.deadline = this.wallDeadline = 0; }
  live() { return this.current && performance.now() < this.deadline && Date.now() < this.wallDeadline; }
  async renew() {
    const start = performance.now(), wall = Date.now();
    const raft = await this.verify();
    if (raft.id !== this.config.id || raft.leader !== raft.id || !raft.identityValid) throw new Error('Raft leadership unavailable');
    const rows = await this.database.query(
      'UPDATE cp_authority SET node_id=?,boot=?,term=?,epoch=epoch+CASE WHEN node_id=? AND boot=? AND term=? THEN 0 ELSE 1 END,expires=unixepoch()+20,changed_at=CASE WHEN node_id=? AND boot=? AND term=? THEN changed_at ELSE unixepoch()*1000 END ' +
      'WHERE id=1 AND (term<? OR (term=? AND node_id=? AND boot=?)) ' +
      'AND EXISTS(SELECT 1 FROM cp_cluster WHERE id=1 AND cluster_id=? AND active=1) ' +
      'AND EXISTS(SELECT 1 FROM cp_raft_state WHERE node_id=? AND cluster_id=? AND owner=? AND lease_until>unixepoch()) RETURNING *',
      [raft.id, raft.boot, raft.term, raft.id, raft.boot, raft.term, raft.id, raft.boot, raft.term,
        raft.term, raft.term, raft.id, raft.boot, this.config.cluster, raft.id, this.config.cluster, raft.boot]);
    if (rows.length !== 1) { this.clear(); return null; }
    // Shorter than the database lease, measured from before the request.
    this.deadline = start + 14_000;
    this.wallDeadline = wall + 14_000;
    this.current = { cluster: this.config.cluster, node: raft.id, boot: raft.boot, term: raft.term, epoch: rows[0].epoch };
    if (!this.live()) { this.clear(); throw new Error('leadership confirmation expired in transit'); }
    return this.current;
  }
  async check(expected = null) {
    if (!this.live()) throw new Error('leadership lease unavailable');
    const before = this.current;
    const raft = await this.verify();
    if (!this.live() || ['node', 'boot', 'term', 'epoch'].some(key => this.current[key] !== before[key]) || raft.leader !== before.node || raft.id !== before.node ||
        raft.boot !== before.boot || raft.term !== before.term || !raft.identityValid) throw new Error('leadership changed');
    if (expected && ['cluster', 'node', 'boot', 'term', 'epoch'].some(key => expected[key] !== before[key])) throw new Error('obsolete control-plane authority');
    return { ...before };
  }
}
