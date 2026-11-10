// Every authority proof comes from a fresh Raft quorum read. All coordination
// state is volatile; the monotonically increasing term fences agent plans.
export class Authority {
  constructor(config, verify) {
    this.config = config;
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
    // A short local cache controls scheduler lifetime; check() still obtains
    // a fresh quorum confirmation for every guarded operation.
    this.deadline = start + 14_000;
    this.wallDeadline = wall + 14_000;
    this.current = { cluster: this.config.cluster, node: raft.id, boot: raft.boot, term: raft.term, epoch: raft.term };
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
