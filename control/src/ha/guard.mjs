// Every incoming operation and outgoing side effect asks the independent
// consensus process again. Single-controller mode has no coordination.
export function authorityGuard(env, unavailable = () => {}) {
  if (env.CP_HA_CHILD !== '1') return { enabled: false, check: async () => null, report() {}, recover: async () => {} };
  const expected = JSON.parse(env.CP_HA_AUTHORITY);
  const headers = { 'x-cp-token': env.CP_CLUSTER_TOKEN, 'content-type': 'application/json' };
  async function call(path, init = {}) {
    const response = await fetch(env.CP_HA_MANAGER + path, { signal: AbortSignal.timeout(6000), ...init, headers });
    if (!response.ok) throw new Error('control-plane leadership is no longer valid');
    return response.json();
  }
  return {
    enabled: true,
    async check() {
      try { return await call('/_cp/verify', { method: 'POST', body: JSON.stringify(expected) }); }
      catch { unavailable(); throw new Error('control-plane leadership is no longer valid'); }
    },
    report(kind, body) {
      call('/_cp/report', { method: 'POST', body: JSON.stringify({ authority: expected, kind, body }) }).catch(() => {});
    },
    async recover(control) {
      const saved = await call('/_cp/recovery');
      for (const report of (saved.reports || []).sort((a, b) => a.at - b.at)) {
        if (report.at < Date.now() - 15 * 60_000) continue;
        try { control.recoverAgent(report.body, Date.now()); } catch { /* Ignore unverifiable inventory. */ }
      }
    },
  };
}
