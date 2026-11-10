import { reconcileCapacity, targetCount } from '../../.github/scripts/capacity.mjs';

export const INTERVAL_MS = 10 * 60_000;
export function refillSlot(now, phase) {
  if (!/^[0-9]$/.test(String(phase))) throw new Error('CAPACITY_PHASE_MIN must be from 0 to 9');
  const offset = Number(phase) * 60_000;
  return Math.floor((now - offset) / INTERVAL_MS) * INTERVAL_MS + offset;
}

// Only a verified primary calls this. The committed claim covers the API
// attempt, including a lost response; no failover retries that same slot.
export async function replenish(provider, { api, authority, claim, now = Date.now, log = () => {} }) {
  const base = '/repos/' + provider.repo;
  const listing = await api(base + '/actions/variables?per_page=100');
  if (!Array.isArray(listing.variables)) throw new Error('invalid provider settings');
  const vars = Object.fromEntries(listing.variables.map(v => [v.name, v.value]));
  const target = targetCount(vars);
  const result = { repo: provider.repo, target, at: now() };
  if (!target) return { ...result, action: 'disabled' };
  if (vars.CAPACITY_TRIGGER !== 'provider') return { ...result, action: 'provider_not_enabled' };
  const slot = refillSlot(now(), vars.CAPACITY_PHASE_MIN || '2');
  if (now() - slot >= 120_000) return { ...result, action: 'waiting_for_phase', next: slot + INTERVAL_MS };
  const repository = await api(base);
  if (!repository.default_branch || repository.archived || repository.disabled) throw new Error('provider repository unavailable');
  const ref = repository.default_branch;
  const plan = await reconcileCapacity({ ...vars, GITHUB_REPOSITORY: provider.repo, MACHINE_REF: ref,
    GITHUB_EVENT_NAME: 'provider_preview', CAPACITY_DRY_RUN: '1' }, { api, now, log });
  Object.assign(result, { reserved: plan.active, missing: plan.missing, aging: plan.aging, slot });
  if (!plan.launch) return { ...result, action: plan.retryAfterMs ? 'launch_spacing' : 'satisfied' };
  // This also avoids replay after an explicitly bootstrapped empty cluster.
  const runs = await api(base + '/actions/workflows/watchdog.yml/runs?event=workflow_dispatch&per_page=100');
  if (!Array.isArray(runs.workflow_runs)) throw new Error('invalid capacity request listing');
  const title = `Machine schedule (refill ${slot})`;
  if (runs.workflow_runs.some(run => run.display_title === title && run.head_branch === ref))
    return { ...result, action: 'already_requested' };
  const fence = await authority.check();
  const committed = await claim(provider.repo.toLowerCase(), slot, fence);
  if (!committed.accepted) return { ...result, action: 'already_claimed' };
  await authority.check(fence);
  await api(base + '/actions/workflows/watchdog.yml/dispatches', {
    method: 'POST', body: { ref, inputs: { refill_slot: String(slot) } },
  });
  return { ...result, action: 'requested' };
}
