// Broad placement groups for the current US fleet. Locations come from the
// control plane's cached IP lookup, never a city's name or a machine slot.
export const REGIONS = {
  "us-west": "US West",
  "us-central": "US Central",
  "us-east": "US East",
};

const states = {
  "us-west": "Alaska|Arizona|California|Colorado|Hawaii|Idaho|Montana|Nevada|New Mexico|Oregon|Utah|Washington|Wyoming",
  "us-central": "Alabama|Arkansas|Illinois|Indiana|Iowa|Kansas|Kentucky|Louisiana|Michigan|Minnesota|Mississippi|Missouri|Nebraska|North Dakota|Ohio|Oklahoma|South Dakota|Tennessee|Texas|Wisconsin",
  "us-east": "Connecticut|Delaware|District of Columbia|Florida|Georgia|Maine|Maryland|Massachusetts|New Hampshire|New Jersey|New York|North Carolina|Pennsylvania|Rhode Island|South Carolina|Vermont|Virginia|West Virginia",
};
const byState = new Map(Object.entries(states).flatMap(([region, names]) => names.split("|").map((name) => [name.toLowerCase(), region])));

export function regionOf(location) {
  const state = String(location ?? "").match(/(?:^|,\s*)([^,]+),\s*US(?:\s*·|$)/)?.[1].trim().toLowerCase();
  return byState.get(state) ?? null;
}

// Replica numbers keep the same regional assignment when machines are replaced
// or the count changes. A list repeats for additional replicas.
export function regionForReplica(policy, replica) {
  if (policy?.region) return policy.region;
  const regions = policy?.regions ?? [];
  return regions.length ? regions[(replica - 1) % regions.length] : null;
}
