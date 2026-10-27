// Desired configuration survives a controller restart. Observations and scheduling
// history are reconstructed from agents and never belong in the durable store.
export const CONFIG_TABLES = ["projects", "versions", "settings", "app_passwords", "app_env", "slots"];
export const durableSetting = (key) => ["pools", "rebalance", "roll"].includes(key) || /^roll_\d+$/.test(key);
