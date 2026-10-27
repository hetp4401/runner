// The single persistence policy used by storage and agent-receipt recovery.
// Desired configuration survives a controller restart; observations and scheduling
// history are reconstructed from agents. Receipts must never overwrite durable settings.
export const CONFIG_TABLES = ["projects", "versions", "settings", "app_passwords", "app_env"];
export const durableSetting = (key) => ["pools", "rebalance", "roll"].includes(key) || /^roll_\d+$/.test(key);
