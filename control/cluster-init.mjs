import { clusterConfig } from './src/ha/config.mjs';
import { ClusterDatabase, CLUSTER_SCHEMA } from './src/ha/database.mjs';
const config = clusterConfig(process.env);
if (!config.enabled) throw new Error('single-controller mode does not initialize cluster coordination');
const action = process.argv[2] || 'prepare';
if (!['prepare', 'activate', 'deactivate', 'update-addresses'].includes(action)) throw new Error('command must be prepare, activate, deactivate or update-addresses');
const database = new ClusterDatabase(process.env);
await database.query(CLUSTER_SCHEMA.map(sql => ({ sql, params: [] })));
const members = JSON.stringify(config.members);
let row = (await database.query('SELECT * FROM cp_cluster WHERE id=1'))[0];
if (!row) {
  if (action !== 'prepare') throw new Error('prepare the cluster before changing its state');
  await database.query([
    { sql: 'INSERT INTO cp_cluster VALUES(1,?,?,?,0)', params: [config.cluster, config.count, members] },
    { sql: "INSERT INTO cp_authority VALUES(1,0,'',0,0,0,0)", params: [] },
  ]);
  row = { cluster_id: config.cluster, replicas: config.count, members, active: 0 };
}
if (row.cluster_id !== config.cluster || row.replicas !== config.count) throw new Error('cluster identity/size differs; refusing automatic reconfiguration');
if (action === 'update-addresses') {
  const before = JSON.parse(row.members).map(member => member.id).sort((a, b) => a - b);
  if (row.active !== 0 || JSON.stringify(before) !== JSON.stringify(config.members.map(member => member.id))) throw new Error('address updates require an inactive cluster and exactly the same voter IDs');
  const changed = await database.query('UPDATE cp_cluster SET members=? WHERE id=1 AND cluster_id=? AND active=0 RETURNING id', [members, config.cluster]);
  if (changed.length !== 1) throw new Error('cluster changed during address update');
} else {
  if (row.members !== members) throw new Error('member configuration differs; use update-addresses while inactive for address-only changes');
  if (action === 'activate') {
    const initialized = await database.query('SELECT node_id FROM cp_raft_state WHERE cluster_id=?', [config.cluster]);
    if (initialized.length !== config.count || config.members.some(member => !initialized.some(row => row.node_id === member.id))) throw new Error('start every configured voter once before activation; missing election state cannot be recreated while active');
  }
  if (action !== 'prepare') await database.query('UPDATE cp_cluster SET active=? WHERE id=1 AND cluster_id=?', [action === 'activate' ? 1 : 0, config.cluster]);
}
console.log(JSON.stringify({ cluster: config.cluster, replicas: config.count, quorum: config.quorum, action }));
