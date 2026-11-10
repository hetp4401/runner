import { clusterConfig } from './src/ha/config.mjs';
import { ClusterDatabase, CLUSTER_SCHEMA } from './src/ha/database.mjs';
const c=clusterConfig(process.env);
if(!c.enabled)throw new Error('single-controller mode does not initialize cluster coordination');
const db=new ClusterDatabase(process.env);
await db.query(CLUSTER_SCHEMA.map(sql=>({sql,params:[]})));
const members=JSON.stringify(c.members);
let row=(await db.query('SELECT * FROM cp_cluster WHERE id=1'))[0];
if(!row){await db.query([
 {sql:'INSERT INTO cp_cluster VALUES(1,?,?,?,0)',params:[c.cluster,c.count,members]},
 {sql:"INSERT INTO cp_authority VALUES(1,0,'',0,0,0,0)",params:[]},
]);row={cluster_id:c.cluster,replicas:c.count,members};}
if(row.cluster_id!==c.cluster||row.replicas!==c.count||row.members!==members)throw new Error('cluster identity/membership differs; refusing automatic reconfiguration');
const action=process.argv[2]||'prepare';
if(!['prepare','activate','deactivate'].includes(action))throw new Error('command must be prepare, activate or deactivate');
if(action!=='prepare')await db.query('UPDATE cp_cluster SET active=? WHERE id=1 AND cluster_id=?',[action==='activate'?1:0,c.cluster]);
console.log(JSON.stringify({cluster:c.cluster,replicas:c.count,quorum:c.quorum,action}));
