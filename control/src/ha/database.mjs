export const CLUSTER_SCHEMA = [
  'CREATE TABLE IF NOT EXISTS cp_cluster(id INTEGER PRIMARY KEY CHECK(id=1),cluster_id TEXT NOT NULL,replicas INTEGER NOT NULL,members TEXT NOT NULL,active INTEGER NOT NULL DEFAULT 0)',
  'CREATE TABLE IF NOT EXISTS cp_raft_state(node_id INTEGER PRIMARY KEY,cluster_id TEXT NOT NULL,owner TEXT NOT NULL,revision INTEGER NOT NULL,lease_until INTEGER NOT NULL,state TEXT NOT NULL)',
  'CREATE TABLE IF NOT EXISTS cp_authority(id INTEGER PRIMARY KEY CHECK(id=1),node_id INTEGER NOT NULL,boot TEXT NOT NULL,term INTEGER NOT NULL,epoch INTEGER NOT NULL,expires INTEGER NOT NULL,changed_at INTEGER NOT NULL)',
];
export class ClusterDatabase {
  constructor(env) {
    const account=env.D1_ACCOUNT_ID||env.ACCOUNT_ID, database=env.D1_DATABASE_ID;
    this.key=env.D1_API_KEY||env.CF_API_KEY; this.email=env.D1_API_EMAIL||env.CF_API_EMAIL;
    if (!account||!database||!this.key||!this.email) throw new Error('D1 credentials are required for cluster metadata');
    this.url=`${env.D1_API_BASE||'https://api.cloudflare.com/client/v4'}/accounts/${account}/d1/database/${database}/query`;
    this.stats={requests:0,rowsRead:0,rowsWritten:0};
  }
  async query(sql,params=[]) {
    const batch=typeof sql==='string'?[{sql,params}]:sql;
    this.stats.requests++;
    let response, body;
    try {
      response=await fetch(this.url,{method:'POST',signal:AbortSignal.timeout(12_000),headers:{'content-type':'application/json','x-auth-key':this.key,'x-auth-email':this.email},body:JSON.stringify({batch})});
      body=await response.json();
    } catch {throw new Error('D1 cluster metadata request unavailable');}
    if(!response.ok||!body.success||!Array.isArray(body.result)||body.result.some(r=>!r.success))throw new Error(`D1 cluster metadata request failed (HTTP ${response.status})`);
    for(const r of body.result){this.stats.rowsRead+=r.meta?.rows_read||0;this.stats.rowsWritten+=r.meta?.rows_written||0;}
    return body.result.flatMap(r=>r.results||[]);
  }
}
