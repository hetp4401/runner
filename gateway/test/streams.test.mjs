import assert from 'node:assert/strict';
import {test} from 'node:test';
import http from 'node:http';
import {once} from 'node:events';
import {createHash} from 'node:crypto';
import {createRouter} from '../router.mjs';

const data = Buffer.alloc(128*1024);
for (let i=0;i<data.length;i++) data[i]=i%251;
const etag = `"${createHash('sha256').update(data).digest('hex')}"`;
const wait = ms=>new Promise(resolve=>setTimeout(resolve,ms));
async function listen(server) {
  server.sockets = new Set();
  server.on('connection',s=>{server.sockets.add(s);s.once('close',()=>server.sockets.delete(s));});
  server.listen(0,'127.0.0.1'); await once(server,'listening');
  return `http://127.0.0.1:${server.address().port}`;
}
async function fixture(t, modes, options={}) {
  const seen = [], resumes = [], servers = [];
  const peers = [];
  for (const [index,mode] of modes.entries()) {
    const server = http.createServer((req,res)=>{
      if (req.method === 'HEAD') {res.end(); return;}
      seen.push({index,url:req.url,headers:req.headers});
      let start=0,end=data.length-1;
      const range=/^bytes=(\d+)-(\d*)$/.exec(req.headers.range??'');
      if (range) {start=Number(range[1]);end=range[2]?Number(range[2]):end;}
      const partial = Boolean(range);
      const headers = {'content-type':'application/octet-stream','accept-ranges':'bytes',
        'content-length':String(end-start+1),etag:mode==='changed'?'"changed-file"':etag};
      if (partial) headers['content-range']=`bytes ${mode==='bad-offset'?start+1:start}-${end}/${data.length}`;
      if (mode==='no-etag') delete headers.etag;
      if (mode==='weak') headers.etag='W/'+etag;
      if (mode==='compressed') headers['content-encoding']='gzip';
      const status=mode==='ignores-range'?200:partial?206:200;
      res.writeHead(status,headers);
      if (['drop','no-etag','weak','compressed','stall'].includes(mode)) {
        res.write(data.subarray(start,Math.min(end+1,start+4096)));
        if (mode!=='stall') setTimeout(()=>res.destroy(),15);
      } else res.end(data.subarray(start,end+1));
    });
    servers.push(server);peers.push(await listen(server));
  }
  const router=createRouter({domain:'example',routes:new Map([['file.example',{name:'file',enabled:true,peers}]]),
    probeTimeout:200,headerTimeout:1000,streamIdleMs:100,streamRecoveryMs:2000,onResume:info=>resumes.push(info),...options});
  const base=await listen(router);servers.push(router);
  t.after(async()=>{for(const server of servers){for(const socket of server.sockets)socket.destroy();server.closeAllConnections();await new Promise(r=>server.close(r));}});
  const request=(headers={})=>new Promise((resolve,reject)=>{
    const request=http.get(base+'/same/path?token=test-only',{headers:{host:'file.example',...headers}},async response=>{
      try {const chunks=[];for await(const chunk of response)chunks.push(chunk);resolve({status:response.statusCode,headers:response.headers,body:Buffer.concat(chunks)});}
      catch(error){reject(error);}
    });request.on('error',reject);
  });
  return {request,seen,resumes,base,router};
}

test('a mid-body disconnect continues the same client response at the exact next byte on another replica',async t=>{
  const s=await fixture(t,['drop','ok']);
  const result=await s.request();
  assert.equal(result.status,200);assert.deepEqual(result.body,data);
  assert.equal(s.resumes.length,1);assert.equal(s.resumes[0].offset,4096);
  assert.equal(s.router.stats.streamResumes,1);assert.equal(s.router.stats.bytesToClients,data.length);
  const next=s.seen.find(x=>x.index===1);
  assert.equal(next.url,'/same/path?token=test-only');
  assert.equal(next.headers.range,`bytes=4096-${data.length-1}`);
  assert.equal(next.headers['if-range'],etag);assert.equal(next.headers['if-match'],etag);
});
test('an existing partial-content request resumes within its original range',async t=>{
  const s=await fixture(t,['drop','ok']);
  const result=await s.request({range:'bytes=1024-20000'});
  assert.equal(result.status,206);assert.deepEqual(result.body,data.subarray(1024,20001));
  assert.equal(s.seen.find(x=>x.index===1).headers.range,'bytes=5120-20000');
});
test('a stream can recover from two different replicas failing in sequence',async t=>{
  const s=await fixture(t,['drop','drop','ok']);
  const result=await s.request();assert.deepEqual(result.body,data);
  assert.deepEqual(s.resumes.map(x=>x.offset),[4096,8192]);
});
test('a stalled replica is replaced without waiting indefinitely for its socket to close',async t=>{
  const s=await fixture(t,['stall','ok'],{streamIdleMs:30});
  const result=await s.request();assert.deepEqual(result.body,data);assert.equal(s.resumes.length,1);
});
for(const invalid of ['changed','bad-offset','ignores-range']) {
  test(`a ${invalid} continuation is rejected before any of its bytes reach the client`,async t=>{
    const s=await fixture(t,['drop',invalid,'ok']);
    const result=await s.request();assert.deepEqual(result.body,data);
    assert.equal(s.resumes.length,1);
    assert.equal(s.seen.filter(x=>x.index===2).length,1);
  });
}
for(const unsupported of ['no-etag','weak','compressed']) {
  test(`a ${unsupported} stream is not spliced unsafely`,async t=>{
    const s=await fixture(t,[unsupported,'ok']);
    await assert.rejects(s.request());
    assert.equal(s.seen.filter(x=>x.index===1).length,0);assert.equal(s.resumes.length,0);
  });
}
test('client disconnection stops the upstream without opening another replica',async t=>{
  const s=await fixture(t,['stall','ok']);
  await new Promise((resolve,reject)=>{
    const req=http.get(s.base+'/same/path',{headers:{host:'file.example'}},response=>{
      response.once('data',()=>{response.destroy();resolve();});response.on('error',()=>{});
    });req.on('error',reject);
  });
  await wait(150);assert.equal(s.seen.filter(x=>x.index===1).length,0);
});
test('retry attempts are bounded when every replacement has a different file',async t=>{
  const s=await fixture(t,['drop','changed']);await assert.rejects(s.request());
  assert.equal(s.seen.length,2);assert.equal(s.resumes.length,0);
});
