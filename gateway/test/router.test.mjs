import assert from 'node:assert/strict';
import {test} from 'node:test';
import http from 'node:http';
import {once} from 'node:events';
import {createRouter,routesFromStatus} from '../router.mjs';
import {createHash} from 'node:crypto';

async function listen(server) {server.testSockets=new Set();server.on('connection',socket=>{server.testSockets.add(socket);socket.once('close',()=>server.testSockets.delete(socket));});server.listen(0,'127.0.0.1');await once(server,'listening');return `http://127.0.0.1:${server.address().port}`;}
async function close(server) {for(const socket of server.testSockets)socket.destroy();server.closeAllConnections();await new Promise(r=>server.close(r));}
function rawRequest(url,options={}) {return new Promise((resolve,reject)=>{
 const req=http.request(url,{method:options.method??'GET',headers:options.headers},async response=>{
  const chunks=[];for await(const c of response)chunks.push(c);
  resolve(new Response(Buffer.concat(chunks),{status:response.statusCode,headers:response.headers}));
 });req.on('error',reject);req.end(options.body);
});}
async function setup(t,handler1,handler2,opts={}) {
 const first=http.createServer(handler1),second=http.createServer(handler2);
 const peers=[await listen(first),await listen(second)];
 const router=createRouter({domain:'example',routes:new Map([['demo.example',{name:'demo',enabled:true,peers}]]),probeTimeout:100,connectTimeout:100,headerTimeout:1000,...opts});
 const base=await listen(router);
 t.after(async()=>{await close(router);await close(first);await close(second);});
 const request=(path='/',options={})=>rawRequest(base+path,{...options,headers:{host:'demo.example',...options.headers}});
 return {first,second,router,peers,request,base};
}

test('uses reported healthy replicas first; excludes control domains and unknown destinations',()=>{
 const status={projects:[{name:'demo',port:80,replicas:2,placed:[{machine:2,key:'demo',replica:2}]},{name:'control',port:80,replicas:1}],runs:[{machine:2,live:true,ready:true,projects:{demo:{s:'healthy'}}}]};
 const routes=routesFromStatus(status,'example');assert.deepEqual(routes.get('demo.example').peers,['https://demo-2.example','https://demo-1.example']);assert(!routes.has('control.example'));
});
test('global URL serves another replica when preferred returns reconnecting',async t=>{
 const {request}=await setup(t,(req,res)=>{res.writeHead(503);res.end('agent reconnecting');},(req,res)=>res.end('healthy replica'));
 const response=await request('/path?a=1');assert.equal(response.status,200);assert.equal(await response.text(),'healthy replica');assert.equal(response.headers.get('x-runner-global-router'),'1');
});
test('a write is sent once and its response is preserved without interpreting app-specific error messages',async t=>{
 let writes1=0,writes2=0,observed;
 const {request}=await setup(t,(req,res)=>{
  if(req.method==='HEAD')return res.end();
  const text=JSON.stringify({error:'agent is reconnecting to this replica'});res.writeHead(503,{'content-type':'application/json','content-length':Buffer.byteLength(text)});res.end(text);
 },async(req,res)=>{
  if(req.method==='HEAD')return res.end();writes2++;observed=[];for await(const c of req)observed.push(c);res.end('accepted');
 });
 const body='{"requestId":"unchanged","message":"hi"}';const response=await request('/api/write',{method:'POST',headers:{'content-length':String(Buffer.byteLength(body))},body});
 assert.equal(response.status,503);assert.equal(writes2,0);assert.equal(observed,undefined);
});
test('ambiguous application failure never replays a write',async t=>{
 let firstWrites=0,secondWrites=0;
 const {request}=await setup(t,(req,res)=>{if(req.method==='HEAD')return res.end();firstWrites++;res.writeHead(502);res.end('application failed after accepting request');},(req,res)=>{if(req.method!=='HEAD')secondWrites++;res.end('second');});
 const response=await request('/api/write',{method:'POST',body:'payload'});assert.equal(response.status,502);assert.equal(firstWrites,1);assert.equal(secondWrites,0);
});
test('preserves query, method, authorization, cookies, browser origin and forwarding host',async t=>{
 const handler=(req,res)=>{if(req.method==='HEAD')return res.end();res.setHeader('content-type','application/json');res.end(JSON.stringify({url:req.url,method:req.method,headers:req.headers}));};
 const {request}=await setup(t,handler,handler);
 const response=await request('/a?x=%26&two=2',{headers:{authorization:'Bearer test-only',cookie:'session=fixture',origin:'https://demo.example','x-forwarded-host':'attacker.invalid'}});
 const data=await response.json();assert.equal(data.url,'/a?x=%26&two=2');assert.equal(data.headers.authorization,'Bearer test-only');assert.equal(data.headers.cookie,'session=fixture');assert.equal(data.headers.origin,'https://demo.example');assert.equal(data.headers['x-forwarded-host'],'demo.example');
});
test('rewrites replica redirects and cookie domain while retaining separate cookies',async t=>{
 let peer;
 const handler=(req,res)=>{if(req.method==='HEAD')return res.end();res.writeHead(302,{location:peer+'/next?q=1','set-cookie':['one=1; Domain=127.0.0.1; Path=/; HttpOnly','two=2; Path=/; Expires=Wed, 01 Jan 2031 00:00:00 GMT']});res.end();};
 const s=await setup(t,handler,handler);peer=s.peers[0];const response=await s.request();assert.equal(response.headers.get('location'),'https://demo.example/next?q=1');assert(!response.headers.get('set-cookie').includes('Domain=127.0.0.1'));assert(response.headers.get('set-cookie').includes('two=2'));
});
test('streams responses and large request bodies without replay',async t=>{
 let bytes=0;
 const handler=async(req,res)=>{if(req.method==='HEAD')return res.end();for await(const c of req)bytes+=c.length;res.write('first\n');setTimeout(()=>res.end('last\n'),15);};
 const {request}=await setup(t,handler,handler,{bodyBufferLimit:10});
 const body='a'.repeat(200000);const response=await request('/stream',{method:'POST',body});assert.equal(await response.text(),'first\nlast\n');assert.equal(bytes,body.length);
});
test('does not treat authentication errors as outages',async t=>{
 let other=0;
 const {request}=await setup(t,(req,res)=>{res.writeHead(403);res.end('denied');},(req,res)=>{other++;res.end('should not run');});
 const response=await request();assert.equal(response.status,403);assert.equal(other,0);
});
test('returns bounded 503 when all replicas are unavailable and 404 for unregistered hosts',async t=>{
 const handler=(req,res)=>{res.writeHead(503);res.end();};const {request,base}=await setup(t,handler,handler);
 assert.equal((await request()).status,503);assert.equal((await rawRequest(base,{headers:{host:'attacker.invalid'}})).status,404);
});
test('WebSocket upgrade fails over before acceptance and transports bytes',async t=>{
 const {first,second,base}=await setup(t,(req,res)=>res.end(),(req,res)=>res.end());
 first.on('upgrade',(req,socket)=>socket.end('HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n'));
 second.on('upgrade',(req,socket)=>{
  const key=createHash('sha1').update(req.headers['sec-websocket-key']+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: '+key+'\r\n\r\n');socket.on('data',data=>socket.write(data));
 });
 const socket=await new Promise((resolve,reject)=>{
  const req=http.request(base+'/socket',{headers:{host:'demo.example',connection:'Upgrade',upgrade:'websocket','sec-websocket-version':'13','sec-websocket-key':Buffer.alloc(16).toString('base64')}});
  req.on('upgrade',(response,socket)=>{assert.equal(response.statusCode,101);resolve(socket);});req.on('error',reject);req.end();
 });
 const data=once(socket,'data');socket.write('opaque-protocol-bytes');assert.equal((await data)[0].toString(),'opaque-protocol-bytes');socket.destroy();
});
test('a double-slash path cannot change the upstream authority',async t=>{
 const handler=(req,res)=>res.end(req.url);
 const {request}=await setup(t,handler,handler);
 const response=await request('//external.invalid/private?secret=fixture');
 assert.equal(response.status,200);assert.equal(await response.text(),'//external.invalid/private?secret=fixture');
});
test('servers without HEAD support remain reachable',async t=>{
 const handler=(req,res)=>{if(req.method==='HEAD'){res.writeHead(501);res.end();}else res.end('legacy server');};
 const {request}=await setup(t,handler,handler);const response=await request();
 assert.equal(response.status,200);assert.equal(await response.text(),'legacy server');
});
test('system metrics are separate from user paths and count traffic without request data',async t=>{
 const {request,base}=await setup(t,(req,res)=>res.end('hello'),(req,res)=>res.end('hello'));
 await request('/private-path?token=test-only');
 const response=await rawRequest(base+'/metrics',{headers:{host:'global-router-1.example'}});
 const stats=await response.json();
 assert.equal(stats.requests,1);assert.equal(stats.bytesToClients,5);assert.equal(stats.active,0);
 assert.equal(stats.streamResumes,0);assert.ok(stats.instance);assert(!JSON.stringify(stats).includes('test-only'));
 assert.equal(await (await request('/metrics')).text(),'hello');
});
test('an app whose name starts with global-router retains its own health and metrics paths',async t=>{
 const routes=new Map();
 const s=await setup(t,(req,res)=>res.end('application path'),(req,res)=>res.end('application path'),{routes});
 routes.set('global-router-tools.example',{name:'global-router-tools',enabled:true,peers:s.peers});
 const response=await s.request('/metrics',{headers:{host:'global-router-tools.example'}});
 assert.equal(await response.text(),'application path');
});
