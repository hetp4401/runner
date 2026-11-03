import http from 'node:http';
import https from 'node:https';
import {randomUUID} from 'node:crypto';
import {forwardResponse} from './streams.mjs';

const SAFE = new Set(['GET', 'HEAD', 'OPTIONS']);
const RETRY = new Set([502, 503, 504, 520, 521, 522, 523, 524, 525, 526, 527, 530]);
const COOKIE = '__Host-runner-replica';
const HOP = new Set(['connection','keep-alive','proxy-authenticate','proxy-authorization','te','trailer','transfer-encoding','upgrade']);

export function routesFromStatus(status, domain, excluded = 'global-router') {
  const routes = new Map();
  for (const p of status.projects ?? []) {
    if (!/^[a-z0-9](?:[a-z0-9-]{0,38}[a-z0-9])?$/.test(p.name) || [excluded,'runners','control'].includes(p.name) || !p.port) continue;
    const healthy = (p.placed ?? []).filter(place => (status.runs ?? []).some(run =>
      run.machine === place.machine && run.live && run.ready && !run.retiring && run.edges !== 0 &&
      run.projects?.[place.key]?.s === 'healthy' && run.projects[place.key].r !== false)).map(x => x.replica);
    const count = Math.min(100, Number(p.replicas) || 0);
    const order = [...new Set([...healthy, ...Array.from({length:count}, (_, i) => i + 1)])].filter(n => n >= 1 && n <= count);
    routes.set(`${p.name}.${domain}`, {name:p.name, enabled:p.enabled !== false && p.enabled !== 0,
      peers:order.map(n => `https://${p.name}-${n}.${domain}`)});
  }
  return routes;
}

export function createRouter(options = {}) {
  const domain = options.domain ?? process.env.DOMAIN;
  if (!domain || !/^[a-z0-9.-]+$/.test(domain)) throw new Error('DOMAIN is required');
  const statusUrl = options.statusUrl ?? process.env.STATUS_URL ?? `https://runners.${domain}/api/status`;
  const ownName = options.name ?? process.env.FLEET_APP ?? 'global-router';
  const bodyBufferLimit = options.bodyBufferLimit ?? 256 * 1024;
  const probeTimeout = options.probeTimeout ?? 1500;
  const connectTimeout = options.connectTimeout ?? 3000;
  const headerTimeout = options.headerTimeout ?? 120000;
  const stats = {instance:randomUUID(),startedAt:Date.now(),requests:0,active:0,bytesToClients:0,
    failedResponses:0,upstreamFailures:0,failovers:0,streamResumes:0,websockets:0};
  let routes = options.routes ?? routesFromStatus(JSON.parse(process.env.ROUTES_FALLBACK ?? '{}'), domain, ownName);
  let refreshed = 0, loading;
  const good = new Map();
  const agent = {http:new http.Agent({keepAlive:true}), https:new https.Agent({keepAlive:true})};

  async function refresh() {
    if (options.routes || Date.now() - refreshed < 10000) return;
    if (!loading) loading = (async () => {
      try {
        const response = await fetch(statusUrl, {signal:AbortSignal.timeout(4000), headers:{'cache-control':'no-cache'}});
        if (!response.ok) throw Error('status unavailable');
        const status = await response.json();
        if (!Array.isArray(status.projects) || !Array.isArray(status.runs)) throw Error('invalid status');
        routes = routesFromStatus(status, domain, ownName);
      } catch { /* Keep the last verified configuration while the controller recovers. */ }
      finally {refreshed = Date.now(); loading = null;}
    })();
    await loading;
  }

  function headersFor(req, peer, upgrade = false) {
    const headers = {...req.headers};
    for (const key of Object.keys(headers)) if (HOP.has(key.toLowerCase())) delete headers[key];
    for (const key of (req.headers.connection ?? '').split(',')) delete headers[key.trim().toLowerCase()];
    headers.host = new URL(peer).host;
    headers['x-forwarded-host'] = req.headers.host;
    headers['x-forwarded-proto'] = 'https';
    headers.forwarded = `proto=https;host="${req.headers.host}"`;
    headers['x-forwarded-for'] = req.headers['cf-connecting-ip'] ?? req.socket.remoteAddress;
    // The application's public origin remains the global domain. Preserve the
    // browser Origin; arbitrary external Origins must not gain same-origin access.
    if (upgrade) {headers.connection = 'Upgrade'; headers.upgrade = req.headers.upgrade;}
    return headers;
  }

  function open(req, peer, {method = req.method, upgrade = false, timeout = headerTimeout, headers:extraHeaders = {}} = {}) {
    return new Promise((resolve, reject) => {
      const target = new URL(peer);
      // Pin authority to a registered replica. A path beginning with // must
      // never turn this gateway into a proxy to an arbitrary external host.
      if (!req.url.startsWith('/') && req.url !== '*') return reject(Error('invalid request target'));
      const transport = target.protocol === 'https:' ? https : http;
      const headers = {...headersFor(req, peer, upgrade),...extraHeaders};
      if (method === 'HEAD') {delete headers['content-length']; delete headers['content-type'];}
      const upstream = transport.request({protocol:target.protocol,hostname:target.hostname,port:target.port||undefined,
        path:req.url,method,headers,agent:agent[target.protocol.slice(0,-1)],signal:req.routerSignal});
      const timer = setTimeout(() => upstream.destroy(Error('upstream response timed out')), timeout);
      const dial = setTimeout(() => upstream.destroy(Error('upstream connection timed out')), Math.min(timeout,connectTimeout));
      const clear = () => {clearTimeout(timer); clearTimeout(dial);};
      upstream.on('socket', socket => {
        if (!socket.connecting) clearTimeout(dial);
        else socket.once(target.protocol === 'https:'?'secureConnect':'connect', () => clearTimeout(dial));
      });
      upstream.once('response', response => {clear(); resolve({response,upstream,peer});});
      upstream.once('upgrade', (response,socket,head) => {clear(); resolve({response,socket,head,upstream,peer});});
      upstream.once('error', error => {clear(); reject(error);});
      if (method === 'HEAD' || upgrade || SAFE.has(method)) upstream.end();
      else if (Buffer.isBuffer(req.routerBody)) upstream.end(req.routerBody);
      else req.pipe(upstream);
    });
  }

  const scope = req => (req.headers.host ?? '') + '/' + req.url.split('?')[0].split('/').slice(1,3).join('/');
  function ordered(req, route) {
    const pin = req.headers.cookie?.match(/(?:^|;\s*)__Host-runner-replica=(\d+)(?:;|$)/)?.[1];
    const preferred = pin && route.peers.find(peer => new URL(peer).hostname === `${route.name}-${pin}.${domain}`);
    const cached = good.get(scope(req));
    return [...new Set([preferred, cached?.until > Date.now() && cached.peer, ...route.peers].filter(Boolean))];
  }
  function remember(req, peer) {
    if (good.size > 5000) good.delete(good.keys().next().value);
    good.set(scope(req), {peer,until:Date.now()+3000});
  }
  async function probe(req, peer) {
    const cached = good.get(scope(req));
    if (cached?.peer === peer && cached.until > Date.now()) return true;
    try {
      const {response} = await open(req,peer,{method:'HEAD',timeout:probeTimeout});
      response.resume();
      // Older HTTP servers answer 501 to HEAD while serving GET normally.
      if (response.statusCode < 500 || response.statusCode === 501) {remember(req,peer); return true;}
    } catch {}
    stats.upstreamFailures++;
    return false;
  }
  async function choose(req, peers) {
    if (!peers.length) return null;
    if (await probe(req,peers[0])) return peers[0];
    // A dead preferred replica must not make clients wait for every other replica serially.
    try {const peer = await Promise.any(peers.slice(1).map(async peer => {
      if (!await probe(req,peer)) throw Error('unavailable');
      return peer;
    }));stats.failovers++;return peer;} catch {return null;}
  }
  function responseHeaders(req, response, peer) {
    const headers = {...response.headers};
    for (const key of Object.keys(headers)) if (HOP.has(key.toLowerCase())) delete headers[key];
    for (const key of (response.headers.connection ?? '').split(',')) delete headers[key.trim().toLowerCase()];
    const original = `https://${req.headers.host}`;
    if (headers.location) {
      try {const location = new URL(headers.location,peer); if (location.origin === peer && (/^https?:/.test(headers.location)||headers.location.startsWith('//'))) headers.location = original + location.pathname + location.search + location.hash;} catch {}
    }
    const host = new URL(peer).hostname;
    const cookies = (headers['set-cookie'] ?? []).map(cookie => cookie.replace(/;\s*Domain=\.?([^;]+)/i, (whole,value) => value.toLowerCase() === host ? '' : whole));
    const replica = host.match(/-(\d+)\./)?.[1];
    if (replica && !req.headers.cookie?.split(';').some(x => x.trim() === `${COOKIE}=${replica}`))
      cookies.push(`${COOKIE}=${replica}; Path=/; Secure; HttpOnly; SameSite=Lax; Max-Age=86400`);
    if (cookies.length) headers['set-cookie'] = cookies;
    headers['x-runner-global-router'] = '1';
    headers['x-runner-replica'] = host;
    return headers;
  }
  function unavailable(res) {
    res.writeHead(503, {'content-type':'application/json','cache-control':'no-store','retry-after':'2','x-runner-global-router':'1'});
    res.end(JSON.stringify({error:'No healthy replica is available. Try again shortly.'}));
  }
  async function prepareBody(req) {
    if (SAFE.has(req.method)) return;
    const length = Number(req.headers['content-length']);
    if (!Number.isFinite(length) || length > bodyBufferLimit) return; // Stream large/unknown bodies once.
    const chunks = []; let size = 0;
    for await (const chunk of req) {
      size += chunk.length;
      if (size > bodyBufferLimit) throw Error('request exceeded declared body size');
      chunks.push(chunk);
    }
    req.routerBody = Buffer.concat(chunks);
  }

  const server = http.createServer(async (req,res) => {
    const controller = new AbortController();
    req.routerSignal = controller.signal;
    res.once('close',()=>{if (!res.writableFinished) controller.abort();});
    try {
      const hostname = req.headers.host?.split(':')[0] ?? '';
      const ownReplica = hostname.startsWith(ownName+'-') && hostname.endsWith('.'+domain) &&
        /^[1-9]\d*$/.test(hostname.slice(ownName.length+1,-(domain.length+1)));
      const systemHost = ['127.0.0.1','localhost','app',`${ownName}.${domain}`].includes(hostname) || ownReplica;
      if ((req.url === '/healthz' || req.url === '/metrics') && systemHost) {
        res.writeHead(200,{'content-type':'application/json','cache-control':'no-store'});
        return res.end(JSON.stringify(req.url === '/healthz' ? {ok:true} : {...stats,now:Date.now()}));
      }
      await refresh();
      if (res.destroyed || controller.signal.aborted) return;
      const route = routes.get(req.headers.host?.split(':')[0]);
      if (!route) {res.writeHead(404); return res.end('Unknown global app domain');}
      stats.requests++;stats.active++;
      res.once('close',()=>{stats.active--;if (!res.writableFinished || res.statusCode >= 500) stats.failedResponses++;});
      if (!route.enabled || !route.peers.length) return unavailable(res);
      await prepareBody(req);
      const peers = ordered(req,route);
      let peer = await choose(req,peers);
      const tried = new Set();
      while (peer && !res.destroyed) {
        tried.add(peer);
        let result;
        try {result = await open(req,peer);} catch {
          stats.upstreamFailures++;
          good.delete(scope(req));
          if (!SAFE.has(req.method)) return unavailable(res); // A write may have reached the app.
          peer = await choose(req,peers.filter(x=>!tried.has(x))); continue;
        }
        const {response} = result;
        const retry = SAFE.has(req.method) && RETRY.has(response.statusCode);
        if (retry) {
          stats.upstreamFailures++;
          response.resume(); good.delete(scope(req));
          peer = await choose(req,peers.filter(x=>!tried.has(x))); continue;
        }
        remember(req,peer);
        res.writeHead(response.statusCode,responseHeaders(req,response,peer));
        res.flushHeaders();
        await forwardResponse({req,res,response,peer,signal:controller.signal,
          idleMs:options.streamIdleMs, recoveryMs:options.streamRecoveryMs, maxAttempts:options.streamMaxAttempts,
          openRange:(next,headers,timeout)=>open(req,next,{headers,timeout:Math.min(timeout,headerTimeout)}),
          getPeers:async()=>{
            stats.upstreamFailures++;
            await refresh();
            const latest = routes.get(req.headers.host?.split(':')[0]);
            return latest?.enabled ? ordered(req,latest) : [];
          },
          onResume:info=>{stats.failovers++;stats.streamResumes++;good.delete(scope(req));remember(req,info.to);options.onResume?.(info);},
          onChunk:bytes=>{stats.bytesToClients+=bytes;},
        });
        return;
      }
      unavailable(res);
    } catch {if (!res.headersSent) unavailable(res); else res.destroy();}
  });

  server.on('upgrade', async (req,socket,head) => {
    socket.on('error',()=>{});
    try {
      await refresh(); const route = routes.get(req.headers.host?.split(':')[0]);
      if (!route?.enabled) throw Error('no app');
      for (const peer of ordered(req,route)) {
        let result;
        try {result = await open(req,peer,{upgrade:true,timeout:Math.min(headerTimeout,5000)});} catch {continue;}
        const {response, socket:upstream, head:upHead} = result;
        if (response.statusCode !== 101 || !upstream) {
          response.resume(); if (RETRY.has(response.statusCode)) continue;
          socket.end(`HTTP/1.1 ${response.statusCode} Rejected\r\nConnection: close\r\nContent-Length: 0\r\n\r\n`); return;
        }
        remember(req,peer);
        stats.websockets++;
        socket.once('close',()=>{stats.websockets--;});
        upstream.on('data',data=>{stats.bytesToClients+=data.length;});
        const headers = {...responseHeaders(req,response,peer),connection:'Upgrade',upgrade:'websocket'};
        let handshake = 'HTTP/1.1 101 Switching Protocols\r\n';
        for (const [key,value] of Object.entries(headers)) for (const item of Array.isArray(value)?value:[value]) handshake += `${key}: ${item}\r\n`;
        socket.write(handshake+'\r\n');
        if (upHead.length) socket.write(upHead); if (head.length) upstream.write(head);
        socket.pipe(upstream).pipe(socket);
        socket.once('close',()=>upstream.destroy()); upstream.once('close',()=>socket.destroy());
        upstream.on('error',()=>socket.destroy()); return;
      }
      throw Error('no replica');
    } catch {socket.end('HTTP/1.1 503 Service Unavailable\r\nRetry-After: 2\r\nConnection: close\r\nContent-Length: 0\r\n\r\n');}
  });
  server.keepAliveTimeout = 65000;
  server.stats = stats;
  return server;
}
