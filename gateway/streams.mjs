import {once} from 'node:events';

const integer = value => typeof value === 'string' && /^\d+$/.test(value) && Number.isSafeInteger(Number(value)) ? Number(value) : null;
const identity = response => !response.headers['content-encoding'] || response.headers['content-encoding'].toLowerCase() === 'identity';
function byteRange(value) {
  const match = typeof value === 'string' && /^bytes (\d+)-(\d+)\/(\d+)$/.exec(value);
  if (!match) return null;
  const [start,end,total] = match.slice(1).map(integer);
  return start !== null && end !== null && total !== null && start <= end && end < total ? {start,end,total} : null;
}

// A strong validator prevents joining bytes from two versions of a resource.
// Never splice compressed, multipart, unknown-length or non-GET responses.
export function resumable(method, response) {
  const length = integer(response.headers['content-length']);
  const etag = response.headers.etag;
  if (method !== 'GET' || !identity(response) || !length ||
      typeof etag !== 'string' || !/^"[\x21\x23-\x7e\x80-\xff]*"$/.test(etag) ||
      /^multipart\//i.test(response.headers['content-type'] ?? '')) return null;
  if (response.statusCode === 200 && /^bytes$/i.test(response.headers['accept-ranges'] ?? '')) {
    return {start:0, end:length-1, total:length, length, etag};
  }
  const range = response.statusCode === 206 && byteRange(response.headers['content-range']);
  return range && range.end - range.start + 1 === length ? {...range,length,etag} : null;
}

function validContinuation(response, original, offset) {
  const range = byteRange(response.headers['content-range']);
  return response.statusCode === 206 && identity(response) && response.headers.etag === original.etag && range &&
    range.start === offset && range.end === original.end && range.total === original.total &&
    integer(response.headers['content-length']) === original.end - offset + 1;
}

// Keep the downstream response open while replacing only its failed upstream.
// The caller provides registered peers and performs the same original request
// with new Range/If-Range headers; this module knows nothing about applications.
export async function forwardResponse({req,res,response,peer,openRange,getPeers,signal,
  idleMs=10_000,recoveryMs=30_000,maxAttempts=32,onResume=()=>{},onChunk=()=>{}}) {
  const original = resumable(req.method, response);
  let sent = 0, attempts = 0, timer, current = response;
  const failures = new Map();
  const abort = () => current.destroy(new Error('client disconnected'));
  signal?.addEventListener('abort', abort);
  try {
    for (;;) {
      const arm = () => {
        if (original && idleMs > 0) timer = setTimeout(() => current.destroy(new Error('upstream stopped sending bytes')), idleMs);
      };
      try {
        arm();
        for await (const chunk of current) {
          clearTimeout(timer);
          if (signal?.aborted || res.destroyed) throw new Error('client disconnected');
          if (original && sent + chunk.length > original.length) throw new Error('origin sent beyond the requested range');
          const flowing = res.write(chunk);
          onChunk(chunk.length);
          sent += chunk.length; // Includes bytes queued in downstream backpressure buffers.
          if (!flowing) await once(res,'drain',{signal});
          arm();
        }
        if (original && sent !== original.length) throw new Error('incomplete upstream response');
        res.end();
        return;
      } catch (error) {
        if (signal?.aborted || res.destroyed || !original) throw error;
        // A disconnect after the complete body needs no additional request.
        if (sent === original.length) {res.end(); return;}
      } finally {clearTimeout(timer);}

      current.destroy();
      failures.set(peer,Date.now());
      const deadline = Date.now() + recoveryMs;
      const candidates = [...new Set(await getPeers())].filter(candidate => candidate !== peer)
        .sort((a,b)=>(failures.get(a)??0)-(failures.get(b)??0));
      let next;
      for (const candidate of candidates) {
        if (signal?.aborted || res.destroyed) throw new Error('client disconnected');
        if (Date.now() >= deadline || attempts >= maxAttempts) break;
        attempts++;
        const offset = original.start + sent;
        let result;
        try {
          result = await openRange(candidate, {
            range:`bytes=${offset}-${original.end}`, 'if-range':original.etag,
            'if-match':original.etag, 'accept-encoding':'identity',
          }, Math.max(1,Math.min(10_000,deadline-Date.now())));
        } catch {failures.set(candidate,Date.now());continue;}
        if (!validContinuation(result.response,original,offset)) {
          result.response.destroy();
          failures.set(candidate,Date.now());
          continue;
        }
        next = result;
        try {onResume({from:peer,to:candidate,offset});} catch {}
        peer = candidate;
        break;
      }
      if (!next) throw new Error('no matching live replica can continue this stream');
      current = next.response;
    }
  } finally {
    clearTimeout(timer);
    signal?.removeEventListener('abort',abort);
    if (!current.complete) current.destroy();
  }
}
