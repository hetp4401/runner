"""Build first, then run: python3 control/test/docker-smoke.py runner-control:local"""
import concurrent.futures
import json
import pathlib
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid

image = sys.argv[1] if len(sys.argv) > 1 else 'runner-control:local'
agent_image = sys.argv[2] if len(sys.argv) > 2 else None
name = 'runner-check-' + uuid.uuid4().hex[:10]
auto_name = name + '-auto'
volume, network, mock_name = name + '-data', name + '-net', name + '-api'
directory = pathlib.Path(tempfile.mkdtemp(prefix='runner-docker-check-'))
checks = 0


def docker(*args, ok=True):
    result = subprocess.run(['docker', *args], capture_output=True, text=True, timeout=90)
    if ok and result.returncode:
        raise RuntimeError('docker command failed: ' + result.stderr.strip())
    return result


def check(message):
    global checks
    checks += 1
    print('ok ' + message, flush=True)


def api(path, method='GET', body=None, headers=None):
    request = urllib.request.Request(origin + path, method=method, headers={'Content-Type': 'application/json', **(headers or {})}, data=json.dumps(body).encode() if body is not None else None)
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            raw = response.read()
            return response.status, json.loads(raw) if 'application/json' in response.headers.get('Content-Type', '') else raw.decode()
    except urllib.error.HTTPError as error:
        return error.code, json.load(error)


def ready():
    for _ in range(50):
        try:
            code, status = api('/api/status')
            if code == 200 and status['controlPlane']['tunnelsReady']:
                return status
        except (OSError, ValueError):
            pass
        time.sleep(.2)
    raise RuntimeError('container did not become ready')


try:
    docker('network', 'create', network)
    docker('volume', 'create', volume)
    common = ['--rm', '-v', volume + ':/data', '-e', 'DOMAIN=example.test']
    docker('run', *common, image, 'init')
    credentials = json.loads(docker('run', *common, image, 'credentials').stdout)
    assert len(credentials['JOIN_TOKEN']) >= 32
    assert docker('run', *common, image, 'init', ok=False).returncode != 0
    check('fresh Docker volume initializes once and generates private fleet credentials')

    mock_file = directory / 'mock.mjs'
    mock_file.write_text("""import http from 'node:http';
let tunnels=[], records=[];
http.createServer(async(req,res)=>{
  const url=new URL(req.url,'http://mock'), chunks=[];
  for await(const chunk of req) chunks.push(chunk);
  const body=chunks.length?JSON.parse(Buffer.concat(chunks)):null;
  let result=[];
  if(url.pathname.endsWith('/token')) result='fake-controller-token';
  else if(url.pathname.includes('/cfd_tunnel')) {
    if(req.method==='POST') {const tunnel={...body,id:'controller-id'};tunnels.push(tunnel);result=tunnel;}
    else result=tunnels;
  } else if(url.pathname.includes('/dns_records')) {
    if(req.method==='POST') {const record={...body,id:'controller-record'};records.push(record);result=record;}
    else result=records;
  }
  res.setHeader('content-type','application/json');
  res.end(JSON.stringify({success:true,result,result_info:{total_count:Array.isArray(result)?result.length:1,total_pages:1}}));
}).listen(8080,'0.0.0.0');
""")
    mock_file.chmod(0o644)
    docker('run', '-d', '--name', mock_name, '--network', network, '--network-alias', 'cloudflare', '-v', str(mock_file) + ':/mock.mjs:ro', '--entrypoint', 'node', image, '/mock.mjs')
    docker('run', '-d', '--name', name, '--init', '--network', network, '-p', '127.0.0.1::8920', '-v', volume + ':/data', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges:true', '-e', 'DOMAIN=example.test', '-e', 'CF_API_TOKEN=fake-key', '-e', 'ACCOUNT_ID=test-account', '-e', 'ZONE=test-zone', '-e', 'AUTO_TUNNEL=0', '-e', 'RECOVERY_MS=0', '-e', 'CF_API_BASE=http://cloudflare:8080', image)
    port = docker('inspect', '--format', '{{(index (index .NetworkSettings.Ports "8920/tcp") 0).HostPort}}', name).stdout.strip()
    origin = 'http://127.0.0.1:' + port
    status = ready()
    assert status['requireDeployAuth'] is True
    assert api('/healthz')[0] == 200
    check('the non-root container listens through Docker networking and recovers tunnel inventory')
    competing = docker('run', '--rm', '-v', volume + ':/data', '-e', 'DOMAIN=example.test', image, ok=False)
    assert competing.returncode == 75, competing.stderr
    assert json.loads(docker('run', *common, image, 'credentials').stdout) == credentials
    check('another scheduler cannot start on the same volume, while credentials remain readable')

    with concurrent.futures.ThreadPoolExecutor() as pool:
        assets = list(pool.map(lambda file: api('/agent/' + file), ['agent.mjs', 'metrics.mjs', 'ws.mjs', 'control-link.mjs', 'runtime.mjs']))
    assert all(code == 200 and len(body) > 100 for code, body in assets)
    script = api('/install.sh')[1]
    assert 'https://runners.example.test/agent' in script and 'githubusercontent.com' not in script
    check('the image serves every bundled agent dependency and its own install command')

    spec = {'compose': 'services:\n  web:\n    image: nginx\n', 'replicas': 1}
    assert api('/api/projects/web', 'PUT', spec)[0] == 401
    admin = {'x-admin-password': credentials['ADMIN_PASSWORD']}
    assert api('/api/projects/web', 'PUT', spec, admin)[0] == 200
    assert api('/api/projects/web/env', 'PUT', {'TOKEN': 'durable-test-value'}, admin)[0] == 200
    version = api('/api/projects/web')[1]['version']
    check('default deployment authorization works through the container HTTP API')

    docker('kill', '--signal', 'KILL', name)
    docker('start', name)
    port = docker('inspect', '--format', '{{(index (index .NetworkSettings.Ports "8920/tcp") 0).HostPort}}', name).stdout.strip()
    origin = 'http://127.0.0.1:' + port
    ready()
    assert api('/api/projects/web')[1]['version'] == version
    assert api('/api/projects/web/env', headers=admin)[1]['keys'] == ['TOKEN']
    assert json.loads(docker('run', *common, image, 'credentials').stdout) == credentials
    inspect = docker('exec', name, 'node', '-e', "const fs=require('fs');const {DatabaseSync}=require('node:sqlite');const d=new DatabaseSync('/data/control.db',{readOnly:true});console.log(JSON.stringify({uid:process.getuid(),socket:fs.existsSync('/var/run/docker.sock'),ca:fs.existsSync('/etc/ssl/certs/ca-certificates.crt'),tables:d.prepare(\"select name from sqlite_master where type='table' and name not like 'sqlite_%' order by name\").all().map(r=>r.name),secret:d.prepare(\"select value from app_env where key='TOKEN'\").get().value==='durable-test-value'}));").stdout
    state = json.loads(inspect)
    assert state['uid'] == 1000 and not state['socket'] and state['secret'] and state['ca'], state
    assert state['tables'] == ['app_env', 'app_passwords', 'projects', 'settings', 'versions']
    check('SIGKILL preserves specs and secrets, with only five tables and no Docker socket')

    missing = docker('run', '--rm', '-e', 'DOMAIN=example.test', '-e', 'ADMIN_PASSWORD=fake', '-e', 'JOIN_TOKEN=fake', '-e', 'STATE_SIGNING_KEY=fake', image, ok=False)
    assert missing.returncode != 0 and 'control.db is missing' in missing.stderr
    check('a missing data volume fails closed instead of creating an empty fleet')
    docker('run', '--rm', '--entrypoint', 'cloudflared', image, '--version')
    docker('stop', '--time', '15', name)
    assert docker('inspect', '--format', '{{.State.ExitCode}}', name).stdout.strip() == '0'
    check('the bundled cloudflared binary runs and Docker stop shuts the controller down cleanly')

    # Exercise the real AUTO_TUNNEL supervisor and its generated ingress config
    # against a local fake connector and fake CF API. No public DNS is touched.
    fake_directory = directory / 'fake-bin'
    fake_directory.mkdir(mode=0o755)
    fake = fake_directory / 'cloudflared'
    fake.write_text(r"""#!/usr/local/bin/node
import http from 'node:http';
import fs from 'node:fs';
const args=process.argv.slice(2), config=fs.readFileSync(args[args.indexOf('--config')+1],'utf8');
const hostname=JSON.parse(config.match(/hostname: (.+)/)[1]);
const target=new URL(config.match(/service: (http:\/\/[^\s]+)/)[1]);
const metrics=args[args.indexOf('--metrics')+1].split(':');
if(process.env.TUNNEL_TOKEN!=='fake-controller-token'||process.env.CF_API_TOKEN||process.env.JOIN_TOKEN) process.exit(90);
fs.writeFileSync('/data/fake-tunnel-state.json',JSON.stringify({pid:process.pid,hostname,target:target.origin,managementKeyAbsent:true}));
const proxy=http.createServer((req,res)=>{
  if(req.headers.host!==hostname){res.writeHead(404);return res.end();}
  const upstream=http.request(new URL(req.url,target),{method:req.method,headers:req.headers},r=>{res.writeHead(r.statusCode,r.headers);r.pipe(res);});
  upstream.on('error',()=>{res.writeHead(502);res.end();});req.pipe(upstream);
}).listen(8081,'0.0.0.0');
const ready=http.createServer(async(req,res)=>{
  try{const response=await fetch(new URL('/healthz',target));res.writeHead(response.status);res.end();}
  catch{res.writeHead(503);res.end();}
}).listen(Number(metrics[1]),metrics[0]);
process.on('SIGTERM',()=>{proxy.close();ready.close();setTimeout(()=>process.exit(0),100).unref();});
""")
    fake.chmod(0o755)
    docker('run', '-d', '--name', auto_name, '--init', '--network', network,
           '-p', '127.0.0.1::8920', '-p', '127.0.0.1::8081', '-v', volume + ':/data',
           '-v', str(fake_directory) + ':/fake-bin:ro', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges:true',
           '-e', 'DOMAIN=example.test', '-e', 'CF_API_TOKEN=fake-key', '-e', 'ACCOUNT_ID=test-account', '-e', 'ZONE=test-zone',
           '-e', 'AUTO_TUNNEL=1', '-e', 'RECOVERY_MS=0', '-e', 'CF_API_BASE=http://cloudflare:8080',
           '-e', 'PATH=/fake-bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin', image)
    port = docker('inspect', '--format', '{{(index (index .NetworkSettings.Ports "8920/tcp") 0).HostPort}}', auto_name).stdout.strip()
    origin = 'http://127.0.0.1:' + port
    ready()
    docker('exec', auto_name, 'node', 'healthcheck.mjs')
    ingress_port = docker('inspect', '--format', '{{(index (index .NetworkSettings.Ports "8081/tcp") 0).HostPort}}', auto_name).stdout.strip()
    request = urllib.request.Request('http://127.0.0.1:' + ingress_port + '/healthz', headers={'Host': 'runners.example.test'})
    with urllib.request.urlopen(request, timeout=5) as response:
        assert response.status == 200 and json.load(response)['tunnelsReady']
    state = json.loads(docker('exec', auto_name, 'node', '-e', "console.log(require('fs').readFileSync('/data/fake-tunnel-state.json','utf8'))").stdout)
    assert state['hostname'] == 'runners.example.test' and state['target'] == 'http://127.0.0.1:8920' and state['managementKeyAbsent']
    check('AUTO_TUNNEL creates its tunnel and DNS, forwards local ingress, and keeps management credentials out of cloudflared')
    docker('exec', auto_name, 'node', '-e', "process.kill(JSON.parse(require('fs').readFileSync('/data/fake-tunnel-state.json','utf8')).pid,'SIGTERM')")
    for _ in range(50):
        if docker('inspect', '--format', '{{.State.Running}}', auto_name).stdout.strip() == 'false':
            break
        time.sleep(.2)
    assert docker('inspect', '--format', '{{.State.ExitCode}}', auto_name).stdout.strip() == '1'
    check('the container stops its scheduler when the supervised tunnel exits')

    if agent_image:
        docker('run', '--rm', '--network', 'none', '--entrypoint', 'sh', agent_image, '-c', 'node --version && docker compose version')
        baseline = directory / 'agent.mjs'
        baseline.write_text("console.log('offline bundled agent started');\n")
        baseline.chmod(0o644)
        offline = docker('run', '--rm', '--network', 'none', '-v', str(baseline) + ':/agent/agent.mjs:ro', agent_image)
        assert 'offline bundled agent started' in offline.stdout
        check('the agent image includes Node and Compose and starts its bundled code with networking disabled')
    print(str(checks) + ' Docker checks passed', flush=True)
finally:
    docker('rm', '-f', name, auto_name, mock_name, ok=False)
    docker('volume', 'rm', volume, ok=False)
    docker('network', 'rm', network, ok=False)
    shutil.rmtree(directory)
