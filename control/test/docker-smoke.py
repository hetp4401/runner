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
name = 'runner-check-' + uuid.uuid4().hex[:10]
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
    mock_file.write_text("import http from 'node:http'; http.createServer((req,res)=>{res.setHeader('content-type','application/json');res.end(JSON.stringify({success:true,result:[],result_info:{total_count:0}}));}).listen(8080,'0.0.0.0');\n")
    mock_file.chmod(0o644)
    docker('run', '-d', '--name', mock_name, '--network', network, '--network-alias', 'cloudflare', '-v', str(mock_file) + ':/mock.mjs:ro', '--entrypoint', 'node', image, '/mock.mjs')
    docker('run', '-d', '--name', name, '--init', '--network', network, '-p', '127.0.0.1::8920', '-v', volume + ':/data', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges:true', '-e', 'DOMAIN=example.test', '-e', 'CF_API_TOKEN=fake-key', '-e', 'ACCOUNT_ID=test-account', '-e', 'ZONE=test-zone', '-e', 'AUTO_TUNNEL=0', '-e', 'RECOVERY_MS=0', '-e', 'CF_API_BASE=http://cloudflare:8080', image)
    port = docker('inspect', '--format', '{{(index (index .NetworkSettings.Ports "8920/tcp") 0).HostPort}}', name).stdout.strip()
    origin = 'http://127.0.0.1:' + port
    status = ready()
    assert status['requireDeployAuth'] is True
    assert api('/healthz')[0] == 200
    check('the non-root container listens through Docker networking and recovers tunnel inventory')

    with concurrent.futures.ThreadPoolExecutor() as pool:
        assets = list(pool.map(lambda file: api('/agent/' + file), ['agent.mjs', 'metrics.mjs', 'ws.mjs', 'control-link.mjs']))
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
    print(str(checks) + ' Docker checks passed', flush=True)
finally:
    docker('rm', '-f', name, mock_name, ok=False)
    docker('volume', 'rm', volume, ok=False)
    docker('network', 'rm', network, ok=False)
    shutil.rmtree(directory)
