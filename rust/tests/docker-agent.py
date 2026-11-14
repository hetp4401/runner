#!/usr/bin/env python3
"""Exercise the packaged agent against a real Linux Docker Engine and router."""
import copy
import hashlib
import http.server
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import threading
import time
import uuid

prefix = 'runner-rust-agent-test-' + uuid.uuid4().hex[:12]
root = Path(__file__).resolve().parents[2]
temporary = tempfile.TemporaryDirectory(prefix=prefix)
data = Path(temporary.name) / 'data'
data.mkdir(mode=0o700)
lock = threading.Lock()
reports = []
desired = {}
agent_name = prefix + '-agent'
image = prefix + ':test'
checks = []

def docker(*args):
    return subprocess.check_output(['docker', *args], text=True, stderr=subprocess.STDOUT).strip()

class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass
    def do_GET(self):
        self.send_error(404)
    def do_POST(self):
        if self.path != '/api/sync':
            self.send_error(404)
            return
        body = json.loads(self.rfile.read(int(self.headers['content-length'])))
        assert self.headers['authorization'] == 'Bearer fixture-join'
        with lock:
            reports.append(body)
            del reports[:-100]
            plan = {'domain': 'example.test', 'desired': copy.deepcopy(desired), 'poll': .2}
        payload = json.dumps(plan).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
threading.Thread(target=server.serve_forever, daemon=True).start()
control = 'http://127.0.0.1:' + str(server.server_port)
owner = 'runner-' + hashlib.sha256(control.encode()).hexdigest()[:12]
with socket.socket() as listener:
    listener.bind(('127.0.0.1', 0))
    router_port = listener.getsockname()[1]

def latest():
    with lock:
        return copy.deepcopy(reports[-1]) if reports else {}

def until(predicate, message, timeout=100):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(.3)
    raise AssertionError(message + '; last status=' + json.dumps(latest().get('status')))

def owned_ids():
    return docker('ps', '-aq', '--filter', 'label=io.runners.owner=' + owner).split()

def workload_id():
    return docker('ps', '-q', '--filter', 'label=io.runners.owner=' + owner,
                  '--filter', 'label=io.runners.copy=worker')

def launch():
    docker('run', '-d', '--name', agent_name, '--network', 'host',
           '-v', '/var/run/docker.sock:/var/run/docker.sock', '-v', f'{data}:{data}',
           '-e', 'CONTROL_URL=' + control, '-e', 'JOIN_TOKEN=fixture-join',
           '-e', 'RUNNER_DATA=' + str(data), '-e', 'SERVER_TAG=standalone',
           '-e', 'ROUTER_PORT=' + str(router_port), image)

try:
    build = Path(temporary.name) / 'build'
    (build / 'bin').mkdir(parents=True)
    shutil.copyfile(root / 'rust/agent.Dockerfile', build / 'Dockerfile')
    for arch in ['amd64', 'arm64']:
        shutil.copyfile(root / f'rust/dist/binaries/{arch}/runner-agent', build / f'bin/runner-agent-linux-{arch}')
    arch = 'arm64' if docker('info', '--format', '{{.Architecture}}') in ['arm64', 'aarch64'] else 'amd64'
    docker('build', '--build-arg', 'TARGETARCH=' + arch, '-t', image, str(build))
    assert 'runner-agent' in docker('run', '--rm', image, '--version')
    for dependency in ['busybox:1.37', 'caddy:2']:
        docker('pull', dependency)
    # A local connector stand-in avoids Cloudflare credentials; the router,
    # Engine API, Compose deployments, health checks and stats are all real.
    docker('run', '-d', '--name', owner + '-tunnel',
           '--label', 'io.runners.owner=' + owner, '--label', 'io.runners.component=tunnel',
           'busybox:1.37', 'sleep', '3600')
    now = int(time.time() * 1000)
    seed = {'v': 1, 'bootId': Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
            'installedRevision': '0.1.0', 'control': control, 'started': now - 10000,
            'agent': prefix, 'run': prefix, 'machine': 1, 'tunnelToken': 'fixture-token',
            'domain': 'example.test', 'wasReady': True, 'projects': [], 'desired': {}, 'metrics': []}
    (data / 'agent-state.json').write_text(json.dumps(seed))
    managed = {'services': {'worker': {'image': 'busybox:1.37', 'command': ['sleep', '3600']}}}
    desired = {'worker': {'v': 1, 'app': 'worker', 'replica': 1, 'port': None,
                         'compose': 'services:\n  worker:\n    image: busybox:1.37\n    command: [sleep, "3600"]\n',
                         'files': {}, 'managedCompose': json.dumps(managed), 'dataFiles': {},
                         'bindSources': [], 'revision': 'first'}}
    launch()
    until(lambda: latest().get('status', {}).get('worker', {}).get('s') == 'healthy', 'workload did not become healthy')
    first = workload_id()
    assert first
    checks.append('packaged agent creates and observes a real Compose workload')
    until(lambda: latest().get('metrics', {}).get('live', {}).get('h', {}).get('memTotal', 0) > 0, 'host metrics missing')
    stats = json.loads(docker('stats', '--no-stream', '--format', '{{json .}}', agent_name))
    rss = int(docker('exec', agent_name, 'sh', '-c', "awk '/^VmRSS:/{print $2*1024}' /proc/1/status"))
    assert 0 < rss < 64 * 1024 * 1024, rss
    checks.append('real host and container metrics with bounded agent memory')
    with lock:
        desired['worker']['compose'] = json.dumps(managed)
        desired['worker']['v'] = 2
        desired['worker']['revision'] = 'format-only'
    until(lambda: latest().get('status', {}).get('worker', {}).get('v') == 2, 'format adoption missing')
    assert workload_id() == first, 'format-only migration rebuilt the workload'
    checks.append('controller serialization changes preserve running containers')
    docker('stop', '-t', '10', agent_name)
    assert workload_id() == first
    docker('rm', agent_name)
    with lock:
        reports.clear()
    launch()
    until(lambda: latest().get('status', {}).get('worker', {}).get('s') == 'healthy', 'restart lost workload inventory')
    assert workload_id() == first
    checks.append('agent restart preserves workload identity and containers')
    docker('kill', first)
    repaired = until(lambda: (value if value and value != first else None) if (value := workload_id()) else None,
                     'failed workload was not repaired', timeout=110)
    until(lambda: latest().get('status', {}).get('worker', {}).get('s') == 'healthy', 'repair never became healthy')
    checks.append('stopped background workload is repaired')
    with lock:
        desired = {}
    until(lambda: not workload_id(), 'removed workload kept running')
    assert json.loads(docker('inspect', owner + '-tunnel'))[0]['State']['Running']
    checks.append('removing a workload keeps infrastructure running')
    print(json.dumps({'checks': checks, 'agentRssBytes': rss, 'containerMemory': stats['MemUsage']}))
except BaseException:
    # Only containers created by this isolated fixture are inspected or logged.
    for name in [agent_name, owner + '-router', owner + '-tunnel']:
        try:
            print(name, docker('inspect', '--format', '{{json .State}}', name), flush=True)
            print(docker('logs', '--tail', '35', name), flush=True)
        except subprocess.CalledProcessError:
            pass
    raise
finally:
    subprocess.run(['docker', 'rm', '-f', agent_name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    ids = owned_ids()
    if ids:
        subprocess.run(['docker', 'rm', '-f', *ids], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    networks = docker('network', 'ls', '-q', '--filter', 'label=com.docker.compose.project=' + owner + '-worker').split()
    if networks:
        subprocess.run(['docker', 'network', 'rm', *networks], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    server.shutdown()
    server.server_close()
    subprocess.run(['docker', 'image', 'rm', image], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    # The real agent creates private root-owned deployment folders. Restore
    # ownership only inside this fixture's uniquely allocated directory.
    subprocess.run(['sudo', 'chown', '-R', '--no-dereference', f'{os.getuid()}:{os.getgid()}', temporary.name], check=True)
    temporary.cleanup()
