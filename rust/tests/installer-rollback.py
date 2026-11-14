#!/usr/bin/env python3
"""Fault-inject an agent installation on an isolated GitHub Linux runner."""
import hashlib
import http.server
import io
import json
import os
from pathlib import Path
import shlex
import subprocess
import tarfile
import tempfile
import threading
import uuid

assert os.environ.get('GITHUB_ACTIONS') == 'true', 'run only on an isolated GitHub runner'
prefix = 'runner-install-test-' + uuid.uuid4().hex[:12]
root = Path(__file__).resolve().parents[2]
temporary = tempfile.TemporaryDirectory(prefix=prefix)
data = Path(temporary.name) / 'data'
(data / 'agent-code').mkdir(parents=True)
archive = io.BytesIO()
with tarfile.open(fileobj=archive, mode='w') as tar:
    content = b'FROM busybox:1.37\nENTRYPOINT ["false"]\n'
    info = tarfile.TarInfo('Dockerfile')
    info.size = len(content)
    tar.addfile(info, io.BytesIO(content))
bundle = archive.getvalue()
digest = hashlib.sha256(bundle).hexdigest()
bad_digest = True

class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass
    def do_GET(self):
        if self.path.endswith('/bundle.sha256'):
            body = (('0' * 64 if bad_digest else digest) + '\n').encode()
        elif self.path.endswith('/bundle.tar'):
            body = bundle
        else:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
threading.Thread(target=server.serve_forever, daemon=True).start()
origin = 'http://127.0.0.1:' + str(server.server_port)
def docker(*args):
    return subprocess.check_output(['docker', *args], text=True, stderr=subprocess.STDOUT).strip()

try:
    assert not docker('ps', '-aq', '--filter', 'name=^/runner-agent$'), 'unrelated agent exists'
    for pointer in ['current.json', 'rust-current.json']:
        (data / 'agent-code' / pointer).write_text(json.dumps({'revision': pointer}))
    old = docker('run', '-d', '--name', 'runner-agent', '--label', 'io.runners.test=' + prefix,
                 '-e', 'CONTROL_URL=' + origin, 'busybox:1.37', 'sleep', '3600')
    script = Path(temporary.name) / 'install.sh'
    script.write_text((root / 'rust/install-agent.sh').read_text().replace('@ORIGIN@', shlex.quote(origin)).replace('@AGENT_URL@', shlex.quote(origin + '/agent')))
    for phase in ['checksum', 'startup']:
        result = subprocess.run(['sudo', 'env', 'JOIN_TOKEN=fixture-join', 'SERVER_TAG=standalone',
                                 'RUNNER_DATA=' + str(data), 'sh', str(script)], text=True, capture_output=True)
        assert result.returncode != 0, phase + ' failure was not detected'
        restored = json.loads(docker('inspect', 'runner-agent'))[0]
        assert restored['Id'] == old and restored['State']['Running'], phase + ' did not preserve the previous agent'
        for pointer in ['current.json', 'rust-current.json']:
            assert json.loads((data / 'agent-code' / pointer).read_text()) == {'revision': pointer}
        if phase == 'startup':
            assert 'previous agent was restored' in result.stderr
        bad_digest = False
    print(json.dumps({'checks': ['bad checksum never stops the old agent', 'failed replacement restores the previous container and both release pointers']}))
finally:
    ids = docker('ps', '-aq', '--filter', 'label=io.runners.test=' + prefix).split()
    if ids:
        subprocess.run(['docker', 'rm', '-f', *ids], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run(['docker', 'image', 'rm', 'runner-agent:rust-' + digest], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    server.shutdown()
    server.server_close()
    temporary.cleanup()
