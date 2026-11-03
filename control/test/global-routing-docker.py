#!/usr/bin/env python3
"""Exercise generated app routes with real Caddy on Linux, without Cloudflare.

Usage: python3 control/test/global-routing-docker.py runner-agent:local
Uses disposable ports, a UUID-named Caddy container and a private temp directory.
"""
import base64
import hashlib
import http.client
import http.server
import json
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import uuid

if sys.platform != "linux":
    raise SystemExit("Run this Docker integration test on Linux")
IMAGE = sys.argv[1] if len(sys.argv) > 1 else "runner-agent:local"
SOURCE = Path(__file__).resolve().parents[2]
ROOT = Path(tempfile.mkdtemp(prefix="runner-global-routing-"))
NAME = "runner-routing-test-" + uuid.uuid4().hex[:12]
STREAM = threading.Event()
servers = []


def docker(*args, **kwargs):
    return subprocess.run(["docker", *map(str, args)], capture_output=True, text=True, check=True, timeout=60, **kwargs)


def port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def upstream(label):
    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):
            if self.path == "/ws":
                key = self.headers["Sec-WebSocket-Key"]
                accept = base64.b64encode(hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()).decode()
                self.send_response(101)
                self.send_header("Connection", "Upgrade")
                self.send_header("Upgrade", "websocket")
                self.send_header("Sec-WebSocket-Accept", accept)
                self.end_headers()
                self.wfile.flush()
                frame = self.rfile.read(8)
                self.wfile.write(frame)
                self.wfile.flush()
                self.close_connection = True
                return
            if self.path == "/stream":
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Content-Length", "13")
                self.end_headers()
                self.wfile.write(b"first\n")
                self.wfile.flush()
                STREAM.wait(5)
                self.wfile.write(b"second\n")
                self.wfile.flush()
                return
            self.respond()

        def do_POST(self):
            self.respond()

        def respond(self):
            body = self.rfile.read(int(self.headers.get("Content-Length", 0))).decode()
            data = json.dumps({"backend": label, "host": self.headers.get("Host"), "path": self.path, "body": body,
                "proto": self.headers.get("X-Forwarded-Proto"), "forwardedHost": self.headers.get("X-Forwarded-Host")}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *_):
            pass
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    servers.append(server)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server.server_port


try:
    router_port, admin_port = port(), port()
    one, two, other = upstream("one"), upstream("two"), upstream("other")
    projects = [{"app": "web", "replica": 2, "port": two, "s": "healthy"},
        {"app": "other", "replica": 1, "port": other, "s": "healthy"},
        {"app": "web", "replica": 1, "port": one, "s": "healthy"}]
    def configure(reload=False):
        generate = 'import {appRoutes} from "/source/agent/runtime.mjs"; import {readFileSync} from "node:fs"; process.stdout.write(JSON.stringify(appRoutes(JSON.parse(readFileSync(0,"utf8")),"example.test")));'
        routes = json.loads(docker("run", "--rm", "--read-only", "--network", "none", "-i", "--entrypoint", "node",
            "-v", f"{SOURCE}:/source:ro", IMAGE, "--input-type=module", "-e", generate, input=json.dumps(projects)).stdout)
        routes.append({"handle": [{"handler": "static_response", "status_code": 404}]})
        config = {"admin": {"listen": f"127.0.0.1:{admin_port}"}, "apps": {"http": {"servers": {"router": {
            "listen": [f"127.0.0.1:{router_port}"], "automatic_https": {"disable": True}, "routes": routes,
            "trusted_proxies": {"source": "static", "ranges": ["127.0.0.1/32", "::1/128"]}}}}}}
        (ROOT / "caddy.json").write_text(json.dumps(config))
        if reload:
            docker("exec", NAME, "caddy", "reload", "--address", f"127.0.0.1:{admin_port}", "--config", "/etc/router/caddy.json")

    def request(host, method="GET", path="/", body=None):
        conn = http.client.HTTPConnection("127.0.0.1", router_port, timeout=5)
        conn.request(method, path, body, {"Host": host + ".example.test", "X-Forwarded-Host": host + ".example.test", "X-Forwarded-Proto": "https"})
        res = conn.getresponse()
        status, data = res.status, res.read()
        conn.close()
        return status, json.loads(data) if status == 200 else data

    configure()
    docker("run", "-d", "--name", NAME, "--network", "host", "--label", f"io.runners.test={NAME}",
        "-v", f"{ROOT}:/etc/router:ro", "caddy:2", "caddy", "run", "--config", "/etc/router/caddy.json")
    for _ in range(50):
        try:
            if request("web")[0] == 200:
                break
        except OSError:
            pass
        time.sleep(0.1)
    assert request("web")[1]["backend"] == "one"
    assert request("web-1")[1]["backend"] == "one"
    assert request("web-2")[1]["backend"] == "two"
    assert request("other")[1]["backend"] == "other"
    assert request("missing")[0] == 404
    print("ok global and numbered domains select the correct app and remapped copy ports", flush=True)

    status, data = request("web", "POST", "/path?q=1", "payload")
    assert status == 200 and data == {"backend": "one", "host": "web.example.test", "path": "/path?q=1", "body": "payload",
        "proto": "https", "forwardedHost": "web.example.test"}
    print("ok global requests preserve the public host, HTTPS scheme, method, path, query and body", flush=True)

    conn = http.client.HTTPConnection("127.0.0.1", router_port, timeout=3)
    conn.request("GET", "/stream", headers={"Host": "web.example.test"})
    res = conn.getresponse()
    assert res.status == 200 and res.read(6) == b"first\n"
    STREAM.set()
    assert res.read() == b"second\n"
    conn.close()
    print("ok streaming responses arrive before the origin completes", flush=True)

    with socket.create_connection(("127.0.0.1", router_port), timeout=3) as sock:
        sock.sendall(b"GET /ws HTTP/1.1\r\nHost: web.example.test\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n")
        handshake = b""
        while b"\r\n\r\n" not in handshake:
            handshake += sock.recv(1)
        assert handshake.startswith(b"HTTP/1.1 101")
        frame = b"\x81\x82\x01\x02\x03\x04ik"
        sock.sendall(frame)
        echo = b""
        while len(echo) < len(frame):
            echo += sock.recv(len(frame) - len(echo))
        assert echo == frame
    print("ok the global domain supports WebSocket upgrades and bidirectional traffic", flush=True)

    projects[2]["r"] = False
    configure(reload=True)
    assert request("web")[1]["backend"] == "two"
    assert request("web-1")[1]["backend"] == "one"
    projects[0]["s"] = "failed"
    configure(reload=True)
    assert request("web")[0] == 503 and request("other")[0] == 200
    projects[2]["r"] = True
    configure(reload=True)
    assert request("web")[1]["backend"] == "one"
    print("ok readiness changes fail over locally, return 503 without healthy copies and recover independently", flush=True)
finally:
    STREAM.set()
    subprocess.run(["docker", "rm", "-f", NAME], capture_output=True, timeout=30)
    for server in servers:
        server.shutdown()
        server.server_close()
    shutil.rmtree(ROOT)
