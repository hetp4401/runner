#!/usr/bin/env python3
"""Linux/Docker smoke test of the actual Caddy visitor logging configuration."""
import http.client
import http.server
import json
import os
import pathlib
import socket
import statistics
import subprocess
import tempfile
import threading
import time
import uuid

root = pathlib.Path(__file__).resolve().parents[2]
node = os.environ.get("NODE", "node")


def unused_port():
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        return reservation.getsockname()[1]


class Backend(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def setup(self):
        super().setup()
        self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    def do_GET(self):
        body = b"ok"
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):
        pass


backend = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Backend)
threading.Thread(target=backend.serve_forever, daemon=True).start()
listener = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
listener.bind(("127.0.0.1", 0))
listener.settimeout(5)
port, admin = unused_port(), unused_port()
name = "runner-visitors-test-" + uuid.uuid4().hex[:8]
records, stop = [], threading.Event()


def request(headers=None):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    start = time.perf_counter()
    connection.request("GET", "/private-path?secret=never-collect-this", headers={"Host": "demo-1.example.test", **(headers or {})})
    response = connection.getresponse()
    assert response.status == 200
    response.read()
    connection.close()
    return (time.perf_counter() - start) * 1000


try:
    with tempfile.TemporaryDirectory(prefix="runner-visitors-router-") as directory:
        config_path = pathlib.Path(directory) / "caddy.json"
        config = {"admin": {"listen": f"127.0.0.1:{admin}"}, "apps": {"http": {"servers": {"router": {
            "listen": [f"127.0.0.1:{port}"], "automatic_https": {"disable": True},
            "trusted_proxies": {"source": "static", "ranges": ["127.0.0.1/32"]},
            "routes": [{"match": [{"host": ["demo-1.example.test"]}], "handle": [{"handler": "reverse_proxy", "upstreams": [{"dial": f"127.0.0.1:{backend.server_port}"}]}]}],
        }}}}}
        config_path.write_text(json.dumps(config))
        subprocess.run(["docker", "run", "--rm", "-d", "--name", name, "--network", "host", "--cpus", "0.5", "--memory", "128m", "-v", f"{directory}:/config-test:ro", "caddy:2", "caddy", "run", "--config", "/config-test/caddy.json"], check=True, capture_output=True)
        for _ in range(100):
            try:
                request()
                break
            except (OSError, AssertionError):
                time.sleep(0.05)
        baseline = [request() for _ in range(300)]
        source = "import { visitorLogging } from './agent/runtime.mjs'; console.log(JSON.stringify(visitorLogging(Number(process.argv[1]), ['demo-1.example.test'])));"
        settings = json.loads(subprocess.check_output([node, "--input-type=module", "-e", source, str(listener.getsockname()[1])], cwd=root))
        config["logging"] = settings["logging"]
        config["apps"]["http"]["servers"]["router"].update({"logs": settings["logs"], "client_ip_headers": ["Cf-Connecting-Ip"]})
        config_path.write_text(json.dumps(config))
        reload = subprocess.run(["docker", "exec", name, "caddy", "reload", "--address", f"127.0.0.1:{admin}", "--config", "/config-test/caddy.json"], capture_output=True, text=True)
        assert reload.returncode == 0, reload.stderr
        request({"Cf-Connecting-Ip": "198.51.100.77", "Cookie": "secret-cookie", "Authorization": "Bearer secret-token", "X-Forwarded-For": "203.0.113.99"})
        record = listener.recv(8192).decode()
        entry = json.loads(record)
        assert entry["request"]["client_ip"] == "198.51.100.77", entry
        assert entry["request"]["remote_ip"] == "127.0.0.1"
        assert entry["request"]["host"] == "demo-1.example.test"
        assert not any(secret in record for secret in ["private-path", "secret-cookie", "secret-token", "never-collect-this", "203.0.113.99"]), record
        assert set(entry["request"]) == {"remote_ip", "client_ip", "host"}, entry
        print("ok actual Caddy reload; Cloudflare IP takes precedence over spoofed XFF; no URL/header/credential metadata emitted")

        def drain():
            listener.settimeout(0.1)
            while not stop.is_set():
                try:
                    listener.recv(8192)
                except socket.timeout:
                    pass

        reader = threading.Thread(target=drain, daemon=True)
        reader.start()
        measured = [request({"Cf-Connecting-Ip": "198.51.100.77"}) for _ in range(300)]
        print(json.dumps({"latencyMs": {"baselineMedian": round(statistics.median(baseline), 3), "withCollectionMedian": round(statistics.median(measured), 3)}, "note": "local smoke measurement, not a production load guarantee"}))
        stop.set()
        reader.join(timeout=1)
        listener.close()
        for _ in range(10):
            request({"Cf-Connecting-Ip": "198.51.100.77"})
        print("ok router continues returning successful responses with collector offline")
finally:
    stop.set()
    listener.close()
    backend.shutdown()
    subprocess.run(["docker", "rm", "-f", name], capture_output=True)
