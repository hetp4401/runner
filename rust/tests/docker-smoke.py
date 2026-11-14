#!/usr/bin/env python3
"""Real Linux container checks. Only uniquely named test resources are removed."""
import json
import os
import socket
import subprocess
import time
import urllib.error
import urllib.request
import uuid

image = os.environ.get("RUST_CONTROL_IMAGE", "runner-control:rust-test")
prefix = "runner-rust-test-" + uuid.uuid4().hex[:12]
volume = prefix + "-data"
containers = []
with socket.socket() as listener:
    listener.bind(("127.0.0.1", 0))
    port = listener.getsockname()[1]
base = f"http://127.0.0.1:{port}"
env = {
    "DOMAIN": "example.test", "CONTROL_HOST": "runners.example.test", "PUBLIC_URL": base,
    "PORT": str(port), "BIND_HOST": "127.0.0.1", "AUTO_TUNNEL": "0", "DNS": "off",
    "RECOVER_TUNNELS": "0", "RECOVER_RUNTIME": "0", "ADMIN_PASSWORD": "fixture-admin",
    "JOIN_TOKEN": "fixture-join", "STATE_SIGNING_KEY": "fixture-signing-key", "QUIET_MS": "0",
}

def docker(*args):
    return subprocess.check_output(["docker", *args], text=True).strip()

def request(path, method="GET", body=None):
    req = urllib.request.Request(base + path, method=method,
        headers={"Content-Type": "application/json", "x-admin-password": "fixture-admin"},
        data=None if body is None else json.dumps(body).encode())
    with urllib.request.urlopen(req, timeout=8) as response:
        return json.load(response)

def start(name):
    args = ["run", "-d", "--name", name, "--network", "host", "-v", volume + ":/data"]
    for key, value in env.items():
        args.extend(["-e", key + "=" + value])
    docker(*args, image)
    containers.append(name)
    deadline = time.monotonic() + 25
    while time.monotonic() < deadline:
        try:
            if request("/readyz")["ok"]:
                return
        except (OSError, urllib.error.URLError):
            pass
        time.sleep(0.2)
    raise AssertionError("native controller did not become ready")

try:
    assert "cloudflared version" in docker("run", "--rm", "--entrypoint", "/usr/local/bin/cloudflared", image, "--version")
    docker("volume", "create", "--label", "io.runners.test=rust", volume)
    args = ["run", "--rm", "-v", volume + ":/data"]
    for key, value in env.items():
        args.extend(["-e", key + "=" + value])
    docker(*args, image, "init")
    start(prefix + "-first")
    assert request("/api/status")["controlPlane"]["runtime"] == "rust"
    project = request("/api/projects/example", "PUT", {"compose": "services:\n  app:\n    image: nginx\n", "env": {"PRIVATE": "test-private-value"}, "password": "test-app-password"})
    assert project["version"] == 1 and project["hasPassword"]
    request("/api/sync", "POST", {"run": "synthetic", "agent": "synthetic", "machine": 1, "started": int(time.time() * 1000) - 3600000, "protocol": 5, "ready": True, "tag": "test", "status": {}})
    assert request("/api/status")["runs"]
    with urllib.request.urlopen(base + "/install.sh") as response:
        installer = response.read().decode()
        assert "@ORIGIN@" not in installer and "/rust/bundle.tar" in installer and "fixture-join" not in installer
    manifest = request("/agent/rust/manifest.json")
    assert set(manifest["targets"]) == {"x86_64-unknown-linux-musl", "aarch64-unknown-linux-musl"}
    usage = request("/api/usage")
    assert 0 < usage["memoryBytes"] < 64 * 1024 * 1024, usage
    docker("stop", "-t", "12", prefix + "-first")
    start(prefix + "-second")
    restored = request("/api/projects/example")
    assert restored["version"] == 1 and restored["hasPassword"]
    assert request("/api/projects/example/env")["keys"] == ["PRIVATE"]
    assert not request("/api/status")["runs"], "runtime must not survive on disk"
    docker("exec", prefix + "-second", "/usr/local/bin/runner-control", "healthcheck")
    print(json.dumps({"checks": ["scratch image runs without a language runtime", "non-root durable configuration", "authenticated APIs", "native installer and artifacts", "runtime remains memory-only", "native health check"], "idleRssBytes": usage["memoryBytes"], "imageBytes": int(docker("image", "inspect", "--format", "{{.Size}}", image))}))
finally:
    for name in containers:
        subprocess.run(["docker", "rm", "-f", name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run(["docker", "volume", "rm", volume], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
