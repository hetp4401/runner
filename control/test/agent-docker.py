#!/usr/bin/env python3
"""Isolated real-Docker agent deployment test (Linux).

Usage: python3 control/test/agent-docker.py runner-agent:test
Requires a locally built agent image containing this checkout's agent code.
Creates only UUID-named containers, projects, image and one temporary directory.
A localhost fake CP and owned infrastructure stubs avoid Cloudflare entirely.
"""
import hashlib
import http.server
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import uuid

IMAGE = sys.argv[1] if len(sys.argv) > 1 else "runner-agent:local"
if sys.platform != "linux":
    raise SystemExit("Run this Docker integration test on Linux")
TAG = uuid.uuid4().hex[:12]
ROOT = Path(tempfile.mkdtemp(prefix=f"runner-managed-{TAG}-"))
DATA = ROOT / "data"
APP = f"review-{TAG}"
APP_IMAGE = f"runner-managed-review:{TAG}"
LOCK = threading.Lock()
DESIRED = {}
REPORT = {}
CONTAINERS = []
PROJECTS = []


def run(*args, check=True, timeout=120):
    result = subprocess.run(["docker", *args], capture_output=True, text=True, timeout=timeout)
    if check and result.returncode:
        raise RuntimeError(f"Docker {args[0]} failed: {result.stderr[-1800:]}")
    return result


def wait_for(check, description, timeout=120):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if check():
            return
        time.sleep(0.25)
    raise AssertionError(f"timed out: {description}")


class API(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_GET(self):
        if self.path == "/metrics":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"cloudflared_tunnel_ha_connections 4\n")
        else:
            self.send_response(404)  # permits the agent's HTTP sync fallback
            self.end_headers()

    def do_POST(self):
        global REPORT
        body = json.loads(self.rfile.read(int(self.headers.get("content-length", "0"))))
        if self.path != "/api/sync":
            self.send_response(500)
            self.end_headers()
            return
        with LOCK:
            REPORT = body
            answer = json.dumps({"domain": "example.test", "desired": DESIRED, "poll": 0.2,
                "recovery": {"payload": json.dumps({"run": body["run"]}), "signature": "test-signature"}}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(answer)))
        self.end_headers()
        self.wfile.write(answer)


API_SERVER = http.server.ThreadingHTTPServer(("127.0.0.1", 0), API)
API_SERVER.daemon_threads = True
threading.Thread(target=API_SERVER.serve_forever, daemon=True).start()
CONTROL = f"http://127.0.0.1:{API_SERVER.server_port}"
OWNER = "runner-" + hashlib.sha256(CONTROL.encode()).hexdigest()[:12]
PROJECT = f"{OWNER}-{APP}"
AGENT = f"{OWNER}-agent-test"

OBSERVED = DATA / "projects" / APP / "data" / "observed.json"
KEEP = OBSERVED.parent / "keep.txt"
OUTSIDE = ROOT / "outside"


def observed():
    try:
        return json.loads(OBSERVED.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def healthy():
    with LOCK:
        return REPORT.get("status", {}).get(APP, {}).get("s") == "healthy"


def app_ids():
    return run("ps", "-aq", "--filter", f"label=com.docker.compose.project={PROJECT}", "--filter", f"label=io.runners.review={TAG}").stdout.split()


def plan(message, secret="fake-secret-one", version=1, attack=False):
    original = {"services": {"worker": {"image": APP_IMAGE, "build": ".", "pull_policy": "build",
        "environment": ["APP_SECRET=${APP_SECRET}"], "volumes": ["./data:/state"], "restart": "unless-stopped", "labels": {"io.runners.review": TAG}}}, "networks": {"default": {"labels": {"io.runners.review": TAG}}}}
    managed = json.loads(json.dumps(original))
    managed["services"]["worker"]["volumes"] = ["${RUNNER_APP_DATA}/data:/state"]
    worker = """import fs from 'node:fs';
const observed={message:fs.readFileSync('/app/nested/message.txt','utf8'),secret:process.env.APP_SECRET,uid:process.getuid()};
fs.writeFileSync('/state/observed.json',JSON.stringify(observed));
fs.appendFileSync('/state/starts.txt','started\\n');
setInterval(()=>{},1000);
"""
    files = {"Dockerfile": f"FROM {IMAGE}\nLABEL io.runners.review={TAG}\nWORKDIR /app\nCOPY worker.mjs ./worker.mjs\nCOPY nested ./nested\nUSER 65532:65532\nENTRYPOINT [\"node\"]\nCMD [\"/app/worker.mjs\"]\n",
        "worker.mjs": worker, "nested/message.txt": message, ".env": f"APP_SECRET={secret}\n"}
    data_files = {}
    if attack:
        files["data/link/victim.txt"] = "escaped"
        data_files["data/link/victim.txt"] = "escaped"
    return {"v": version, "app": APP, "replica": 1, "port": None, "compose": json.dumps(original),
        "managedCompose": json.dumps(managed), "files": files, "dataFiles": data_files,
        "bindSources": [{"path": "data", "readOnly": False}], "services": ["worker"], "oneshotServices": [],
        "revision": hashlib.sha256(json.dumps(files, sort_keys=True).encode()).hexdigest()}


def set_plan(value):
    global DESIRED
    with LOCK:
        DESIRED = {APP: value}


try:
    run("image", "inspect", IMAGE)
    for project in [APP, PROJECT]:
        assert not run("ps", "-aq", "--filter", f"label=com.docker.compose.project={project}").stdout.strip(), "test project unexpectedly exists"
    assert run("image", "inspect", APP_IMAGE, check=False).returncode != 0, "test image unexpectedly exists"
    PROJECTS.extend([APP, PROJECT])
    DATA.mkdir(mode=0o700)
    (DATA / "router").mkdir()
    OBSERVED.parent.mkdir(parents=True)
    OBSERVED.parent.chmod(0o777)  # disposable fixture: application runs as UID65532
    KEEP.write_text(f"persistent-{TAG}")
    OUTSIDE.mkdir()
    (OUTSIDE / "victim.txt").write_text("outside-must-not-change")
    (DATA / "agent-state.json").write_text(json.dumps({"v": 1, "control": CONTROL,
        "bootId": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
        "started": int(time.time() * 1000), "machine": 1, "agent": f"host-{TAG}", "run": f"run-{TAG}",
        "tunnelToken": "fake-unused-token", "domain": "example.test", "desired": {}, "projects": [], "metrics": []}))
    (DATA / "agent-state.json").chmod(0o600)

    fake_cp = ROOT / "fake-cp"
    fake_cp.mkdir()
    cp_compose = fake_cp / "compose.json"
    cp_compose.write_text(json.dumps({"services": {"control": {"image": IMAGE,
        "entrypoint": ["node"], "command": ["-e", "setInterval(()=>{},1000)"], "labels": {"io.runners.review": TAG}}},
        "networks": {"default": {"labels": {"io.runners.review": TAG}}}}))
    run("compose", "-p", APP, "-f", str(cp_compose), "up", "-d")
    cp_id = run("compose", "-p", APP, "-f", str(cp_compose), "ps", "-q", "control").stdout.strip()
    assert cp_id

    for component in ("router", "tunnel"):
        name = f"{OWNER}-{component}"
        CONTAINERS.append(name)
        run("run", "-d", "--name", name, "--label", f"io.runners.owner={OWNER}",
            "--label", f"io.runners.component={component}", "--label", f"io.runners.review={TAG}", "--entrypoint", "node", IMAGE,
            "-e", "setInterval(()=>{},1000)")
    set_plan(plan("source-one"))
    CONTAINERS.append(AGENT)
    run("run", "-d", "--name", AGENT, "--label", f"io.runners.review={TAG}", "--user", "0:0", "--network", "host",
        "-v", "/var/run/docker.sock:/var/run/docker.sock", "-v", f"{DATA}:{DATA}",
        "-e", f"CONTROL_URL={CONTROL}", "-e", "JOIN_TOKEN=fake-join", "-e", f"RUNNER_DATA={DATA}",
        "-e", f"ROUTER_PORT={API_SERVER.server_port - 1}", "--entrypoint", "node", IMAGE, "/agent/agent.mjs")
    wait_for(lambda: observed().get("message") == "source-one" and healthy(), "first managed deployment")
    assert observed()["secret"] == "fake-secret-one" and observed()["uid"] == 65532
    assert json.loads(run("inspect", cp_id).stdout)[0]["State"]["Running"]
    assert KEEP.read_text() == f"persistent-{TAG}"
    print("ok real Compose uses private build source and .env as non-root without replacing the colocated CP", flush=True)

    first_ids = app_ids()
    assert len(first_ids) == 1
    set_plan(plan("source-two", "fake-secret-two"))  # same numeric version, changed source and secret
    wait_for(lambda: observed().get("message") == "source-two" and observed().get("secret") == "fake-secret-two" and healthy(), "same-version source/secret update")
    assert app_ids() != first_ids
    assert KEEP.read_text() == f"persistent-{TAG}"
    print("ok same-version content update replaces the app and preserves its stable bind data", flush=True)

    unchanged_ids = app_ids()
    set_plan(plan("source-two", "fake-secret-two", version=2))
    def version_acknowledged():
        with LOCK:
            return REPORT.get("status", {}).get(APP, {}).get("v") == 2 and healthy_unlocked()
    def healthy_unlocked():
        return REPORT.get("status", {}).get(APP, {}).get("s") == "healthy"
    wait_for(version_acknowledged, "metadata-only version acknowledgement")
    assert app_ids() == unchanged_ids
    print("ok version-only acknowledgement preserves the running container", flush=True)

    starts_before = len((OBSERVED.parent / "starts.txt").read_text().splitlines())
    run("stop", "-t", "2", *unchanged_ids)
    wait_for(lambda: len((OBSERVED.parent / "starts.txt").read_text().splitlines()) > starts_before and healthy(), "background worker repair")
    assert KEEP.read_text() == f"persistent-{TAG}"
    print("ok a stopped background worker is repaired through real Docker while bind data survives", flush=True)

    os.symlink(OUTSIDE, OBSERVED.parent / "link")
    set_plan(plan("source-two", "fake-secret-two", version=2, attack=True))
    def failed_safely():
        with LOCK:
            return REPORT.get("status", {}).get(APP, {}).get("s") == "failed"
    wait_for(failed_safely, "symlink rejection")
    assert (OUTSIDE / "victim.txt").read_text() == "outside-must-not-change"
    assert KEEP.read_text() == f"persistent-{TAG}"
    assert json.loads(run("inspect", cp_id).stdout)[0]["State"]["Running"]
    print("ok an app-writable symlink cannot overwrite the outside fixture", flush=True)
    print("5 real-Docker agent deployment checks passed", flush=True)
except Exception:
    logs = run("logs", "--tail", "35", AGENT, check=False)
    print(logs.stdout[-6000:] + logs.stderr[-6000:], file=sys.stderr)
    raise
finally:
    # Every target is a UUID-derived test resource, never a production name.
    for name in reversed(CONTAINERS):
        label = run("inspect", "--format", '{{ index .Config.Labels "io.runners.review" }}', name, check=False).stdout.strip()
        if label == TAG:
            run("rm", "-fv", name, check=False, timeout=30)
    for project in PROJECTS:
        ids = run("ps", "-aq", "--filter", f"label=com.docker.compose.project={project}", "--filter", f"label=io.runners.review={TAG}", check=False).stdout.split()
        if ids:
            run("rm", "-fv", *ids, check=False, timeout=30)
        networks = run("network", "ls", "-q", "--filter", f"label=com.docker.compose.project={project}", "--filter", f"label=io.runners.review={TAG}", check=False).stdout.split()
        if networks:
            run("network", "rm", *networks, check=False, timeout=30)
    image_label = run("image", "inspect", "--format", '{{ index .Config.Labels "io.runners.review" }}', APP_IMAGE, check=False).stdout.strip()
    if image_label == TAG:
        run("image", "rm", "-f", APP_IMAGE, check=False, timeout=30)
    API_SERVER.shutdown()
    API_SERVER.server_close()
    shutil.rmtree(ROOT, ignore_errors=True)
