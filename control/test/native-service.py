"""Exercise the native supervisor through launchd with disposable, offline state."""
import argparse
import http.server
import json
import os
from pathlib import Path
import re
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--node", required=True)
    args = parser.parse_args()
    node = str(Path(args.node).resolve(strict=True))
    control = Path(__file__).resolve().parents[1]
    label = "com.hetpatel.runners-native-test-" + uuid.uuid4().hex
    service = f"gui/{os.getuid()}/{label}"
    plist = Path.home() / "Library/LaunchAgents" / (label + ".plist")

    class Mock(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            body = json.dumps({"success": True, "result": [],
                               "result_info": {"total_pages": 1}}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *unused):
            pass

    mock = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Mock)
    thread = threading.Thread(target=mock.serve_forever, daemon=True)
    thread.start()
    with tempfile.TemporaryDirectory(prefix="runner-native-test-") as tmp:
        root = Path(tmp)
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        origin = f"http://127.0.0.1:{port}"
        env = {"DOMAIN": "native-test.example", "DATA_DIR": str(root / "data"),
               "PORT": str(port), "AUTO_TUNNEL": "0", "DNS": "off",
               "CF_API_BASE": f"http://127.0.0.1:{mock.server_port}",
               "CF_API_TOKEN": "test-token", "ACCOUNT_ID": "test-account", "ZONE": "test-zone",
               "JOIN_TOKEN": "test-join", "ADMIN_PASSWORD": "test-admin",
               "STATE_SIGNING_KEY": "test-signing", "RECOVERY_MS": "100",
               "CONTROLLER_KIND": "mac", "CONTROLLER_NAME": "Native test Mac"}
        config = root / "environment.json"
        config.write_text(json.dumps(env))
        config.chmod(0o600)
        launcher = [sys.executable, str(control / "native-service.py"), "--config", str(config), "--node", node]
        result = subprocess.run(launcher, capture_output=True, text=True)
        assert result.returncode and not (root / "data/control.db").exists(), "missing state was initialized"
        initialize = "const m=await import(process.argv[1]);m.initialize(m.settings(JSON.parse(process.argv[2])));"
        subprocess.run([node, "--input-type=module", "-e", initialize,
                        (control / "src/bootstrap.mjs").as_uri(), json.dumps(env)], check=True)
        config.chmod(0o644)
        result = subprocess.run(launcher + ["--check"], capture_output=True, text=True)
        assert result.returncode and "owner-only" in result.stderr
        config.chmod(0o600)
        print("ok missing state and readable credentials are refused", flush=True)

        def status():
            with urllib.request.urlopen(origin + "/api/status", timeout=1) as response:
                return json.load(response)

        def pid():
            result = subprocess.run(["launchctl", "print", service], capture_output=True, text=True)
            match = re.search(r"^\s+pid = (\d+)$", result.stdout, re.M)
            return int(match[1]) if match else None

        def wait_for(predicate, seconds=25):
            deadline = time.monotonic() + seconds
            while time.monotonic() < deadline:
                try:
                    value = predicate()
                    if value:
                        return value
                except (OSError, ValueError, urllib.error.URLError):
                    pass
                time.sleep(0.1)
            logs = [p.read_text() for p in (root / "logs").glob("*.log")]
            state = subprocess.run(["launchctl", "print", service], capture_output=True, text=True)
            raise AssertionError("service did not become ready\n" + "\n".join(logs) + state.stdout + state.stderr)

        try:
            subprocess.run([sys.executable, str(control / "install-native.py"), "--config", str(config),
                            "--node", node, "--logs", str(root / "logs"), "--label", label, "--start"],
                           cwd="/", check=True, capture_output=True, text=True)
            current = wait_for(status)
            assert current["controlPlane"]["owner"] == "mac"
            assert current["controlPlane"]["name"] == "Native test Mac"
            assert current["controlPlane"]["mode"] == "native-v1"
            result = subprocess.run(launcher, capture_output=True, text=True, timeout=10, cwd="/")
            assert result.returncode and "another local controller" in result.stderr
            print("ok launchd starts the Mac controller and duplicate ownership is refused", flush=True)
            before = pid()
            assert before
            os.kill(before, signal.SIGKILL)
            after = wait_for(lambda: (p if (p := pid()) and p != before else None))
            wait_for(status)
            assert after != before
            print("ok launchd cleans up a crashed supervisor and restarts a healthy controller", flush=True)
            # Keep a WAL connection open so copying only control.db would miss
            # this committed fixture. The backup must use SQLite's online API.
            writer = sqlite3.connect(root / "data/control.db")
            writer.execute("INSERT OR REPLACE INTO settings(key,value) VALUES('rebalance','false')")
            writer.commit()
            try:
                subprocess.run([sys.executable, str(control / "backup-native.py"), "--config", str(config),
                                "--destination", str(root / "backup")], check=True, capture_output=True, text=True)
                snapshot = sqlite3.connect(root / "backup/control.db")
                try:
                    assert snapshot.execute("SELECT value FROM settings WHERE key='rebalance'").fetchone()[0] == "false"
                finally:
                    snapshot.close()
                assert (root / "backup/identity.json").read_bytes() == (root / "data/identity.json").read_bytes()
                assert all(p.stat().st_mode & 0o077 == 0 for p in (root / "backup").iterdir())
            finally:
                writer.close()
            print("ok online backup includes committed WAL changes, identity, and private permissions", flush=True)
        finally:
            subprocess.run(["launchctl", "bootout", service], capture_output=True)
            plist.unlink(missing_ok=True)
            mock.shutdown()
            mock.server_close()
        print("Native service checks passed", flush=True)


if __name__ == "__main__":
    main()
