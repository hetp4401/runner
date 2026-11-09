"""Run the existing controller and tunnel under a native service manager.

The private JSON configuration contains environment values, not shell code.
The inherited advisory lock prevents two local supervisors sharing one database.
Use a service manager that cleans up the process group when the supervisor exits.
"""
import argparse
import fcntl
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys


def configuration(filename, node):
    config = Path(filename).expanduser().resolve(strict=True)
    if stat.S_IMODE(config.stat().st_mode) & 0o077:
        raise ValueError("configuration must be owner-only; use chmod 600")
    values = json.loads(config.read_text())
    if not isinstance(values, dict) or not all(
        isinstance(k, str) and isinstance(v, str) for k, v in values.items()
    ):
        raise ValueError("configuration must be a JSON object of string environment values")
    if not values.get("DATA_DIR") or not Path(values["DATA_DIR"]).is_absolute():
        raise ValueError("DATA_DIR must be an absolute path to the restored fleet")
    data = Path(values["DATA_DIR"]).resolve(strict=True)
    if not (data / "control.db").is_file():
        raise ValueError("control.db is missing; restore the fleet before starting")
    binary = Path(node).expanduser().resolve(strict=True)
    version = subprocess.check_output([str(binary), "--version"], text=True, timeout=10).strip()
    if not version.startswith("v") or int(version[1:].split(".")[0]) < 24:
        raise ValueError("the controller requires Node.js 24 or newer")
    env = os.environ | values
    env.pop("INIT_CONTROL_DB", None)
    env["DATA_DIR"] = str(data)
    env.setdefault("BIND_HOST", "127.0.0.1")
    env.setdefault("CONTROLLER_KIND", "mac" if sys.platform == "darwin" else "native")
    env["CONTROLLER_RUNTIME"] = "native"
    env.setdefault("CONTROLLER_NAME", "Mac" if sys.platform == "darwin" else "Native host")
    env.setdefault("RUNTIME_DIR", str(data / "run"))
    env["PATH"] = str(binary.parent) + os.pathsep + values.get(
        "PATH", "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
    )
    if env.get("AUTO_TUNNEL", "1") not in ("0", "false") and not shutil.which("cloudflared", path=env["PATH"]):
        raise ValueError("cloudflared is missing from the configured PATH")
    return env, binary, version


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--node", required=True)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    os.umask(0o077)
    env, node, version = configuration(args.config, args.node)
    if args.check:
        print(json.dumps({"valid": True, "node": version, "data": env["DATA_DIR"],
                          "bind": env["BIND_HOST"], "controller": env["CONTROLLER_NAME"]}))
        return
    lock = os.open(str(Path(env["DATA_DIR"]) / "controller.lock"),
                   os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(lock)
        raise RuntimeError("another local controller owns this fleet database")
    os.fchmod(lock, 0o600)
    os.ftruncate(lock, 0)
    os.write(lock, str(os.getpid()).encode())
    os.set_inheritable(lock, True)
    if sys.platform == "darwin" and env.get("PREVENT_IDLE_SLEEP") == "1":
        # -s prevents idle system sleep only on AC power. Lid closure and an
        # explicit sleep request still take the controller offline.
        subprocess.Popen(["/usr/bin/caffeinate", "-s", "-w", str(os.getpid())],
                         stdin=subprocess.DEVNULL, close_fds=True)
    entry = Path(__file__).resolve().parent / "docker-entrypoint.mjs"
    os.execve(str(node), [str(node), str(entry), "start"], env)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(1)
