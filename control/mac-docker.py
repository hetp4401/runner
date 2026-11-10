"""Start and supervise the Docker controller on a Mac.

The JSON service configuration contains paths, never fleet credentials. Use a
dedicated Colima profile; controller data lives on its Linux Docker volume.
"""
import argparse
import fcntl
import json
import os
from pathlib import Path
import plistlib
import re
import shutil
import signal
import subprocess
import sys
import threading


def configuration(filename):
    path = Path(filename).expanduser().resolve(strict=True)
    if path.stat().st_mode & 0o077:
        raise ValueError("service configuration must be owner-only (chmod 600)")
    config = json.loads(path.read_text())
    for key in ("source", "environment_file", "logs"):
        if not Path(config[key]).is_absolute():
            raise ValueError(key + " must be an absolute path")
    envfile = Path(config["environment_file"])
    if envfile.stat().st_mode & 0o077:
        raise ValueError("environment_file must be owner-only (chmod 600)")
    if not (Path(config["source"]) / "compose.yaml").is_file():
        raise ValueError("source must contain compose.yaml")
    for key in ("profile", "project"):
        if not re.fullmatch(r"[a-z][a-z0-9-]{0,40}", config[key]):
            raise ValueError("invalid " + key)
    # Explicit context and project prevent accidentally operating another engine.
    env = os.environ.copy()
    for key in list(env):
        if key.startswith(("DOCKER_", "COMPOSE_")):
            env.pop(key)
    env["PATH"] = "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
    env["DOCKER_CONTEXT"] = "colima-" + config["profile"]
    env["CONTROL_ENV_FILE"] = config["environment_file"]
    env["COMPOSE_DISABLE_ENV_FILE"] = "1"
    for binary in ("docker", "colima"):
        if not shutil.which(binary, path=env["PATH"]):
            raise ValueError(binary + " is missing")
    config["filename"] = str(path)
    return config, env


def command(args, env, timeout=180, check=True):
    result = subprocess.run(args, env=env, capture_output=True, text=True, timeout=timeout)
    if check and result.returncode:
        # Compose may mention expanded environment values in an error. Keep
        # unattended logs private and do not echo command output or credentials.
        raise RuntimeError(args[0] + " failed with exit " + str(result.returncode))
    return result


def compose(config):
    return ["docker", "compose", "--project-name", config["project"],
            "--project-directory", config["source"], "--file",
            str(Path(config["source"]) / "compose.yaml")]


def serve(config, env):
    logs = Path(config["logs"])
    logs.mkdir(parents=True, exist_ok=True, mode=0o700)
    lock = (logs / "mac-docker.lock").open("a")
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    stopping = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stopping.set())
    caffeine = subprocess.Popen(["/usr/bin/caffeinate", "-s", "-w", str(os.getpid())])
    started = False
    try:
        while not stopping.is_set():
            try:
                if command(["docker", "info"], env, timeout=20, check=False).returncode:
                    command(["colima", "start", config["profile"], "--vm-type", "vz", "--mount-type", "virtiofs",
                             "--cpu", "2", "--memory", "2", "--disk", "10", "--root-disk", "8", "--activate=false"], env, timeout=300)
                if stopping.is_set():
                    break
                # Idempotent; never initializes data, builds, pulls or removes volumes.
                command(compose(config) + ["up", "-d", "--no-build", "--pull", "never", "control"], env)
                if not started:
                    print("Docker controller started; monitoring dedicated Colima engine", flush=True)
                    started = True
            except (RuntimeError, subprocess.TimeoutExpired) as error:
                print(type(error).__name__ + ": Docker controller unavailable; retrying", flush=True)
            stopping.wait(30)
    finally:
        try:
            command(compose(config) + ["stop", "--timeout", "15", "control"], env, timeout=25, check=False)
        finally:
            caffeine.terminate()
            lock.close()


def install(config, label):
    if sys.platform != "darwin" or not re.fullmatch(r"[A-Za-z0-9.-]+", label):
        raise ValueError("install requires macOS and a valid launchd label")
    logs = Path(config["logs"])
    logs.mkdir(parents=True, exist_ok=True, mode=0o700)
    launch = Path.home() / "Library/LaunchAgents"
    launch.mkdir(parents=True, exist_ok=True)
    destination = launch / (label + ".plist")
    if destination.exists():
        raise ValueError("LaunchAgent already exists: " + str(destination))
    service = {"Label": label, "ProgramArguments": [sys.executable, str(Path(__file__).resolve()),
               "run", "--config", config["filename"]], "WorkingDirectory": config["source"],
               "StandardOutPath": str(logs / (label + ".log")),
               "StandardErrorPath": str(logs / (label + "-error.log")), "ProcessType": "Background",
               "RunAtLoad": True, "KeepAlive": True, "ThrottleInterval": 15,
               "AbandonProcessGroup": False, "ExitTimeOut": 45}
    with os.fdopen(os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "wb") as output:
        plistlib.dump(service, output)
    print("Prepared (not started): " + str(destination))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("check", "install", "run"))
    parser.add_argument("--config", required=True)
    parser.add_argument("--label", default="io.runners.docker")
    args = parser.parse_args()
    os.umask(0o077)
    config, env = configuration(args.config)
    if args.operation == "check":
        command(compose(config) + ["config", "--quiet"], env)
        print("Private configuration and Compose definition are valid")
    elif args.operation == "install":
        install(config, args.label)
    else:
        serve(config, env)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(1)
