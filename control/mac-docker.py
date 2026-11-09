"""Start the Docker controller on a Mac and export consistent private backups.

The JSON service configuration contains paths, never fleet credentials. Use a
dedicated Colima profile; controller data lives on its Linux Docker volume.
"""
import argparse
import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import plistlib
import re
import shutil
import signal
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
import threading
import uuid


def configuration(filename):
    path = Path(filename).expanduser().resolve(strict=True)
    if path.stat().st_mode & 0o077:
        raise ValueError("service configuration must be owner-only (chmod 600)")
    config = json.loads(path.read_text())
    for key in ("source", "environment_file", "logs", "backups"):
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


def container(config, env):
    ids = command(compose(config) + ["ps", "--all", "--quiet", "control"], env).stdout.split()
    if len(ids) != 1:
        raise RuntimeError("expected exactly one control container")
    return ids[0]


def export_snapshot(identity, remote, destination, env):
    # docker cp cannot read files on some tmpfs mounts. Stream from the running
    # container's mount namespace, accepting only the three expected plain files.
    archive = destination / ".snapshot.tar"
    try:
        with archive.open("xb") as output:
            result = subprocess.run(["docker", "exec", identity, "tar", "-C", remote,
                                     "-cf", "-", "control.db", "identity.json", "manifest.json"],
                                    env=env, stdout=output, stderr=subprocess.PIPE, timeout=180)
        if result.returncode:
            raise RuntimeError("snapshot export failed")
        with tarfile.open(archive) as source:
            members = source.getmembers()
            if (len(members) != 3 or {m.name for m in members} != {"control.db", "identity.json", "manifest.json"}
                    or not all(m.isfile() for m in members)):
                raise ValueError("unexpected backup archive contents")
            for member in members:
                path = destination / member.name
                with path.open("xb") as output, source.extractfile(member) as content:
                    shutil.copyfileobj(content, output)
                path.chmod(0o600)
    finally:
        archive.unlink(missing_ok=True)


def backup(config, env, destination=None):
    root = Path(config["backups"])
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    destination = Path(destination) if destination else root / ("docker-" + stamp)
    if destination.exists():
        raise ValueError("backup destination already exists")
    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = Path(tempfile.mkdtemp(prefix=".backup-", dir=destination.parent))
    remote = "/tmp/backup-" + uuid.uuid4().hex
    identity = container(config, env)
    try:
        command(["docker", "exec", identity, "node", "backup.mjs", remote], env)
        export_snapshot(identity, remote, temporary, env)
        manifest = json.loads((temporary / "manifest.json").read_text())
        for name, digest in manifest["files"].items():
            if name not in ("control.db", "identity.json") or hashlib.sha256((temporary / name).read_bytes()).hexdigest() != digest:
                raise ValueError("backup checksum mismatch")
        with sqlite3.connect((temporary / "control.db").as_uri() + "?mode=ro", uri=True) as database:
            if database.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                raise ValueError("exported database integrity check failed")
        for source, name in ((config["environment_file"], "docker.env"), (config["filename"], "service.json")):
            shutil.copyfile(source, temporary / name)
            manifest["files"][name] = hashlib.sha256((temporary / name).read_bytes()).hexdigest()
        (temporary / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        for file in temporary.iterdir():
            file.chmod(0o600)
        temporary.rename(destination)
        print(json.dumps({"backup": str(destination), "projects": len(manifest["projects"])}), flush=True)
    finally:
        command(["docker", "exec", identity, "rm", "-rf", remote], env, check=False)
        if temporary.exists():
            shutil.rmtree(temporary)


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
    definitions = []
    for operation, suffix in (("run", ""), ("backup", ".backup")):
        name = label + suffix
        destination = launch / (name + ".plist")
        if destination.exists():
            raise ValueError("LaunchAgent already exists: " + str(destination))
        service = {"Label": name, "ProgramArguments": [sys.executable, str(Path(__file__).resolve()),
                   operation, "--config", config["filename"]], "WorkingDirectory": config["source"],
                   "StandardOutPath": str(logs / (name + ".log")),
                   "StandardErrorPath": str(logs / (name + "-error.log")), "ProcessType": "Background"}
        if operation == "run":
            service.update(RunAtLoad=True, KeepAlive=True, ThrottleInterval=15,
                           AbandonProcessGroup=False, ExitTimeOut=45)
        else:
            service["StartCalendarInterval"] = {"Hour": 4, "Minute": 15}
        definitions.append((destination, service))
    for destination, service in definitions:
        with os.fdopen(os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "wb") as output:
            plistlib.dump(service, output)
        print("Prepared (not started): " + str(destination))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("check", "install", "run", "backup"))
    parser.add_argument("--config", required=True)
    parser.add_argument("--label", default="io.runners.docker")
    parser.add_argument("--destination")
    args = parser.parse_args()
    os.umask(0o077)
    config, env = configuration(args.config)
    if args.operation == "check":
        command(compose(config) + ["config", "--quiet"], env)
        print("Private configuration and Compose definition are valid")
    elif args.operation == "install":
        install(config, args.label)
    elif args.operation == "backup":
        backup(config, env, args.destination)
    else:
        serve(config, env)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(1)
