"""Make a consistent private backup of a native controller's database and identity."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import tempfile


def backup(config, destination):
    config = Path(config).resolve(strict=True)
    if config.stat().st_mode & 0o077:
        raise ValueError("configuration must be owner-only")
    env = json.loads(config.read_text())
    data = Path(env["DATA_DIR"]).resolve(strict=True)
    destination = Path(destination).resolve()
    if destination.exists():
        raise ValueError("backup destination already exists")
    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = Path(tempfile.mkdtemp(prefix=".backup-", dir=destination.parent))
    try:
        source = sqlite3.connect((data / "control.db").as_uri() + "?mode=ro", uri=True)
        target = sqlite3.connect(temporary / "control.db")
        try:
            source.backup(target)
            if target.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                raise ValueError("backup database integrity check failed")
            projects = target.execute("SELECT name,version,enabled FROM projects ORDER BY name").fetchall()
        finally:
            target.close()
            source.close()
        shutil.copyfile(config, temporary / "environment.json")
        if (data / "identity.json").exists():
            shutil.copyfile(data / "identity.json", temporary / "identity.json")
        manifest = {
            "schema": 1,
            "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "source_revision": env.get("SOURCE_REVISION"),
            "projects": [{"name": n, "version": v, "enabled": bool(e)} for n, v, e in projects],
            "files": {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in temporary.iterdir()},
        }
        (temporary / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        for file in temporary.iterdir():
            file.chmod(0o600)
        temporary.rename(destination)
        return manifest
    except Exception:
        shutil.rmtree(temporary)
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--destination", required=True)
    args = parser.parse_args()
    os.umask(0o077)
    result = backup(args.config, args.destination)
    print(json.dumps({"backup": str(Path(args.destination).resolve()),
                      "projects": len(result["projects"]), "created_at": result["created_at"]}))
