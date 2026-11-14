#!/usr/bin/env python3
"""Package verified native agents and unchanged legacy bundles; build time only."""
import argparse
import base64
import hashlib
import io
import json
import re
import shutil
import tarfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

def digest(data):
    return hashlib.sha256(data).hexdigest()

def encode(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()

def archive(path, files):
    with tarfile.open(path, "w", format=tarfile.USTAR_FORMAT) as tar:
        for name, source in sorted(files.items()):
            data = source if isinstance(source, bytes) else source.read_bytes()
            info = tarfile.TarInfo(name)
            info.size = len(data)
            info.mode = 0o755 if name.startswith("bin/") else 0o644
            info.mtime = 0
            tar.addfile(info, io.BytesIO(data))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--binaries", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    out = args.output
    (out / "rust").mkdir(parents=True, exist_ok=True)
    sources = {p.name: p.read_text() for p in sorted((ROOT / "agent").glob("*.mjs"))}
    assert all(re.fullmatch(r"[a-z][a-z0-9-]*\.mjs", n) for n in sources)
    hashes = {name: digest(text.encode()) for name, text in sources.items()}
    revision = digest(encode(sorted(hashes.items())))
    legacy = {"Dockerfile": ROOT / "control/agent.Dockerfile", "control/agent-launcher.mjs": ROOT / "control/agent-launcher.mjs"}
    legacy.update({"agent/" + name: text.encode() for name, text in sources.items()})
    archive(out / "bundle.tar", legacy)
    archive_hash = digest((out / "bundle.tar").read_bytes())
    (out / "bundle.json").write_bytes(encode({"revision": revision, "hashes": hashes, "files": sources}))
    (out / "manifest.json").write_bytes(encode({"revision": revision, "hashes": hashes, "archiveHash": archive_hash}))
    (out / "bundle.sha256").write_text(archive_hash + "\n")
    for name, text in sources.items():
        if name == "agent.mjs":
            embedded = "data:text/javascript;base64," + base64.b64encode(sources["runtime.mjs"].encode()).decode()
            text, count = re.subn(r'''\bfrom\s+(["'])\./runtime\.mjs\1''', lambda _: "from " + json.dumps(embedded), text)
            assert count == 1
        (out / name).write_text(text)
    manifest = {"schema": 1, "runtime": "rust", "targets": {}}
    native = {"Dockerfile": ROOT / "rust/agent.Dockerfile"}
    for arch, target in [("amd64", "x86_64-unknown-linux-musl"), ("arm64", "aarch64-unknown-linux-musl")]:
        source = args.binaries / arch / "runner-agent"
        data = source.read_bytes()
        assert data[:4] == b"\x7fELF" and 0 < len(data) <= 32 * 1024 * 1024, "a native Linux agent binary is required"
        assert data[4:6] == b"\x02\x01" and int.from_bytes(data[18:20], "little") == (62 if arch == "amd64" else 183), "agent architecture does not match its manifest target"
        name = "runner-agent-linux-" + arch
        shutil.copyfile(source, out / "rust" / name)
        manifest["targets"][target] = {"sha256": digest(data), "size": len(data), "path": "/agent/rust/" + name}
        native["bin/" + name] = source
    (out / "rust/manifest.json").write_bytes(encode(manifest))
    archive(out / "rust/bundle.tar", native)
    (out / "rust/bundle.sha256").write_text(digest((out / "rust/bundle.tar").read_bytes()) + "\n")

if __name__ == "__main__":
    main()
