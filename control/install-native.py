"""Install a macOS LaunchAgent for an already-restored fleet.

Writes the service definition without starting it unless --start is supplied.
Configuration secrets stay in the private JSON file, outside the plist.
"""
import argparse
import os
from pathlib import Path
import plistlib
import re
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--node", required=True)
    parser.add_argument("--label", default="com.hetpatel.runners-native")
    parser.add_argument("--logs", required=True)
    parser.add_argument("--start", action="store_true")
    args = parser.parse_args()
    if sys.platform != "darwin":
        raise ValueError("this installer targets macOS launchd")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9.-]+", args.label):
        raise ValueError("invalid LaunchAgent label")
    config = Path(args.config).expanduser().resolve(strict=True)
    node = Path(args.node).expanduser().resolve(strict=True)
    launcher = Path(__file__).resolve().parent / "native-service.py"
    subprocess.run([sys.executable, str(launcher), "--config", str(config),
                    "--node", str(node), "--check"], check=True)
    logs = Path(args.logs).expanduser().resolve()
    logs.mkdir(parents=True, exist_ok=True, mode=0o700)
    destination = Path.home() / "Library/LaunchAgents" / (args.label + ".plist")
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        raise ValueError("LaunchAgent already exists; review and remove it before reinstalling")
    service = {
        "Label": args.label,
        "ProgramArguments": [sys.executable, str(launcher), "--config", str(config), "--node", str(node)],
        "WorkingDirectory": str(launcher.parent),
        "RunAtLoad": True,
        "KeepAlive": True,
        "ThrottleInterval": 10,
        "ProcessType": "Background",
        "AbandonProcessGroup": False,
        "ExitTimeOut": 20,
        "StandardOutPath": str(logs / "controller.log"),
        "StandardErrorPath": str(logs / "controller-error.log"),
    }
    descriptor = os.open(destination, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(descriptor, "wb") as output:
        plistlib.dump(service, output)
    if args.start:
        subprocess.run(["launchctl", "bootstrap", f"gui/{os.getuid()}", str(destination)], check=True)
    print(f"LaunchAgent {'started' if args.start else 'prepared, not started'}: {destination}")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(1)
