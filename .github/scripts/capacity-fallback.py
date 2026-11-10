"""Provider watchdog fallback, independent of the controller's scheduler.

Read counts from repository variables and dispatch the existing serialized
watchdog when a provider is below its target. Credentials are read from private
operator-selected sources and retained only in process memory.
"""
import argparse
import datetime
import json
import pathlib
import re
import time
import urllib.error
import urllib.request

ACTIVE = {"requested", "pending", "waiting", "queued", "in_progress"}
TOKEN = re.compile(r"(?:gh[pousr]_[A-Za-z0-9_]{30,255}|github_pat_[A-Za-z0-9_]{40,255})")
HTTP_REQUESTS = 0


def github(path, token, method="GET", body=None):
    global HTTP_REQUESTS
    HTTP_REQUESTS += 1
    request = urllib.request.Request("https://api.github.com" + path, method=method,
        headers={"Authorization": "Bearer " + token, "Accept": "application/vnd.github+json",
                 "Content-Type": "application/json", "User-Agent": "runner-capacity-fallback"},
        data=None if body is None else json.dumps(body).encode())
    with urllib.request.urlopen(request, timeout=20) as response:
        raw = response.read()
        return json.loads(raw) if raw else None


def recover_tokens(config):
    candidates = set()
    for filename in config["credential_sources"]:
        with pathlib.Path(filename).open(errors="replace") as source:
            for line in source:
                candidates.update(TOKEN.findall(line))
    wanted = {p["login"]: p["repo"] for p in config["providers"]}
    found = {}
    for token in candidates:
        try:
            login = github("/user", token).get("login")
            if login not in wanted or login in found:
                continue
            github("/repos/" + wanted[login] + "/actions/variables?per_page=100", token)
            found[login] = token
            if len(found) == len(wanted):
                break
        except (urllib.error.URLError, TimeoutError):
            continue
    return found


def recovering(status):
    plane = status.get("controlPlane", {})
    return (status.get("fleetMode") != "passive" or not plane.get("tunnelsReady")
            or status.get("now", 0) < plane.get("recoveryUntil", 0))


def read_status(config):
    # Follow the fleet's stable origin unless an operator explicitly supplies a
    # private endpoint. A replica-local default stops working after failover.
    url = config.get("status_url") or config["control_url"].rstrip("/") + "/api/status"
    request = urllib.request.Request(url, headers={
        "User-Agent": "runner-capacity-fallback",
        "Accept": "application/json",
    })
    with urllib.request.urlopen(request, timeout=15) as response:
        return json.load(response)


def reconcile(provider, token, status, control_url, api=github, dry=False):
    result = {"repo": provider["repo"]}
    if recovering(status):
        return {**result, "action": "controller_recovering"}
    base = "/repos/" + provider["repo"]
    variables = {x["name"]: x["value"] for x in api(base + "/actions/variables?per_page=100", token)["variables"]}
    if variables.get("CONTROL_URL", "").rstrip("/") != control_url.rstrip("/"):
        raise ValueError("provider belongs to a different control origin")
    raw = variables.get("MACHINE_COUNT") or variables.get("POOL_SIZE") or "0"
    if not re.fullmatch(r"0|[1-9]\d*", raw) or int(raw) > 50:
        raise ValueError("invalid provider target")
    target = int(raw)
    tag = variables.get("SERVER_TAG") or variables.get("POOL") or "github"
    online = next((x["online"] for x in status["groups"] if x["tag"] == tag), 0)
    result.update({"tag": tag, "target": target, "online": online})
    if online >= target:
        return {**result, "action": "satisfied"}
    recent = api(base + "/actions/workflows/watchdog.yml/runs?per_page=10", token)["workflow_runs"]
    if any(x["status"] in ACTIVE for x in recent):
        return {**result, "action": "watchdog_pending"}
    if not dry:
        branch = api(base, token)["default_branch"]
        api(base + "/actions/workflows/watchdog.yml/dispatches", token, "POST", {"ref": branch})
    return {**result, "action": "would_dispatch" if dry else "dispatched"}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--state", required=True)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    config = json.loads(pathlib.Path(args.config).read_text())
    started_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
    tokens = recover_tokens(config)
    status_requests = 0
    while True:
        result = {"at": datetime.datetime.now(datetime.timezone.utc).isoformat(), "providers": []}
        try:
            status_requests += 1
            status = read_status(config)
            for provider in config["providers"]:
                try:
                    if provider["login"] not in tokens:
                        tokens.update(recover_tokens(config))
                    token = tokens.get(provider["login"])
                    if not token:
                        raise PermissionError("provider credential unavailable")
                    result["providers"].append(reconcile(provider, token, status, config["control_url"], dry=args.dry_run))
                except Exception as error:
                    if getattr(error, "code", None) in (401, 403):
                        tokens.pop(provider["login"], None)
                    result["providers"].append({"repo": provider["repo"], "action": "error", "error": type(error).__name__, "http_status": getattr(error, "code", None)})
        except Exception as error:
            result["error"] = type(error).__name__
        result.update({"started_at": started_at, "github_requests_out": HTTP_REQUESTS, "control_checks_out": status_requests})
        path = pathlib.Path(args.state)
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(result, separators=(",", ":")) + "\n")
        temporary.replace(path)
        if args.once or any(x.get("action") in ("dispatched", "error") for x in result["providers"]) or "error" in result:
            print(json.dumps(result), flush=True)
        if args.once:
            return
        time.sleep(max(60, int(config.get("interval_seconds", 300))))


if __name__ == "__main__":
    main()
