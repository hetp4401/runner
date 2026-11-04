import importlib.util
import pathlib
import unittest

spec = importlib.util.spec_from_file_location("capacity_fallback", pathlib.Path(__file__).with_name("capacity-fallback.py"))
fallback = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fallback)


class FallbackTests(unittest.TestCase):
    def setUp(self):
        self.calls = []
        self.pending = False
        self.status = {"fleetMode": "passive", "now": 100, "controlPlane": {"tunnelsReady": True, "recoveryUntil": 50}, "groups": [{"tag": "test", "online": 4}]}
        self.provider = {"repo": "owner/repo", "login": "owner"}

    def api(self, path, token, method="GET", body=None):
        self.calls.append((path, method, body))
        if "variables" in path:
            return {"variables": [{"name": k, "value": v} for k, v in {"CONTROL_URL": "https://control.example", "MACHINE_COUNT": "6", "SERVER_TAG": "test"}.items()]}
        if "/runs?" in path:
            return {"workflow_runs": [{"status": "queued" if self.pending else "completed"}]}
        return {"default_branch": "main"}

    def run_check(self, dry=False):
        return fallback.reconcile(self.provider, "private", self.status, "https://control.example", self.api, dry)

    def test_deficit_dispatches_existing_watchdog_once(self):
        self.assertEqual(self.run_check()["action"], "dispatched")
        self.assertEqual([x for x in self.calls if x[1] == "POST"], [("/repos/owner/repo/actions/workflows/watchdog.yml/dispatches", "POST", {"ref": "main"})])

    def test_satisfied_provider_and_pending_watchdog_do_not_dispatch(self):
        self.status["groups"][0]["online"] = 6
        self.assertEqual(self.run_check()["action"], "satisfied")
        self.status["groups"][0]["online"] = 4
        self.pending = True
        self.assertEqual(self.run_check()["action"], "watchdog_pending")
        self.assertFalse(any(x[1] == "POST" for x in self.calls))

    def test_controller_recovery_and_dry_run_do_not_dispatch(self):
        self.status["controlPlane"]["recoveryUntil"] = 101
        self.assertEqual(self.run_check()["action"], "controller_recovering")
        self.assertEqual(self.calls, [])
        self.status["controlPlane"]["recoveryUntil"] = 0
        self.assertEqual(self.run_check(True)["action"], "would_dispatch")
        self.assertFalse(any(x[1] == "POST" for x in self.calls))


unittest.main()
