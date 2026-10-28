import json
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path

from tests import _path  # noqa: F401
from etl_ingest.api import safe_inbox_path, serve
from etl_ingest.auth import issue_token
from etl_ingest.settings import Settings
from etl_ingest.state import InMemoryStateStore

ROOT = Path(__file__).resolve().parents[1]
SECRET = "k" * 48


class ApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        out = Path(cls.tmp.name)
        cls.settings = Settings(
            state_url="memory://", db_path=out / "w.db", rejects_dir=out / "rejects",
            pipelines_dir=ROOT / "pipelines", inbox_dir=ROOT / "sample_data",
            jwt_secret=SECRET, jwt_issuer=None, jwt_audience=None,
        )
        cls.server = serve(cls.settings, InMemoryStateStore(), port=0)
        cls.base = f"http://127.0.0.1:{cls.server.server_address[1]}"
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.tmp.cleanup()

    def call(self, method, path, token=None, body=None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.base + path, data=data, method=method)
        if token:
            req.add_header("Authorization", f"Bearer {token}")
        if data:
            req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                return r.status, json.loads(r.read())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read())

    def token(self, *roles):
        return issue_token("tester", roles, SECRET)

    def test_health_needs_no_token(self):
        self.assertEqual(self.call("GET", "/health"), (200, {"status": "ok"}))

    def test_run_requires_token_and_role(self):
        body = {"source": "acme_clients.csv"}
        self.assertEqual(self.call("POST", "/pipelines/clients/runs", body=body)[0], 401)
        self.assertEqual(self.call("POST", "/pipelines/clients/runs", "not.a.jwt", body)[0], 401)
        self.assertEqual(self.call("POST", "/pipelines/clients/runs", self.token("etl:read"), body)[0], 403)

    def test_run_then_fetch_it(self):
        status, run = self.call("POST", "/pipelines/clients/runs", self.token("etl:run"),
                                {"source": "acme_clients.csv", "force": True})
        self.assertEqual(status, 201)
        self.assertEqual((run["status"], run["triggered_by"], run["quality"]["loaded"]), ("succeeded", "tester", 4))
        status, fetched = self.call("GET", f"/runs/{run['run_id']}", self.token("etl:read"))
        self.assertEqual((status, fetched["run_id"]), (200, run["run_id"]))
        self.assertEqual(self.call("GET", f"/runs/{run['run_id']}", self.token("etl:run"))[0], 403)

    def test_source_outside_inbox_refused(self):
        for src in ["../pipelines/clients.json", "../../etc/passwd", "C:\\Windows\\win.ini", "", 123]:
            status, _ = self.call("POST", "/pipelines/clients/runs", self.token("etl:run"), {"source": src})
            self.assertEqual(status, 400, src)

    def test_unknown_pipeline_and_bad_paths(self):
        self.assertEqual(self.call("POST", "/pipelines/nope/runs", self.token("etl:run"),
                                   {"source": "acme_clients.csv"})[0], 404)
        self.assertEqual(self.call("POST", "/pipelines/../runs", self.token("etl:run"), {"source": "x"})[0], 404)
        self.assertEqual(self.call("GET", "/runs/" + "0" * 32, self.token("etl:read"))[0], 404)


class SafeInboxPathTests(unittest.TestCase):
    def test_only_files_inside_inbox(self):
        inbox = ROOT / "sample_data"
        self.assertIsNotNone(safe_inbox_path(inbox, "acme_clients.csv"))
        for bad in ["../README.md", "missing.csv", "", None, "a\x00b"]:
            self.assertIsNone(safe_inbox_path(inbox, bad), repr(bad))


if __name__ == "__main__":
    unittest.main()
