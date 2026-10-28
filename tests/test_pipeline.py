import json
import sqlite3
from contextlib import closing
import tempfile
import unittest
from pathlib import Path

from tests import _path  # noqa: F401
from etl_ingest.pipeline import Pipeline
from etl_ingest.schema import ConfigError, load_config, parse_config
from etl_ingest.state import InMemoryStateStore

ROOT = Path(__file__).resolve().parents[1]
CSV_CONFIG = ROOT / "pipelines" / "clients.json"
JSONL_CONFIG = ROOT / "pipelines" / "clients_jsonl.json"
ACME = ROOT / "sample_data" / "acme_clients.csv"
GLOBEX = ROOT / "sample_data" / "globex_clients.jsonl"


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.db = self.dir / "warehouse.db"
        self.state = InMemoryStateStore()

    def tearDown(self):
        self.tmp.cleanup()

    def pipeline(self, config=CSV_CONFIG):
        return Pipeline(load_config(config), self.state, self.db, self.dir / "rejects")

    def rows(self):
        # closing(): sqlite3's own context manager commits but doesn't close,
        # and Windows can't delete an open database file.
        with closing(sqlite3.connect(self.db)) as c:
            return {r[0]: r for r in c.execute(
                "SELECT client_id, full_name, email, phone, country, signup_date, annual_revenue, active FROM clients")}

    def test_csv_run_loads_clean_rows_and_reports_rejects(self):
        result = self.pipeline().run(ACME)
        self.assertEqual(result.status, "succeeded")
        q = result.quality
        self.assertEqual((q["total"], q["loaded"], q["rejected"], q["duplicates"]), (10, 4, 6, 1))
        rows = self.rows()
        self.assertEqual(rows["AC-001"], ("AC-001", "Wanjiru Kamau", "wanjiru.kamau@example.com",
                                          "+254712345678", "KE", "2024-03-15", "12500.00", "true"))
        self.assertEqual(rows["AC-007"][4], "GB")       # "Britain" via the config's country_aliases
        self.assertIsNone(rows["AC-007"][6])            # "N/A" revenue stored as NULL
        reasons = {(e["row"], e["field"]): e["reason"] for e in q["errors"]}
        self.assertIn("duplicate key", reasons[(7, "client_id")])
        self.assertEqual(reasons[(9, "country")], "unknown country")

    def test_rejects_file_lists_every_rejected_row(self):
        result = self.pipeline().run(ACME)
        lines = [json.loads(ln) for ln in Path(result.rejects_file).read_text(encoding="utf-8").splitlines()]
        self.assertEqual(sorted(r["row"] for r in lines), [3, 4, 5, 7, 9, 10])
        self.assertTrue(all(r["errors"] for r in lines))

    def test_two_client_formats_land_in_one_standard_table(self):
        self.pipeline().run(ACME)
        result = self.pipeline(JSONL_CONFIG).run(GLOBEX)
        self.assertEqual(result.quality["loaded"], 3)
        rows = self.rows()
        self.assertEqual(rows["GX-101"][2:6], ("ruth.achieng@example.com", "0755123987", "KE", "2024-03-18"))
        self.assertEqual(rows["GX-103"][4], "US")
        self.assertEqual(len(rows), 7)

    def test_identical_file_is_skipped_unless_forced(self):
        p = self.pipeline()
        self.assertEqual(p.run(ACME).status, "succeeded")
        self.assertEqual(p.run(ACME).status, "skipped")
        self.assertEqual(p.run(ACME, force=True).status, "succeeded")
        self.assertEqual(len(self.rows()), 4)  # upsert: re-running doesn't duplicate rows

    def test_run_is_refused_while_another_worker_holds_the_lock(self):
        token = self.state.acquire_lock("clients", 60)
        result = self.pipeline().run(ACME)
        self.assertEqual(result.status, "locked")
        self.state.release_lock("clients", token)
        self.assertEqual(self.pipeline().run(ACME).status, "succeeded")

    def test_lock_released_after_failure(self):
        bad = self.dir / "bad.csv"
        bad.write_text("unrelated,columns\n1,2\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "missing required columns"):
            self.pipeline().run(bad)
        self.assertIsNotNone(self.state.acquire_lock("clients", 1))

    def test_run_record_saved(self):
        result = self.pipeline().run(ACME, triggered_by="tester")
        saved = self.state.get_run(result.run_id)
        self.assertEqual((saved["status"], saved["triggered_by"]), ("succeeded", "tester"))

    def test_missing_source_reports_failure(self):
        self.assertEqual(self.pipeline().run(self.dir / "nope.csv").status, "failed")


class ConfigTests(unittest.TestCase):
    base = {"name": "c", "key": "id", "fields": [{"name": "id"}]}

    def test_valid(self):
        self.assertEqual(parse_config(self.base).table, "c")

    def test_rejects_unsafe_identifiers(self):
        for bad in [{"name": "c; DROP TABLE x"}, {"table": "x\"--"}]:
            with self.assertRaises(ConfigError):
                parse_config({**self.base, **bad})
        with self.assertRaises(ConfigError):
            parse_config({**self.base, "fields": [{"name": "id) --"}]})

    def test_rejects_bad_key_type_and_format(self):
        with self.assertRaises(ConfigError):
            parse_config({**self.base, "key": "missing"})
        with self.assertRaises(ConfigError):
            parse_config({**self.base, "fields": [{"name": "id", "type": "uuid"}]})
        with self.assertRaises(ConfigError):
            parse_config({**self.base, "source_format": "xml"})


if __name__ == "__main__":
    unittest.main()
