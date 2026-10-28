"""The same contract tests run against every state store.

InMemoryStateStore always runs. RedisStateStore runs when REDIS_URL is set
(CI starts a Redis service container for this).
"""

import os
import time
import unittest
import uuid

from tests import _path  # noqa: F401
from etl_ingest.state import InMemoryStateStore, RedisStateStore, state_from_url


class StateContract:
    def make(self):
        raise NotImplementedError

    def setUp(self):
        self.store = self.make()
        self.name = f"test_{uuid.uuid4().hex[:8]}"

    def test_lock_is_exclusive(self):
        token = self.store.acquire_lock(self.name, 30)
        self.assertIsNotNone(token)
        self.assertIsNone(self.store.acquire_lock(self.name, 30))
        self.assertTrue(self.store.release_lock(self.name, token))
        self.assertIsNotNone(self.store.acquire_lock(self.name, 30))

    def test_cannot_release_someone_elses_lock(self):
        token = self.store.acquire_lock(self.name, 30)
        self.assertFalse(self.store.release_lock(self.name, "not-my-token"))
        self.assertIsNone(self.store.acquire_lock(self.name, 30))
        self.store.release_lock(self.name, token)

    def test_lock_expires(self):
        self.assertIsNotNone(self.store.acquire_lock(self.name, 0.2))
        time.sleep(0.35)
        self.assertIsNotNone(self.store.acquire_lock(self.name, 30))

    def test_processed_ledger_is_per_pipeline(self):
        self.assertFalse(self.store.is_processed(self.name, "abc"))
        self.store.mark_processed(self.name, "abc")
        self.assertTrue(self.store.is_processed(self.name, "abc"))
        self.assertFalse(self.store.is_processed(self.name + "_other", "abc"))

    def test_runs_round_trip(self):
        run_id = uuid.uuid4().hex
        self.store.save_run({"run_id": run_id, "status": "succeeded", "quality": {"loaded": 3}})
        self.assertEqual(self.store.get_run(run_id)["quality"], {"loaded": 3})
        self.assertIsNone(self.store.get_run(uuid.uuid4().hex))


class InMemoryStateTests(StateContract, unittest.TestCase):
    def make(self):
        return InMemoryStateStore()


@unittest.skipUnless(os.environ.get("REDIS_URL"), "set REDIS_URL to run Redis tests")
class RedisStateTests(StateContract, unittest.TestCase):
    def make(self):
        return RedisStateStore.from_url(os.environ["REDIS_URL"], prefix=f"etltest{uuid.uuid4().hex[:6]}")


class StateFromUrlTests(unittest.TestCase):
    def test_memory_url(self):
        self.assertIsInstance(state_from_url("memory://"), InMemoryStateStore)

    def test_unknown_url(self):
        with self.assertRaises(ValueError):
            state_from_url("postgres://x")


if __name__ == "__main__":
    unittest.main()
