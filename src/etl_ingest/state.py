"""Shared pipeline state: run locks, processed-file ledger, and run history.

Several workers may pick up the same client feed. Redis keeps them in step:

* a **lock** per pipeline (``SET NX PX``) so only one worker runs it at a time,
  released with a compare-and-delete script so a worker can never release a
  lock that has expired and been taken by someone else;
* a **processed set** of file checksums so re-delivered files are skipped;
* **run records** so any worker or the API can report on any run.

``InMemoryStateStore`` implements the same interface for tests and single-
process use.
"""

from __future__ import annotations

import json
import secrets
import threading
import time
from abc import ABC, abstractmethod
from typing import Any, Optional

RUN_TTL_SECONDS = 30 * 24 * 3600  # keep run records for 30 days


class StateStore(ABC):
    @abstractmethod
    def acquire_lock(self, name: str, ttl_seconds: int) -> Optional[str]:
        """Return a token if the lock was acquired, else None."""

    @abstractmethod
    def release_lock(self, name: str, token: str) -> bool:
        """Release only if we still own it. Returns True if released."""

    @abstractmethod
    def is_processed(self, pipeline: str, checksum: str) -> bool: ...

    @abstractmethod
    def mark_processed(self, pipeline: str, checksum: str) -> None: ...

    @abstractmethod
    def save_run(self, run: dict[str, Any]) -> None: ...

    @abstractmethod
    def get_run(self, run_id: str) -> Optional[dict[str, Any]]: ...


class InMemoryStateStore(StateStore):
    def __init__(self) -> None:
        self._mutex = threading.Lock()
        self._locks: dict[str, tuple[str, float]] = {}
        self._processed: dict[str, set[str]] = {}
        self._runs: dict[str, dict[str, Any]] = {}

    def acquire_lock(self, name: str, ttl_seconds: int) -> Optional[str]:
        with self._mutex:
            current = self._locks.get(name)
            if current and current[1] > time.monotonic():
                return None
            token = secrets.token_hex(16)
            self._locks[name] = (token, time.monotonic() + ttl_seconds)
            return token

    def release_lock(self, name: str, token: str) -> bool:
        with self._mutex:
            current = self._locks.get(name)
            if current and current[0] == token:
                del self._locks[name]
                return True
            return False

    def is_processed(self, pipeline: str, checksum: str) -> bool:
        with self._mutex:
            return checksum in self._processed.get(pipeline, set())

    def mark_processed(self, pipeline: str, checksum: str) -> None:
        with self._mutex:
            self._processed.setdefault(pipeline, set()).add(checksum)

    def save_run(self, run: dict[str, Any]) -> None:
        with self._mutex:
            self._runs[run["run_id"]] = json.loads(json.dumps(run))  # store a copy

    def get_run(self, run_id: str) -> Optional[dict[str, Any]]:
        with self._mutex:
            run = self._runs.get(run_id)
            return json.loads(json.dumps(run)) if run else None


# Delete the key only if it still holds our token (atomic on the Redis server).
_RELEASE_SCRIPT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
end
return 0
"""


class RedisStateStore(StateStore):
    def __init__(self, client: Any, prefix: str = "etl") -> None:
        self.r = client
        self.prefix = prefix
        self._release = client.register_script(_RELEASE_SCRIPT)

    @classmethod
    def from_url(cls, url: str, prefix: str = "etl") -> "RedisStateStore":
        import redis  # optional dependency: only needed when Redis is used

        return cls(redis.Redis.from_url(url, decode_responses=True), prefix)

    def _k(self, *parts: str) -> str:
        return ":".join((self.prefix, *parts))

    def acquire_lock(self, name: str, ttl_seconds: int) -> Optional[str]:
        token = secrets.token_hex(16)
        ok = self.r.set(self._k("lock", name), token, nx=True, px=int(ttl_seconds * 1000))
        return token if ok else None

    def release_lock(self, name: str, token: str) -> bool:
        return bool(self._release(keys=[self._k("lock", name)], args=[token]))

    def is_processed(self, pipeline: str, checksum: str) -> bool:
        return bool(self.r.sismember(self._k("processed", pipeline), checksum))

    def mark_processed(self, pipeline: str, checksum: str) -> None:
        self.r.sadd(self._k("processed", pipeline), checksum)

    def save_run(self, run: dict[str, Any]) -> None:
        self.r.set(self._k("run", run["run_id"]), json.dumps(run, default=str), ex=RUN_TTL_SECONDS)

    def get_run(self, run_id: str) -> Optional[dict[str, Any]]:
        raw = self.r.get(self._k("run", run_id))
        return json.loads(raw) if raw else None


def state_from_url(url: str) -> StateStore:
    """``memory://`` for local runs, ``redis://host:6379/0`` for shared state."""
    if url.startswith("memory://"):
        return InMemoryStateStore()
    if url.startswith(("redis://", "rediss://", "unix://")):
        return RedisStateStore.from_url(url)
    raise ValueError(f"unsupported state store URL: {url!r}")
