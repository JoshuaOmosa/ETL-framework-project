"""Extract (read source files) and Load (write to SQLite / reject files)."""

from __future__ import annotations

import csv
import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any, Iterator

from .schema import PipelineConfig

Row = tuple[int, dict[str, Any]]  # (1-based data row number, raw record)


def file_checksum(path: str | Path) -> str:
    """SHA-256 of the file contents; identical re-deliveries get skipped."""
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_csv(path: str | Path) -> tuple[list[str], Iterator[Row]]:
    f = open(path, newline="", encoding="utf-8-sig")  # utf-8-sig drops Excel's BOM
    reader = csv.DictReader(f)
    headers = list(reader.fieldnames or [])

    def rows() -> Iterator[Row]:
        try:
            for i, record in enumerate(reader, start=1):
                yield i, record
        finally:
            f.close()

    return headers, rows()


def read_jsonl(path: str | Path) -> tuple[list[str], Iterator[Row]]:
    records: list[Row] = []
    headers: dict[str, None] = {}
    with open(path, encoding="utf-8") as f:
        for i, line in enumerate((ln for ln in f if ln.strip()), start=1):
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                record = {"__parse_error__": "line is not valid JSON"}
            if not isinstance(record, dict):
                record = {"__parse_error__": "line is not a JSON object"}
            headers.update(dict.fromkeys(k for k in record if k != "__parse_error__"))
            records.append((i, record))
    return list(headers), iter(records)


READERS = {"csv": read_csv, "jsonl": read_jsonl}


class SQLiteSink:
    """Upserts clean records into one table keyed on the pipeline's key field."""

    def __init__(self, db_path: str | Path, config: PipelineConfig):
        self.config = config
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        # Table and column names were validated as plain identifiers in schema.py,
        # so quoting them here is safe; values always go through placeholders.
        cols = ", ".join(f'"{f}" TEXT' for f in config.field_names)
        self.conn.execute(
            f'CREATE TABLE IF NOT EXISTS "{config.table}" ({cols}, '
            f'"_loaded_at" TEXT, "_run_id" TEXT, PRIMARY KEY ("{config.key}"))'
        )

    def write(self, records: list[dict[str, Any]], run_id: str, loaded_at: str) -> int:
        if not records:
            return 0
        names = self.config.field_names + ["_loaded_at", "_run_id"]
        cols = ", ".join(f'"{n}"' for n in names)
        marks = ", ".join("?" for _ in names)
        updates = ", ".join(f'"{n}" = excluded."{n}"' for n in names if n != self.config.key)
        sql = (f'INSERT INTO "{self.config.table}" ({cols}) VALUES ({marks}) '
               f'ON CONFLICT("{self.config.key}") DO UPDATE SET {updates}')
        rows = [[_to_cell(r.get(n)) for n in self.config.field_names] + [loaded_at, run_id] for r in records]
        with self.conn:  # one transaction: all rows land or none do
            self.conn.executemany(sql, rows)
        return len(rows)

    def count(self) -> int:
        return self.conn.execute(f'SELECT COUNT(*) FROM "{self.config.table}"').fetchone()[0]

    def close(self) -> None:
        self.conn.close()


def _to_cell(value: Any) -> Any:
    if isinstance(value, bool):
        return "true" if value else "false"
    return None if value is None else str(value)


def write_rejects(path: str | Path, rejects: list[dict[str, Any]]) -> None:
    """One JSON object per rejected record, with the reasons, for client follow-up."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for item in rejects:
            f.write(json.dumps(item, ensure_ascii=False, default=str) + "\n")
