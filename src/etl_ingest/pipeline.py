"""The ETL run: extract -> transform/validate -> load, guarded by shared state."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from .io import READERS, SQLiteSink, file_checksum, write_rejects
from .quality import QualityReport, RecordError
from .schema import PipelineConfig
from .state import StateStore
from .transforms import NORMALIZERS

LOCK_TTL_SECONDS = 15 * 60


@dataclass
class RunResult:
    run_id: str
    pipeline: str
    status: str               # succeeded | skipped | locked | failed
    source: str
    checksum: str
    started_at: str
    finished_at: str
    triggered_by: Optional[str] = None
    quality: Optional[dict[str, Any]] = None
    rejects_file: Optional[str] = None
    message: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def transform_record(config: PipelineConfig, header_map: dict[str, str], row: int,
                     raw: dict[str, Any]) -> tuple[Optional[dict[str, Any]], list[RecordError]]:
    """Clean one source record. Returns (clean_record, []) or (None, errors)."""
    if "__parse_error__" in raw:
        return None, [RecordError(row=row, reason=raw["__parse_error__"])]

    mapped = {header_map[h]: v for h, v in raw.items() if h in header_map}
    options = config.options
    clean: dict[str, Any] = {}
    errors: list[RecordError] = []
    for spec in config.fields:
        value = mapped.get(spec.name)
        try:
            result = NORMALIZERS[spec.type](
                value,
                date_formats=options.get("date_formats"),
                country_aliases=options.get("country_aliases"),
            )
        except ValueError as e:
            errors.append(RecordError(row=row, field=spec.name, value=value, reason=str(e)))
            continue
        if result is None and spec.required:
            errors.append(RecordError(row=row, field=spec.name, value=value, reason="required field is empty"))
            continue
        clean[spec.name] = result
    return (None, errors) if errors else (clean, [])


class Pipeline:
    def __init__(self, config: PipelineConfig, state: StateStore, db_path: str | Path,
                 rejects_dir: str | Path = "output/rejects"):
        self.config = config
        self.state = state
        self.db_path = Path(db_path)
        self.rejects_dir = Path(rejects_dir)

    def run(self, source: str | Path, triggered_by: Optional[str] = None, force: bool = False) -> RunResult:
        source = Path(source)
        result = RunResult(
            run_id=uuid.uuid4().hex, pipeline=self.config.name, status="failed",
            source=source.name, checksum="", started_at=_now(), finished_at="",
            triggered_by=triggered_by,
        )
        try:
            result.checksum = file_checksum(source)
        except OSError as e:
            return self._finish(result, "failed", f"cannot read source: {e.strerror}")

        if not force and self.state.is_processed(self.config.name, result.checksum):
            return self._finish(result, "skipped", "identical file already processed")

        token = self.state.acquire_lock(self.config.name, LOCK_TTL_SECONDS)
        if token is None:
            return self._finish(result, "locked", "another worker is running this pipeline")
        try:
            self._process(source, result)
            self.state.mark_processed(self.config.name, result.checksum)
            return self._finish(result, "succeeded")
        except Exception as e:  # report the failure in the run record, then re-raise
            self._finish(result, "failed", f"{type(e).__name__}: {e}")
            raise
        finally:
            self.state.release_lock(self.config.name, token)

    def _process(self, source: Path, result: RunResult) -> None:
        headers, rows = READERS[self.config.source_format](source)
        header_map = self.config.header_map(headers)
        missing = [f.name for f in self.config.fields if f.required and f.name not in header_map.values()]
        if missing:
            raise ValueError(f"source is missing required columns: {', '.join(missing)}")

        report = QualityReport()
        good: dict[Any, dict[str, Any]] = {}
        rejects: list[dict[str, Any]] = []
        for row, raw in rows:
            report.total += 1
            clean, errors = transform_record(self.config, header_map, row, raw)
            if clean is not None and clean[self.config.key] in good:
                report.duplicates += 1
                errors = [RecordError(row=row, field=self.config.key, value=clean[self.config.key],
                                      reason="duplicate key in this file (first occurrence kept)")]
                clean = None
            if clean is None:
                report.reject(errors)
                rejects.append({"row": row, "record": raw, "errors": [e.to_dict() for e in errors]})
            else:
                good[clean[self.config.key]] = clean

        sink = SQLiteSink(self.db_path, self.config)
        try:
            report.loaded = sink.write(list(good.values()), result.run_id, _now())
        finally:
            sink.close()
        if rejects:
            path = self.rejects_dir / f"{self.config.name}_{result.run_id}.jsonl"
            write_rejects(path, rejects)
            result.rejects_file = str(path)
        result.quality = report.to_dict()

    def _finish(self, result: RunResult, status: str, message: Optional[str] = None) -> RunResult:
        result.status = status
        result.message = message
        result.finished_at = _now()
        self.state.save_run(result.to_dict())
        return result
