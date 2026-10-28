"""Data-quality bookkeeping: what was loaded, what was rejected, and why."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class RecordError:
    row: int                 # 1-based data row in the source file
    reason: str
    field: Optional[str] = None
    value: Any = None

    def to_dict(self) -> dict[str, Any]:
        return {"row": self.row, "field": self.field, "value": self.value, "reason": self.reason}


@dataclass
class QualityReport:
    total: int = 0
    loaded: int = 0
    rejected: int = 0
    duplicates: int = 0
    errors: list[RecordError] = field(default_factory=list)

    def reject(self, errors: list[RecordError]) -> None:
        self.rejected += 1
        self.errors.extend(errors)

    @property
    def errors_by_field(self) -> dict[str, int]:
        return dict(Counter(e.field or "(record)" for e in self.errors))

    @property
    def acceptance_rate(self) -> float:
        return round(self.loaded / self.total, 4) if self.total else 0.0

    def to_dict(self, max_errors: int = 50) -> dict[str, Any]:
        return {
            "total": self.total,
            "loaded": self.loaded,
            "rejected": self.rejected,
            "duplicates": self.duplicates,
            "acceptance_rate": self.acceptance_rate,
            "errors_by_field": self.errors_by_field,
            "errors": [e.to_dict() for e in self.errors[:max_errors]],
            "errors_truncated": len(self.errors) > max_errors,
        }
