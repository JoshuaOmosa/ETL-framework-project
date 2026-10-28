"""Runtime settings, read from environment variables (see .env.example)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


def load_dotenv(path: str | Path = ".env") -> None:
    """Minimal .env reader: KEY=VALUE lines; real environment variables win."""
    p = Path(path)
    if not p.is_file():
        return
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = (s.strip() for s in line.split("=", 1))
        os.environ.setdefault(key, value.strip("\"'"))


@dataclass
class Settings:
    state_url: str
    db_path: Path
    rejects_dir: Path
    pipelines_dir: Path
    inbox_dir: Path
    jwt_secret: Optional[str]
    jwt_issuer: Optional[str]
    jwt_audience: Optional[str]

    @classmethod
    def from_env(cls) -> "Settings":
        e = os.environ.get
        return cls(
            state_url=e("ETL_STATE_URL", "memory://"),
            db_path=Path(e("ETL_DB_PATH", "output/warehouse.db")),
            rejects_dir=Path(e("ETL_REJECTS_DIR", "output/rejects")),
            pipelines_dir=Path(e("ETL_PIPELINES_DIR", "pipelines")),
            inbox_dir=Path(e("ETL_INBOX_DIR", "sample_data")),
            jwt_secret=e("ETL_JWT_SECRET") or None,
            jwt_issuer=e("ETL_JWT_ISSUER") or None,
            jwt_audience=e("ETL_JWT_AUDIENCE") or None,
        )
