"""Pipeline configuration: which fields a client file should produce and how.

A pipeline is described in JSON so a new client feed means a new config file,
not new code. Example: ``pipelines/clients.json``.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from .transforms import NORMALIZERS, normalize_header

IDENTIFIER_RE = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")


class ConfigError(ValueError):
    pass


@dataclass
class FieldSpec:
    name: str
    type: str = "string"
    required: bool = False
    aliases: list[str] = field(default_factory=list)

    def header_names(self) -> set[str]:
        """Every normalised header that maps to this field."""
        return {normalize_header(self.name), *(normalize_header(a) for a in self.aliases)}


@dataclass
class PipelineConfig:
    name: str
    key: str
    table: str
    fields: list[FieldSpec]
    source_format: str = "csv"
    options: dict[str, Any] = field(default_factory=dict)

    @property
    def field_names(self) -> list[str]:
        return [f.name for f in self.fields]

    def header_map(self, headers: list[str]) -> dict[str, str]:
        """Map source headers to field names, e.g. {'E-mail': 'email'}."""
        lookup: dict[str, str] = {}
        for spec in self.fields:
            for h in spec.header_names():
                lookup[h] = spec.name
        return {h: lookup[normalize_header(h)] for h in headers if normalize_header(h) in lookup}


def _identifier(value: Any, what: str) -> str:
    if not isinstance(value, str) or not IDENTIFIER_RE.match(value):
        raise ConfigError(f"{what} must be lower_snake_case letters, digits or '_': {value!r}")
    return value


def parse_config(data: dict[str, Any]) -> PipelineConfig:
    if not isinstance(data, dict):
        raise ConfigError("pipeline config must be a JSON object")
    name = _identifier(data.get("name"), "name")
    table = _identifier(data.get("table", name), "table")
    raw_fields = data.get("fields")
    if not isinstance(raw_fields, list) or not raw_fields:
        raise ConfigError("fields must be a non-empty list")

    fields: list[FieldSpec] = []
    for raw in raw_fields:
        spec = FieldSpec(
            name=_identifier(raw.get("name"), "field name"),
            type=raw.get("type", "string"),
            required=bool(raw.get("required", False)),
            aliases=[str(a) for a in raw.get("aliases", [])],
        )
        if spec.type not in NORMALIZERS:
            raise ConfigError(f"field {spec.name!r}: unknown type {spec.type!r} (choose from {sorted(NORMALIZERS)})")
        fields.append(spec)

    names = [f.name for f in fields]
    if len(names) != len(set(names)):
        raise ConfigError("field names must be unique")
    key = data.get("key")
    if key not in names:
        raise ConfigError(f"key {key!r} must be one of the fields")
    source_format = data.get("source_format", "csv")
    if source_format not in {"csv", "jsonl"}:
        raise ConfigError("source_format must be 'csv' or 'jsonl'")

    return PipelineConfig(
        name=name, key=key, table=table, fields=fields,
        source_format=source_format, options=dict(data.get("options", {})),
    )


def load_config(path: str | Path) -> PipelineConfig:
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise ConfigError(f"{path}: invalid JSON ({e})") from None
    return parse_config(data)


def find_config(pipelines_dir: str | Path, name: str) -> Optional[PipelineConfig]:
    """Load ``<pipelines_dir>/<name>.json`` if the name is a safe identifier."""
    if not IDENTIFIER_RE.match(name):
        return None
    path = Path(pipelines_dir) / f"{name}.json"
    return load_config(path) if path.is_file() else None
