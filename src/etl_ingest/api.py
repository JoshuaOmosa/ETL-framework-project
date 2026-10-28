"""A small HTTP API for triggering and inspecting runs (standard library only).

    GET  /health                      no auth
    POST /pipelines/<name>/runs       role etl:run    body {"source": "file.csv", "force": false}
    GET  /runs/<run_id>               role etl:read

Every call except /health needs ``Authorization: Bearer <JWT>``. Sources are
looked up inside the configured inbox folder only, so a caller can't point
the pipeline at arbitrary files on the server.
"""

from __future__ import annotations

import json
import re
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Optional

from .auth import AuthError, verify_token
from .pipeline import Pipeline
from .schema import ConfigError, find_config
from .settings import Settings
from .state import StateStore

MAX_BODY_BYTES = 16 * 1024
RUN_PATH = re.compile(r"^/pipelines/([a-z_][a-z0-9_]*)/runs$")
GET_RUN_PATH = re.compile(r"^/runs/([0-9a-f]{32})$")


def safe_inbox_path(inbox: Path, name: Any) -> Optional[Path]:
    """Resolve ``name`` inside ``inbox``; None if it escapes the folder or is missing."""
    if not isinstance(name, str) or not name or "\x00" in name:
        return None
    root = inbox.resolve()
    candidate = (root / name).resolve()
    if not candidate.is_relative_to(root) or not candidate.is_file():
        return None
    return candidate


def make_handler(settings: Settings, state: StateStore) -> type[BaseHTTPRequestHandler]:
    if not settings.jwt_secret:
        raise ValueError("ETL_JWT_SECRET must be set to run the API")

    class Handler(BaseHTTPRequestHandler):
        server_version = "etl-ingest"

        def log_message(self, fmt: str, *args: Any) -> None:  # keep test output quiet
            pass

        def _send(self, status: HTTPStatus, body: dict[str, Any]) -> None:
            data = json.dumps(body, default=str).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(data)

        def _claims(self, role: str) -> Optional[dict[str, Any]]:
            header = self.headers.get("Authorization", "")
            if not header.startswith("Bearer "):
                self._send(HTTPStatus.UNAUTHORIZED, {"error": "missing bearer token"})
                return None
            try:
                return verify_token(header[7:], settings.jwt_secret, required_role=role,
                                    issuer=settings.jwt_issuer, audience=settings.jwt_audience)
            except AuthError as e:
                status = HTTPStatus.FORBIDDEN if str(e).startswith("missing role") else HTTPStatus.UNAUTHORIZED
                self._send(status, {"error": str(e)})
                return None

        def do_GET(self) -> None:
            if self.path == "/health":
                return self._send(HTTPStatus.OK, {"status": "ok"})
            m = GET_RUN_PATH.match(self.path)
            if not m:
                return self._send(HTTPStatus.NOT_FOUND, {"error": "not found"})
            if self._claims("etl:read") is None:
                return
            run = state.get_run(m.group(1))
            if run is None:
                return self._send(HTTPStatus.NOT_FOUND, {"error": "run not found"})
            self._send(HTTPStatus.OK, run)

        def do_POST(self) -> None:
            m = RUN_PATH.match(self.path)
            if not m:
                return self._send(HTTPStatus.NOT_FOUND, {"error": "not found"})
            claims = self._claims("etl:run")
            if claims is None:
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                length = -1
            if not 0 < length <= MAX_BODY_BYTES:
                return self._send(HTTPStatus.BAD_REQUEST, {"error": "JSON body required (max 16 KB)"})
            try:
                body = json.loads(self.rfile.read(length))
                assert isinstance(body, dict)
            except (ValueError, AssertionError):
                return self._send(HTTPStatus.BAD_REQUEST, {"error": "body must be a JSON object"})

            try:
                config = find_config(settings.pipelines_dir, m.group(1))
            except ConfigError as e:
                return self._send(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": f"pipeline config invalid: {e}"})
            if config is None:
                return self._send(HTTPStatus.NOT_FOUND, {"error": "unknown pipeline"})
            source = safe_inbox_path(settings.inbox_dir, body.get("source"))
            if source is None:
                return self._send(HTTPStatus.BAD_REQUEST, {"error": "source must be a file in the inbox"})

            pipeline = Pipeline(config, state, settings.db_path, settings.rejects_dir)
            try:
                result = pipeline.run(source, triggered_by=claims.get("sub"), force=bool(body.get("force")))
            except Exception as e:
                return self._send(HTTPStatus.UNPROCESSABLE_ENTITY, {"error": str(e)})
            status = {"succeeded": HTTPStatus.CREATED, "skipped": HTTPStatus.OK,
                      "locked": HTTPStatus.CONFLICT}.get(result.status, HTTPStatus.INTERNAL_SERVER_ERROR)
            self._send(status, result.to_dict())

    return Handler


def serve(settings: Settings, state: StateStore, host: str = "127.0.0.1", port: int = 8080) -> ThreadingHTTPServer:
    return ThreadingHTTPServer((host, port), make_handler(settings, state))
