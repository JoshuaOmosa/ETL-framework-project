"""Command line: run a pipeline, issue a token, or start the API.

    python -m etl_ingest run pipelines/clients.json sample_data/acme_clients.csv
    python -m etl_ingest token --sub ops-bot --role etl:run --role etl:read
    python -m etl_ingest serve --port 8080
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Optional

from .auth import issue_token
from .pipeline import Pipeline
from .schema import ConfigError, load_config
from .settings import Settings, load_dotenv
from .state import state_from_url


def main(argv: Optional[list[str]] = None) -> int:
    load_dotenv()
    parser = argparse.ArgumentParser(prog="etl_ingest", description="Config-driven ETL ingestion framework")
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="run a pipeline on one source file")
    run.add_argument("config", help="pipeline config JSON")
    run.add_argument("source", help="CSV or JSONL file to ingest")
    run.add_argument("--force", action="store_true", help="re-process even if this exact file was seen")

    tok = sub.add_parser("token", help="issue a JWT for the API (needs ETL_JWT_SECRET)")
    tok.add_argument("--sub", required=True, help="who the token is for")
    tok.add_argument("--role", action="append", default=[], help="etl:run and/or etl:read")
    tok.add_argument("--ttl", type=int, default=3600, help="lifetime in seconds")

    srv = sub.add_parser("serve", help="start the HTTP API")
    srv.add_argument("--host", default="127.0.0.1")
    srv.add_argument("--port", type=int, default=8080)

    args = parser.parse_args(argv)
    settings = Settings.from_env()

    if args.command == "run":
        try:
            config = load_config(args.config)
        except (ConfigError, OSError) as e:
            print(f"Error: {e}", file=sys.stderr)
            return 2
        result = Pipeline(config, state_from_url(settings.state_url), settings.db_path,
                          settings.rejects_dir).run(args.source, triggered_by="cli", force=args.force)
        print(json.dumps(result.to_dict(), indent=2))
        return 0 if result.status in {"succeeded", "skipped"} else 1

    if not settings.jwt_secret:
        print("Error: set ETL_JWT_SECRET (32+ characters) first", file=sys.stderr)
        return 2

    if args.command == "token":
        print(issue_token(args.sub, args.role, settings.jwt_secret, args.ttl,
                          issuer=settings.jwt_issuer, audience=settings.jwt_audience))
        return 0

    from .api import serve
    server = serve(settings, state_from_url(settings.state_url), args.host, args.port)
    print(f"Listening on http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
