"""Administrator entry point for target-resident metadata bootstrap/onboarding."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict


BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from services.metadata_repository import metadata_repository, metadata_repository_for_target
from services.source_connection_validation import validate_deployment_database_connection


def _json_object(path: str) -> Dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain one JSON object.")
    return payload


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Bootstrap and onboard target metadata.")
    parser.add_argument("--platform", required=True, choices=("databricks", "snowflake"))
    parser.add_argument("--environment", required=True)
    parser.add_argument("--namespace", help="Validated catalog/database override for administrator bootstrap")
    subcommands = parser.add_subparsers(dest="command", required=True)
    subcommands.add_parser("bootstrap")
    subcommands.add_parser("preflight")
    onboard = subcommands.add_parser("onboard-database")
    onboard.add_argument("--source-system-json", required=True)
    onboard.add_argument("--connection-json", required=True)
    return parser


def main() -> int:
    args = _parser().parse_args()
    repository = (
        metadata_repository(
            platform=args.platform,
            environment=args.environment,
            namespace=args.namespace,
            schema="metadata",
        )
        if args.namespace
        else metadata_repository_for_target(platform=args.platform, environment=args.environment)
    )
    if args.command == "bootstrap":
        repository.bootstrap()
    elif args.command == "preflight":
        repository.preflight()
    else:
        source = repository.upsert_source_system(_json_object(args.source_system_json))
        connection_payload = _json_object(args.connection_json)
        connection_payload["source_system_id"] = int(source["source_system_id"])
        connection = repository.upsert_connection_draft(connection_payload)
        connection = repository.validate_and_activate_connection(
            int(connection["connection_id"]),
            int(connection["config_version"]),
            lambda row: validate_deployment_database_connection(row, target_platform=args.platform),
        )
        print(
            json.dumps(
                {
                    "source_system_id": source["source_system_id"],
                    "connection_id": connection["connection_id"],
                    "config_version": connection["config_version"],
                    "active_flag": connection["active_flag"],
                },
                sort_keys=True,
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
