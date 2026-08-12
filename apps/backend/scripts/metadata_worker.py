"""Execute one target-resident metadata queue item."""

from __future__ import annotations

import argparse
import json
import os
import socket
import sys
import uuid
from pathlib import Path


BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from services.metadata_repository import metadata_repository_for_target
from services.metadata_runtime_worker import process_next_metadata_work
from utilis.env import load_backend_env


def main() -> int:
    load_backend_env()
    parser = argparse.ArgumentParser(description="Process one metadata ingestion queue item.")
    parser.add_argument("--platform", required=True, choices=("databricks", "snowflake"))
    parser.add_argument("--environment", required=True)
    parser.add_argument(
        "--worker-id",
        default=f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4()}",
        help="Unique lease owner ID; the default is unique per worker process.",
    )
    parser.add_argument("--lease-seconds", type=int, default=300)
    args = parser.parse_args()
    repository = metadata_repository_for_target(platform=args.platform, environment=args.environment)
    result = process_next_metadata_work(
        repository,
        worker_id=args.worker_id,
        lease_seconds=args.lease_seconds,
    )
    print(json.dumps({"status": "IDLE"} if result is None else result, default=str, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
