from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Iterable


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def quote_ident(value: str) -> str:
    text = str(value or "").strip().strip('"')
    if not text:
        raise ValueError("Snowflake identifier cannot be empty")
    return '"' + text.replace('"', '""') + '"'


def layer_targets(extra_targets: Iterable[str]) -> list[tuple[str, str]]:
    targets = [
        (
            os.getenv("SNOWFLAKE_BRONZE_CATALOG") or os.getenv("SNOWFLAKE_DATABASE") or "ATHENA_DB",
            os.getenv("SNOWFLAKE_BRONZE_SCHEMA") or "BRONZE",
        ),
        (
            os.getenv("SNOWFLAKE_SILVER_CATALOG") or os.getenv("SNOWFLAKE_DATABASE") or "ATHENA_DB",
            os.getenv("SNOWFLAKE_SILVER_SCHEMA") or "SILVER",
        ),
        (
            os.getenv("SNOWFLAKE_GOLD_CATALOG")
            or os.getenv("SNOWFLAKE_SILVER_CATALOG")
            or os.getenv("SNOWFLAKE_DATABASE")
            or "ATHENA_DB",
            os.getenv("SNOWFLAKE_GOLD_SCHEMA") or "GOLD",
        ),
    ]
    for item in extra_targets:
        parts = [part.strip() for part in str(item or "").split(".")]
        if len(parts) != 2 or not all(parts):
            raise ValueError(f"Invalid --target {item!r}; expected DATABASE.SCHEMA")
        targets.append((parts[0], parts[1]))

    seen = set()
    unique = []
    for database, schema in targets:
        key = (str(database).upper(), str(schema).upper())
        if key in seen:
            continue
        seen.add(key)
        unique.append((database, schema))
    return unique


def normalize_account(value: str) -> str:
    text = str(value or "").strip()
    if "://" in text:
        text = text.split("://", 1)[1]
    if "/" in text:
        text = text.split("/", 1)[0]
    return text


def connect():
    import snowflake.connector

    required = {
        "SNOWFLAKE_USER": os.getenv("SNOWFLAKE_USER"),
        "SNOWFLAKE_PASSWORD": os.getenv("SNOWFLAKE_PASSWORD"),
        "SNOWFLAKE_ACCOUNT": os.getenv("SNOWFLAKE_ACCOUNT"),
    }
    missing = [key for key, value in required.items() if not value]
    if missing:
        raise RuntimeError(f"Missing Snowflake env vars: {', '.join(missing)}")

    kwargs = {
        "user": required["SNOWFLAKE_USER"],
        "password": required["SNOWFLAKE_PASSWORD"],
        "account": normalize_account(str(required["SNOWFLAKE_ACCOUNT"])),
    }
    optional = {
        "SNOWFLAKE_WAREHOUSE": "warehouse",
        "SNOWFLAKE_ROLE": "role",
    }
    for env_key, arg_key in optional.items():
        if os.getenv(env_key):
            kwargs[arg_key] = os.getenv(env_key)
    return snowflake.connector.connect(**kwargs)


def fetch_objects(cur, database: str, schema: str) -> list[tuple[str, str]]:
    cur.execute(f"SHOW TABLES IN SCHEMA {quote_ident(database)}.{quote_ident(schema)}")
    tables = [("TABLE", str(row[1])) for row in cur.fetchall()]
    cur.execute(f"SHOW VIEWS IN SCHEMA {quote_ident(database)}.{quote_ident(schema)}")
    views = [("VIEW", str(row[1])) for row in cur.fetchall()]
    return tables + views


def main() -> int:
    parser = argparse.ArgumentParser(description="Drop generated Athena Snowflake Bronze/Silver/Gold tables/views.")
    parser.add_argument("--execute", action="store_true", help="Actually drop objects. Omit for dry-run.")
    parser.add_argument(
        "--target",
        action="append",
        default=[],
        help="Extra DATABASE.SCHEMA to clean, repeatable. Defaults to configured Bronze/Silver/Gold schemas.",
    )
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    load_dotenv(root / ".env")

    targets = layer_targets(args.target)
    conn = connect()
    dropped = []
    try:
        cur = conn.cursor()
        for database, schema in targets:
            objects = fetch_objects(cur, database, schema)
            if not objects:
                print(f"EMPTY {database}.{schema}")
                continue
            for object_type, name in objects:
                qualified = f"{quote_ident(database)}.{quote_ident(schema)}.{quote_ident(name)}"
                sql = f"DROP {object_type} IF EXISTS {qualified}"
                if args.execute:
                    cur.execute(sql)
                    dropped.append(f"{object_type} {database}.{schema}.{name}")
                    print(f"DROPPED {object_type} {database}.{schema}.{name}")
                else:
                    print(f"DRY_RUN {sql}")
        if args.execute:
            conn.commit()
    finally:
        conn.close()

    print(f"SUMMARY mode={'EXECUTE' if args.execute else 'DRY_RUN'} objects={len(dropped)} targets={len(targets)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
