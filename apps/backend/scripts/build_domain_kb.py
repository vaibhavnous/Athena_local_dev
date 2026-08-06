from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from utilis.domain_kb import build_and_upsert_client_db_kb, get_domain_kb_config


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build the domain knowledge base from the configured client/source DB and upsert it to Pinecone."
    )
    parser.add_argument("--database", help="Source/client database name. Defaults to AZURE_SQL_SOURCE_DATABASE.")
    parser.add_argument("--schema", help="Source/client schema name. Defaults to AZURE_SQL_SOURCE_SCHEMA.")
    parser.add_argument("--knowledge-base-id", choices=["PC_Insurance_V1", "BASEL_DW_V1"])
    parser.add_argument("--domain-profile", choices=["Insurance", "Basel"])
    parser.add_argument("--index", help="Pinecone index override. Defaults to the selected KB index.")
    parser.add_argument("--namespace", help="Pinecone namespace override. Defaults to the selected KB ID.")
    parser.add_argument("--no-refresh", action="store_true", help="Do not delete previous KB rows before upsert.")
    args = parser.parse_args()

    cfg = get_domain_kb_config(
        knowledge_base_id=args.knowledge_base_id,
        domain_profile=args.domain_profile,
    )
    result = build_and_upsert_client_db_kb(
        database_name=args.database,
        schema_name=args.schema,
        knowledge_base_id=cfg.knowledge_base_id,
        domain_profile=cfg.domain_profile,
        index_name=args.index,
        namespace=args.namespace,
        refresh=not args.no_refresh,
    )
    result.update(
        {
            "feature_flag_enabled_for_runtime": cfg.enabled,
            "domain_profile": cfg.domain_profile,
        }
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
