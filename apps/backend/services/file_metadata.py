from __future__ import annotations

import json
import os
from typing import Any, Dict, Iterable, List, Mapping

from services.metadata_contracts import (
    bronze_target_data_type,
    canonical_json_hash,
    normalize_bronze_column_name,
    stable_bigint,
)
from services.metadata_selection import validated_metadata_selection


FILE_STAGES = (
    ("SOURCE_TO_BRONZE", "SOURCE", "BRONZE"),
    ("BRONZE_TO_SILVER", "BRONZE", "SILVER"),
    ("SILVER_TO_GOLD", "SILVER", "GOLD"),
)


def _target_names(state: Mapping[str, Any], entity: str) -> Dict[str, str]:
    platform = str(state.get("target_warehouse") or "databricks").lower()
    if platform == "snowflake":
        catalog = str(os.getenv("SNOWFLAKE_BRONZE_CATALOG") or "ATHENA_DB")
        return {
            "bronze": f"{catalog}.{os.getenv('SNOWFLAKE_BRONZE_SCHEMA', 'BRONZE')}.bronze_{entity}",
            "silver": f"{catalog}.{os.getenv('SNOWFLAKE_SILVER_SCHEMA', 'SILVER')}.silver_{entity}",
            "gold": f"{catalog}.{os.getenv('SNOWFLAKE_GOLD_SCHEMA', 'GOLD')}.gold_{entity}",
        }
    catalog = str(os.getenv("BRONZE_CATALOG") or "main")
    return {
        "bronze": f"{catalog}.{os.getenv('BRONZE_SCHEMA', 'bronze')}.bronze_{entity}",
        "silver": f"{os.getenv('SILVER_CATALOG', catalog)}.{os.getenv('SILVER_SCHEMA', 'silver')}.silver_{entity}",
        "gold": f"{os.getenv('GOLD_CATALOG', catalog)}.{os.getenv('GOLD_SCHEMA', 'gold')}.gold_{entity}",
    }


def _object_row(state: Mapping[str, Any], table: Mapping[str, Any]) -> Dict[str, Any]:
    source_system_id = int(state["source_system_id"])
    connection_id = int(state["source_connection_id"])
    source_path = str(table.get("source_path") or table.get("landing_path") or "").strip()
    entity = str(table.get("table_name") or table.get("entity") or "").strip().lower()
    if not source_path or not entity:
        raise ValueError("Approved ADLS files require a canonical source path and entity name.")
    targets = _target_names(state, entity)
    object_id = int(stable_bigint("file_ingestion_object", source_system_id, connection_id, source_path))
    parser_options = table.get("parser_options_json") or json.dumps(
        table.get("parser_options") or {}, sort_keys=True
    )
    executable = {
        "source_system_id": source_system_id,
        "connection_id": connection_id,
        "source_path": source_path,
        "payload_format": str(table.get("file_format") or table.get("format") or "").upper(),
        "parser_options_json": parser_options,
        "target_bronze_table": targets["bronze"],
        "target_silver_table": targets["silver"],
        "target_gold_table": targets["gold"],
    }
    return {
        "ingestion_object_id": object_id,
        "source_system_id": source_system_id,
        "connection_id": connection_id,
        "object_kind": "INGESTION",
        "ingestion_type": "FILE",
        "processing_stage": "SOURCE_TO_BRONZE",
        "source_layer": "SOURCE",
        "target_layer": "BRONZE",
        "object_name": source_path,
        "object_type": "FILE",
        "source_resource_type": "FILE",
        "payload_format": executable["payload_format"],
        "container_format": "ADLS_GEN2",
        "source_path": source_path,
        "file_pattern": str(table.get("file_name") or source_path.rsplit("/", 1)[-1]),
        "database_schema": str(table.get("schema_name") or "source"),
        "table_name": entity,
        "parser_options_json": parser_options,
        "normalization_options_json": json.dumps({"flatten_nested": True}, sort_keys=True),
        "schema_inference_policy": "INFER_AND_REVIEW",
        "schema_evolution_policy": "FAIL",
        "load_type": "FULL",
        # The immutable ETag/size cache contract is enforced before queue execution;
        # target work is therefore a stateless FULL load.
        "checkpoint_type": None,
        "target_bronze_table": targets["bronze"],
        "target_silver_table": targets["silver"],
        "target_gold_table": targets["gold"],
        "target_table": targets["bronze"],
        "write_mode": "APPEND",
        "validation_policy_json": json.dumps(
            {"fail_on_schema_mismatch": True, "fail_on_empty_source": True}, sort_keys=True
        ),
        "config_hash": canonical_json_hash(executable),
        "config_version": 1,
        "is_current": True,
        "active_flag": True,
    }


def _mapping_rows(
    state: Mapping[str, Any], table: Mapping[str, Any], ingestion_object: Mapping[str, Any]
) -> List[Dict[str, Any]]:
    columns = [item for item in table.get("columns") or [] if isinstance(item, Mapping)]
    if not columns:
        raise ValueError(f"Approved ADLS file has no inferred columns: {table.get('source_path')}")
    object_id = int(ingestion_object["ingestion_object_id"])
    targets = {
        "SOURCE_TO_BRONZE": str(ingestion_object["target_bronze_table"]),
        "BRONZE_TO_SILVER": str(ingestion_object["target_silver_table"]),
        "SILVER_TO_GOLD": str(ingestion_object["target_gold_table"]),
    }
    source_objects = {
        "SOURCE_TO_BRONZE": str(ingestion_object["source_path"]),
        "BRONZE_TO_SILVER": targets["SOURCE_TO_BRONZE"],
        "SILVER_TO_GOLD": targets["BRONZE_TO_SILVER"],
    }
    rows: List[Dict[str, Any]] = []
    for stage, source_layer, target_layer in FILE_STAGES:
        normalized_columns = []
        for ordinal, column in enumerate(columns, start=1):
            source_name = str(column.get("column_name") or column.get("source_field_path") or "").strip()
            if not source_name:
                raise ValueError("Inferred file columns require a non-empty name.")
            target_name = normalize_bronze_column_name(source_name)
            target_type = bronze_target_data_type(str(state.get("target_warehouse") or "databricks"), column)
            normalized_columns.append(
                {
                    "source_field_path": source_name,
                    "source_data_type": str(column.get("data_type_full") or column.get("data_type") or "string"),
                    "target_column_name": target_name,
                    "target_data_type": target_type,
                    "is_nullable": bool(column.get("is_nullable", True)),
                    "is_array": False,
                    "is_primary_key": bool(column.get("is_primary_key") or column.get("is_join_key")),
                    "ordinal_position": int(column.get("ordinal_position") or ordinal),
                }
            )
        normalized_columns.sort(
            key=lambda item: (item["ordinal_position"], item["source_field_path"].casefold())
        )
        input_objects = json.dumps(
            [
                {
                    "ingestion_object_id": object_id,
                    "config_version": int(ingestion_object["config_version"]),
                    "config_hash": str(ingestion_object["config_hash"]),
                }
            ],
            sort_keys=True,
            separators=(",", ":"),
        )
        mapping_group = (
            f"{stage}:{object_id}:{int(ingestion_object['config_version'])}"
        )
        transformation_language = (
            "SQL"
            if str(state.get("target_warehouse") or "").lower() == "snowflake"
            else "PYSPARK_EXPR"
        )
        if stage == "SOURCE_TO_BRONZE":
            mapping_hash = canonical_json_hash(
                {
                    "ingestion_object_id": object_id,
                    "object_config_version": int(ingestion_object["config_version"]),
                    "object_config_hash": str(ingestion_object["config_hash"]),
                    "processing_stage": stage,
                    "source_object": source_objects[stage],
                    "target_table": targets[stage],
                    "contract": {
                        "source_layer": source_layer,
                        "target_layer": target_layer,
                        "mapping_group": mapping_group,
                        "input_objects_json": input_objects,
                        "transformation_rule": "CAST",
                        "transformation_language": transformation_language,
                    },
                    "columns": normalized_columns,
                }
            )
            mapping_version = (
                int(mapping_hash.removeprefix("sha256:")[:8], 16)
                & ((1 << 31) - 1)
            ) or 1
        else:
            mapping_hash = canonical_json_hash(
                {
                    "ingestion_object_id": object_id,
                    "processing_stage": stage,
                    "source_object": source_objects[stage],
                    "target_object": targets[stage],
                    "columns": normalized_columns,
                }
            )
            mapping_version = (
                int(mapping_hash.removeprefix("sha256:")[:8], 16)
                & ((1 << 31) - 1)
            ) or 1
        for column in normalized_columns:
            rows.append(
                {
                    "mapping_id": int(
                        stable_bigint(
                            "file_mapping",
                            object_id,
                            stage,
                            column["source_field_path"],
                            column["target_column_name"],
                        )
                    ),
                    "ingestion_object_id": object_id,
                    "processing_stage": stage,
                    "source_layer": source_layer,
                    "target_layer": target_layer,
                    "source_object_name": source_objects[stage],
                    "target_object_name": targets[stage],
                    "target_table": targets[stage],
                    "mapping_group": mapping_group,
                    "input_objects_json": input_objects,
                    "build_order": {"SOURCE_TO_BRONZE": 1, "BRONZE_TO_SILVER": 10, "SILVER_TO_GOLD": 20}[stage],
                    **column,
                    "transformation_rule": "CAST",
                    "transformation_language": transformation_language,
                    "mapping_hash": mapping_hash,
                    "mapping_version": mapping_version,
                    "is_current": True,
                    "active_flag": True,
                }
            )
    return rows


def _merge_rows(repository: Any, table_name: str, rows: List[Dict[str, Any]], key: str) -> None:
    for offset in range(0, len(rows), 40):
        chunk = rows[offset : offset + 40]
        names, source, parameters = repository._source_rows(chunk, prefix=f"file_{table_name}_{offset}_")
        updates = ", ".join(f"{name} = source.{name}" for name in names if name != key)
        repository.execute(
            f"MERGE INTO {repository.table(table_name)} AS target USING ({source}) AS source "
            f"ON target.{key} = source.{key} "
            f"WHEN MATCHED THEN UPDATE SET {updates}, updated_at = CURRENT_TIMESTAMP() "
            f"WHEN NOT MATCHED THEN INSERT ({', '.join(names)}, created_at, updated_at) VALUES ("
            + ", ".join("source." + name for name in names)
            + ", CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())",
            parameters,
        )


def persist_file_design(state: Mapping[str, Any], tables: Iterable[Mapping[str, Any]]) -> Dict[str, Any]:
    approved = [dict(item) for item in tables]
    if not approved:
        raise ValueError("At least one ADLS source file must be approved.")
    selection = validated_metadata_selection(state)
    if not selection:
        raise ValueError("ADLS design metadata requires a selected source system and connection.")
    object_rows = [_object_row(state, table) for table in approved]
    object_ids = [int(item["ingestion_object_id"]) for item in object_rows]
    if len(object_ids) != len(set(object_ids)):
        raise ValueError("Approved ADLS files contain duplicate canonical source identities.")
    mapping_rows = [
        mapping
        for table, ingestion_object in zip(approved, object_rows)
        for mapping in _mapping_rows(state, table, ingestion_object)
    ]
    repository = selection.repository
    with repository.unit_of_work():
        _merge_rows(repository, "cfg_ingestion_object", object_rows, "ingestion_object_id")
        parameters = {f"object_{index}": object_id for index, object_id in enumerate(object_ids)}
        repository.execute(
            f"UPDATE {repository.table('cfg_mapping')} SET active_flag = :disabled, is_current = :disabled, "
            "effective_to = CURRENT_TIMESTAMP(), updated_at = CURRENT_TIMESTAMP() "
            "WHERE ingestion_object_id IN ("
            + ", ".join(f":object_{index}" for index in range(len(object_ids)))
            + ")",
            {**parameters, "disabled": False},
        )
        _merge_rows(repository, "cfg_mapping", mapping_rows, "mapping_id")
        persisted = repository.query(
            f"SELECT ingestion_object_id, config_version, config_hash FROM {repository.table('cfg_ingestion_object')} "
            "WHERE ingestion_object_id IN ("
            + ", ".join(f":object_{index}" for index in range(len(object_ids)))
            + ")",
            parameters,
        )
    if len(persisted) != len(object_rows):
        raise RuntimeError(
            "ADLS ingestion-object persistence violated the one-source-file/one-row contract."
        )
    by_path = {str(row["source_path"]).casefold(): row for row in object_rows}
    mapping_by_object_stage: Dict[tuple[int, str], Dict[str, Any]] = {}
    for row in mapping_rows:
        mapping_by_object_stage.setdefault(
            (int(row["ingestion_object_id"]), str(row["processing_stage"])), row
        )
    certified = []
    discovered_tables = []
    for table in approved:
        ingestion_object = by_path[str(table["source_path"]).casefold()]
        source_mapping = mapping_by_object_stage[(int(ingestion_object["ingestion_object_id"]), "SOURCE_TO_BRONZE")]
        reference = {
            **table,
            "ingestion_object_id": int(ingestion_object["ingestion_object_id"]),
            "ingestion_object_config_version": int(ingestion_object["config_version"]),
            "ingestion_object_config_hash": str(ingestion_object["config_hash"]),
            "source_to_bronze_mapping_version": int(source_mapping["mapping_version"]),
            "source_to_bronze_mapping_hash": str(source_mapping["mapping_hash"]),
            "target_bronze_table": ingestion_object["target_bronze_table"],
        }
        certified.append(reference)
        discovered_tables.append(reference)
    return {
        **dict(state),
        "certified_tables": certified,
        "ingestion_objects": object_rows,
        "discovered_metadata": {
            "tables": discovered_tables,
            "columns": [column for table in discovered_tables for column in table.get("columns") or []],
        },
        "file_ingestion_object_count": len(object_rows),
    }


def activate_file_bronze_artifacts(state: Mapping[str, Any]) -> Dict[str, Any]:
    """Bind reviewed file artifacts to their existing source objects atomically."""
    from services.metadata_contracts import validate_execution_spec
    from utilis.generated_code_paths import verified_execution_artifact

    selection = validated_metadata_selection(state)
    if not selection:
        raise ValueError("ADLS artifact activation requires application metadata.")
    repository = selection.repository
    generated = [dict(item) for item in state.get("bronze_generation_results") or []]
    results = []
    with repository.unit_of_work():
        for result in generated:
            object_id = int(result.get("ingestion_object_id") or 0)
            config_version = int(result.get("ingestion_object_config_version") or 0)
            config_hash = str(result.get("ingestion_object_config_hash") or "")
            mapping_version = int(result.get("mapping_version") or 0)
            mapping_hash = str(result.get("mapping_hash") or "")
            if object_id <= 0 or config_version <= 0 or not config_hash:
                raise ValueError("Generated ADLS Bronze artifact is missing its source-object pin.")
            source_object = repository.get_ingestion_object(object_id, config_version)
            if (
                not source_object
                or str(source_object.get("config_hash") or "") != config_hash
                or str(source_object.get("processing_stage") or "").upper() != "SOURCE_TO_BRONZE"
            ):
                raise RuntimeError("The reviewed ADLS Bronze artifact does not match its source object.")
            target_table = str(
                source_object.get("target_table")
                or source_object.get("target_bronze_table")
                or ""
            ).strip()
            repository.get_mapping_bundle(
                ingestion_object_id=object_id,
                processing_stage="SOURCE_TO_BRONZE",
                mapping_version=mapping_version,
                expected_hash=mapping_hash,
                expected_target=target_table,
                require_active=True,
            )
            spec = validate_execution_spec(
                result.get("execution_spec") or {}, platform=repository.context.platform
            )
            verified_execution_artifact(spec, platform=repository.context.platform)
            if int(spec["mapping_version"]) != mapping_version:
                raise RuntimeError("The ADLS Bronze artifact uses a different mapping version.")
            spec = {
                **spec,
                "design_config_version": config_version,
                "design_config_hash": config_hash,
                "mapping_hash": mapping_hash,
                "processing_stage": "SOURCE_TO_BRONZE",
                "source_file": {
                    "path": str(source_object.get("source_path") or ""),
                    "format": str(source_object.get("payload_format") or ""),
                },
            }
            repository.execute(
                f"UPDATE {repository.table('cfg_ingestion_object')} SET execution_spec_json = :spec, "
                "updated_at = CURRENT_TIMESTAMP() WHERE ingestion_object_id = :object_id "
                "AND config_version = :config_version AND config_hash = :config_hash "
                "AND active_flag = :active_flag AND is_current = :is_current",
                {
                    "object_id": object_id,
                    "config_version": config_version,
                    "config_hash": config_hash,
                    "active_flag": True,
                    "is_current": True,
                    "spec": json.dumps(spec, sort_keys=True, separators=(",", ":")),
                },
            )
            active = repository.get_active_ingestion_object(object_id)
            if not active or json.loads(str(active.get("execution_spec_json") or "{}")) != spec:
                raise RuntimeError("ADLS Bronze artifact activation postcondition failed.")
            results.append(
                {
                    **result,
                    "execution_spec": spec,
                    "active_ingestion_object_config_version": config_version,
                    "active_ingestion_object_config_hash": config_hash,
                    "metadata_activation_status": "ACTIVE",
                }
            )
    return {**dict(state), "bronze_generation_results": results}


def bind_file_dbt_package(state: Mapping[str, Any]) -> Dict[str, Any]:
    """Bind one frozen Snowflake dbt package to existing file-source objects."""
    from pathlib import Path

    from services.metadata_contracts import file_sha256

    package_hash = str(state.get("snowflake_dbt_artifact_set_hash") or "").strip()
    package_id = str(state.get("snowflake_dbt_idempotency_key") or "").strip()
    if not package_hash or not package_id:
        raise RuntimeError("The finalized ADLS Snowflake dbt package identity is missing.")
    if str(state.get("snowflake_dbt_validation_status") or "").upper() != "STATIC_VALIDATED":
        raise RuntimeError("ADLS Snowflake dbt metadata requires a statically validated package.")
    selection = validated_metadata_selection(state)
    if not selection:
        raise ValueError("ADLS Snowflake dbt binding requires application metadata.")
    repository = selection.repository
    bronze_results = [dict(item) for item in state.get("bronze_generation_results") or []]
    if not bronze_results:
        raise RuntimeError("ADLS Snowflake dbt binding found no source artifacts.")
    project_root = Path(str(state.get("snowflake_dbt_artifact_path") or "")).resolve()
    if not project_root.is_dir():
        raise RuntimeError("The finalized ADLS Snowflake dbt project directory is missing.")
    package_manifest = []
    for layer in ("bronze", "silver", "gold"):
        artifacts = [
            dict(item)
            for item in state.get(f"{layer}_generation_results") or []
            if isinstance(item, Mapping)
        ]
        if not artifacts:
            raise RuntimeError(f"The finalized ADLS Snowflake dbt package has no {layer} artifacts.")
        for artifact in artifacts:
            path = Path(str(artifact.get("script_path") or "")).resolve()
            if not path.is_file() or not path.is_relative_to(project_root):
                raise RuntimeError(
                    f"An approved ADLS {layer} artifact is outside the finalized dbt package."
                )
            package_manifest.append(
                {
                    "layer": layer,
                    "path": path.relative_to(project_root).as_posix(),
                    "artifact_hash": file_sha256(path),
                }
            )
    manifest_hash = canonical_json_hash(package_manifest)
    bound_bronze = []
    with repository.unit_of_work():
        for result in bronze_results:
            object_id = int(result.get("ingestion_object_id") or 0)
            active = repository.get_active_ingestion_object(object_id)
            spec = dict(result.get("execution_spec") or {})
            if (
                not active
                or str(active.get("processing_stage") or "").upper() != "SOURCE_TO_BRONZE"
                or str(spec.get("mapping_hash") or "") != str(result.get("mapping_hash") or "")
                or str(spec.get("engine") or "").upper() != "SNOWFLAKE_DBT"
            ):
                raise RuntimeError("The frozen dbt package does not match an active ADLS source object.")
            spec.update(
                {
                    "dbt_package_hash": package_hash,
                    "dbt_package_id": package_id,
                    "dbt_package_model_count": int(state.get("snowflake_dbt_model_count") or 0),
                    "dbt_package_manifest_hash": manifest_hash,
                }
            )
            repository.execute(
                f"UPDATE {repository.table('cfg_ingestion_object')} SET execution_spec_json = :spec, "
                "updated_at = CURRENT_TIMESTAMP() WHERE ingestion_object_id = :object_id "
                "AND active_flag = :active_flag AND is_current = :is_current",
                {
                    "object_id": object_id,
                    "active_flag": True,
                    "is_current": True,
                    "spec": json.dumps(spec, sort_keys=True, separators=(",", ":")),
                },
            )
            persisted = repository.get_active_ingestion_object(object_id)
            if not persisted or json.loads(str(persisted.get("execution_spec_json") or "{}")) != spec:
                raise RuntimeError("ADLS Snowflake dbt package binding postcondition failed.")
            bound_bronze.append({**result, "execution_spec": spec, "metadata_activation_status": "ACTIVE"})
    return {
        **dict(state),
        "bronze_generation_results": bound_bronze,
        "silver_generation_results": [
            {**dict(item), "metadata_activation_status": "ACTIVE"}
            for item in state.get("silver_generation_results") or []
            if isinstance(item, Mapping)
        ],
        "gold_generation_results": [
            {**dict(item), "metadata_activation_status": "ACTIVE"}
            for item in state.get("gold_generation_results") or []
            if isinstance(item, Mapping)
        ],
        "adls_dbt_package_manifest_hash": manifest_hash,
    }


def prepare_file_silver_generation(
    state: Mapping[str, Any], review_artifact: Mapping[str, Any]
) -> Dict[str, Any]:
    """Pin reviewed Silver mappings to the existing physical-file objects."""
    if str(state.get("source") or "").lower() != "adls_gen2":
        return dict(state)
    selection = validated_metadata_selection(state)
    if not selection:
        raise ValueError("ADLS Silver metadata requires a selected source system and connection.")

    def entity_name(value: Any) -> str:
        name = str(value or "").split(".")[-1].strip('"').casefold()
        for prefix in ("bronze_", "silver_"):
            if name.startswith(prefix):
                return name[len(prefix) :]
        return name

    reviewed_keys = {
        entity_name(
            feed.get("table")
            or feed.get("table_name")
            or feed.get("entity")
            or feed.get("feed_id")
            or feed.get("target_table")
        ): {
            normalize_bronze_column_name(str(key))
            for key in (feed.get("merge_keys") or feed.get("primary_keys") or [])
            if str(key).strip()
        }
        for feed in review_artifact.get("feeds") or []
        if isinstance(feed, Mapping)
    }
    certified = [item for item in state.get("certified_tables") or [] if isinstance(item, Mapping)]
    bronze_by_object = {
        int(item.get("ingestion_object_id") or 0): item
        for item in state.get("bronze_generation_results") or []
        if isinstance(item, Mapping) and int(item.get("ingestion_object_id") or 0) > 0
    }
    if not certified or len(bronze_by_object) != len(certified):
        raise ValueError("ADLS Silver generation requires one generated Bronze artifact per approved source file.")

    mapping_rows: List[Dict[str, Any]] = []
    table_refs: List[Dict[str, Any]] = []
    repository = selection.repository
    with repository.unit_of_work():
        for table in certified:
            object_id = int(table.get("ingestion_object_id") or 0)
            config_version = int(table.get("ingestion_object_config_version") or 0)
            ingestion_object = repository.get_ingestion_object(object_id, config_version)
            if (
                not ingestion_object
                or str(ingestion_object.get("config_hash") or "")
                != str(table.get("ingestion_object_config_hash") or "")
                or str(ingestion_object.get("processing_stage") or "").upper()
                != "SOURCE_TO_BRONZE"
            ):
                raise RuntimeError(f"The pinned ADLS source-file object changed: {object_id}/{config_version}.")
            keys = reviewed_keys.get(entity_name(table.get("table_name") or table.get("entity"))) or set()
            if not keys:
                raise ValueError(f"Approved Silver merge keys are missing for {table.get('table_name') or table.get('entity')}.")
            reviewed_table = {
                **dict(table),
                "columns": [
                    {
                        **dict(column),
                        "is_primary_key": normalize_bronze_column_name(
                            str(column.get("column_name") or column.get("source_field_path") or "")
                        )
                        in keys,
                        "is_join_key": normalize_bronze_column_name(
                            str(column.get("column_name") or column.get("source_field_path") or "")
                        )
                        in keys,
                    }
                    for column in table.get("columns") or []
                    if isinstance(column, Mapping)
                ],
            }
            rows = [
                row
                for row in _mapping_rows(state, reviewed_table, ingestion_object)
                if row["processing_stage"] == "BRONZE_TO_SILVER"
            ]
            if not rows or not any(bool(row.get("is_primary_key")) for row in rows):
                raise ValueError(f"Reviewed Silver keys do not match inferred columns for {table.get('table_name')}.")
            mapping_rows.extend(rows)
            target_table = str(rows[0]["target_object_name"])
            source_table = str(rows[0]["source_object_name"])
            table_name = entity_name(target_table)
            columns = [
                {
                    "table_name": table_name,
                    "source_column_name": str(row["source_field_path"]),
                    "column_name": str(row["target_column_name"]),
                    "data_type": str(row["target_data_type"]),
                    "type": str(row["target_data_type"]),
                    "is_join_key": bool(row.get("is_primary_key")),
                    "transformation_rule": str(row.get("transformation_rule") or ""),
                }
                for row in sorted(rows, key=lambda item: int(item.get("ordinal_position") or 0))
            ]
            table_refs.append(
                {
                    "database_name": str(table.get("database_name") or ""),
                    "schema_name": str(table.get("schema_name") or ""),
                    "table_name": table_name,
                    "ingestion_object_id": object_id,
                    "bronze_table": source_table,
                    "silver_table": target_table,
                    "existing_script_path": None,
                    "source_columns": columns,
                    "mapping_columns": columns,
                    "metadata_driven": True,
                    "bronze_model_name": bronze_by_object[object_id].get("dbt_model_name"),
                    "bronze_to_silver_mapping_version": int(rows[0]["mapping_version"]),
                    "bronze_to_silver_mapping_hash": str(rows[0]["mapping_hash"]),
                }
            )
        _merge_rows(repository, "cfg_mapping", mapping_rows, "mapping_id")

    return {
        **dict(state),
        "silver_generation_table_refs": table_refs,
        "bronze_to_silver_mapping_bundles": [
            {
                "ingestion_object_id": int(row["ingestion_object_id"]),
                "mapping_version": int(row["mapping_version"]),
                "mapping_hash": str(row["mapping_hash"]),
            }
            for row in mapping_rows
            if int(row.get("ordinal_position") or 0) == min(
                int(item.get("ordinal_position") or 0)
                for item in mapping_rows
                if item["ingestion_object_id"] == row["ingestion_object_id"]
            )
        ],
    }
