from nodes.column_profiling import _resolve_tables_for_profiling, profile_column
from nodes.metadata_discovery import _resolve_tables_for_discovery


def test_ingestion_object_id_flows_from_gate2_through_discovery_and_profiles(monkeypatch):
    gate2_state = {
        "certified_tables": [
            {
                "database_name": "ClaimsDB",
                "schema_name": "dbo",
                "table_name": "Claims",
                "ingestion_object_id": 123,
                "ingestion_object_config_version": 1,
            }
        ]
    }
    discovered_ref = _resolve_tables_for_discovery(gate2_state)[0]
    profiling_state = {
        "discovered_metadata": {
            "tables": [
                {
                    **discovered_ref,
                    "table_status": "COMPLETED",
                    "columns": [{"column_name": "claim_id", "data_type": "int"}],
                }
            ]
        }
    }
    profiling_ref = _resolve_tables_for_profiling(profiling_state)[0]
    monkeypatch.setattr(
        "nodes.column_profiling.pass1_pushdown_profile",
        lambda *_args: {"total_rows": 1, "non_null_count": 1, "null_rate": 0.0},
    )

    profile = profile_column(profiling_ref, profiling_ref.columns[0], "design-run")

    assert discovered_ref["ingestion_object_id"] == 123
    assert profiling_ref.ingestion_object_id == 123
    assert profile.ingestion_object_id == 123
    assert profile.ingestion_object_config_version == 1
