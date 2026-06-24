from __future__ import annotations

from nodes import silver_gen


def test_silver_table_resolution_ignores_existing_silver_outputs(monkeypatch, tmp_path):
    output_dir = tmp_path / "silver"
    output_dir.mkdir()
    stale_name = (
        "silver_transform_run_a_run_b_run_c_claim_payment_expenses.py"
    )
    (output_dir / stale_name).write_text("# stale output\n", encoding="utf-8")

    monkeypatch.setattr(silver_gen, "_silver_output_dir", lambda: str(output_dir))
    monkeypatch.setattr(silver_gen, "_load_bronze_bundle", lambda: {"scripts": []})

    refs = silver_gen._resolve_tables_for_silver(
        {
            "certified_tables": [
                {
                    "database_name": "insurance",
                    "schema_name": "dbo",
                    "table_name": "claim_payment_expenses",
                }
            ],
            "bronze_schema": "bronze",
            "silver_schema": "silver",
        }
    )

    assert [ref["table_name"] for ref in refs] == ["claim_payment_expenses"]


def test_silver_file_slug_caps_long_table_names():
    long_name = "018c963b_38fe_4567_b413_ae0f7dba5a68_" * 4 + "claim_payment_expenses"

    slug = silver_gen._file_slug(long_name)

    assert len(slug) <= 64
    assert slug.endswith("_" + silver_gen.hashlib.sha1(long_name.encode("utf-8")).hexdigest()[:8])
