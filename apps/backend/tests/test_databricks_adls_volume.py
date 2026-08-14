from __future__ import annotations

import pytest

from services import adls_source, databricks_runtime


def test_adls_bronze_stages_only_supplied_approved_scripts(monkeypatch):
    uploaded = []

    def download(source_path, destination, **kwargs):
        assert source_path == "/INSURANCE_SFTP/insurance/claims/claims.csv"
        assert kwargs == {"expected_etag": "etag-1", "expected_size": 12}
        destination.write_bytes(b"claim_id\n1\n")
        return {"size": destination.stat().st_size, "etag": "etag-1"}

    def upload(local_path, volume_path):
        uploaded.append((local_path.read_bytes(), volume_path))
        return {"volume_path": volume_path, "size": local_path.stat().st_size}

    monkeypatch.setattr(adls_source, "download_file_to_path", download)
    monkeypatch.setattr(databricks_runtime, "_volume_upload_file", upload)
    monkeypatch.setattr(databricks_runtime, "_volume_cache_hit", lambda _script: False)
    monkeypatch.setattr(databricks_runtime, "_volume_write_json", lambda *_args: None)
    script = {
        "script_path": "bronze_claims.py",
        "adls_remote_path": "/INSURANCE_SFTP/insurance/claims/claims.csv",
        "adls_source_etag": "etag-1",
        "adls_source_size": 12,
        "source_file_name": "claims.csv",
        "volume_source_path": "/Volumes/workspace/bronze_schema/vol_bronze/csv/claims.csv",
    }

    staged = databricks_runtime._stage_adls_bronze_sources(
        {"run_id": "run-1", "source": "adls_gen2"}, [script]
    )

    assert uploaded == [
        (
            b"claim_id\n1\n",
            "/Volumes/workspace/bronze_schema/vol_bronze/csv/claims.csv",
        )
    ]
    assert staged[0]["etag"] == "etag-1"
    assert staged[0]["cache_hit"] is False


def test_database_bronze_never_stages_volume_files(monkeypatch):
    monkeypatch.setattr(
        databricks_runtime,
        "_volume_upload_file",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not upload")),
    )

    assert databricks_runtime._stage_adls_bronze_sources(
        {"source": "database"}, [{"script_path": "database_bronze.py"}]
    ) == []


def test_adls_bronze_staging_rejects_paths_outside_format_file_shape():
    with pytest.raises(ValueError, match="configured-volume"):
        databricks_runtime._validated_staging_path(
            "/Volumes/workspace/bronze_schema/vol_bronze/probe.txt"
        )


def test_cached_adls_file_skips_download_and_upload(monkeypatch):
    monkeypatch.setattr(databricks_runtime, "_volume_cache_hit", lambda _script: True)
    monkeypatch.setattr(
        adls_source,
        "download_file_to_path",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not download")),
    )
    monkeypatch.setattr(
        databricks_runtime,
        "_volume_upload_file",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not upload")),
    )
    script = {
        "script_path": "bronze_claims.py",
        "adls_remote_path": "/root/claims.csv",
        "adls_source_etag": "etag-1",
        "adls_source_size": 12,
        "source_file_name": "claims.csv",
        "volume_source_path": "/Volumes/workspace/bronze_schema/vol_bronze/csv/claims.csv",
    }

    staged = databricks_runtime._stage_adls_bronze_sources(
        {"source": "adls_gen2"}, [script]
    )

    assert staged[0]["cache_hit"] is True


def test_cache_rejects_same_filename_from_different_source_even_when_size_changes(monkeypatch):
    monkeypatch.setattr(
        databricks_runtime,
        "_files_api_request",
        lambda *_args, **_kwargs: {"content-length": "99"},
    )
    monkeypatch.setattr(
        databricks_runtime,
        "_volume_read_json",
        lambda _path: {"source_path": "/another/feed/claims.csv", "etag": "old", "size": 99},
    )

    with pytest.raises(RuntimeError, match="filename collision"):
        databricks_runtime._volume_cache_hit({
            "adls_remote_path": "/approved/feed/claims.csv",
            "adls_source_etag": "new",
            "adls_source_size": 12,
            "volume_source_path": "/Volumes/workspace/bronze_schema/vol_bronze/csv/claims.csv",
        })
