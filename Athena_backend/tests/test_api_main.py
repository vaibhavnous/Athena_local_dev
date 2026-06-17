import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from api.main import app

client = TestClient(app)


def test_health_endpoint():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "athena-fastapi"}


def test_pipeline_run_requires_brd_text_for_database_source(monkeypatch, tmp_path):
    payload = {"source": "database", "brd_text": ""}
    response = client.post("/pipeline/run", json=payload)
    assert response.status_code == 400
    assert response.json()["detail"] == "brd_text is required"


def test_upload_brd_creates_file(tmp_path, monkeypatch):
    upload_dir = Path(__file__).resolve().parents[1] / "uploads"
    monkeypatch.setattr("api.main.ROOT_DIR", Path(__file__).resolve().parents[1])

    file_content = b"test content"
    response = client.post(
        "/pipeline/upload-brd",
        files={"file": ("sample.brd", file_content, "text/plain")},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["filename"] == "sample.brd"
    assert body["status"] == "uploaded"
    assert Path(body["path"]).exists()
    assert Path(body["path"]).read_bytes() == file_content


def test_settings_roundtrip():
    response = client.get("/settings")
    assert response.status_code == 200
    assert response.json()["provider"] == "azure_openai"

    payload = {"provider": "azure_openai", "budget": 42}
    response = client.put("/settings", json=payload)
    assert response.status_code == 200
    assert response.json()["budget"] == 42
