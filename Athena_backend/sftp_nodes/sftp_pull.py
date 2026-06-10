from __future__ import annotations

import os
import stat
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from utilis.db import config, get_pipeline_connection
from utilis.env import load_backend_env
from utilis.logger import logger

try:
    import paramiko
except ImportError:
    paramiko = None  # type: ignore


load_backend_env()


def _pipeline_schema() -> str:
    return config["azure_sql"]["pipeline_schema"]


def _get_required_env(name: str) -> str:
    value = os.getenv(name, "")
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _ensure_manifest_columns() -> None:
    conn = get_pipeline_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            IF COL_LENGTH('{_pipeline_schema()}.file_feed_manifest', 'file_name') IS NULL
                ALTER TABLE [{_pipeline_schema()}].[file_feed_manifest] ADD [file_name] NVARCHAR(1024) NULL;
            IF COL_LENGTH('{_pipeline_schema()}.file_feed_manifest', 'modified_time') IS NULL
                ALTER TABLE [{_pipeline_schema()}].[file_feed_manifest] ADD [modified_time] DATETIME2(7) NULL;
            """
        )
        conn.commit()
    finally:
        conn.close()


def _load_approved_feed(feed_id: str) -> Optional[Dict[str, Any]]:
    conn = get_pipeline_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            WITH ranked AS (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY feed_id ORDER BY updated_at DESC, created_at DESC) AS rn
                FROM [{_pipeline_schema()}].[file_feed_registry]
                WHERE feed_id = ?
            )
            SELECT feed_id, vendor, entity, format, file_name, file_path, remote_path, status, source
            FROM ranked
            WHERE rn = 1 AND UPPER(status) = 'APPROVED'
            """,
            feed_id,
        )
        row = cursor.fetchone()
        if not row:
            return None
        return {
            "feed_id": row.feed_id,
            "vendor": row.vendor,
            "entity": row.entity,
            "format": row.format,
            "file_name": row.file_name,
            "file_path": row.file_path,
            "remote_path": row.remote_path,
            "status": row.status,
            "source": row.source,
        }
    finally:
        conn.close()


def _manifest_exists(feed_id: str, file_name: str, file_size: int, modified_time: str) -> bool:
    _ensure_manifest_columns()
    conn = get_pipeline_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT TOP 1 id
            FROM [{_pipeline_schema()}].[file_feed_manifest]
            WHERE feed_id = ? AND file_name = ? AND file_size = ? AND modified_time = ?
            ORDER BY created_at DESC
            """,
            feed_id,
            file_name,
            file_size,
            modified_time,
        )
        return cursor.fetchone() is not None
    finally:
        conn.close()


def _insert_manifest(feed_id: str, file_name: str, file_path: str, remote_path: str, file_size: int, modified_time: str, state: str) -> Optional[int]:
    _ensure_manifest_columns()
    conn = get_pipeline_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            INSERT INTO [{_pipeline_schema()}].[file_feed_manifest]
            (feed_id, file_name, file_path, remote_path, file_size, checksum, digest_algorithm, state, found_at, downloaded_at, modified_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            feed_id,
            file_name,
            file_path,
            remote_path,
            file_size,
            None,
            "SHA256",
            state,
            datetime.now(timezone.utc).isoformat(),
            datetime.now(timezone.utc).isoformat() if state == "SYNCED" else None,
            modified_time,
        )
        cursor.execute("SELECT CAST(SCOPE_IDENTITY() AS int)")
        row = cursor.fetchone()
        conn.commit()
        return int(row[0]) if row else None
    finally:
        conn.close()


def _insert_sync_log(feed_id: str, manifest_id: Optional[int], sync_status: str, started_at: str, completed_at: Optional[str], error_message: Optional[str]) -> None:
    conn = get_pipeline_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            INSERT INTO [{_pipeline_schema()}].[file_feed_sync_log]
            (feed_id, manifest_id, sync_status, attempt, started_at, completed_at, processed_rows, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            feed_id,
            manifest_id,
            sync_status,
            1,
            started_at,
            completed_at,
            None,
            error_message,
        )
        conn.commit()
    finally:
        conn.close()


class SFTPPullClient:
    def __init__(self) -> None:
        if paramiko is None:
            raise ImportError("paramiko is required for SFTP operations. Install it with pip install paramiko.")
        self.host = _get_required_env("ATHENA_SFTP_HOST")
        self.username = _get_required_env("ATHENA_SFTP_USERNAME")
        self.password = os.getenv("ATHENA_SFTP_PASSWORD")
        self.port = int(os.getenv("ATHENA_SFTP_PORT", "22"))
        self.private_key_path = os.getenv("ATHENA_SFTP_PRIVATE_KEY_PATH")
        self.private_key_passphrase = os.getenv("ATHENA_SFTP_PRIVATE_KEY_PASSPHRASE")

    def connect(self) -> "paramiko.SFTPClient":
        transport = paramiko.Transport((self.host, self.port))
        if self.private_key_path:
            pkey = paramiko.RSAKey.from_private_key_file(
                self.private_key_path,
                password=self.private_key_passphrase or None,
            )
            transport.connect(username=self.username, pkey=pkey)
        else:
            if self.password is None:
                raise ValueError("SFTP password or private key path is required")
            transport.connect(username=self.username, password=self.password)
        return paramiko.SFTPClient.from_transport(transport)

    def list_remote_files(self, sftp: "paramiko.SFTPClient", remote_path: str) -> List[Dict[str, Any]]:
        entries = sftp.listdir_attr(remote_path)
        files: List[Dict[str, Any]] = []
        for entry in entries:
            if stat.S_ISREG(entry.st_mode):
                files.append(
                    {
                        "file_name": entry.filename,
                        "file_size": int(entry.st_size),
                        "modified_time": datetime.fromtimestamp(int(entry.st_mtime), tz=timezone.utc).isoformat(),
                    }
                )
        return sorted(files, key=lambda item: item["file_name"])

    def download_file(self, sftp: "paramiko.SFTPClient", remote_path: str, file_name: str, destination: Path) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        sftp.get(f"{remote_path.rstrip('/')}/{file_name}", str(destination))


def sftp_pull_node(state: Dict[str, Any]) -> Dict[str, Any]:
    new_state = dict(state)
    source = str(new_state.get("source") or "").lower()
    if source != "sftp":
        new_state["sftp_pull_status"] = "SKIPPED"
        return new_state
    if str(new_state.get("status") or "").upper() == "FAILED":
        return new_state

    candidate_feed = new_state.get("candidate_feed") or {}
    feed_id = str(candidate_feed.get("feed_id") or "")
    if not feed_id:
        new_state["status"] = "FAILED"
        new_state["sftp_pull_status"] = "FAILED"
        new_state["error"] = "SFTP pull failed: missing feed_id"
        return new_state

    approved_feed = _load_approved_feed(feed_id)
    if not approved_feed:
        new_state["status"] = "FAILED"
        new_state["sftp_pull_status"] = "FAILED"
        new_state["error"] = f"SFTP pull failed: feed {feed_id} is not approved in file_feed_registry"
        return new_state

    remote_path = str(approved_feed.get("remote_path") or "").strip()
    if not remote_path:
        new_state["status"] = "FAILED"
        new_state["sftp_pull_status"] = "FAILED"
        new_state["error"] = f"SFTP pull failed: remote_path missing in file_feed_registry for {feed_id}"
        return new_state

    landing_root = Path(os.getenv("ATHENA_SFTP_LANDING_ROOT", "/Volumes/sftp_landing"))
    vendor = str(approved_feed.get("vendor") or "Vendor1")
    entity = str(approved_feed.get("entity") or "unknown")
    landing_path = landing_root / vendor / entity
    landing_path.mkdir(parents=True, exist_ok=True)

    client = SFTPPullClient()
    pulled_files: List[str] = []
    files_pulled = 0
    sftp = client.connect()
    try:
        for remote_file in client.list_remote_files(sftp, remote_path):
            file_name = remote_file["file_name"]
            file_size = int(remote_file["file_size"])
            modified_time = str(remote_file["modified_time"])
            if _manifest_exists(feed_id, file_name, file_size, modified_time):
                logger.info("Skipping SFTP file already present in SQL manifest: %s", file_name, extra={"node": "sftp_pull"})
                continue

            started_at = datetime.now(timezone.utc).isoformat()
            destination = landing_path / file_name
            try:
                client.download_file(sftp, remote_path, file_name, destination)
                manifest_id = _insert_manifest(
                    feed_id=feed_id,
                    file_name=file_name,
                    file_path=str(destination),
                    remote_path=remote_path,
                    file_size=file_size,
                    modified_time=modified_time,
                    state="SYNCED",
                )
                _insert_sync_log(feed_id, manifest_id, "SUCCESS", started_at, datetime.now(timezone.utc).isoformat(), None)
                pulled_files.append(str(destination))
                files_pulled += 1
            except Exception as exc:
                manifest_id = _insert_manifest(
                    feed_id=feed_id,
                    file_name=file_name,
                    file_path=str(destination),
                    remote_path=remote_path,
                    file_size=file_size,
                    modified_time=modified_time,
                    state="FAILED",
                )
                _insert_sync_log(feed_id, manifest_id, "FAILED", started_at, datetime.now(timezone.utc).isoformat(), str(exc))
                raise
    except Exception as exc:
        logger.error("SFTP pull node failed: %s", exc, extra={"node": "sftp_pull"})
        new_state["status"] = "FAILED"
        new_state["sftp_pull_status"] = "FAILED"
        new_state["error"] = str(exc)
        return new_state
    finally:
        sftp.close()

    new_state["candidate_feed"] = {**candidate_feed, **approved_feed}
    new_state["landing_path"] = str(landing_path)
    new_state["files_pulled"] = files_pulled
    new_state["pulled_files"] = pulled_files
    new_state["sftp_pull_status"] = "COMPLETED"
    return new_state
