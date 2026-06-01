from __future__ import annotations

import json
import os
import traceback
from io import BytesIO
from pathlib import Path, PurePosixPath
from typing import Any, Dict, Tuple

import pandas as pd
import paramiko

from state import Stage01State
from utilis.logger import logger


SFTP_HOST = "localhost"
SFTP_PORT = 2222
SFTP_USERNAME = "user"
SFTP_KEY_PATH = r"C:\Users\vaibhavmalik\.ssh\id_rsa"
SFTP_VENDOR_ROOT = "/cash-project/Vendor1"
SFTP_ENTITY_DIRS = {
    "transactions": f"{SFTP_VENDOR_ROOT}/transactions/",
    "employee": f"{SFTP_VENDOR_ROOT}/employee/",
}

ADLS_ACCOUNT_URL = os.getenv("ADLS_ACCOUNT_URL", "https://atheastorage.dfs.core.windows.net")
ADLS_FILE_SYSTEM = os.getenv("ADLS_FILE_SYSTEM", "").strip()
ADLS_VENDOR_ROOT = os.getenv("ADLS_VENDOR_ROOT", "cash-project/Vendor1").strip().strip("/")
ADLS_ENTITY_DIRS = {
    "transactions": f"{ADLS_VENDOR_ROOT}/transactions/",
    "employee": f"{ADLS_VENDOR_ROOT}/employee/",
}


def _load_private_key(key_path: str) -> paramiko.PKey:
    try:
        return paramiko.RSAKey.from_private_key_file(key_path)
    except paramiko.SSHException:
        return paramiko.Ed25519Key.from_private_key_file(key_path)


def _parse_sftp_content(content: bytes, file_name: str) -> pd.DataFrame:
    lower_name = file_name.lower()
    if lower_name.endswith(".csv"):
        return pd.read_csv(BytesIO(content))
    if lower_name.endswith(".json"):
        payload = json.loads(content.decode("utf-8"))
        if isinstance(payload, list):
            return pd.DataFrame(payload)
        return pd.DataFrame([payload])
    raise ValueError(f"Unsupported file type: {file_name}")


def _read_one_file_from_sftp(remote_dir: str) -> Tuple[pd.DataFrame, str, bytes]:
    private_key = _load_private_key(SFTP_KEY_PATH)

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        client.connect(
            hostname=SFTP_HOST,
            port=SFTP_PORT,
            username=SFTP_USERNAME,
            pkey=private_key,
            timeout=10,
            banner_timeout=30,
            auth_timeout=30,
        )
        sftp = client.open_sftp()
        try:
            files = [
                item.filename
                for item in sftp.listdir_attr(remote_dir)
                if item.filename.lower().endswith(".csv") or item.filename.lower().endswith(".json")
            ]
            if not files:
                raise FileNotFoundError(f"No CSV/JSON files found in {remote_dir}")

            remote_path = str(PurePosixPath(remote_dir) / sorted(files)[0])
            try:
                _ = sftp.stat(remote_path)
            except Exception as exc:
                raise RuntimeError(
                    f"SFTP STAT failed for {remote_path!r}. Detail: {type(exc).__name__}: {exc}"
                ) from exc

            with sftp.open(remote_path, "rb") as remote_file:
                content = remote_file.read()
            dataframe = _parse_sftp_content(content, Path(remote_path).name)
            return dataframe, remote_path, content
        finally:
            sftp.close()
    except Exception as exc:
        tb = traceback.format_exc(limit=20)
        if isinstance(exc, OSError) and str(exc) == "Failure":
            hint = (
                "SFTP server returned generic OPEN Failure. "
                "This usually means the SFTP server implementation does not support file OPEN/READ "
                "(listdir/stat may still work) or it is denying read permissions."
            )
        else:
            hint = "SFTP connection/read failed."
        raise RuntimeError(
            f"SFTP read failed ({SFTP_HOST}:{SFTP_PORT}). {hint} "
            f"Detail: {type(exc).__name__}: {exc}\n{tb}"
        ) from exc
    finally:
        client.close()


def _read_one_file_from_adls(remote_dir: str) -> Tuple[pd.DataFrame, str, bytes]:
    if not ADLS_FILE_SYSTEM:
        raise RuntimeError("ADLS_FILE_SYSTEM is required for source=adls_gen2")

    try:
        from azure.identity import DefaultAzureCredential
        from azure.storage.filedatalake import DataLakeServiceClient
    except Exception as exc:
        raise RuntimeError(
            "Missing ADLS dependencies. Install `azure-identity` and `azure-storage-file-datalake`."
        ) from exc

    credential = DefaultAzureCredential()
    service_client = DataLakeServiceClient(account_url=ADLS_ACCOUNT_URL, credential=credential)
    fs = service_client.get_file_system_client(ADLS_FILE_SYSTEM)

    remote_dir = remote_dir.strip("/").rstrip("/") + "/"
    candidates = []
    for item in fs.get_paths(path=remote_dir, recursive=False):
        if getattr(item, "is_directory", False):
            continue
        name = str(item.name)
        lower = name.lower()
        if lower.endswith(".csv") or lower.endswith(".json"):
            candidates.append(name)
    if not candidates:
        raise FileNotFoundError(f"No CSV/JSON files found in adls://{ADLS_FILE_SYSTEM}/{remote_dir}")

    remote_path = sorted(candidates)[0]
    file_client = fs.get_file_client(remote_path)
    downloader = file_client.download_file()
    content = downloader.readall()
    dataframe = _parse_sftp_content(content, Path(remote_path).name)
    return dataframe, "/" + remote_path, content


def _dummy_rdbms_dataframe() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "vendor_txn_id": "dummy-001",
                "atm_id": "ATM-001",
                "txn_amt": 100.0,
                "txn_ts": "2026-05-19T10:00:00",
                "currency_cd": "USD",
            }
        ]
    )


def source_ingestion_node(state: Stage01State) -> Stage01State:
    new_state: Dict[str, Any] = dict(state)
    source = str(new_state.get("source") or "").lower()
    sftp_entity = str(new_state.get("sftp_entity") or "transactions").lower()
    log_context = {
        "run_id": new_state.get("run_id", "unknown"),
        "node": "sftp_source_ingestion",
        "stage": "sftp_source_ingestion",
    }

    try:
        if source == "sftp":
            logger.info(
                "SFTP source ingestion starting for entity=%s",
                sftp_entity,
                extra={**log_context, "event_type": "stage_start", "sftp_entity": sftp_entity},
            )
            entities = ["transactions", "employee"] if sftp_entity == "both" else [sftp_entity]
            if not all(entity in SFTP_ENTITY_DIRS for entity in entities):
                raise ValueError("Unsupported sftp_entity. Expected transactions, employee, or both.")

            frames = []
            local_paths = []
            for entity in entities:
                dataframe_entity, remote_path, raw_content = _read_one_file_from_sftp(SFTP_ENTITY_DIRS[entity])
                dataframe_entity = dataframe_entity.copy()
                dataframe_entity["__entity"] = entity
                frames.append(dataframe_entity)

                local_dir = (
                    Path(__file__).resolve().parents[1]
                    / "uploads"
                    / "sftp"
                    / "cash-project"
                    / "Vendor1"
                    / entity
                )
                local_dir.mkdir(parents=True, exist_ok=True)
                local_file = local_dir / Path(remote_path).name
                local_file.write_bytes(raw_content)
                local_paths.append(str(local_file))

            dataframe = pd.concat(frames, ignore_index=True, sort=False)
            new_state["file_path"] = local_paths[0]
            new_state["sftp_files"] = local_paths
            new_state["sftp_entity"] = sftp_entity
            new_state["vendor"] = "Vendor1"
        elif source == "adls_gen2":
            logger.info(
                "ADLS Gen2 source ingestion starting for entity=%s",
                sftp_entity,
                extra={**log_context, "event_type": "stage_start", "sftp_entity": sftp_entity},
            )
            entities = ["transactions", "employee"] if sftp_entity == "both" else [sftp_entity]
            if not all(entity in ADLS_ENTITY_DIRS for entity in entities):
                raise ValueError("Unsupported sftp_entity. Expected transactions, employee, or both.")

            frames = []
            local_paths = []
            for entity in entities:
                dataframe_entity, remote_path, raw_content = _read_one_file_from_adls(ADLS_ENTITY_DIRS[entity])
                dataframe_entity = dataframe_entity.copy()
                dataframe_entity["__entity"] = entity
                frames.append(dataframe_entity)

                local_dir = (
                    Path(__file__).resolve().parents[1]
                    / "uploads"
                    / "adls"
                    / "cash-project"
                    / "Vendor1"
                    / entity
                )
                local_dir.mkdir(parents=True, exist_ok=True)
                local_file = local_dir / Path(remote_path).name
                local_file.write_bytes(raw_content)
                local_paths.append(str(local_file))

            dataframe = pd.concat(frames, ignore_index=True, sort=False)
            new_state["file_path"] = local_paths[0]
            new_state["sftp_files"] = local_paths
            new_state["sftp_entity"] = sftp_entity
            new_state["vendor"] = "Vendor1"
        elif source == "rdbms":
            dataframe = _dummy_rdbms_dataframe()
        else:
            raise ValueError("Unsupported source. Expected 'sftp', 'adls_gen2', or 'rdbms'.")

        new_state["data"] = dataframe
        new_state["source_ingestion_status"] = "COMPLETED"
        new_state["source_row_count"] = len(dataframe)
        new_state["source_columns"] = list(dataframe.columns)
        if source in {"sftp", "adls_gen2"}:
            logger.info(
                "%s source ingestion completed: entity=%s rows=%d columns=%s",
                "ADLS Gen2" if source == "adls_gen2" else "SFTP",
                sftp_entity,
                len(dataframe),
                ", ".join(new_state["source_columns"][:8]),
                extra={
                    **log_context,
                    "event_type": "stage_end",
                    "sftp_entity": sftp_entity,
                    "source_row_count": len(dataframe),
                },
            )

        new_state.setdefault("metadata", {})
        return new_state

    except Exception as exc:
        new_state["status"] = "FAILED"
        new_state["source_ingestion_status"] = "FAILED"
        new_state["error"] = f"Source ingestion failed: {exc}"
        logger.error(
            "SFTP source ingestion failed for entity=%s: %s",
            sftp_entity,
            exc,
            extra={**log_context, "sftp_entity": sftp_entity},
        )
        return new_state
