from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from state import Stage01State
from utilis.db import config, get_pipeline_connection
from utilis.logger import logger


def _log_bronze_file_ingestion(
    feed_id: str,
    target_table: str,
    source_file_path: str,
    record_count: int,
    status: str,
    error_message: str | None = None,
) -> None:
    db_conf = config["azure_sql"]
    schema = db_conf["pipeline_schema"]
    conn = get_pipeline_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            INSERT INTO [{schema}].[bronze_file_ingestion_log]
            (feed_id, target_table, source_file_path, record_count, status, started_at, completed_at, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            feed_id,
            target_table,
            source_file_path,
            record_count,
            status,
            datetime.now(timezone.utc).isoformat(),
            datetime.now(timezone.utc).isoformat(),
            error_message,
        )
        conn.commit()
    except Exception as exc:
        logger.warning("Bronze file ingestion log persistence skipped: %s", exc)
    finally:
        conn.close()


def sftp_bronze_ingestion_node(state: Stage01State) -> Stage01State:
    """
    Bronze ingestion placeholder for SFTP source.

    This node represents the pipeline step immediately after Layer 0 SFTP pull.
    It validates that landing files are available for Bronze ingestion and
    attaches Bronze-specific state to the pipeline.
    """

    new_state = state.copy()
    log_context = {
        "run_id": new_state.get("run_id", "unknown"),
        "node": "sftp_bronze_ingestion",
        "stage": "bronze_ingestion",
    }

    landing_path = str(new_state.get("landing_path") or "").strip()
    if not landing_path:
        logger.error(
            "SFTP Bronze ingestion failed: missing landing_path",
            extra=log_context,
        )
        candidate = new_state.get("candidate_feed", {})
        feed_id = str(candidate.get("feed_id") or new_state.get("feed_id") or new_state.get("run_id", "unknown"))
        vendor = str(candidate.get("vendor") or new_state.get("vendor") or "unknown")
        entity = str(candidate.get("entity") or new_state.get("entity") or "unknown")
        target_table = f"bronze.{vendor}_{entity}_raw"
        _log_bronze_file_ingestion(
            feed_id=feed_id,
            target_table=target_table,
            source_file_path=landing_path,
            record_count=0,
            status="FAILED",
            error_message="Missing landing_path from sftp_pull",
        )
        new_state["bronze_ingestion_status"] = "FAILED"
        new_state["error"] = "Missing landing_path from sftp_pull"
        new_state["status"] = "FAILED"
        return new_state

    landing_obj = Path(landing_path)
    if not landing_obj.exists() or not landing_obj.is_dir():
        logger.error(
            "SFTP Bronze ingestion failed: landing path is not available %s",
            landing_path,
            extra=log_context,
        )
        candidate = new_state.get("candidate_feed", {})
        feed_id = str(candidate.get("feed_id") or new_state.get("feed_id") or new_state.get("run_id", "unknown"))
        vendor = str(candidate.get("vendor") or new_state.get("vendor") or "unknown")
        entity = str(candidate.get("entity") or new_state.get("entity") or "unknown")
        target_table = f"bronze.{vendor}_{entity}_raw"
        _log_bronze_file_ingestion(
            feed_id=feed_id,
            target_table=target_table,
            source_file_path=landing_path,
            record_count=0,
            status="FAILED",
            error_message=f"Landing path does not exist: {landing_path}",
        )
        new_state["bronze_ingestion_status"] = "FAILED"
        new_state["error"] = f"Landing path does not exist: {landing_path}"
        new_state["status"] = "FAILED"
        return new_state

    # Prefer explicit pulled_files from state
    pulled_files: List[str] = new_state.get("pulled_files") or []
    files_pulled = int(new_state.get("files_pulled") or 0)

    candidate = new_state.get("candidate_feed", {})
    feed_id = str(candidate.get("feed_id") or new_state.get("feed_id") or new_state.get("run_id", "unknown"))
    vendor = str(candidate.get("vendor") or new_state.get("vendor") or "unknown")
    entity = str(candidate.get("entity") or new_state.get("entity") or "unknown")
    target_table = f"bronze.{vendor}_{entity}_raw"

    ready_files: List[str] = []

    # No pulled_files and no files_pulled => NO_NEW_FILES
    if not pulled_files and files_pulled == 0:
        logger.info("SFTP Bronze ingestion: no new files found", extra=log_context)
        try:
            _log_bronze_file_ingestion(
                feed_id=feed_id,
                target_table=target_table,
                source_file_path=landing_path,
                record_count=0,
                status="NO_NEW_FILES",
                error_message=None,
            )
        except Exception:
            logger.warning("Failed to write NO_NEW_FILES summary log", extra=log_context)

        new_state["bronze_ingestion_status"] = "NO_NEW_FILES"
        new_state["bronze_landing_path"] = landing_path
        new_state["bronze_file_count"] = 0
        new_state["bronze_target_table"] = target_table
        new_state["bronze_ready_files"] = []
        return new_state

    if not pulled_files and files_pulled > 0:
        # Unexpected: count present but no file list — treat as FAILED to force operator attention
        err = "files_pulled > 0 but pulled_files list missing from state"
        logger.error(err, extra=log_context)
        _log_bronze_file_ingestion(
            feed_id=feed_id,
            target_table=target_table,
            source_file_path=landing_path,
            record_count=0,
            status="FAILED",
            error_message=err,
        )
        new_state["bronze_ingestion_status"] = "FAILED"
        new_state["error"] = err
        new_state["status"] = "FAILED"
        return new_state

    # Validate each pulled file
    for f in pulled_files:
        try:
            fpath = Path(f)
            if not fpath.exists() or not fpath.is_file():
                raise FileNotFoundError(f"Pulled file not found: {f}")

            # Ensure file is under landing_path
            try:
                fpath.resolve().relative_to(landing_obj.resolve())
            except Exception:
                raise ValueError(f"Pulled file is not under landing_path: {f}")

            if fpath.stat().st_size <= 0:
                raise ValueError(f"Pulled file has zero size: {f}")

            # All validations passed for this file
            ready_files.append(str(fpath))
        except Exception as exc:
            logger.error("Validation failed for pulled file %s: %s", f, exc, extra=log_context)
            _log_bronze_file_ingestion(
                feed_id=feed_id,
                target_table=target_table,
                source_file_path=str(f),
                record_count=0,
                status="FAILED",
                error_message=str(exc),
            )
            new_state["bronze_ingestion_status"] = "FAILED"
            new_state["error"] = f"Validation failed for pulled file: {f} -> {exc}"
            new_state["status"] = "FAILED"
            return new_state

    # Log each ready file into bronze_file_ingestion_log
    for rf in ready_files:
        try:
            _log_bronze_file_ingestion(
                feed_id=feed_id,
                target_table=target_table,
                source_file_path=rf,
                record_count=0,
                status="READY_FOR_BRONZE",
                error_message=None,
            )
        except Exception:
            logger.warning("Failed to persist ready file log for %s", rf, extra=log_context)

    # Update state
    new_state["bronze_ingestion_status"] = "READY_FOR_BRONZE"
    new_state["bronze_target_table"] = target_table
    new_state["bronze_landing_path"] = landing_path
    new_state["bronze_file_count"] = len(ready_files)
    new_state["bronze_ready_files"] = ready_files
    logger.info(
        "SFTP Bronze ingestion ready: landing_path=%s ready_files=%d",
        landing_path,
        len(ready_files),
        extra=log_context,
    )
    return new_state
