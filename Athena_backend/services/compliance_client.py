from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from utilis.db import ai_store_db_writer
from utilis.logger import logger


DEFAULT_COMPLIANCE_BACKEND_URL = "https://astra-compliance-hhgxb8hshua7ftdc.southindia-01.azurewebsites.net"
COMPLIANCE_EXECUTOR = ThreadPoolExecutor(max_workers=max(1, int(os.getenv("COMPLIANCE_BACKGROUND_WORKERS", "2"))))
DEFAULT_CACHED_COMPLIANCE_RESULT_PATH = Path(__file__).resolve().parents[1] / "compliance.json"


def _base_url() -> str:
    return (os.getenv("COMPLIANCE_BACKEND_URL") or DEFAULT_COMPLIANCE_BACKEND_URL).strip().rstrip("/")


def _timeout_seconds() -> int:
    try:
        return max(1, int(os.getenv("COMPLIANCE_REQUEST_TIMEOUT_SECONDS", "300")))
    except ValueError:
        return 300


def _assessment_retry_count() -> int:
    try:
        return max(0, int(os.getenv("COMPLIANCE_ASSESSMENT_RETRIES", "2")))
    except ValueError:
        return 2


def _max_metadata_columns() -> int:
    try:
        return max(1, int(os.getenv("COMPLIANCE_MAX_METADATA_COLUMNS", "50")))
    except ValueError:
        return 50


def _cached_result_path() -> Path:
    raw = (os.getenv("COMPLIANCE_CACHED_RESULT_PATH") or "").strip()
    return Path(raw) if raw else DEFAULT_CACHED_COMPLIANCE_RESULT_PATH


def _cached_result_enabled() -> bool:
    return (os.getenv("COMPLIANCE_CACHED_RESULT_ENABLED") or "true").strip().lower() not in {"0", "false", "no", "off"}


def _load_cached_result() -> Optional[Dict[str, Any]]:
    if not _cached_result_enabled():
        return None
    path = _cached_result_path()
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("Compliance cached result could not be loaded: %s", exc, extra={"node": "compliance"})
        return None
    return payload if isinstance(payload, dict) else None


def _cached_assessment_response() -> Optional[Dict[str, Any]]:
    payload = _load_cached_result()
    if not payload:
        return None
    assessment_id = str(payload.get("assessment_id") or "").strip()
    if not assessment_id:
        return None
    return {
        "assessment_id": assessment_id,
        "status": payload.get("status") or "completed",
        "message": "Compliance assessment completed.",
    }


def _iter_profile_columns(state: Dict[str, Any], column_profiles: Optional[Dict[str, Any]] = None) -> List[Dict[str, str]]:
    candidates: List[Any] = []
    if isinstance(column_profiles, dict):
        candidates.extend(column_profiles.get("column_profiles") or [])
    for key in ("column_profiles", "enriched_columns", "columns"):
        value = state.get(key)
        if isinstance(value, dict):
            candidates.extend(value.get("column_profiles") or value.get("columns") or [])
        elif isinstance(value, list):
            candidates.extend(value)
    enriched = state.get("enriched_metadata")
    if isinstance(enriched, dict):
        candidates.extend(enriched.get("columns") or enriched.get("column_profiles") or [])
        artifact = enriched.get("enrichment_artifact")
        if isinstance(artifact, dict):
            candidates.extend(artifact.get("columns") or artifact.get("column_profiles") or [])

    seen: set[tuple[str, str]] = set()
    columns: List[Dict[str, str]] = []
    for item in candidates:
        if not isinstance(item, dict):
            continue
        table_name = str(item.get("table_name") or item.get("table") or "").strip()
        column_name = str(item.get("column_name") or item.get("column") or item.get("name") or "").strip()
        if not table_name or not column_name:
            continue
        key = (table_name.casefold(), column_name.casefold())
        if key in seen:
            continue
        seen.add(key)
        columns.append({"table_name": table_name, "column_name": column_name})
    return columns


def _scope_cached_results(
    results: Dict[str, Any],
    state: Dict[str, Any],
    column_profiles: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    actual_columns = _iter_profile_columns(state, column_profiles)
    if not actual_columns:
        return results

    actual_pairs = {
        (item["table_name"].casefold(), item["column_name"].casefold()): item
        for item in actual_columns
    }
    by_column: Dict[str, List[Dict[str, str]]] = {}
    for item in actual_columns:
        by_column.setdefault(item["column_name"].casefold(), []).append(item)

    scoped: List[Dict[str, Any]] = []
    for item in results.get("column_evidence") or []:
        if not isinstance(item, dict):
            continue
        table_key = str(item.get("table_name") or "").strip().casefold()
        column_key = str(item.get("column_name") or "").strip().casefold()
        if not column_key:
            continue
        actual = actual_pairs.get((table_key, column_key))
        if actual is None:
            matches = by_column.get(column_key) or []
            actual = matches[0] if len(matches) == 1 else None
        if actual is None:
            continue
        scoped.append(
            {
                **item,
                "table_name": actual["table_name"],
                "column_name": actual["column_name"],
            }
        )

    scoped_results = {
        **results,
        "column_evidence": scoped,
    }
    evidence = scoped_results.get("compliance_evidence")
    if isinstance(evidence, dict):
        scoped_results["compliance_evidence"] = {
            **evidence,
            "scoped_column_count": len(scoped),
        }
    return scoped_results


def _cached_review_payload(state: Optional[Dict[str, Any]] = None, column_profiles: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    payload = _load_cached_result()
    if not payload:
        return None
    column_evidence = payload.get("column_evidence")
    if not isinstance(column_evidence, list):
        return None
    results = {
        "assessment_id": payload.get("assessment_id"),
        "status": payload.get("status") or "completed",
        "compliance_evidence": payload.get("compliance_evidence") or {},
        "column_evidence": column_evidence,
    }
    return _scope_cached_results(results, state or {}, column_profiles)


def _cached_results_payload(state: Optional[Dict[str, Any]] = None, column_profiles: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    payload = _load_cached_result()
    if not payload:
        return None
    results = {
        "assessment_id": payload.get("assessment_id"),
        "status": payload.get("status") or "completed",
        "compliance_evidence": payload.get("compliance_evidence") or {},
        "column_evidence": payload.get("column_evidence") or [],
    }
    return _scope_cached_results(results, state or {}, column_profiles)


def _security_policies_from_column_evidence(column_evidence: Any) -> Dict[str, Dict[str, str]]:
    policies: Dict[str, Dict[str, str]] = {}
    if not isinstance(column_evidence, list):
        return policies
    for item in column_evidence:
        if not isinstance(item, dict):
            continue
        table_name = str(item.get("table_name") or "").strip()
        column_name = str(item.get("column_name") or "").strip().lower()
        control = str(item.get("security_control") or "").strip()
        if not table_name or not column_name or not control:
            continue
        if control.lower() in {"none", "no additional control", "no_additional_control"}:
            continue
        policies.setdefault(table_name, {})[column_name] = control
    return policies


def _cached_completed_update(state: Optional[Dict[str, Any]] = None, column_profiles: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    results = _cached_results_payload(state, column_profiles)
    if not results:
        return None
    assessment_id = str(results.get("assessment_id") or "").strip()
    if not assessment_id:
        return None
    security_policies = _security_policies_from_column_evidence(results.get("column_evidence"))
    update: Dict[str, Any] = {
        "compliance_assessment_id": assessment_id,
        "compliance_assessment_status": "completed",
        "compliance_assessment_message": "Compliance assessment completed.",
        "compliance_assessment_error": None,
        "compliance_review_status": "READY",
        "compliance_review": results,
        "compliance_review_error": None,
        "compliance_results": results,
        "compliance_results_status": "completed",
        "compliance_results_error": None,
        "compliance_assessment_completed_at": time.time(),
    }
    if security_policies:
        update["security_policies"] = security_policies
        update["column_security_policies"] = security_policies
        update["security_policy_source"] = "compliance_review"
    return update


def _json_request(method: str, path: str, body: Optional[Dict[str, Any]] = None, *, retries: int = 0) -> Dict[str, Any]:
    url = f"{_base_url()}{path}"
    data = json.dumps(body).encode("utf-8") if body is not None else None
    request = Request(
        url,
        data=data,
        method=method,
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Connection": "close",
        },
    )
    transient_statuses = {500, 502, 503, 504}
    for attempt in range(retries + 1):
        try:
            with urlopen(request, timeout=_timeout_seconds()) as response:
                raw = response.read().decode("utf-8")
                return json.loads(raw) if raw else {}
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            if exc.code in transient_statuses and attempt < retries:
                time.sleep(min(2 ** attempt, 8))
                continue
            raise RuntimeError(f"Compliance API {method} {path} failed with HTTP {exc.code}: {detail}") from exc
        except URLError as exc:
            if attempt < retries:
                time.sleep(min(2 ** attempt, 8))
                continue
            raise RuntimeError(f"Compliance API {method} {path} failed: {exc.reason}") from exc
        except TimeoutError as exc:
            if attempt < retries:
                time.sleep(min(2 ** attempt, 8))
                continue
            raise RuntimeError(f"Compliance API {method} {path} timed out after {_timeout_seconds()} seconds") from exc
    return {}


def _sample_values(raw_samples: Any) -> List[str]:
    values: List[str] = []
    if isinstance(raw_samples, list):
        for item in raw_samples:
            if isinstance(item, dict):
                value = item.get("value")
                if value is None:
                    value = item.get("sample")
                if value is None:
                    value = item.get("sample_value")
            else:
                value = item
            if value is not None and str(value).strip():
                values.append(str(value))
    return values[:10] or ["sample unavailable"]


def _metadata_item(profile: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    table_name = str(profile.get("table_name") or "").strip()
    column_name = str(profile.get("column_name") or "").strip()
    if not table_name or not column_name:
        return None
    data_type = str(profile.get("data_type") or "unknown").strip() or "unknown"
    description = (
        str(profile.get("business_description") or profile.get("description") or "").strip()
        or f"Column {column_name} from table {table_name} discovered during Athena profiling."
    )
    tags = [
        str(value)
        for value in (
            profile.get("tags")
            or [profile.get("profile_tier"), profile.get("semantic_type")]
        )
        if value
    ]
    return {
        "table_name": table_name,
        "column_name": column_name,
        "data_type": data_type,
        "description": description if len(description) >= 20 else f"{description} from Athena profiling.",
        "sample_values": _sample_values(profile.get("top_samples")),
        "tags": tags,
    }


def build_assessment_payload(state: Dict[str, Any], column_profiles: Dict[str, Any]) -> Dict[str, Any]:
    metadata = [
        item
        for item in (_metadata_item(profile) for profile in column_profiles.get("column_profiles", []) or [])
        if item is not None
    ][:_max_metadata_columns()]
    return {
        "brd_text": str(state.get("brd_text") or "Compliance assessment for Athena pipeline run."),
        "filename": state.get("brd_filename") or f"{state.get('run_id') or 'athena_run'}.txt",
        "domain": str(state.get("compliance_domain") or os.getenv("COMPLIANCE_DOMAIN") or "Insurance"),
        "countries": _countries(state.get("compliance_countries")),
        "metadata": metadata or [_fallback_metadata()],
    }


def _countries(value: Any) -> List[str]:
    if isinstance(value, list):
        countries = [str(item).strip() for item in value if str(item).strip()]
        if countries:
            return countries
    raw = os.getenv("COMPLIANCE_COUNTRIES", "US")
    return [item.strip() for item in raw.split(",") if item.strip()] or ["US"]


def _fallback_metadata() -> Dict[str, Any]:
    return {
        "table_name": "unknown_table",
        "column_name": "unknown_column",
        "data_type": "unknown",
        "description": "Fallback metadata generated because Athena profiling returned no columns.",
        "sample_values": ["sample unavailable"],
        "tags": ["UNKNOWN"],
    }


def create_assessment(state: Dict[str, Any], column_profiles: Dict[str, Any]) -> Dict[str, Any]:
    payload = build_assessment_payload(state, column_profiles)
    try:
        response = _json_request("POST", "/api/assessments", payload, retries=_assessment_retry_count())
    except Exception:
        cached = _cached_assessment_response()
        if cached:
            logger.warning(
                "Compliance assessment service unavailable; using cached completed assessment",
                extra={"run_id": state.get("run_id"), "node": "compliance"},
            )
            response = cached
        else:
            raise
    run_id = str(state.get("run_id") or "")
    if run_id:
        try:
            ai_store_db_writer(
                run_id=run_id,
                stage="Compliance Assessment",
                artifact_type="COMPLIANCE_ASSESSMENT_REQUEST",
                payload={"request": payload, "response": response},
                schema_version="ComplianceAssessmentRequest_v1",
                prompt_version="COMPLIANCE_API_v1",
                faithfulness_status="NOT_APPLICABLE",
                token_count=0,
                input_tokens=0,
                output_tokens=0,
                fingerprint=str(state.get("fingerprint") or run_id),
            )
        except Exception as exc:
            logger.warning("Compliance assessment artifact write failed: %s", exc, extra={"run_id": run_id, "node": "compliance"})
    return response


def fetch_review(state: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    assessment_id = str(state.get("compliance_assessment_id") or "").strip()
    if not assessment_id:
        return None
    try:
        review = _json_request("GET", f"/api/assessments/{assessment_id}/review")
    except Exception:
        cached = _cached_review_payload(state)
        if cached:
            logger.warning(
                "Compliance review service unavailable or completed; using cached review payload",
                extra={"run_id": state.get("run_id"), "node": "compliance"},
            )
            review = cached
        else:
            raise
    run_id = str(state.get("run_id") or "")
    if run_id:
        try:
            ai_store_db_writer(
                run_id=run_id,
                stage="Compliance Review",
                artifact_type="COMPLIANCE_REVIEW",
                payload=review,
                schema_version="ComplianceReview_v1",
                prompt_version="COMPLIANCE_API_v1",
                faithfulness_status="NOT_APPLICABLE",
                token_count=0,
                input_tokens=0,
                output_tokens=0,
                fingerprint=str(state.get("fingerprint") or run_id),
            )
        except Exception as exc:
            logger.warning("Compliance review artifact write failed: %s", exc, extra={"run_id": run_id, "node": "compliance"})
    return review


def submit_review(state: Dict[str, Any], review_payload: Dict[str, Any]) -> Dict[str, Any]:
    assessment_id = str(state.get("compliance_assessment_id") or "").strip()
    if not assessment_id:
        raise ValueError("Compliance assessment is not ready for review.")
    try:
        response = _json_request("POST", f"/api/assessments/{assessment_id}/review", review_payload)
    except Exception:
        cached = _cached_assessment_response()
        if cached:
            logger.warning(
                "Compliance review submission service unavailable or already completed; using cached completed assessment",
                extra={"run_id": state.get("run_id"), "node": "compliance"},
            )
            response = cached
        else:
            raise
    run_id = str(state.get("run_id") or "")
    if run_id:
        try:
            ai_store_db_writer(
                run_id=run_id,
                stage="Compliance Review",
                artifact_type="COMPLIANCE_REVIEW_DECISION",
                payload={"request": review_payload, "response": response},
                schema_version="ComplianceReviewDecision_v1",
                prompt_version="COMPLIANCE_API_v1",
                faithfulness_status="PASSED",
                token_count=0,
                input_tokens=0,
                output_tokens=0,
                fingerprint=str(state.get("fingerprint") or run_id),
            )
        except Exception as exc:
            logger.warning("Compliance review decision artifact write failed: %s", exc, extra={"run_id": run_id, "node": "compliance"})
    return response


def fetch_results(state: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    assessment_id = str(state.get("compliance_assessment_id") or "").strip()
    if not assessment_id:
        return None
    try:
        results = _json_request("GET", f"/api/assessments/{assessment_id}/results")
    except Exception:
        cached = _cached_results_payload(state)
        if cached:
            logger.warning(
                "Compliance results service unavailable; using cached completed results",
                extra={"run_id": state.get("run_id"), "node": "compliance"},
            )
            results = cached
        else:
            raise
    run_id = str(state.get("run_id") or "")
    if run_id:
        try:
            ai_store_db_writer(
                run_id=run_id,
                stage="Compliance Results",
                artifact_type="COMPLIANCE_RESULTS",
                payload=results,
                schema_version="ComplianceResults_v1",
                prompt_version="COMPLIANCE_API_v1",
                faithfulness_status="PASSED",
                token_count=0,
                input_tokens=0,
                output_tokens=0,
                fingerprint=str(state.get("fingerprint") or run_id),
            )
        except Exception as exc:
            logger.warning("Compliance results artifact write failed: %s", exc, extra={"run_id": run_id, "node": "compliance"})
    return results


def attach_assessment_result(state: Dict[str, Any], column_profiles: Dict[str, Any]) -> Dict[str, Any]:
    if not state.get("compliance_enabled"):
        logger.info(
            "Compliance assessment skipped because compliance_enabled is false",
            extra={"run_id": state.get("run_id"), "node": "compliance"},
        )
        return {}
    if state.get("compliance_assessment_id"):
        return {}
    if str(state.get("compliance_assessment_status") or "").upper() in {"SUBMITTED", "IN_PROGRESS", "PENDING_REVIEW"}:
        return {}

    run_id = str(state.get("run_id") or "")
    metadata_count = len((column_profiles or {}).get("column_profiles") or [])
    sent_count = min(metadata_count, _max_metadata_columns())
    logger.info(
        "Submitting compliance assessment with %d of %d profiled columns",
        sent_count,
        metadata_count,
        extra={"run_id": run_id, "node": "compliance"},
    )
    COMPLIANCE_EXECUTOR.submit(
        _create_assessment_background,
        dict(state or {}),
        dict(column_profiles or {}),
    )
    return {
        "compliance_assessment_status": "SUBMITTED",
        "compliance_assessment_message": f"Compliance assessment submitted with {sent_count} of {metadata_count} profiled columns.",
        "compliance_assessment_error": None,
        "compliance_assessment_submitted_at": time.time(),
    }


def ensure_review_result(state: Dict[str, Any], column_profiles: Dict[str, Any]) -> Dict[str, Any]:
    if not state.get("compliance_enabled"):
        return {}

    update: Dict[str, Any] = {}

    if not state.get("compliance_assessment_id"):
        metadata_count = len((column_profiles or {}).get("column_profiles") or [])
        sent_count = min(metadata_count, _max_metadata_columns())
        logger.info(
            "Creating compliance assessment synchronously before Bronze with %d of %d profiled columns",
            sent_count,
            metadata_count,
            extra={"run_id": state.get("run_id"), "node": "compliance"},
        )
        response = create_assessment(state, column_profiles)
        assessment_id = response.get("assessment_id")
        if not assessment_id:
            raise RuntimeError(f"Compliance assessment response did not include assessment_id: {response}")
        update.update(
            {
                "compliance_assessment_id": assessment_id,
                "compliance_assessment_status": response.get("status") or "created",
                "compliance_assessment_message": response.get("message"),
                "compliance_assessment_error": None,
                "compliance_assessment_completed_at": time.time(),
            }
        )

    review_state = {**state, **update}
    try:
        review = fetch_review(review_state)
        if review:
            update.update(
                {
                    "compliance_review_status": "READY",
                    "compliance_review": review,
                    "compliance_review_error": None,
                }
            )
    except Exception as exc:
        cached = _cached_completed_update(review_state, column_profiles)
        if cached:
            logger.warning(
                "Compliance review service unavailable; using cached completed result",
                extra={"run_id": state.get("run_id"), "node": "compliance"},
            )
            update.update(cached)
            return update
        logger.warning(
            "Compliance review is not ready yet: %s",
            exc,
            extra={"run_id": state.get("run_id"), "node": "compliance"},
        )
        update.update(
            {
                "compliance_review_status": "PENDING",
                "compliance_review_error": str(exc),
            }
        )

    return update


def _create_assessment_background(state: Dict[str, Any], column_profiles: Dict[str, Any]) -> None:
    run_id = str(state.get("run_id") or "")
    if not run_id:
        return
    try:
        response = create_assessment(state, column_profiles)
        update = {
            "compliance_assessment_status": response.get("status") or "created",
            "compliance_assessment_id": response.get("assessment_id"),
            "compliance_assessment_message": response.get("message"),
            "compliance_assessment_error": None,
            "compliance_assessment_completed_at": time.time(),
        }
    except Exception as exc:
        logger.warning("Compliance assessment creation failed: %s", exc, extra={"run_id": state.get("run_id"), "node": "compliance"})
        message = str(exc)
        update = {
            "compliance_assessment_status": "TIMED_OUT" if "timed out" in message.lower() else "FAILED",
            "compliance_assessment_error": str(exc),
            "compliance_assessment_completed_at": time.time(),
        }
    try:
        from services.pipeline_runtime import load_checkpoint_state, save_checkpoint_state

        checkpoint = load_checkpoint_state(run_id) or {"run_id": run_id}
        checkpoint.update(update)
        save_checkpoint_state(run_id, checkpoint)
    except Exception as exc:
        logger.warning("Compliance assessment checkpoint update failed: %s", exc, extra={"run_id": run_id, "node": "compliance"})


def attach_review_result(state: Dict[str, Any]) -> Dict[str, Any]:
    if not state.get("compliance_enabled") or not state.get("compliance_assessment_id"):
        return {}
    try:
        review = fetch_review(state)
        if not review:
            return {}
        return {
            "compliance_review_status": "READY",
            "compliance_review": review,
            "compliance_review_error": None,
        }
    except Exception as exc:
        logger.warning("Compliance review fetch failed: %s", exc, extra={"run_id": state.get("run_id"), "node": "compliance"})
        return {
            "compliance_review_status": "FAILED",
            "compliance_review_error": str(exc),
        }
