from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json
import os
import re
import time
from typing import Any, Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from utilis.db import ai_store_db_writer
from utilis.logger import logger


DEFAULT_COMPLIANCE_BACKEND_URL = "https://astra-compliance-hhgxb8hshua7ftdc.southindia-01.azurewebsites.net"
PRODUCTION_ENV_VALUES = {"prod", "production"}
REDACTED_VALUE = "[redacted]"
SECRET_KEY_PARTS = (
    "api_key",
    "apikey",
    "authorization",
    "connection_string",
    "credential",
    "password",
    "private_key",
    "secret",
    "token",
)
SECRET_TEXT_PATTERN = re.compile(
    r"([\"']?\b(password|passwd|pwd|secret|token|api[_-]?key|access[_-]?key|private[_-]?key|connection[_-]?string)\b"
    r"[\"']?\s*[:=]\s*[\"']?)([^\"'\s,;}]+)",
    re.IGNORECASE,
)


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    try:
        return max(minimum, int(os.getenv(name, str(default))))
    except ValueError:
        return default


def _is_production() -> bool:
    for name in ("ATHENA_ENV", "APP_ENV", "ENVIRONMENT", "ENV"):
        if os.getenv(name, "").strip().lower() in PRODUCTION_ENV_VALUES:
            return True
    return False


COMPLIANCE_EXECUTOR = ThreadPoolExecutor(max_workers=_env_int("COMPLIANCE_BACKGROUND_WORKERS", 2))


def _base_url() -> str:
    configured_url = os.getenv("COMPLIANCE_BACKEND_URL", "").strip().rstrip("/")
    if not configured_url:
        if _is_production():
            raise RuntimeError("COMPLIANCE_BACKEND_URL must be set before compliance egress is enabled in production.")
        configured_url = DEFAULT_COMPLIANCE_BACKEND_URL
    if _is_production() and not configured_url.lower().startswith("https://"):
        raise RuntimeError("COMPLIANCE_BACKEND_URL must use https:// in production.")
    return configured_url


def _compliance_egress_enabled() -> bool:
    return _env_bool("COMPLIANCE_EGRESS_ENABLED", True)


def _send_sample_values() -> bool:
    return _env_bool("COMPLIANCE_SEND_SAMPLE_VALUES", not _is_production())


def _store_full_artifacts() -> bool:
    return _env_bool("COMPLIANCE_STORE_FULL_ARTIFACTS", not _is_production())


def _timeout_seconds() -> int:
    return _env_int("COMPLIANCE_REQUEST_TIMEOUT_SECONDS", 300)


def _max_metadata_columns() -> int:
    return _env_int("COMPLIANCE_MAX_METADATA_COLUMNS", 50)


def _redact_text(value: str) -> str:
    return SECRET_TEXT_PATTERN.sub(lambda match: f"{match.group(1)}{REDACTED_VALUE}", value)


def _redact_for_storage(value: Any) -> Any:
    if _store_full_artifacts():
        return value
    if isinstance(value, dict):
        redacted: Dict[str, Any] = {}
        for key, item in value.items():
            key_text = str(key).lower()
            if key_text == "brd_text":
                redacted[key] = f"{REDACTED_VALUE} ({len(str(item or ''))} chars)"
            elif key_text == "sample_values":
                redacted[key] = [REDACTED_VALUE] if item else []
            elif any(part in key_text for part in SECRET_KEY_PARTS):
                redacted[key] = REDACTED_VALUE
            else:
                redacted[key] = _redact_for_storage(item)
        return redacted
    if isinstance(value, list):
        return [_redact_for_storage(item) for item in value]
    if isinstance(value, str):
        return _redact_text(value)
    return value


def _safe_remote_error_detail(detail: str) -> str:
    detail = _redact_text(detail.strip())
    if not detail or _is_production():
        return ""
    return f": {detail[:500]}"


def _safe_error_message(exc: Exception) -> str:
    if _is_production():
        return exc.__class__.__name__
    return _redact_text(str(exc))[:500]


def _json_request(method: str, path: str, body: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if not _compliance_egress_enabled():
        raise RuntimeError("Compliance egress is disabled by COMPLIANCE_EGRESS_ENABLED.")
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
    try:
        with urlopen(request, timeout=_timeout_seconds()) as response:
            raw = response.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"Compliance API {method} {path} failed with HTTP {exc.code}{_safe_remote_error_detail(detail)}"
        ) from exc
    except URLError as exc:
        raise RuntimeError(f"Compliance API {method} {path} failed: {_redact_text(str(exc.reason))}") from exc
    except TimeoutError as exc:
        raise RuntimeError(f"Compliance API {method} {path} timed out after {_timeout_seconds()} seconds") from exc


def _sample_values(raw_samples: Any) -> List[str]:
    if not _send_sample_values():
        return [REDACTED_VALUE]
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
                values.append(_redact_text(str(value)))
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
        "brd_text": _redact_text(str(state.get("brd_text") or "Compliance assessment for Athena pipeline run.")),
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
    if not _compliance_egress_enabled():
        raise RuntimeError("Compliance egress is disabled by COMPLIANCE_EGRESS_ENABLED.")
    payload = build_assessment_payload(state, column_profiles)
    response = _json_request("POST", "/api/assessments", payload)
    run_id = str(state.get("run_id") or "")
    if run_id:
        try:
            ai_store_db_writer(
                run_id=run_id,
                stage="Compliance Assessment",
                artifact_type="COMPLIANCE_ASSESSMENT_REQUEST",
                payload={"request": _redact_for_storage(payload), "response": _redact_for_storage(response)},
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
    review = _json_request("GET", f"/api/assessments/{assessment_id}/review")
    run_id = str(state.get("run_id") or "")
    if run_id:
        try:
            ai_store_db_writer(
                run_id=run_id,
                stage="Compliance Review",
                artifact_type="COMPLIANCE_REVIEW",
                payload=_redact_for_storage(review),
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
    if not _compliance_egress_enabled():
        raise RuntimeError("Compliance egress is disabled by COMPLIANCE_EGRESS_ENABLED.")
    assessment_id = str(state.get("compliance_assessment_id") or "").strip()
    if not assessment_id:
        raise ValueError("Compliance assessment is not ready for review.")
    response = _json_request("POST", f"/api/assessments/{assessment_id}/review", review_payload)
    run_id = str(state.get("run_id") or "")
    if run_id:
        try:
            ai_store_db_writer(
                run_id=run_id,
                stage="Compliance Review",
                artifact_type="COMPLIANCE_REVIEW_DECISION",
                payload={"request": _redact_for_storage(review_payload), "response": _redact_for_storage(response)},
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
    results = _json_request("GET", f"/api/assessments/{assessment_id}/results")
    run_id = str(state.get("run_id") or "")
    if run_id:
        try:
            ai_store_db_writer(
                run_id=run_id,
                stage="Compliance Results",
                artifact_type="COMPLIANCE_RESULTS",
                payload=_redact_for_storage(results),
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
    if not _compliance_egress_enabled():
        return {
            "compliance_assessment_status": "SKIPPED",
            "compliance_assessment_message": "Compliance assessment skipped because compliance egress is disabled.",
            "compliance_assessment_error": None,
        }
    try:
        _base_url()
    except RuntimeError as exc:
        message = _redact_text(str(exc))[:500]
        logger.warning("Compliance assessment skipped: %s", message, extra={"run_id": state.get("run_id"), "node": "compliance"})
        return {
            "compliance_assessment_status": "SKIPPED",
            "compliance_assessment_message": "Compliance assessment skipped because compliance backend is not configured.",
            "compliance_assessment_error": message,
        }
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
        message = _safe_error_message(exc)
        logger.warning("Compliance assessment creation failed: %s", message, extra={"run_id": state.get("run_id"), "node": "compliance"})
        update = {
            "compliance_assessment_status": "TIMED_OUT" if "timed out" in message.lower() else "FAILED",
            "compliance_assessment_error": message,
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
        message = _safe_error_message(exc)
        logger.warning("Compliance review fetch failed: %s", message, extra={"run_id": state.get("run_id"), "node": "compliance"})
        return {
            "compliance_review_status": "FAILED",
            "compliance_review_error": message,
        }
