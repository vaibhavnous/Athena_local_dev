from __future__ import annotations

import os
import re
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urlparse


CODE_ROOT_ENV = "ATHENA_ADLS_CODE_ROOT"
_STORAGE_LOCK = threading.RLock()


def _code_root_uri() -> str:
    return str(os.getenv(CODE_ROOT_ENV) or "").strip().rstrip("/")


def adls_script_storage_configured() -> bool:
    return bool(_code_root_uri())


def _safe_name(value: Any, *, fallback: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value or "").strip()).strip("._")
    return cleaned[:160] or fallback


@dataclass(frozen=True)
class _StorageRoot:
    uri: str
    account_url: str = ""
    file_system: str = ""
    base_path: str = ""
    local_path: Optional[Path] = None


def _parse_root(uri: str) -> _StorageRoot:
    value = str(uri or "").strip().rstrip("/")
    if not value:
        raise RuntimeError(f"{CODE_ROOT_ENV} is required for ADLS script storage.")
    parsed = urlparse(value)
    if parsed.scheme in {"abfs", "abfss"}:
        if "@" not in parsed.netloc:
            raise ValueError(f"Invalid ADLS URI: {uri!r}")
        file_system, host = parsed.netloc.split("@", 1)
        return _StorageRoot(
            uri=value,
            account_url=f"https://{host}",
            file_system=file_system,
            base_path=parsed.path.strip("/"),
        )
    if parsed.scheme in {"http", "https"} and parsed.netloc:
        parts = parsed.path.strip("/").split("/", 1)
        if not parts or not parts[0]:
            raise ValueError(f"Invalid ADLS HTTPS URI: {uri!r}")
        return _StorageRoot(
            uri=value,
            account_url=f"https://{parsed.netloc.replace('.blob.', '.dfs.')}",
            file_system=parts[0],
            base_path=parts[1] if len(parts) > 1 else "",
        )
    return _StorageRoot(uri=value, local_path=Path(value))


class _ObjectStore:
    def __init__(self, root_uri: str) -> None:
        self.root = _parse_root(root_uri)
        self._fs = None

    def _path(self, relative_path: str) -> str:
        return "/".join(part for part in (self.root.base_path, relative_path.strip("/")) if part)

    def child_uri(self, relative_path: str) -> str:
        return f"{self.root.uri}/{relative_path.strip('/')}"

    def _client(self):
        if self._fs is not None:
            return self._fs
        try:
            from azure.identity import ClientSecretCredential, DefaultAzureCredential
            from azure.storage.filedatalake import DataLakeServiceClient
        except Exception as exc:  # pragma: no cover - deployment dependency check
            raise RuntimeError("Missing azure-identity or azure-storage-file-datalake.") from exc

        tenant_id = str(os.getenv("AZURE_TENANT_ID") or "").strip()
        client_id = str(os.getenv("AZURE_CLIENT_ID") or "").strip()
        client_secret = str(os.getenv("AZURE_CLIENT_SECRET") or "").strip()
        credential = (
            ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)
            if tenant_id and client_id and client_secret
            else DefaultAzureCredential(exclude_interactive_browser_credential=True)
        )
        service = DataLakeServiceClient(account_url=self.root.account_url, credential=credential)
        self._fs = service.get_file_system_client(file_system=self.root.file_system)
        return self._fs

    @staticmethod
    def _not_found(exc: Exception) -> bool:
        return type(exc).__name__ == "ResourceNotFoundError" or (
            type(exc).__name__ == "HttpResponseError"
            and any(marker in str(exc).lower() for marker in ("not found", "does not exist", "blobnotfound", "pathnotfound"))
        )

    def read_text(self, relative_path: str) -> Optional[str]:
        try:
            if self.root.local_path is not None:
                path = self.root.local_path / relative_path
                return path.read_text(encoding="utf-8") if path.exists() else None
            file_client = self._client().get_file_client(self._path(relative_path))
            return file_client.download_file().readall().decode("utf-8")
        except Exception as exc:
            if self._not_found(exc):
                return None
            raise

    def write_text(self, relative_path: str, value: str) -> None:
        if self.root.local_path is not None:
            path = self.root.local_path / relative_path
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = path.with_name(f".{path.name}.tmp")
            temporary.write_text(value, encoding="utf-8")
            temporary.replace(path)
            return
        file_client = self._client().get_file_client(self._path(relative_path))
        file_client.upload_data(value.encode("utf-8"), overwrite=True)


def _target_layout(state: Dict[str, Any]) -> List[str]:
    run_id = _safe_name(state.get("run_id"), fallback="run")[:96]
    warehouse = str(state.get("target_warehouse") or "databricks").strip().lower()
    if warehouse == "snowflake":
        engine = str(state.get("execution_engine") or "native").strip().lower()
        return ["snowflake", "dbt" if engine == "dbt" else "native", run_id]
    return ["databricks", run_id]


def _layer_items(state: Dict[str, Any], layer: str) -> Iterable[Dict[str, Any]]:
    if layer == "metadata":
        review = state.get("metadata_ddl_review") or {}
        if isinstance(review, dict) and review.get("script_body"):
            yield {
                **review,
                "script_path": review.get("file_name") or "metadata_schema.sql",
                "script_body": review.get("script_body"),
            }
        return
    for item in state.get(f"{layer}_generation_results") or []:
        if isinstance(item, dict):
            yield item


def _script_body(item: Dict[str, Any]) -> str:
    body = str(item.get("script_body") or item.get("body") or "")
    if body:
        return body
    source_path = Path(str(item.get("script_path") or ""))
    return source_path.read_text(encoding="utf-8") if source_path.is_file() else ""


def _script_extension(state: Dict[str, Any], item: Dict[str, Any], layer: str) -> str:
    suffix = Path(str(item.get("script_path") or "")).suffix.lower()
    if suffix in {".py", ".sql", ".yml", ".yaml"}:
        return suffix
    if layer == "metadata" or str(state.get("target_warehouse") or "").lower() == "snowflake":
        return ".sql"
    return ".py"


def _script_name(state: Dict[str, Any], item: Dict[str, Any], layer: str, index: int) -> str:
    extension = _script_extension(state, item, layer)
    source_path = str(item.get("script_path") or "").strip()
    if source_path:
        return _safe_name(Path(source_path).name, fallback=f"{layer}_{index}{extension}")
    base = item.get("name") or item.get("table") or item.get("target_table") or item.get("kpi_name") or f"{layer}_{index}"
    name = _safe_name(base, fallback=f"{layer}_{index}")
    return name if name.lower().endswith(extension) else f"{name}{extension}"


def _relative_path(state: Dict[str, Any], layer: str, name: str) -> str:
    layout = _target_layout(state)
    parts = [*layout, "models", layer, name] if layout[:2] == ["snowflake", "dbt"] and layer != "metadata" else [*layout, layer, name]
    return "/".join(parts)


def persist_generated_scripts(run_id: str, state: Dict[str, Any], layer: str) -> None:
    normalized_layer = str(layer or "").strip().lower()
    if normalized_layer not in {"metadata", "bronze", "silver", "gold"}:
        raise ValueError(f"Unsupported generated script layer: {layer!r}")
    if not run_id or not adls_script_storage_configured():
        return
    artifact_state = {**(state or {}), "run_id": str(run_id)}
    with _STORAGE_LOCK:
        store = _ObjectStore(_code_root_uri())
        for index, item in enumerate(_layer_items(artifact_state, normalized_layer), start=1):
            body = _script_body(item)
            if not body:
                continue
            name = _script_name(artifact_state, item, normalized_layer, index)
            relative_path = _relative_path(artifact_state, normalized_layer, name)
            if store.read_text(relative_path) != body:
                store.write_text(relative_path, body)


def load_generated_scripts(run_id: str, state: Dict[str, Any], layer: str) -> Dict[str, Any]:
    artifact_state = {**(state or {}), "run_id": str(run_id)}
    scripts: List[Dict[str, Any]] = []
    with _STORAGE_LOCK:
        store = _ObjectStore(_code_root_uri())
        for index, item in enumerate(_layer_items(artifact_state, layer), start=1):
            name = _script_name(artifact_state, item, layer, index)
            relative_path = _relative_path(artifact_state, layer, name)
            body = store.read_text(relative_path)
            if body is None:
                continue
            scripts.append(
                {
                    **item,
                    "name": name,
                    "language": "python" if name.lower().endswith(".py") else "sql",
                    "layer": layer,
                    "uri": store.child_uri(relative_path),
                    "script_body": body,
                }
            )
    return {"run_id": str(run_id), "scripts": scripts}
