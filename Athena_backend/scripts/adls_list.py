"""
List ADLS Gen2 file systems and (optionally) paths under a prefix.

Usage (PowerShell):
  $env:ADLS_ACCOUNT_URL="https://atheastorage.dfs.core.windows.net"
  $env:ADLS_FILE_SYSTEM="your-container-name"
  $env:ADLS_PREFIX="cash-project/Vendor1"   # optional
  python Athena_backend/scripts/adls_list.py

Auth:
  Uses DefaultAzureCredential. Make sure one of these works:
  - az login (Azure CLI)
  - Managed Identity
  - VS/VS Code signed-in, etc.
"""

from __future__ import annotations

import os
import sys
from typing import Optional


def _get_env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def main() -> int:
    try:
        from azure.identity import (
            AzureCliCredential,
            DefaultAzureCredential,
            DeviceCodeCredential,
            EnvironmentCredential,
        )
        from azure.storage.filedatalake import DataLakeServiceClient
    except Exception as exc:  # pragma: no cover
        print(f"Missing Azure libs. Install backend requirements. Error: {exc}")
        return 2

    account_url = _get_env("ADLS_ACCOUNT_URL", "https://atheastorage.dfs.core.windows.net")
    file_system = _get_env("ADLS_FILE_SYSTEM")
    prefix = _get_env("ADLS_PREFIX")

    print("ADLS_ACCOUNT_URL =", account_url)
    print("ADLS_FILE_SYSTEM =", file_system or "(not set)")
    print("ADLS_PREFIX      =", prefix or "(not set)")
    print("")

    # Prefer CLI credential because it's deterministic and fast when `az login` is set up.
    credential = None
    last_error = None
    for candidate in (
        (
            "EnvironmentCredential",
            lambda: EnvironmentCredential(),
        ),
        (
            "DeviceCodeCredential",
            lambda: DeviceCodeCredential(
                disable_automatic_authentication=False,
                prompt_callback=lambda *args, **kwargs: print(*args, *kwargs.values(), sep="\n", flush=True),
            ),
        ),
        ("AzureCliCredential", lambda: AzureCliCredential()),
        (
            "DefaultAzureCredential",
            lambda: DefaultAzureCredential(
                exclude_interactive_browser_credential=True,
                exclude_visual_studio_code_credential=True,
                exclude_shared_token_cache_credential=True,
            ),
        ),
    ):
        name, factory = candidate
        try:
            credential = factory()
            # Force auth early so we can show prompts deterministically.
            try:
                credential.get_token("https://storage.azure.com/.default")
            except Exception:
                # Some credential types lazily auth; DataLake client calls will surface errors.
                pass
            service_client = DataLakeServiceClient(account_url=account_url, credential=credential)
            print(f"Using credential: {name}")
            break
        except Exception as exc:
            last_error = exc
            credential = None
            continue

    if credential is None:
        print("Failed to create ADLS client:", type(last_error).__name__, last_error)
        print("Hint: run `az login` in this machine/user context, or set service principal env vars.")
        return 1

    list_failed = False
    try:
        print("File systems:")
        for fs in service_client.list_file_systems():
            print(" -", fs.name)
    except Exception as exc:
        list_failed = True
        print("Failed listing file systems:", type(exc).__name__, exc)
        if not file_system:
            print("Set ADLS_FILE_SYSTEM to test a specific container even without list permission.")
            return 1

    if not file_system:
        print("\nSet ADLS_FILE_SYSTEM to list paths.")
        return 0

    try:
        fs_client = service_client.get_file_system_client(file_system)
    except Exception as exc:
        print("Failed getting file system client:", type(exc).__name__, exc)
        return 1

    def list_paths(path_prefix: Optional[str]) -> None:
        label = path_prefix or "/"
        print(f"\nPaths under {label}:")
        count = 0
        # get_paths wants no leading slash; normalize gently
        norm = (path_prefix or "").lstrip("/")
        for p in fs_client.get_paths(path=norm):
            count += 1
            suffix = "/" if getattr(p, "is_directory", False) else ""
            print(" -", p.name + suffix)
            if count >= 200:
                print(" ... (truncated at 200)")
                break
        if count == 0:
            print(" (no paths found)")

    list_paths(prefix or None)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
