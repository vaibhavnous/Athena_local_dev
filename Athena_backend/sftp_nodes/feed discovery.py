import json
import os

import pandas as pd


def call_llm_for_semantics(columns):
    """
    LLM helper function.
    Replace this with your actual LLM call (OpenAI / Azure / etc.).
    """
    _prompt = f"""
    Given these dataset columns:

    {columns}

    Identify:
    1. Dataset type (transactions, employee, invoice, etc.)
    2. Key fields (primary identifiers)
    3. Numeric measure columns (like amount, salary)
    """
    return {
        "dataset_type": "unknown",
        "primary_keys": [],
        "measures": [],
    }


def feed_discovery_node(state):
    """
    Phase 1: Discovery (hybrid)
    """
    file_path = state.get("file_path")
    if not file_path:
        raise ValueError("file_path missing")

    parts = file_path.replace("\\", "/").split("/")
    try:
        vendor = parts[-3]
        entity = parts[-2]
        file_name = parts[-1]
    except Exception as exc:
        raise ValueError("Invalid folder structure") from exc

    _, ext = os.path.splitext(file_name)
    ext = ext.lower()
    if ext == ".csv":
        file_format = "csv"
    elif ext == ".json":
        file_format = "json"
    else:
        file_format = "unknown"

    try:
        if file_format == "csv":
            df = pd.read_csv(file_path, nrows=50)
        elif file_format == "json":
            with open(file_path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
            df = pd.DataFrame(data[:50]) if isinstance(data, list) else pd.DataFrame([data])
        else:
            raise ValueError("Unsupported format")
    except Exception as exc:
        raise RuntimeError(f"Failed to read file: {exc}") from exc

    columns = [col.lower() for col in df.columns]
    llm_output = call_llm_for_semantics(columns)
    dataset_type = llm_output.get("dataset_type", entity)
    primary_keys = llm_output.get("primary_keys", [])
    measures = llm_output.get("measures", [])

    candidate_feed = {
        "feed_id": f"{vendor}_{entity}",
        "vendor": vendor,
        "entity": entity,
        "semantic_type": dataset_type,
        "format": file_format,
        "file_name": file_name,
        "file_path": file_path,
        "columns": columns,
        "sample_row_count": len(df),
        "primary_keys": primary_keys,
        "measures": measures,
        "source": state.get("source", "sftp"),
        "status": "CANDIDATE",
    }

    state["candidate_feed"] = candidate_feed
    return state
