from __future__ import annotations

from nodes.ingestion import ingestion_node as _shared_ingestion_node
from state import Stage01State


def sftp_ingestion_node(state: Stage01State) -> Stage01State:
    return _shared_ingestion_node(state)
