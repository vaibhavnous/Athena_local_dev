from __future__ import annotations

from nodes.memory_lookup import memory_lookup_node
from state import Stage01State


def sftp_memory_check_node(state: Stage01State) -> Stage01State:
    return memory_lookup_node(state)
