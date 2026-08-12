from __future__ import annotations

from langgraph.graph import END, StateGraph

from sftp_nodes.governance import sftp_gate1_node
from sftp_nodes.ingestion import sftp_ingestion_node
from sftp_nodes.kpi_extraction import sftp_kpi_extraction_node
from sftp_nodes.memory_check import sftp_memory_check_node
from sftp_nodes.req_extraction import sftp_req_extraction_node
from state import Stage01State


def build_source_ingestion_graph():
    """Build the ADLS pre-nomination graph with the same opening stages as database v2."""
    graph = StateGraph(Stage01State)
    graph.add_node("ingestion", sftp_ingestion_node)
    graph.add_node("memory", sftp_memory_check_node)
    graph.add_node("requirements", sftp_req_extraction_node)
    graph.add_node("kpis", sftp_kpi_extraction_node)
    graph.add_node("gate1", sftp_gate1_node)
    graph.set_entry_point("ingestion")
    graph.add_edge("ingestion", "memory")
    graph.add_edge("memory", "requirements")
    graph.add_edge("requirements", "kpis")
    graph.add_edge("kpis", "gate1")
    graph.add_edge("gate1", END)
    return graph.compile()
