from __future__ import annotations

from langgraph.graph import END, StateGraph

from sftp_nodes.column_profiling import sftp_column_profiling_node
from sftp_nodes.governance import sftp_feed_discovery_node, sftp_gate1_node, sftp_gate2_node
from sftp_nodes.ingestion import sftp_ingestion_node
from sftp_nodes.kpi_extraction import sftp_kpi_extraction_node
from sftp_nodes.metadata_discovery import sftp_metadata_discovery_node
from sftp_nodes.req_extraction import sftp_req_extraction_node
from sftp_nodes.semantic_enrichment import sftp_gate3_node, sftp_semantic_enrichment_node
from sftp_nodes.source_ingestion import source_ingestion_node
from state import Stage01State


def build_source_ingestion_graph():
    graph = StateGraph(Stage01State)

    graph.add_node("source_ingestion", source_ingestion_node)
    graph.add_node("ingestion", sftp_ingestion_node)
    graph.add_node("req_extraction", sftp_req_extraction_node)
    graph.add_node("kpi_extraction", sftp_kpi_extraction_node)
    graph.add_node("sftp_gate1", sftp_gate1_node)
    graph.add_node("feed_discovery", sftp_feed_discovery_node)
    graph.add_node("sftp_gate2", sftp_gate2_node)
    graph.add_node("sftp_metadata_discovery", sftp_metadata_discovery_node)
    graph.add_node("sftp_column_profiling", sftp_column_profiling_node)
    graph.add_node("sftp_semantic_enrichment", sftp_semantic_enrichment_node)
    graph.add_node("sftp_gate3", sftp_gate3_node)

    graph.set_entry_point("source_ingestion")

    def route_after_source_ingestion(state: Stage01State) -> str:
        if state.get("status") == "FAILED":
            return END
        return "ingestion"

    graph.add_conditional_edges("source_ingestion", route_after_source_ingestion)
    graph.add_edge("ingestion", "req_extraction")
    graph.add_edge("req_extraction", "kpi_extraction")
    graph.add_edge("kpi_extraction", "sftp_gate1")

    def route_after_sftp_gate1(state: Stage01State) -> str:
        if state.get("status") == "FAILED":
            return END
        gate1 = state.get("gate1") or {}
        if gate1.get("decision") != "APPROVED":
            return END
        return "feed_discovery"

    graph.add_conditional_edges("sftp_gate1", route_after_sftp_gate1)
    graph.add_edge("feed_discovery", "sftp_gate2")

    def route_after_sftp_gate2(state: Stage01State) -> str:
        if state.get("status") == "FAILED":
            return END
        gate2 = state.get("gate2") or {}
        if gate2.get("decision") != "APPROVED":
            return END
        return "sftp_metadata_discovery"

    graph.add_conditional_edges("sftp_gate2", route_after_sftp_gate2)
    graph.add_edge("sftp_metadata_discovery", "sftp_column_profiling")
    graph.add_edge("sftp_column_profiling", "sftp_semantic_enrichment")
    graph.add_edge("sftp_semantic_enrichment", "sftp_gate3")
    graph.add_edge("sftp_gate3", END)

    return graph.compile()


def main() -> None:
    app = build_source_ingestion_graph()
    initial_state: Stage01State = {"source": "sftp"}
    final_state = app.invoke(initial_state)

    print("Final state:")
    print(final_state)


if __name__ == "__main__":
    main()
