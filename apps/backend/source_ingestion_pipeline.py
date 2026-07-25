from __future__ import annotations

from langgraph.graph import END, StateGraph

from sftp_nodes.bronze_ingestion import sftp_bronze_ingestion_node
from sftp_nodes.column_profiling import sftp_column_profiling_node
from sftp_nodes.feed_nomination import sftp_feed_nomination_node
from sftp_nodes.governance import sftp_feed_discovery_node, sftp_gate1_node, sftp_gate2_node
from sftp_nodes.ingestion import sftp_ingestion_node
from sftp_nodes.kpi_extraction import sftp_kpi_extraction_node
from sftp_nodes.metadata_discovery import file_metadata_discovery_node
from sftp_nodes.req_extraction import sftp_req_extraction_node
from sftp_nodes.review_gates import (
    bronze_validation_node,
    dq_validation_node,
    source_access_readiness_check_node,
    sftp_gate4_node,
    sftp_gate5_node,
)
from sftp_nodes.sftp_pull import sftp_pull_node
from sftp_nodes.semantic_enrichment import sftp_gate3_node, sftp_semantic_enrichment_node
from sftp_nodes.source_ingestion import source_ingestion_node
from sftp_nodes.bronze_code_generation import sftp_bronze_code_generation_node
from sftp_nodes.silver_code_generation import sftp_silver_code_generation_node
from sftp_nodes.gold_code_generation import sftp_gold_code_generation_node
from state import Stage01State


def _visible_stage(stage_key, runner):
    def run(state):
        from services.pipeline_runtime import run_with_minimum_stage_runtime

        return run_with_minimum_stage_runtime(stage_key, runner, state)

    return run


def build_source_ingestion_graph():
    graph = StateGraph(Stage01State)

    graph.add_node("source_ingestion", _visible_stage("discovery", source_ingestion_node))
    graph.add_node("ingestion_context_setup", _visible_stage("ingestion", sftp_ingestion_node))
    graph.add_node("req_extraction", _visible_stage("requirements", sftp_req_extraction_node))
    graph.add_node("kpi_extraction", _visible_stage("kpis", sftp_kpi_extraction_node))
    graph.add_node("sftp_gate1", sftp_gate1_node)
    graph.add_node("feed_discovery", sftp_feed_discovery_node)
    graph.add_node("feed_nomination", sftp_feed_nomination_node)
    graph.add_node("sftp_gate2", sftp_gate2_node)
    graph.add_node("file_metadata_discovery", _visible_stage("schema", file_metadata_discovery_node))
    graph.add_node("column_profiling", sftp_column_profiling_node)
    graph.add_node("semantic_enrichment", _visible_stage("enrichment", sftp_semantic_enrichment_node))
    graph.add_node("sftp_gate3", sftp_gate3_node)
    graph.add_node("source_access_readiness_check", source_access_readiness_check_node)
    graph.add_node("bronze_code_generation", sftp_bronze_code_generation_node)
    graph.add_node("sftp_gate4", sftp_gate4_node)
    graph.add_node("sftp_pull", sftp_pull_node)
    graph.add_node("bronze_ingestion", sftp_bronze_ingestion_node)
    graph.add_node("bronze_validation", bronze_validation_node)
    graph.add_node("silver_code_gen", sftp_silver_code_generation_node)
    graph.add_node("sftp_gate5", sftp_gate5_node)
    graph.add_node("dq_validation", dq_validation_node)
    graph.add_node("gold_code_gen", sftp_gold_code_generation_node)

    graph.set_entry_point("ingestion_context_setup")

    def route_after_source_ingestion(state: Stage01State) -> str:
        if state.get("status") == "FAILED":
            return END
        return "feed_discovery"

    graph.add_conditional_edges("source_ingestion", route_after_source_ingestion)
    graph.add_edge("ingestion_context_setup", "req_extraction")
    graph.add_edge("req_extraction", "kpi_extraction")
    graph.add_edge("kpi_extraction", "sftp_gate1")

    def route_after_sftp_gate1(state: Stage01State) -> str:
        if state.get("status") == "FAILED":
            return END
        gate1 = state.get("gate1") or {}
        if gate1.get("decision") != "APPROVED":
            return END
        return "source_ingestion"

    graph.add_conditional_edges("sftp_gate1", route_after_sftp_gate1)
    graph.add_edge("feed_discovery", "feed_nomination")
    graph.add_edge("feed_nomination", "sftp_gate2")

    def route_after_sftp_gate2(state: Stage01State) -> str:
        if state.get("status") == "FAILED":
            return END
        gate2 = state.get("gate2") or {}
        if gate2.get("decision") != "APPROVED":
            return END
        return "file_metadata_discovery"

    graph.add_conditional_edges("sftp_gate2", route_after_sftp_gate2)
    graph.add_edge("file_metadata_discovery", "column_profiling")
    graph.add_edge("column_profiling", "semantic_enrichment")
    graph.add_edge("semantic_enrichment", "sftp_gate3")
    graph.add_edge("sftp_gate3", "source_access_readiness_check")
    graph.add_edge("source_access_readiness_check", "bronze_code_generation")
    graph.add_edge("bronze_code_generation", "sftp_gate4")
    graph.add_edge("sftp_pull", "bronze_ingestion")
    graph.add_edge("bronze_ingestion", "bronze_validation")
    graph.add_edge("bronze_validation", "silver_code_gen")
    graph.add_edge("silver_code_gen", "sftp_gate5")
    graph.add_edge("dq_validation", "gold_code_gen")
    graph.add_edge("gold_code_gen", END)

    def route_after_sftp_gate3(state: Stage01State) -> str:
        if state.get("status") == "FAILED":
            return END
        if str(state.get("enrichment_review_decision") or "").upper() != "APPROVED":
            return END
        return "source_access_readiness_check"

    def route_after_sftp_gate4(state: Stage01State) -> str:
        if state.get("status") == "FAILED":
            return END
        decision = str((state.get("gate4") or {}).get("decision") or state.get("bronze_review_decision") or "").upper()
        if decision == "REGENERATE":
            return "bronze_code_generation"
        if decision != "APPROVED":
            return END
        if str(state.get("source") or "").lower() == "sftp":
            return "sftp_pull"
        return "bronze_ingestion"

    def route_after_bronze_ingestion(state: Stage01State) -> str:
        if state.get("status") == "FAILED":
            return END
        return "bronze_validation"

    def route_after_sftp_gate5(state: Stage01State) -> str:
        if state.get("status") == "FAILED":
            return END
        decision = str((state.get("gate5") or {}).get("decision") or state.get("silver_review_decision") or "").upper()
        if decision == "REGENERATE":
            return "silver_code_gen"
        if decision != "APPROVED":
            return END
        return "dq_validation"

    graph.add_conditional_edges("sftp_gate3", route_after_sftp_gate3)
    graph.add_conditional_edges("sftp_gate4", route_after_sftp_gate4)
    graph.add_conditional_edges("bronze_ingestion", route_after_bronze_ingestion)
    graph.add_conditional_edges("sftp_gate5", route_after_sftp_gate5)

    return graph.compile()


def main() -> None:
    app = build_source_ingestion_graph()
    initial_state: Stage01State = {"source": "sftp"}
    final_state = app.invoke(initial_state)

    print("Final state:")
    print(final_state)


if __name__ == "__main__":
    main()
