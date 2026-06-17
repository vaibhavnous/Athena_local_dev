from __future__ import annotations

from nodes.kpi_extraction import build_kpi_extraction_node


_SFTP_KPI_EXTRACTION_NODE = None


def sftp_kpi_extraction_node(state):
    global _SFTP_KPI_EXTRACTION_NODE
    if _SFTP_KPI_EXTRACTION_NODE is None:
        _SFTP_KPI_EXTRACTION_NODE = build_kpi_extraction_node()
    return _SFTP_KPI_EXTRACTION_NODE(state)
