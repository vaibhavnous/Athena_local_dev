from __future__ import annotations

from nodes.req_extraction import build_req_extraction_node


_SFTP_REQ_EXTRACTION_NODE = None


def sftp_req_extraction_node(state):
    global _SFTP_REQ_EXTRACTION_NODE
    if _SFTP_REQ_EXTRACTION_NODE is None:
        _SFTP_REQ_EXTRACTION_NODE = build_req_extraction_node()
    return _SFTP_REQ_EXTRACTION_NODE(state)
