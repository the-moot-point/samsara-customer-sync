from __future__ import annotations

import logging

from .samsara_client import SamsaraClient

LOG = logging.getLogger(__name__)

MANAGED_BY_TAG = "ManagedBy:EncompassSync"
CANDIDATE_DELETE_TAG = "CandidateDelete"


def build_tag_index(client: SamsaraClient) -> dict[str, str]:
    """Return mapping of lowercased tag name -> tag id"""
    index: dict[str, str] = {}
    for t in client.list_tags():
        name = (t.get("name") or "").strip()
        if not name:
            continue
        tid = str(t.get("id") or t.get("tagId") or "")
        if not tid:
            continue
        index[name.lower()] = tid
    return index


def resolve_tag_id(index: dict[str, str], name: str) -> str | None:
    if not name:
        return None
    return index.get(name.lower())
