from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict

LOG = logging.getLogger(__name__)

DEFAULT_STATE = {"fingerprints": {}, "candidate_deletes": {}}

def load_state(path: str) -> dict:
    if not os.path.exists(path):
        return DEFAULT_STATE.copy()
    with open(path, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            return DEFAULT_STATE.copy()
    if not isinstance(data, dict):
        return DEFAULT_STATE.copy()
    # ensure keys
    data.setdefault("fingerprints", {})
    data.setdefault("candidate_deletes", {})
    return data

def save_state(path: str, state: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, sort_keys=True)
