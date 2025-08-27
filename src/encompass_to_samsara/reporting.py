from __future__ import annotations

import csv
import json
import logging
import os
from dataclasses import dataclass, asdict
from typing import Any, Dict, List

from .transform import normalize_geofence

LOG = logging.getLogger(__name__)

@dataclass
class Action:
    at: str
    kind: str  # create|update|delete|quarantine|skip|error
    address_id: str | None
    encompass_id: str | None
    reason: str
    payload: dict | None = None
    diff: dict | None = None

def ensure_out_dir(out_dir: str) -> None:
    os.makedirs(out_dir, exist_ok=True)

def write_jsonl(path: str, actions: list[Action]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for a in actions:
            data = asdict(a)
            payload = data.get("payload")
            if isinstance(payload, dict):
                geo = normalize_geofence(payload.get("geofence"))
                if geo:
                    payload["geofence"] = geo
                    circle = geo.get("circle")
                    if isinstance(circle, dict) and "radiusMeters" in circle:
                        try:
                            circle["radiusMeters"] = int(circle["radiusMeters"])
                        except (TypeError, ValueError):
                            pass
            f.write(json.dumps(data, ensure_ascii=False) + "\n")

def write_csv(path: str, rows: list[dict], fieldnames: list[str]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in fieldnames})

def summarize(actions: list[Action]) -> dict:
    counts: dict[str, int] = {}
    for a in actions:
        counts[a.kind] = counts.get(a.kind, 0) + 1
    return counts
