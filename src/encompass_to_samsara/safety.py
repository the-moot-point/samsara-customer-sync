from __future__ import annotations

import csv
import logging
from datetime import UTC, datetime, timedelta

LOG = logging.getLogger(__name__)

def load_warehouses(path: str) -> tuple[set[str], set[str]]:
    """
    Return (ids, names) sets to never modify/delete.
    CSV headers: samsara_id,name
    YAML supported if file endswith .yaml/.yml
    """
    ids: set[str] = set()
    names: set[str] = set()
    if path.lower().endswith((".yaml", ".yml")):
        import yaml  # type: ignore
        with open(path, encoding="utf-8") as f:
            data = yaml.safe_load(f) or []
            if isinstance(data, dict):
                data = data.get("warehouses") or []
            for item in data:
                sid = str(item.get("samsara_id") or "").strip()
                nm = str(item.get("name") or "").strip()
                if sid:
                    ids.add(sid)
                if nm:
                    names.add(nm.lower())
    else:
        with open(path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for r in reader:
                sid = str(r.get("samsara_id") or "").strip()
                nm = str(r.get("name") or "").strip()
                if sid:
                    ids.add(sid)
                if nm:
                    names.add(nm.lower())
    return ids, names

def is_warehouse(address: dict, warehouse_ids: set[str], warehouse_names_lc: set[str]) -> bool:
    aid = str(address.get("id") or "")
    aname = str(address.get("name") or "").lower()
    if aid and aid in warehouse_ids:
        return True
    if aname and aname in warehouse_names_lc:
        return True
    return False

def is_managed(address: dict, managed_tag_id: str | None) -> bool:
    ext = address.get("externalIds") or {}
    if ext.get("encompass_id") or ext.get("ENCOMPASS_ID") or ext.get("EncompassId"):
        return True
    # Look for ManagedBy tag
    if managed_tag_id:
        tags = address.get("tagIds") or address.get("tags") or []
        tag_ids: list[str] = []
        if isinstance(tags, list):
            for t in tags:
                if isinstance(t, dict):
                    tid = t.get("id") or t.get("tagId")
                    if tid:
                        tag_ids.append(str(tid))
                else:
                    tag_ids.append(str(t))
        if managed_tag_id in tag_ids:
            return True
    return False

def now_utc_iso() -> str:
    return datetime.utcnow().replace(tzinfo=UTC).isoformat()

def eligible_for_hard_delete(addr_id: str, state: dict, retention_days: int) -> bool:
    cand = (state.get("candidate_deletes") or {}).get(str(addr_id))
    if not cand:
        return False
    try:
        ts = datetime.fromisoformat(cand)
    except Exception:
        return False
    return (datetime.utcnow().replace(tzinfo=UTC) - ts) >= timedelta(days=retention_days)
