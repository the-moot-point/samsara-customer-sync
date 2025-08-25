from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from .samsara_client import SamsaraClient
from .tags import MANAGED_BY_TAG, CANDIDATE_DELETE_TAG, build_tag_index, resolve_tag_id
from .transform import read_encompass_csv, to_address_payload, diff_address, compute_fingerprint, validate_lat_lon
from .matcher import index_addresses_by_external_id, probable_match
from .safety import load_warehouses, is_warehouse, is_managed, now_utc_iso, eligible_for_hard_delete
from .state import load_state, save_state
from .reporting import Action, ensure_out_dir, write_jsonl, write_csv, summarize

LOG = logging.getLogger(__name__)


def _ext_encompass_id(ext: Dict[str, Any]) -> str | None:
    """Return the Encompass external ID if present."""
    return ext.get("encompass_id") or ext.get("ENCOMPASS_ID") or ext.get("EncompassId")

def run_full(
    client: SamsaraClient,
    *, encompass_csv: str, warehouses_path: str, out_dir: str,
    radius_m: int, apply: bool, retention_days: int, confirm_delete: bool,
) -> None:
    ensure_out_dir(out_dir)
    actions: list[Action] = []
    errors_rows: list[dict] = []
    dry_rows: list[dict] = []
    dup_rows: list[dict] = []

    # Load state
    state_path = f"{out_dir.rstrip('/')}/state.json"
    state = load_state(state_path)

    # Read warehouses
    wh_ids, wh_names = load_warehouses(warehouses_path)

    # Read Encompass
    src_rows = read_encompass_csv(encompass_csv)
    # Check duplicate encompass ids
    seen_eids: dict[str, int] = {}
    for r in src_rows:
        if not r.encompass_id:
            errors_rows.append({"error": "missing_encompass_id", "row_name": r.name})
            continue
        seen_eids[r.encompass_id] = seen_eids.get(r.encompass_id, 0) + 1
    for eid, count in seen_eids.items():
        if count > 1:
            dup_rows.append({"type": "encompass_duplicate", "encompass_id": eid, "count": count})

    # Fetch Samsara
    tags_index = build_tag_index(client)
    managed_tag_id = resolve_tag_id(tags_index, MANAGED_BY_TAG)
    candidate_tag_id = resolve_tag_id(tags_index, CANDIDATE_DELETE_TAG)

    samsara_addrs = client.list_addresses()
    # Index by encompass_id and by id
    by_eid = index_addresses_by_external_id(samsara_addrs)
    by_id = {str(a.get("id")): a for a in samsara_addrs if a.get("id")}

    # Build set of encompass ids from source
    src_eids = set(r.encompass_id for r in src_rows if r.encompass_id)

    # Upsert loop
    created_ids: list[str] = []
    updated_ids: list[str] = []
    unchanged_ids: list[str] = []

    for r in src_rows:
        if not r.encompass_id:
            continue
        desired = to_address_payload(r, tags_index, radius_m=radius_m, managed_tag_name=MANAGED_BY_TAG)
        desired_fp = desired["externalIds"]["ENCOMPASS_FINGERPRINT"]
        existing = by_eid.get(r.encompass_id)

        # If no direct match, try probable match among non-managed addresses only.
        if not existing:
            candidates = [a for a in samsara_addrs if not is_managed(a, managed_tag_id)]
            pm = probable_match(r.name, r.address, r.lat, r.lon, candidates, distance_threshold_m=25.0)
            if pm:
                # Attach encompass id and markers
                existing = pm
                by_eid[r.encompass_id] = existing  # attach for future lookups

        if existing:
            # Merge: ensure scope markers (tag + external ids) are included in desired diff computation
            # Build patch
            diff = diff_address(existing, desired)
            # If fingerprint unchanged and no missing scope markers, skip
            ext = existing.get("externalIds") or {}
            fp_old = ext.get("ENCOMPASS_FINGERPRINT")
            needs_scope = False
            # ensure scope tag is present
            e_tags = existing.get("tagIds") or existing.get("tags") or []
            tag_ids = []
            if isinstance(e_tags, list):
                for t in e_tags:
                    if isinstance(t, dict):
                        tid = str(t.get("id") or t.get("tagId") or "")
                        if tid:
                            tag_ids.append(tid)
                    else:
                        tag_ids.append(str(t))
            if managed_tag_id and managed_tag_id not in tag_ids:
                needs_scope = True
            if ext.get("ENCOMPASS_MANAGED") != "1":
                needs_scope = True
            if _ext_encompass_id(ext) != r.encompass_id:
                needs_scope = True
            if needs_scope and "externalIds" not in diff:
                # inject ext and tags
                diff["externalIds"] = (existing.get("externalIds") or {}).copy()
                diff["externalIds"]["ENCOMPASS_MANAGED"] = "1"
                diff["externalIds"]["encompass_id"] = r.encompass_id
            if needs_scope:
                # ensure tagIds
                d_tags = desired.get("tagIds") or []
                if managed_tag_id and managed_tag_id not in d_tags:
                    d_tags = d_tags + [managed_tag_id]
                if set(tag_ids) != set(d_tags):
                    diff["tagIds"] = d_tags

            if not diff or (fp_old == desired_fp and len(diff.keys()) == 1 and "tagIds" in diff and set(tag_ids) == set(diff["tagIds"])):
                # unchanged
                unchanged_ids.append(str(existing.get("id")))
                actions.append(Action(at=now_utc_iso(), kind="skip", address_id=str(existing.get("id")), encompass_id=r.encompass_id, reason="unchanged"))
            else:
                if apply:
                    client.patch_address(str(existing.get("id")), diff)
                updated_ids.append(str(existing.get("id")))
                actions.append(Action(at=now_utc_iso(), kind="update", address_id=str(existing.get("id")), encompass_id=r.encompass_id, reason="update", payload=diff, diff=diff))
                # update state fingerprint
                state["fingerprints"][str(existing.get("id"))] = desired_fp
        else:
            # Create
            if apply:
                created = client.create_address(desired)
                aid = str(created.get("id") or "")
            else:
                aid = None
            created_ids.append(aid or "(dry)")
            actions.append(Action(at=now_utc_iso(), kind="create", address_id=aid, encompass_id=r.encompass_id, reason="create", payload=desired))
            if aid:
                state["fingerprints"][aid] = desired_fp

        # prepare dry-run diff row
        dry_rows.append({
            "encompass_id": r.encompass_id,
            "name": r.name,
            "action": actions[-1].kind if actions else "skip",
        })

    # Orphan detection: managed samsara addresses not in src_eids
    for addr in samsara_addrs:
        aid = str(addr.get("id") or "")
        if not aid:
            continue
        if is_warehouse(addr, wh_ids, wh_names):
            continue
        if not is_managed(addr, managed_tag_id):
            continue
        ext = addr.get("externalIds") or {}
        eid = str(_ext_encompass_id(ext) or "")
        if eid and eid in src_eids:
            continue
        # Orphan
        if candidate_tag_id:
            # add tag if not present
            tags = addr.get("tagIds") or addr.get("tags") or []
            tag_ids = []
            if isinstance(tags, list):
                for t in tags:
                    if isinstance(t, dict):
                        tid = str(t.get("id") or t.get("tagId") or "")
                        if tid:
                            tag_ids.append(tid)
                    else:
                        tag_ids.append(str(t))
            if candidate_tag_id not in tag_ids:
                patch = {"tagIds": tag_ids + [candidate_tag_id]}
                if apply:
                    client.patch_address(aid, patch)
                actions.append(Action(at=now_utc_iso(), kind="quarantine", address_id=aid, encompass_id=eid or None, reason="orphan_candidate_delete", payload=patch))
        else:
            # fallback to externalIds marker
            ext2 = addr.get("externalIds") or {}
            if ext2.get("ENCOMPASS_DELETE_CANDIDATE") != "1":
                patch = {"externalIds": ext2 | {"ENCOMPASS_DELETE_CANDIDATE": "1"}}
                if apply:
                    client.patch_address(aid, patch)
                actions.append(Action(at=now_utc_iso(), kind="quarantine", address_id=aid, encompass_id=eid or None, reason="orphan_candidate_delete_extid", payload=patch))

        # record in state candidate_deletes
        if aid not in state.get("candidate_deletes", {}):
            state["candidate_deletes"][aid] = now_utc_iso()

        # Hard delete if allowed
        if confirm_delete and eligible_for_hard_delete(aid, state, retention_days):
            if apply:
                client.delete_address(aid)
            actions.append(Action(at=now_utc_iso(), kind="delete", address_id=aid, encompass_id=eid or None, reason="hard_delete_after_retention"))
            # cleanup state
            state["candidate_deletes"].pop(aid, None)
            state["fingerprints"].pop(aid, None)

    # Write outputs
    write_jsonl(f"{out_dir}/actions.jsonl", actions)
    write_csv(f"{out_dir}/dry_run_diff.csv", dry_rows, ["encompass_id", "name", "action"])
    summary = summarize(actions)
    report_rows = [{"metric": k, "value": v} for k, v in sorted(summary.items())]
    write_csv(f"{out_dir}/sync_report.csv", report_rows, ["metric", "value"])
    if dup_rows:
        write_csv(f"{out_dir}/duplicates.csv", dup_rows, ["type", "encompass_id", "count"])
    if errors_rows:
        write_csv(f"{out_dir}/errors.csv", errors_rows, ["error", "row_name"])

    save_state(state_path, state)
