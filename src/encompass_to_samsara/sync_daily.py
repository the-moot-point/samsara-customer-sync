from __future__ import annotations

import logging
from typing import Any, Dict, List

from .samsara_client import SamsaraClient
from .tags import MANAGED_BY_TAG, CANDIDATE_DELETE_TAG, build_tag_index, resolve_tag_id
from .transform import read_encompass_csv, to_address_payload, diff_address, compute_fingerprint
from .matcher import index_addresses_by_external_id
from .safety import load_warehouses, is_warehouse, now_utc_iso, eligible_for_hard_delete, is_managed
from .state import load_state, save_state
from .reporting import Action, ensure_out_dir, write_jsonl, write_csv, summarize

LOG = logging.getLogger(__name__)

def run_daily(
    client: SamsaraClient,
    *, encompass_delta: str, warehouses_path: str, out_dir: str,
    radius_m: int, apply: bool, retention_days: int, confirm_delete: bool,
) -> None:
    ensure_out_dir(out_dir)
    actions: list[Action] = []

    state_path = f"{out_dir.rstrip('/')}/state.json"
    state = load_state(state_path)

    # Read inputs
    rows = read_encompass_csv(encompass_delta)

    # Fetch tags and addresses
    tags_index = build_tag_index(client)
    managed_tag_id = resolve_tag_id(tags_index, MANAGED_BY_TAG)
    candidate_tag_id = resolve_tag_id(tags_index, CANDIDATE_DELETE_TAG)

    samsara_addrs = client.list_addresses()
    by_eid = index_addresses_by_external_id(samsara_addrs)

    # Warehouses
    wh_ids, wh_names = load_warehouses(warehouses_path)

    for r in rows:
        action = (r.action or "upsert").lower()
        existing = by_eid.get(r.encompass_id)
        if action not in ("upsert", "delete"):
            action = "upsert"

        if action == "delete":
            if existing and not is_warehouse(existing, wh_ids, wh_names) and is_managed(existing, managed_tag_id):
                aid = str(existing.get("id"))
                # quarantine
                if candidate_tag_id:
                    tags = existing.get("tagIds") or existing.get("tags") or []
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
                        actions.append(Action(at=now_utc_iso(), kind="quarantine", address_id=aid, encompass_id=r.encompass_id, reason="delta_delete_candidate", payload=patch))
                else:
                    ext = existing.get("externalIds") or {}
                    if ext.get("ENCOMPASS_DELETE_CANDIDATE") != "1":
                        patch = {"externalIds": ext | {"ENCOMPASS_DELETE_CANDIDATE": "1"}}
                        if apply:
                            client.patch_address(aid, patch)
                        actions.append(Action(at=now_utc_iso(), kind="quarantine", address_id=aid, encompass_id=r.encompass_id, reason="delta_delete_candidate_extid", payload=patch))
                # record state candidate
                if aid not in state.get("candidate_deletes", {}):
                    state["candidate_deletes"][aid] = now_utc_iso()
                # hard delete if allowed
                if confirm_delete and eligible_for_hard_delete(aid, state, retention_days):
                    if apply:
                        client.delete_address(aid)
                    actions.append(Action(at=now_utc_iso(), kind="delete", address_id=aid, encompass_id=r.encompass_id, reason="delta_hard_delete_after_retention"))
                    state["candidate_deletes"].pop(aid, None)
                    state["fingerprints"].pop(aid, None)
            else:
                actions.append(Action(at=now_utc_iso(), kind="skip", address_id=None, encompass_id=r.encompass_id, reason="delete_noop_not_found_or_protected"))
            continue

        # UPSERT
        desired = to_address_payload(r, tags_index, radius_m=radius_m, managed_tag_name=MANAGED_BY_TAG)
        desired_fp = desired["externalIds"]["ENCOMPASS_FINGERPRINT"]
        existing_fp = None
        if existing:
            eid = str(existing.get("id"))
            existing_fp = (existing.get("externalIds") or {}).get("ENCOMPASS_FINGERPRINT") or state["fingerprints"].get(eid)

        if existing:
            # Skip if fingerprint unchanged
            if existing_fp == desired_fp:
                actions.append(Action(at=now_utc_iso(), kind="skip", address_id=str(existing.get("id")), encompass_id=r.encompass_id, reason="unchanged_fingerprint"))
                continue
            diff = diff_address(existing, desired)
            if diff:
                if apply:
                    client.patch_address(str(existing.get("id")), diff)
                actions.append(Action(at=now_utc_iso(), kind="update", address_id=str(existing.get("id")), encompass_id=r.encompass_id, reason="delta_update", payload=diff, diff=diff))
                state["fingerprints"][str(existing.get("id"))] = desired_fp
            else:
                actions.append(Action(at=now_utc_iso(), kind="skip", address_id=str(existing.get("id")), encompass_id=r.encompass_id, reason="no_diff"))
        else:
            if apply:
                created = client.create_address(desired)
                aid = str(created.get("id") or "")
            else:
                aid = None
            actions.append(Action(at=now_utc_iso(), kind="create", address_id=aid, encompass_id=r.encompass_id, reason="delta_create", payload=desired))
            if aid:
                state["fingerprints"][aid] = desired_fp

    # Write outputs
    write_jsonl(f"{out_dir}/actions.jsonl", actions)
    summary = summarize(actions)
    report_rows = [{"metric": k, "value": v} for k, v in sorted(summary.items())]
    write_csv(f"{out_dir}/sync_report.csv", report_rows, ["metric", "value"])
    save_state(state_path, state)
