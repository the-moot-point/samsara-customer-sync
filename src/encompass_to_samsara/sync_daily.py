from __future__ import annotations

import logging

import requests

from .matcher import index_addresses_by_external_id
from .reporting import Action, ensure_out_dir, summarize, write_csv, write_jsonl
from .safety import eligible_for_hard_delete, is_managed, is_warehouse, load_warehouses, now_utc_iso
from .samsara_client import ExternalIdConflictError, SamsaraClient
from .state import load_state, save_state
from .tags import CANDIDATE_DELETE_TAG, MANAGED_BY_TAG, build_tag_index, resolve_tag_id
from .transform import (
    clean_external_ids,
    diff_address,
    read_encompass_csv,
    to_address_payload,
)

LOG = logging.getLogger(__name__)


def run_daily(
    client: SamsaraClient,
    *,
    encompass_delta: str,
    warehouses_path: str,
    out_dir: str,
    radius_m: int,
    apply: bool,
    retention_days: int,
    confirm_delete: bool,
    progress: bool | None = None,
) -> None:
    ensure_out_dir(out_dir)
    actions: list[Action] = []
    dup_rows: list[dict] = []
    errors_rows: list[dict] = []

    state_path = f"{out_dir.rstrip('/')}/state.json"
    state = load_state(state_path)

    # Read inputs
    rows = read_encompass_csv(encompass_delta)
    total = len(rows)
    use_progress = bool(progress)
    try:
        from tqdm.auto import tqdm
    except Exception:  # pragma: no cover - import guard
        use_progress = False
    iterable = (
        tqdm(rows, total=total, unit="row", desc="Daily sync", dynamic_ncols=True, leave=False)
        if use_progress
        else rows
    )

    # Fetch tags and addresses
    tags_index = build_tag_index(client)
    managed_tag_id = resolve_tag_id(tags_index, MANAGED_BY_TAG)
    candidate_tag_id = resolve_tag_id(tags_index, CANDIDATE_DELETE_TAG)

    samsara_addrs = client.list_addresses()
    by_eid = index_addresses_by_external_id(samsara_addrs)

    # Warehouses
    wh_ids, wh_names = load_warehouses(warehouses_path)

    error_count = 0
    for r in iterable:
        action = (r.action or "upsert").lower()
        if r.status.strip().upper() == "INACTIVE" and action != "delete":
            actions.append(
                Action(
                    at=now_utc_iso(),
                    kind="skip",
                    address_id=None,
                    encompass_id=r.encompass_id,
                    reason="inactive_status",
                )
            )
            continue
        existing = by_eid.get(r.encompass_id)
        if action not in ("upsert", "delete"):
            action = "upsert"

        if action == "delete":
            if (
                existing
                and not is_warehouse(existing, wh_ids, wh_names)
                and is_managed(existing, managed_tag_id)
            ):
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
                        actions.append(
                            Action(
                                at=now_utc_iso(),
                                kind="quarantine",
                                address_id=aid,
                                encompass_id=r.encompass_id,
                                reason="delta_delete_candidate",
                                payload=patch,
                            )
                        )
                else:
                    ext = clean_external_ids(existing.get("externalIds") or {})
                    if "encompass_delete_candidate" not in ext:
                        marker = f"{now_utc_iso()[:19].replace(':', '').replace('-', '')}-{aid}"
                        patch = {"externalIds": ext | {"ENCOMPASS_DELETE_CANDIDATE": marker}}
                        if apply:
                            client.patch_address(aid, patch)
                        actions.append(
                            Action(
                                at=now_utc_iso(),
                                kind="quarantine",
                                address_id=aid,
                                encompass_id=r.encompass_id,
                                reason="delta_delete_candidate_extid",
                                payload=patch,
                            )
                        )
                # record state candidate
                if aid not in state.get("candidate_deletes", {}):
                    state["candidate_deletes"][aid] = now_utc_iso()
                # hard delete if allowed
                if confirm_delete and eligible_for_hard_delete(aid, state, retention_days):
                    if apply:
                        client.delete_address(aid)
                    actions.append(
                        Action(
                            at=now_utc_iso(),
                            kind="delete",
                            address_id=aid,
                            encompass_id=r.encompass_id,
                            reason="delta_hard_delete_after_retention",
                        )
                    )
                    state["candidate_deletes"].pop(aid, None)
                    state["fingerprints"].pop(aid, None)
            else:
                actions.append(
                    Action(
                        at=now_utc_iso(),
                        kind="skip",
                        address_id=None,
                        encompass_id=r.encompass_id,
                        reason="delete_noop_not_found_or_protected",
                    )
                )
            continue

        # UPSERT
        desired = to_address_payload(
            r, tags_index, radius_m=radius_m, managed_tag_name=MANAGED_BY_TAG
        )
        desired_fp = desired["externalIds"]["fingerprint"]
        existing_fp = None
        if existing:
            eid = str(existing.get("id"))
            existing_fp = clean_external_ids(existing.get("externalIds") or {}).get(
                "fingerprint"
            ) or state["fingerprints"].get(eid)

        if existing:
            # Skip if fingerprint unchanged
            if existing_fp == desired_fp:
                actions.append(
                    Action(
                        at=now_utc_iso(),
                        kind="skip",
                        address_id=str(existing.get("id")),
                        encompass_id=r.encompass_id,
                        reason="unchanged_fingerprint",
                    )
                )
                continue
            diff = diff_address(existing, desired)
            if diff:
                aid = str(existing.get("id"))
                if apply:
                    try:
                        client.patch_address(aid, diff)
                    except ExternalIdConflictError:
                        actions.append(
                            Action(
                                at=now_utc_iso(),
                                kind="error",
                                address_id=aid,
                                encompass_id=r.encompass_id,
                                reason="update_duplicate_external_id",
                            )
                        )
                        dup_rows.append(
                            {
                                "type": "samsara_duplicate",
                                "encompass_id": r.encompass_id,
                                "count": 2,
                            }
                        )
                        error_count += 1
                    except requests.HTTPError as e:
                        actions.append(
                            Action(
                                at=now_utc_iso(),
                                kind="error",
                                address_id=aid,
                                encompass_id=r.encompass_id,
                                reason="update_http_error",
                            )
                        )
                        errors_rows.append(
                            {"error": f"patch_failed: {e}", "row_name": r.name}
                        )
                        error_count += 1
                    else:
                        actions.append(
                            Action(
                                at=now_utc_iso(),
                                kind="update",
                                address_id=aid,
                                encompass_id=r.encompass_id,
                                reason="delta_update",
                                payload=diff,
                                diff=diff,
                            )
                        )
                        state["fingerprints"][aid] = desired_fp
                else:
                    actions.append(
                        Action(
                            at=now_utc_iso(),
                            kind="update",
                            address_id=aid,
                            encompass_id=r.encompass_id,
                            reason="delta_update",
                            payload=diff,
                            diff=diff,
                        )
                    )
                    state["fingerprints"][aid] = desired_fp
            else:
                actions.append(
                    Action(
                        at=now_utc_iso(),
                        kind="skip",
                        address_id=str(existing.get("id")),
                        encompass_id=r.encompass_id,
                        reason="no_diff",
                    )
                )
        else:
            if apply:
                try:
                    created = client.create_address(desired)
                except requests.HTTPError as e:
                    actions.append(
                        Action(
                            at=now_utc_iso(),
                            kind="error",
                            address_id=None,
                            encompass_id=r.encompass_id,
                            reason="create_http_error",
                        )
                    )
                    errors_rows.append(
                        {"error": f"create_failed: {e}", "row_name": r.name}
                    )
                    error_count += 1
                    aid = None
                else:
                    aid = str(created.get("id") or "")
                    actions.append(
                        Action(
                            at=now_utc_iso(),
                            kind="create",
                            address_id=aid,
                            encompass_id=r.encompass_id,
                            reason="delta_create",
                            payload=desired,
                        )
                    )
                    if aid:
                        state["fingerprints"][aid] = desired_fp
            else:
                aid = None
                actions.append(
                    Action(
                        at=now_utc_iso(),
                        kind="create",
                        address_id=aid,
                        encompass_id=r.encompass_id,
                        reason="delta_create",
                        payload=desired,
                    )
                )
                if aid:
                    state["fingerprints"][aid] = desired_fp

        if use_progress:
            iterable.set_postfix_str(
                f"creates={sum(1 for a in actions if a.kind=='create')} "
                f"updates={sum(1 for a in actions if a.kind=='update')} errs={error_count}"
            )

    # Write outputs
    write_jsonl(f"{out_dir}/actions.jsonl", actions)
    summary = summarize(actions)
    report_rows = [{"metric": k, "value": v} for k, v in sorted(summary.items())]
    write_csv(f"{out_dir}/sync_report.csv", report_rows, ["metric", "value"])
    if dup_rows:
        write_csv(f"{out_dir}/duplicates.csv", dup_rows, ["type", "encompass_id", "count"])
    if errors_rows:
        write_csv(f"{out_dir}/errors.csv", errors_rows, ["error", "row_name"])
    save_state(state_path, state)
