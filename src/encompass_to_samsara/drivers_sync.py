from __future__ import annotations

import copy
import json
import logging
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .driver_transform import EMPLOYEE_CODE_KEYS, transform_driver_payload
from .reporting import write_csv
from .samsara_client import SamsaraClient
from .tags import MANAGED_BY_DRIVER_TAG, build_tag_index, resolve_tag_id
from .transform import clean_external_ids, normalize, sanitize_external_id_value

LOG = logging.getLogger(__name__)


_EMPLOYEE_CODE_FALLBACK_KEYS = (
    "Employee Code",
    "employee code",
    "EMPLOYEE CODE",
)

_STATUS_KEYS = (
    "Status",
    "status",
    "Employment Status",
    "employmentStatus",
    "Employee Status",
)

_FIRST_NAME_KEYS = ("firstName", "First_Name", "First Name", "first_name")
_LAST_NAME_KEYS = ("lastName", "Last_Name", "Last Name", "last_name")
_EMAIL_KEYS = ("email", "Email", "Work_Email", "Work Email", "workEmail")
_USERNAME_KEYS = (
    "username",
    "Username",
    "userName",
    "Samsara Username",
    "Samsara_Username",
    "samsaraUsername",
)
_DRIVER_ID_KEYS = ("driverId", "DriverId", "driver_id", "Driver_ID", "id")
_TAG_KEYS = ("tagIds", "TagIds", "tag_ids", "tags", "Tags")


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _first_non_empty(row: Mapping[str, Any], keys: Iterable[str]) -> str:
    for key in keys:
        if key in row:
            value = row.get(key)
            if value is None:
                continue
            if isinstance(value, str):
                stripped = value.strip()
                if stripped:
                    return stripped
            else:
                text = str(value).strip()
                if text:
                    return text
    return ""


def _normalize_username(value: str | None) -> str | None:
    if not value:
        return None
    username = str(value).strip()
    if not username:
        return None
    return username.lower()


def _coerce_tag_ids(value: Any) -> list[str]:
    if value is None:
        return []
    tags: list[str] = []
    if isinstance(value, list | tuple | set):
        for item in value:
            if item is None:
                continue
            if isinstance(item, Mapping):
                candidate = item.get("id") or item.get("tagId") or item.get("value")
                if candidate is None:
                    continue
                candidate_str = str(candidate).strip()
                if candidate_str:
                    tags.append(candidate_str)
                continue
            candidate_str = str(item).strip()
            if candidate_str:
                tags.append(candidate_str)
        return sorted({t for t in tags if t})
    if isinstance(value, str):
        parts = [p.strip() for p in value.replace("|", ",").replace(";", ",").split(",")]
        return [p for p in parts if p]
    return []


def _normalize_external_ids(raw: Any) -> dict[str, Any]:
    if isinstance(raw, Mapping):
        return clean_external_ids(dict(raw))
    if isinstance(raw, list):
        mapping: dict[str, Any] = {}
        for item in raw:
            if not isinstance(item, Mapping):
                continue
            key = item.get("key") or item.get("name")
            val = item.get("value") or item.get("id") or item.get("externalId")
            if key is None or val is None:
                continue
            mapping[str(key)] = val
        return clean_external_ids(mapping)
    return {}


def _extract_existing_tag_ids(driver: Mapping[str, Any]) -> list[str]:
    tags: set[str] = set()
    tag_ids_field = driver.get("tagIds")
    tags.update(_coerce_tag_ids(tag_ids_field))
    tags_field = driver.get("tags")
    if isinstance(tags_field, list):
        for tag in tags_field:
            if isinstance(tag, Mapping):
                tid = tag.get("id") or tag.get("tagId")
                if tid:
                    tags.add(str(tid))
            elif tag is not None:
                tags.add(str(tag))
    return sorted(tags)


def _extract_employee_code_from_row(
    row: Mapping[str, Any], existing_code: str | None = None
) -> str | None:
    for key in (*EMPLOYEE_CODE_KEYS, *_EMPLOYEE_CODE_FALLBACK_KEYS):
        if key in row:
            value = row.get(key)
            if value is None:
                continue
            text = str(value).strip()
            if text:
                return text
    return existing_code


def _classify_status(row: Mapping[str, Any]) -> str:
    normalized = normalize(_first_non_empty(row, _STATUS_KEYS))
    if not normalized or normalized == "active":
        return "active"
    if "not hire" in normalized:
        return "not_hired"
    inactive_tokens = ("inactive", "terminate", "termination", "leave", "suspend", "retire")
    if any(token in normalized for token in inactive_tokens):
        return "inactive"
    if "term" in normalized:
        return "inactive"
    return "active"


def _extract_first_name(row: Mapping[str, Any], existing: Mapping[str, Any] | None) -> str:
    value = _first_non_empty(row, _FIRST_NAME_KEYS)
    if value:
        return value
    if existing:
        existing_value = existing.get("firstName") or existing.get("preferredName")
        if isinstance(existing_value, str):
            return existing_value
    return ""


def _extract_last_name(row: Mapping[str, Any], existing: Mapping[str, Any] | None) -> str:
    value = _first_non_empty(row, _LAST_NAME_KEYS)
    if value:
        return value
    if existing:
        existing_value = existing.get("lastName")
        if isinstance(existing_value, str):
            return existing_value
    return ""


def _extract_email(row: Mapping[str, Any], existing: Mapping[str, Any] | None) -> str | None:
    value = _first_non_empty(row, _EMAIL_KEYS)
    if value:
        return value.strip().lower()
    if existing:
        existing_email = existing.get("email")
        if isinstance(existing_email, str) and existing_email.strip():
            return existing_email.strip().lower()
    return None

def _extract_username(row: Mapping[str, Any], existing: Mapping[str, Any] | None) -> str | None:
    value = _first_non_empty(row, _USERNAME_KEYS)
    if value:
        return value
    if existing:
        existing_username = existing.get("username") or existing.get("userName")
        if isinstance(existing_username, str):
            return existing_username
    return None


def _extract_driver_id(row: Mapping[str, Any]) -> str | None:
    value = _first_non_empty(row, _DRIVER_ID_KEYS)
    if not value:
        return None
    return value


def _extract_row_tag_ids(row: Mapping[str, Any]) -> list[str]:
    for key in _TAG_KEYS:
        if key in row:
            return _coerce_tag_ids(row.get(key))
    return []


def _stringify(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str | int | float):
        return str(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


@dataclass(slots=True)
class DriverRecord:
    data: dict[str, Any]
    id: str | None
    username: str | None
    username_key: str | None
    employee_code: str | None
    fingerprint: str | None
    is_deactivated: bool
    tag_ids: list[str]

    @property
    def key(self) -> str:
        if self.id:
            return f"id:{self.id}"
        if self.employee_code:
            return f"employee_code:{self.employee_code}"
        if self.username_key:
            return f"username:{self.username_key}"
        return f"anon:{id(self)}"


@dataclass(slots=True)
class DriverIndex:
    records: list[DriverRecord] = field(default_factory=list)
    by_id: dict[str, DriverRecord] = field(default_factory=dict)
    by_employee_code: dict[str, DriverRecord] = field(default_factory=dict)
    by_username: dict[str, DriverRecord] = field(default_factory=dict)

    @classmethod
    def build(cls, drivers: Iterable[Mapping[str, Any]]) -> DriverIndex:
        index = cls()
        for driver in drivers:
            if not isinstance(driver, Mapping):
                continue
            data = copy.deepcopy(dict(driver))
            ext = _normalize_external_ids(data.get("externalIds"))
            employee_code = ext.get("employeeCode") or ext.get("employeecode")
            fingerprint = ext.get("paycom_fingerprint") or ext.get("paycomfingerprint")
            username = data.get("username") or data.get("userName")
            username_key = _normalize_username(username)
            driver_id = data.get("id") or data.get("driverId")
            driver_id_str = str(driver_id).strip() if driver_id is not None else None
            if driver_id_str == "":
                driver_id_str = None
            record = DriverRecord(
                data=data,
                id=driver_id_str,
                username=username if isinstance(username, str) else None,
                username_key=username_key,
                employee_code=employee_code,
                fingerprint=fingerprint,
                is_deactivated=bool(data.get("isDeactivated")),
                tag_ids=_extract_existing_tag_ids(data),
            )
            index.records.append(record)
            if record.id and record.id not in index.by_id:
                index.by_id[record.id] = record
            if record.employee_code and record.employee_code not in index.by_employee_code:
                index.by_employee_code[record.employee_code] = record
            if record.username_key and record.username_key not in index.by_username:
                index.by_username[record.username_key] = record
        return index

    def find(
        self,
        *,
        employee_code: str | None = None,
        driver_id: str | None = None,
        username: str | None = None,
    ) -> DriverRecord | None:
        if employee_code:
            record = self.by_employee_code.get(employee_code)
            if record:
                return record
        if driver_id:
            record = self.by_id.get(driver_id)
            if record:
                return record
        if username:
            record = self.by_username.get(_normalize_username(username))
            if record:
                return record
        return None


@dataclass(slots=True)
class PlannedDriverAction:
    action: str
    employee_code: str | None
    driver_id: str | None
    username: str | None
    fingerprint: str | None
    payload: dict[str, Any]
    api_payload: dict[str, Any]
    fingerprint_source: Mapping[str, Any] | None
    patch: dict[str, Any]
    diff: dict[str, dict[str, Any]]
    existing: Mapping[str, Any] | None
    reason: str | None = None
    status: str | None = None
    row: dict[str, Any] | None = None


@dataclass(slots=True)
class DriverSyncResult:
    action: PlannedDriverAction
    status: str
    message: str | None = None
    response: dict[str, Any] | None = None
    at: str = field(default_factory=_utc_now)


def _copy_time_zone(row: Mapping[str, Any], existing: Mapping[str, Any] | None) -> Any:
    for key in ("timeZone", "timezone", "time_zone"):
        if key in row:
            return row.get(key)
    if existing and existing.get("timeZone") is not None:
        return existing.get("timeZone")
    return None


def build_desired_payload(
    row: Mapping[str, Any],
    *,
    managed_tag_id: str | None,
    existing: Mapping[str, Any] | None,
    status_kind: str,
) -> dict[str, Any]:
    existing_map = dict(existing) if existing else {}
    existing_ext = _normalize_external_ids(existing_map.get("externalIds")) if existing else {}
    existing_code = existing_ext.get("employeeCode")
    employee_code = _extract_employee_code_from_row(row, existing_code)
    if not employee_code:
        raise ValueError("Employee_Code is required to sync drivers")

    payload: dict[str, Any] = {"Employee_Code": employee_code}

    first_name = _extract_first_name(row, existing_map)
    if first_name or not existing:
        payload["firstName"] = first_name

    last_name = _extract_last_name(row, existing_map)
    if last_name or not existing:
        payload["lastName"] = last_name

    email = _extract_email(row, existing_map)
    if email is not None:
        payload["email"] = email or None

    username = _extract_username(row, existing_map)
    if username:
        payload["username"] = username

    time_zone = _copy_time_zone(row, existing_map)
    if time_zone is not None:
        payload["timeZone"] = time_zone

    tag_ids = set(_extract_row_tag_ids(row))
    tag_ids.update(_extract_existing_tag_ids(existing_map))
    if managed_tag_id:
        tag_ids.add(managed_tag_id)
    if tag_ids:
        payload["tagIds"] = sorted({t for t in tag_ids if t})

    metadata = row.get("metadata")
    if isinstance(metadata, Mapping):
        payload["metadata"] = dict(metadata)

    row_ext = row.get("externalIds")
    if isinstance(row_ext, Mapping):
        payload["externalIds"] = dict(row_ext)

    payload["isDeactivated"] = status_kind != "active"
    return payload


def compute_diff(
    existing: Mapping[str, Any] | None,
    desired: Mapping[str, Any],
) -> dict[str, dict[str, Any]]:
    diff: dict[str, dict[str, Any]] = {}
    existing_map = dict(existing) if isinstance(existing, Mapping) else {}
    desired_ext = clean_external_ids(desired.get("externalIds") or {})
    existing_ext = clean_external_ids(existing_map.get("externalIds") or {}) if existing else {}

    for key, value in desired.items():
        if key == "externalIds":
            continue
        if key == "tagIds":
            new_tags = _coerce_tag_ids(value)
            old_tags = _coerce_tag_ids(existing_map.get("tagIds"))
            if new_tags != old_tags:
                diff[key] = {"from": old_tags, "to": new_tags}
            continue
        if key == "isDeactivated":
            new_bool = bool(value)
            old_bool = bool(existing_map.get(key))
            if new_bool != old_bool:
                diff[key] = {"from": old_bool, "to": new_bool}
            continue
        old_val = existing_map.get(key)
        if value != old_val:
            diff[key] = {"from": old_val, "to": value}

    for key, new_val in desired_ext.items():
        old_val = existing_ext.get(key)
        if new_val != old_val:
            diff[f"externalIds.{key}"] = {"from": old_val, "to": new_val}
    for key, old_val in existing_ext.items():
        if key not in desired_ext:
            diff[f"externalIds.{key}"] = {"from": old_val, "to": None}
    return diff


def build_patch(existing: Mapping[str, Any] | None, desired: Mapping[str, Any]) -> dict[str, Any]:
    patch: dict[str, Any] = {}
    existing_map = dict(existing) if isinstance(existing, Mapping) else {}
    desired_ext = clean_external_ids(desired.get("externalIds") or {})
    existing_ext = clean_external_ids(existing_map.get("externalIds") or {}) if existing else {}

    for key, value in desired.items():
        if key == "externalIds":
            continue
        if key == "tagIds":
            new_tags = _coerce_tag_ids(value)
            old_tags = _coerce_tag_ids(existing_map.get("tagIds"))
            if new_tags != old_tags:
                patch[key] = new_tags
            continue
        if key == "isDeactivated":
            new_bool = bool(value)
            old_bool = bool(existing_map.get(key))
            if new_bool != old_bool:
                patch[key] = new_bool
            continue
        if value != existing_map.get(key):
            patch[key] = value

    if desired_ext != existing_ext:
        patch["externalIds"] = desired_ext
    return patch


def _classify_action(
    existing: Mapping[str, Any] | None,
    desired: Mapping[str, Any],
    status_kind: str,
    diff: Mapping[str, Any],
) -> str:
    if existing is None:
        if status_kind in {"inactive", "not_hired"}:
            return "skip"
        return "create"

    existing_deactivated = bool(existing.get("isDeactivated"))
    desired_deactivated = bool(desired.get("isDeactivated"))
    if existing_deactivated and not desired_deactivated:
        return "reactivate"
    if not existing_deactivated and desired_deactivated:
        return "deactivate"
    if diff:
        return "update"
    return "skip"

def plan_driver_actions(
    payroll_rows: Sequence[Mapping[str, Any]],
    existing_drivers: Sequence[Mapping[str, Any]],
    *,
    managed_tag_id: str | None = None,
) -> list[PlannedDriverAction]:
    index = DriverIndex.build(existing_drivers)
    planned: list[PlannedDriverAction] = []
    matched_keys: set[str] = set()

    for row in payroll_rows:
        if not isinstance(row, Mapping):
            continue
        row_dict = dict(row)
        status_kind = _classify_status(row_dict)
        raw_employee_code = _extract_employee_code_from_row(row_dict)
        sanitized_code = (
            sanitize_external_id_value(raw_employee_code) if raw_employee_code else None
        )
        driver_id = _extract_driver_id(row_dict)
        username = _extract_username(row_dict, None)

        existing_record = index.find(
            employee_code=sanitized_code,
            driver_id=driver_id,
            username=username,
        )
        existing_data = existing_record.data if existing_record else None

        if raw_employee_code is None and existing_record is None:
            planned.append(
                PlannedDriverAction(
                    action="skip",
                    employee_code=None,
                    driver_id=None,
                    username=username,
                    fingerprint=None,
                    payload={},
                    api_payload={},
                    fingerprint_source=None,
                    patch={},
                    diff={},
                    existing=None,
                    reason="missing_employee_code",
                    status=status_kind,
                    row=row_dict,
                )
            )
            continue

        try:
            raw_payload = build_desired_payload(
                row_dict,
                managed_tag_id=managed_tag_id,
                existing=existing_data,
                status_kind=status_kind,
            )
        except ValueError:
            planned.append(
                PlannedDriverAction(
                    action="skip",
                    employee_code=None,
                    driver_id=existing_record.id if existing_record else driver_id,
                    username=username,
                    fingerprint=None,
                    payload={},
                    api_payload={},
                    fingerprint_source=None,
                    patch={},
                    diff={},
                    existing=existing_data,
                    reason="missing_employee_code",
                    status=status_kind,
                    row=row_dict,
                )
            )
            continue

        transformed = transform_driver_payload(raw_payload, fingerprint_source=raw_payload)
        desired_ext = clean_external_ids(transformed.get("externalIds") or {})
        employee_code = desired_ext.get("employeeCode") or sanitized_code
        fingerprint = desired_ext.get("paycom_fingerprint") or desired_ext.get("paycomfingerprint")
        username_final = transformed.get("username") or (
            existing_record.username if existing_record else username
        )
        driver_id_final = existing_record.id if existing_record else driver_id

        diff = compute_diff(existing_data, transformed)
        patch = build_patch(existing_data, transformed)
        action = _classify_action(existing_data, transformed, status_kind, diff)

        reason: str | None = None
        if status_kind == "not_hired" and existing_record is None:
            reason = "status_not_hired"
        elif status_kind == "inactive" and existing_record is None:
            reason = "status_inactive"

        api_payload = {}
        if action == "create":
            api_payload = dict(raw_payload)
        elif action != "skip":
            api_payload = dict(patch)

        planned.append(
            PlannedDriverAction(
                action=action,
                employee_code=employee_code,
                driver_id=driver_id_final,
                username=username_final,
                fingerprint=fingerprint,
                payload=transformed,
                api_payload=api_payload,
                fingerprint_source=raw_payload,
                patch=patch,
                diff=diff,
                existing=existing_data,
                reason=reason,
                status=status_kind,
                row=row_dict,
            )
        )

        if existing_record:
            matched_keys.add(existing_record.key)

    for record in index.records:
        if record.key in matched_keys:
            continue
        in_scope = bool(record.employee_code) or (
            managed_tag_id is not None and managed_tag_id in record.tag_ids
        )
        if not in_scope:
            continue
        if record.is_deactivated:
            continue
        if not record.employee_code:
            LOG.debug(
                "Skipping orphan driver %s due to missing employee code",
                record.id or record.username,
            )
            continue

        existing_data = copy.deepcopy(record.data)
        desired = copy.deepcopy(record.data)
        desired["isDeactivated"] = True
        tags = _coerce_tag_ids(desired.get("tagIds"))
        if managed_tag_id and managed_tag_id not in tags:
            tags.append(managed_tag_id)
        if tags:
            desired["tagIds"] = sorted({t for t in tags if t})
        diff = compute_diff(existing_data, desired)
        patch = build_patch(existing_data, desired)
        if not patch:
            continue
        fingerprint_source = copy.deepcopy(existing_data)
        fingerprint_source["Employee_Code"] = record.employee_code
        planned.append(
            PlannedDriverAction(
                action="deactivate",
                employee_code=record.employee_code,
                driver_id=record.id,
                username=record.username,
                fingerprint=record.fingerprint,
                payload=desired,
                api_payload=patch,
                fingerprint_source=fingerprint_source,
                patch=patch,
                diff=diff,
                existing=existing_data,
                reason="orphan",
                status="inactive",
                row=None,
            )
        )

    return planned


def _write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def write_reports(
    out_dir: str,
    plan: Sequence[PlannedDriverAction],
    results: Sequence[DriverSyncResult],
) -> None:
    base = Path(out_dir) / "drivers"
    base.mkdir(parents=True, exist_ok=True)

    diff_rows: list[dict[str, Any]] = []
    for action in plan:
        for diff_field, change in sorted(action.diff.items()):
            diff_rows.append(
                {
                    "employee_code": action.employee_code or "",
                    "driver_id": action.driver_id or "",
                    "username": action.username or "",
                    "action": action.action,
                    "field": diff_field,
                    "current": _stringify(change.get("from")),
                    "desired": _stringify(change.get("to")),
                }
            )
    write_csv(
        base / "dry_run_diff.csv",
        diff_rows,
        ["employee_code", "driver_id", "username", "action", "field", "current", "desired"],
    )

    plan_rows: list[dict[str, Any]] = []
    for action in plan:
        plan_rows.append(
            {
                "employee_code": action.employee_code or "",
                "driver_id": action.driver_id or "",
                "username": action.username or "",
                "action": action.action,
                "status": action.status or "",
                "reason": action.reason or "",
                "fingerprint": action.fingerprint or "",
            }
        )
    write_csv(
        base / "drivers_sync_plan.csv",
        plan_rows,
        ["employee_code", "driver_id", "username", "action", "status", "reason", "fingerprint"],
    )

    result_rows: list[dict[str, Any]] = []
    for result in results:
        action = result.action
        result_rows.append(
            {
                "employee_code": action.employee_code or "",
                "driver_id": action.driver_id or "",
                "username": action.username or "",
                "action": action.action,
                "status": result.status,
                "message": result.message or "",
            }
        )
    write_csv(
        base / "drivers_sync_results.csv",
        result_rows,
        ["employee_code", "driver_id", "username", "action", "status", "message"],
    )

    json_rows: list[dict[str, Any]] = []
    for result in results:
        action = result.action
        json_rows.append(
            {
                "timestamp": result.at,
                "action": action.action,
                "employee_code": action.employee_code,
                "driver_id": action.driver_id,
                "username": action.username,
                "status": result.status,
                "reason": action.reason,
                "diff": action.diff,
                "payload": action.api_payload,
                "fingerprint": action.fingerprint,
                "message": result.message,
                "response": result.response,
            }
        )
    _write_jsonl(base / "actions.jsonl", json_rows)


def sync_drivers(
    client: SamsaraClient,
    payroll_rows: Sequence[Mapping[str, Any]],
    *,
    out_dir: str,
    apply: bool = False,
) -> list[DriverSyncResult]:
    tag_index = build_tag_index(client)
    managed_tag_id = resolve_tag_id(tag_index, MANAGED_BY_DRIVER_TAG)

    existing = client.list_all_drivers()
    plan = plan_driver_actions(payroll_rows, existing, managed_tag_id=managed_tag_id)

    results: list[DriverSyncResult] = []
    for action in plan:
        if action.action == "skip":
            results.append(
                DriverSyncResult(action=action, status="skipped", message=action.reason)
            )
            continue

        identifier = action.driver_id or action.employee_code or action.username
        if not identifier:
            results.append(
                DriverSyncResult(action=action, status="error", message="missing_driver_identifier")
            )
            continue

        if not apply:
            results.append(DriverSyncResult(action=action, status="dry_run"))
            continue

        try:
            if action.action == "create":
                response = client.create_driver(
                    action.api_payload,
                    fingerprint_source=action.fingerprint_source or action.api_payload,
                )
            else:
                response = client.patch_driver(
                    identifier,
                    action.api_payload,
                    fingerprint_source=action.fingerprint_source or action.api_payload,
                )
            results.append(DriverSyncResult(action=action, status="applied", response=response))
        except Exception as exc:  # pragma: no cover - network errors handled at runtime
            LOG.exception(
                "Driver sync action %s failed for employee %s", action.action, action.employee_code
            )
            results.append(DriverSyncResult(action=action, status="error", message=str(exc)))

    write_reports(out_dir, plan, results)
    return results
