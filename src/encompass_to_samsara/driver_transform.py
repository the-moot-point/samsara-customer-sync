from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from typing import Any, Iterable

from .transform import clean_external_ids

# Keys that should never influence the computed fingerprint. These values are
# derived from managed attributes within Samsara so mutating them would result
# in unnecessary writes. External IDs are recomputed separately, so they are
# ignored when building the fingerprint payload.
DEFAULT_FINGERPRINT_IGNORE_KEYS = frozenset({"externalIds"})

# Accept common variants of the employee code column to make the transform more
# forgiving when invoked by different sync routines.
EMPLOYEE_CODE_KEYS = (
    "Employee_Code",
    "employeeCode",
    "employee_code",
    "EmployeeCode",
)


def _normalize_for_hash(value: Any) -> Any:
    """Return ``value`` converted into a JSON-serializable, stable form."""

    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, set):
        return sorted(_normalize_for_hash(v) for v in value)
    if isinstance(value, Mapping):
        return {
            str(k): _normalize_for_hash(v)
            for k, v in sorted(value.items(), key=lambda item: str(item[0]))
        }
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        return [_normalize_for_hash(v) for v in value]
    return value


def compute_paycom_fingerprint(
    payload: Mapping[str, Any], *, ignore_keys: Iterable[str] | None = None
) -> str:
    """Compute a deterministic fingerprint for a Paycom driver payload.

    The fingerprint is built by normalizing the payload, excluding any keys
    provided in ``ignore_keys`` (defaults to external IDs), and hashing the
    JSON representation. The hash is stable regardless of dictionary order and
    resilient to incidental whitespace changes in string values.
    """

    ignore = {k.lower() for k in (ignore_keys or DEFAULT_FINGERPRINT_IGNORE_KEYS)}
    normalized: dict[str, Any] = {}
    for key, value in payload.items():
        key_str = str(key)
        if key_str.lower() in ignore:
            continue
        normalized[key_str] = _normalize_for_hash(value)
    serialized = json.dumps(
        normalized,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _extract_employee_code(*candidates: Mapping[str, Any] | None) -> str:
    for mapping in candidates:
        if not mapping:
            continue
        for key in EMPLOYEE_CODE_KEYS:
            if key in mapping:
                raw = mapping[key]
                code = str(raw).strip() if raw is not None else ""
                if code:
                    return code
    raise ValueError("Employee_Code is required to sync drivers")


def transform_driver_payload(
    payload: Mapping[str, Any], *, fingerprint_source: Mapping[str, Any] | None = None
) -> dict[str, Any]:
    """Return a copy of ``payload`` augmented with required external IDs.

    ``fingerprint_source`` may be supplied when the payload represents a diff
    (e.g., during PATCH operations) but the fingerprint should be computed from
    a more complete record. When omitted, the payload itself is used.
    """

    source = fingerprint_source or payload
    employee_code = _extract_employee_code(payload, source)
    fingerprint = compute_paycom_fingerprint(source)

    existing_ext = payload.get("externalIds") if isinstance(payload, Mapping) else None
    ext: dict[str, Any] = dict(existing_ext or {})
    ext["employeeCode"] = employee_code
    ext["paycom_fingerprint"] = fingerprint

    cleaned_ext = clean_external_ids(ext)
    if "employeecode" in cleaned_ext:
        cleaned_ext["employeeCode"] = cleaned_ext.pop("employeecode")

    transformed: dict[str, Any] = {}
    for key, value in payload.items():
        if isinstance(key, str) and key in EMPLOYEE_CODE_KEYS:
            continue
        if key == "externalIds":
            continue
        transformed[key] = value

    transformed["externalIds"] = cleaned_ext
    return transformed
