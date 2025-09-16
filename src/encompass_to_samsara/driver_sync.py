from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from typing import Any

_WHITESPACE_RE = re.compile(r"\s+")

_CONTACT_FIELDS: dict[str, dict[str, tuple[str, ...] | str]] = {
    "Work_Email": {
        "patch_key": "email",
        "lookup_keys": ("Work_Email", "workEmail", "email"),
    },
    "Primary_Phone": {
        "patch_key": "primaryPhone",
        "lookup_keys": ("Primary_Phone", "primaryPhone", "primary_phone"),
    },
    "Secondary_Phone": {
        "patch_key": "secondaryPhone",
        "lookup_keys": ("Secondary_Phone", "secondaryPhone", "secondary_phone"),
    },
}

_FINGERPRINT_IGNORED_KEYS = frozenset(_CONTACT_FIELDS)


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    s = str(value).strip()
    if not s:
        return ""
    return _WHITESPACE_RE.sub(" ", s)


def _normalize_contact(value: Any) -> str | None:
    normalized = _normalize_text(value)
    return normalized or None


def compute_paycom_fingerprint(row: Mapping[str, Any]) -> str:
    """Return a stable fingerprint for a Paycom driver row."""

    parts: list[str] = []
    for key in sorted(row.keys()):
        if key in _FINGERPRINT_IGNORED_KEYS:
            continue
        parts.append(f"{key}={_normalize_text(row.get(key))}")
    payload = "\u001f".join(parts).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _get_from_mapping(mapping: Mapping[str, Any] | None, keys: tuple[str, ...]) -> Any:
    if not isinstance(mapping, Mapping):
        return None
    for key in keys:
        if key in mapping:
            return mapping.get(key)
    return None


def _get_driver_contact(driver: Mapping[str, Any], row_key: str) -> str | None:
    config = _CONTACT_FIELDS[row_key]
    lookup_keys = config["lookup_keys"]
    value = _get_from_mapping(driver, lookup_keys)
    if value is not None:
        return _normalize_contact(value)
    metadata = driver.get("metadata") if isinstance(driver, Mapping) else None
    value = _get_from_mapping(metadata, lookup_keys)
    if value is not None:
        return _normalize_contact(value)
    return None


def build_driver_patch(row: Mapping[str, Any], samsara_driver: Mapping[str, Any]) -> dict[str, Any]:
    """Build a PATCH payload for a Samsara driver based on a Paycom row.

    The payload updates ``externalIds.paycom_fingerprint`` whenever the
    newly computed fingerprint differs from the value stored on the Samsara
    driver. It also updates Work Email and phone contact information if
    those values differ between the Paycom row and the Samsara record.

    Parameters
    ----------
    row:
        Paycom CSV row represented as a mapping.
    samsara_driver:
        Existing Samsara driver record.

    Returns
    -------
    dict[str, Any]
        PATCH payload. Empty when no updates are required.
    """

    patch: dict[str, Any] = {}

    new_fp = compute_paycom_fingerprint(row)
    ext_ids = samsara_driver.get("externalIds") if isinstance(samsara_driver, Mapping) else None
    existing_fp = None
    if isinstance(ext_ids, Mapping):
        existing_fp = ext_ids.get("paycom_fingerprint")
    if existing_fp != new_fp:
        ext_patch = dict(ext_ids) if isinstance(ext_ids, Mapping) else {}
        ext_patch["paycom_fingerprint"] = new_fp
        patch["externalIds"] = ext_patch

    metadata = samsara_driver.get("metadata") if isinstance(samsara_driver, Mapping) else None
    metadata_base = dict(metadata) if isinstance(metadata, Mapping) else {}
    metadata_patch: dict[str, Any] | None = None

    for row_key, config in _CONTACT_FIELDS.items():
        new_val = _normalize_contact(row.get(row_key))
        existing_val = _get_driver_contact(samsara_driver, row_key)
        if new_val != existing_val:
            patch[config["patch_key"]] = new_val

        metadata_existing = _normalize_contact(metadata_base.get(row_key))
        if new_val != metadata_existing:
            if metadata_patch is None:
                metadata_patch = dict(metadata_base)
            if new_val is None:
                metadata_patch.pop(row_key, None)
            else:
                metadata_patch[row_key] = new_val

    if metadata_patch is not None:
        patch["metadata"] = metadata_patch

    return patch
