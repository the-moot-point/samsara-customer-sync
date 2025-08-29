from __future__ import annotations

import csv
import hashlib
import logging
import re
from dataclasses import dataclass
from typing import Any

LOG = logging.getLogger(__name__)

RE_SPACES = re.compile(r"\s+")
RE_PUNCT = re.compile(r"[^\w\s]")
RE_EXT_ID_ALLOWED = re.compile(r"[^A-Za-z0-9:_-]")
MAX_EXT_ID_LEN = 32

REQUIRED_COLUMNS = [
    "Customer ID",
    "Customer Name",
    "Account Status",
    "Latitude",
    "Longitude",
    "Report Address",
    "Location",
    "Company",
]

@dataclass
class SourceRow:
    encompass_id: str
    name: str
    status: str
    lat: float | None
    lon: float | None
    address: str
    location: str
    company: str
    action: str | None = None  # for daily delta

    def __init__(
        self,
        encompass_id: str,
        name: str,
        status: str,
        lat: float | None,
        lon: float | None,
        address: str,
        location: str,
        company: str,
        action: str | None = None,
        **_: Any,
    ) -> None:
        self.encompass_id = encompass_id
        self.name = name
        self.status = status
        self.lat = lat
        self.lon = lon
        self.address = address
        self.location = location
        self.company = company
        self.action = action

def normalize(s: str | None) -> str:
    if not s:
        return ""
    s2 = s.strip().lower()
    s2 = RE_PUNCT.sub("", s2)
    s2 = RE_SPACES.sub(" ", s2)
    return s2.strip()

def canonical_address(addr: str | None) -> str:
    return normalize(addr)

def safe_float(x: str | None) -> float | None:
    if x is None or x == "":
        return None
    try:
        return float(x)
    except ValueError:
        return None

def read_encompass_csv(path: str) -> list[SourceRow]:
    with open(path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        cols = [c.strip() for c in reader.fieldnames or []]
        for req in REQUIRED_COLUMNS:
            if req not in cols:
                raise ValueError(f"Missing required column: {req}")
        out: list[SourceRow] = []
        for r in reader:
            out.append(
                SourceRow(
                    encompass_id=str(r.get("Customer ID") or "").strip(),
                    name=str(r.get("Customer Name") or "").strip(),
                    status=str(r.get("Account Status") or "").strip(),
                    lat=safe_float(str(r.get("Latitude") or "").strip() or None),
                    lon=safe_float(str(r.get("Longitude") or "").strip() or None),
                    address=str(r.get("Report Address") or "").strip(),
                    location=str(r.get("Location") or "").strip(),
                    company=str(r.get("Company") or "").strip(),
                    action=(str(r.get("Action") or "").strip().lower() or None),
                )
            )
        return out

def validate_lat_lon(lat: float | None, lon: float | None) -> bool:
    if lat is None or lon is None:
        return False
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        return False
    return True

def compute_fingerprint(name: str, status: str, formatted_addr: str) -> str:
    payload = f"{normalize(name)}|{normalize(status)}|{normalize(formatted_addr)}".encode()
    return hashlib.sha256(payload).hexdigest()


def sanitize_external_id_value(v: Any) -> str | None:
    """Return ``v`` sanitized for use as an external ID value.

    Removes disallowed characters, truncates values beyond ``MAX_EXT_ID_LEN``
    characters, and returns ``None`` if nothing remains.  A warning is logged
    whenever a value is modified or dropped.
    """
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    cleaned = RE_EXT_ID_ALLOWED.sub("", s)
    if cleaned != s:
        if cleaned:
            LOG.warning("External ID value %r contained invalid characters; sanitized to %r", s, cleaned)
        else:
            LOG.warning("External ID value %r contained only invalid characters and was dropped", s)
            return None
    if len(cleaned) > MAX_EXT_ID_LEN:
        LOG.warning(
            "External ID value %r exceeded %d characters and was truncated",
            s,
            MAX_EXT_ID_LEN,
        )
        cleaned = cleaned[:MAX_EXT_ID_LEN]
    return cleaned


def sanitize_external_id_key(k: Any) -> str | None:
    """Return ``k`` sanitized for use as an external ID key.

    Mirrors :func:`sanitize_external_id_value` but always lowercases the result
    to ensure canonical key names.  Keys consisting solely of invalid
    characters are dropped.
    """
    if k is None:
        return None
    s = str(k).strip()
    if not s:
        return None
    cleaned = RE_EXT_ID_ALLOWED.sub("", s)
    if cleaned != s:
        if cleaned:
            LOG.warning(
                "External ID key %r contained invalid characters; sanitized to %r",
                s,
                cleaned,
            )
        else:
            LOG.warning(
                "External ID key %r contained only invalid characters and was dropped",
                s,
            )
            return None
    if len(cleaned) > MAX_EXT_ID_LEN:
        LOG.warning(
            "External ID key %r exceeded %d characters and was truncated",
            s,
            MAX_EXT_ID_LEN,
        )
        cleaned = cleaned[:MAX_EXT_ID_LEN]
    return cleaned.lower()


def normalize_geofence(geo: dict | None) -> dict | None:
    """Return geofence in canonical circle form.

    Accepts existing API geofence schemas like
    ``{"radiusMeters": 50, "center": {"latitude": 1, "longitude": 2}}``
    and returns ``{"circle": {"latitude": 1, "longitude": 2, "radiusMeters": 50}}``.
    If a polygon geofence is provided, it is returned unchanged.
    """
    if not geo:
        return None
    # Preserve polygons untouched
    if isinstance(geo, dict) and geo.get("polygon"):
        return geo
    circle = geo.get("circle") if isinstance(geo, dict) else None
    if isinstance(circle, dict):
        radius = circle.get("radiusMeters")
        try:
            radius = int(radius) if radius is not None else radius
        except (TypeError, ValueError):
            pass
        return {
            "circle": {
                "latitude": circle.get("latitude"),
                "longitude": circle.get("longitude"),
                "radiusMeters": radius,
            }
        }
    center = geo.get("center") if isinstance(geo, dict) else None
    radius = geo.get("radiusMeters") if isinstance(geo, dict) else None
    if isinstance(center, dict) and radius is not None:
        try:
            radius = int(radius)
        except (TypeError, ValueError):
            pass
        return {
            "circle": {
                "latitude": center.get("latitude"),
                "longitude": center.get("longitude"),
                "radiusMeters": radius,
            }
        }
    return geo


def clean_external_ids(ext: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of ``ext`` using canonical external ID keys.

    Legacy keys with underscores or mixed case are mapped to the new
    lowercase names to ensure backward compatibility.  Values are sanitized
    and any that become empty are dropped.
    """
    out: dict[str, Any] = {}
    for k, v in ext.items():
        sk = sanitize_external_id_key(k)
        sv = sanitize_external_id_value(v)
        if sk and sv is not None:
            out[sk] = sv

    eid = out.pop("encompass_id", None)
    if eid and "encompassid" not in out:
        out["encompassid"] = eid

    status = out.pop("encompass_status", None)
    if status and "encompassstatus" not in out:
        out["encompassstatus"] = status

    managed = out.pop("encompass_managed", None)
    if managed and "encompassmanaged" not in out:
        out["encompassmanaged"] = managed

    fp = out.pop("encompass_fingerprint", None) or out.pop("fingerprint", None)
    if fp and "fingerprint" not in out:
        out["fingerprint"] = fp

    return out

def to_address_payload(
    row: SourceRow,
    tag_name_to_id: dict[str, str],
    *, radius_m: int = 50,
    managed_tag_name: str = "ManagedBy:EncompassSync",
) -> dict[str, Any]:
    # Compose formatted address
    formatted_addr = row.address or ""
    fp = compute_fingerprint(row.name, row.status, formatted_addr)
    radius_m = int(radius_m)

    tag_ids: list[str] = []
    # scope tag
    t = tag_name_to_id.get(normalize(managed_tag_name))
    if t:
        tag_ids.append(t)
    # Location & Company tags
    for tag_val in [row.location, row.company]:
        if tag_val:
            tid = tag_name_to_id.get(normalize(tag_val))
            if tid and tid not in tag_ids:
                tag_ids.append(tid)

    geofence = None
    if validate_lat_lon(row.lat, row.lon):
        geofence = normalize_geofence(
            {
                "circle": {
                    "latitude": row.lat,
                    "longitude": row.lon,
                    "radiusMeters": radius_m,
                }
            }
        )

    payload: dict[str, Any] = {
        "name": row.name,
        "formattedAddress": formatted_addr,
    }

    ext_ids = {
        "encompassid": sanitize_external_id_value(row.encompass_id),
        "encompassstatus": sanitize_external_id_value(row.status),
        "encompassmanaged": sanitize_external_id_value("1"),
        "fingerprint": sanitize_external_id_value(fp),
    }
    payload["externalIds"] = {k: v for k, v in ext_ids.items() if v is not None}

    if geofence:
        payload["geofence"] = geofence
    if tag_ids:
        payload["tagIds"] = tag_ids
    return payload


def _extract_tag_names(addr: dict) -> set[str]:
    """Return set of tag names from an address payload."""
    names: set[str] = set()
    tags_field = addr.get("tags") or []
    if isinstance(tags_field, list):
        for t in tags_field:
            if isinstance(t, dict):
                n = t.get("name") or t.get("tagName")
                if n:
                    names.add(str(n))
            elif isinstance(t, str):
                names.add(t)
    tag_ids = addr.get("tagIds") or []
    id_to_name: dict[str, str] = {}
    for t in tags_field:
        if isinstance(t, dict):
            tid = t.get("id") or t.get("tagId")
            n = t.get("name")
            if tid and n:
                id_to_name[str(tid)] = str(n)
    for mapping_key in ["tagNames", "tagIdToName"]:
        m = addr.get(mapping_key)
        if isinstance(m, dict):
            for k, v in m.items():
                id_to_name[str(k)] = str(v)
    for tid in tag_ids:
        if isinstance(tid, dict):
            n = tid.get("name")
            if n:
                names.add(str(n))
            t_id = tid.get("id") or tid.get("tagId")
            if t_id and (t_id_str := str(t_id)) in id_to_name:
                names.add(id_to_name[t_id_str])
        else:
            tid_str = str(tid)
            if tid_str in id_to_name:
                names.add(id_to_name[tid_str])
    return names


def _has_updated_geofence_tag(addr: dict) -> bool:
    return any(normalize(n) == "updated geofence" for n in _extract_tag_names(addr))

def diff_address(existing: dict, desired: dict) -> dict:
    """
    Compute a minimal patch diff (shallow) between existing and desired address payload.
    Only compares key fields we manage.
    """
    patch: dict = {}
    # name
    if (existing.get("name") or "") != (desired.get("name") or ""):
        patch["name"] = desired.get("name")
    # formatted address
    if (existing.get("formattedAddress") or "") != (desired.get("formattedAddress") or ""):
        patch["formattedAddress"] = desired.get("formattedAddress")
    # geofence (compare circle lat/lon and radius)
    e_geo = normalize_geofence(existing.get("geofence")) or {}
    d_geo = normalize_geofence(desired.get("geofence")) or {}
    skip_geofence = bool((existing.get("geofence") or {}).get("polygon")) or _has_updated_geofence_tag(existing)
    if not skip_geofence:
        if bool(d_geo) != bool(e_geo):
            patch["geofence"] = d_geo or None
        else:
            e_circle = e_geo.get("circle") or {}
            d_circle = d_geo.get("circle") or {}
            if (
                e_circle.get("radiusMeters") != d_circle.get("radiusMeters")
                or e_circle.get("latitude") != d_circle.get("latitude")
                or e_circle.get("longitude") != d_circle.get("longitude")
            ):
                patch["geofence"] = d_geo or None
    # externalIds merge (add/replace keys we own, keep others intact on server)
    e_ext_raw = existing.get("externalIds") or {}
    e_ext = clean_external_ids(e_ext_raw)
    d_ext = clean_external_ids(desired.get("externalIds") or {})
    ext_patch = {}
    for k in [
        "encompassid",
        "encompassstatus",
        "encompassmanaged",
        "fingerprint",
    ]:
        if k in d_ext and e_ext.get(k) != d_ext.get(k):
            ext_patch[k] = d_ext.get(k)
    if ext_patch or e_ext != e_ext_raw:
        ext_merged = e_ext.copy()
        ext_merged.update(ext_patch)
        for k, v in d_ext.items():
            if k not in ext_merged:
                ext_merged[k] = v
        patch["externalIds"] = clean_external_ids(ext_merged)

    # tags
    # existing may store 'tags' as array of ids or objects; normalize to ids
    e_tag_ids: list[str] = []
    e_tags = existing.get("tagIds") or existing.get("tags") or []
    if isinstance(e_tags, list):
        for t in e_tags:
            if isinstance(t, dict):
                tid = t.get("id") or t.get("tagId")
                if tid:
                    e_tag_ids.append(str(tid))
            else:
                e_tag_ids.append(str(t))
    d_tag_ids = [str(x) for x in desired.get("tagIds") or []]
    if set(e_tag_ids) != set(d_tag_ids):
        patch["tagIds"] = d_tag_ids
    return patch
