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

REQUIRED_COLUMNS = [
    "Customer ID",
    "Customer Name",
    "Account Status",
    "Latitude",
    "Longitude",
    "Report Company Address",
    "Location",
    "Company",
    "Customer Type",
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
    ctype: str
    action: str | None = None  # for daily delta

def normalize(s: str | None) -> str:
    if not s:
        return ""
    s2 = s.strip().lower()
    s2 = RE_SPACES.sub(" ", s2)
    s2 = RE_PUNCT.sub("", s2)
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
                    address=str(r.get("Report Company Address") or "").strip(),
                    location=str(r.get("Location") or "").strip(),
                    company=str(r.get("Company") or "").strip(),
                    ctype=str(r.get("Customer Type") or "").strip(),
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


def clean_external_ids(ext: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of ``ext`` with a single canonical Encompass ID key."""
    out = ext.copy()
    eid = out.pop("EncompassId", None) or out.pop("ENCOMPASS_ID", None)
    if eid and "encompass_id" not in out:
        out["encompass_id"] = eid
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
    t = tag_name_to_id.get(managed_tag_name.lower())
    if t:
        tag_ids.append(t)
    # Location & Company tags
    for tag_val in [row.location, row.company]:
        if tag_val:
            tid = tag_name_to_id.get(tag_val.lower())
            if tid and tid not in tag_ids:
                tag_ids.append(tid)

    geofence = None
    if validate_lat_lon(row.lat, row.lon):
        geofence = {
            "circle": {
                "latitude": row.lat,
                "longitude": row.lon,
                "radiusMeters": radius_m,
            }
        }

    payload: dict[str, Any] = {
        "name": row.name,
        "formattedAddress": formatted_addr,
        "externalIds": {
            "encompass_id": row.encompass_id,
            "ENCOMPASS_STATUS": row.status,
            "ENCOMPASS_MANAGED": "1",
            "ENCOMPASS_FINGERPRINT": fp,
        },
    }
    if row.ctype:
        payload["externalIds"]["ENCOMPASS_TYPE"] = row.ctype

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
    # geofence (compare center lat/lon and radius)
    e_geo = existing.get("geofence") or {}
    d_geo = desired.get("geofence") or {}
    skip_geofence = "polygon" in e_geo or _has_updated_geofence_tag(existing)
    if not skip_geofence:
        if bool(d_geo) != bool(e_geo):
            patch["geofence"] = d_geo or None
        else:
            e_center = (e_geo.get("center") or {})
            d_center = (d_geo.get("center") or {})
            if (
                (e_geo.get("radiusMeters") != d_geo.get("radiusMeters"))
                or (e_center.get("latitude") != d_center.get("latitude"))
                or (e_center.get("longitude") != d_center.get("longitude"))
            ):
                patch["geofence"] = d_geo or None
    # externalIds merge (add/replace keys we own, keep others intact on server)
    e_ext_raw = existing.get("externalIds") or {}
    e_ext = clean_external_ids(e_ext_raw)
    d_ext = clean_external_ids(desired.get("externalIds") or {})
    ext_patch = {}
    for k in [
        "encompass_id",
        "ENCOMPASS_STATUS",
        "ENCOMPASS_MANAGED",
        "ENCOMPASS_FINGERPRINT",
        "ENCOMPASS_TYPE",
    ]:
        if k in d_ext and e_ext.get(k) != d_ext.get(k):
            ext_patch[k] = d_ext.get(k)
    if ext_patch or e_ext != e_ext_raw:
        ext_merged = e_ext.copy()
        ext_merged.update(ext_patch)
        patch["externalIds"] = ext_merged

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
