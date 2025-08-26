from __future__ import annotations

import logging
import math

from .transform import canonical_address, normalize

LOG = logging.getLogger(__name__)


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371000.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def index_addresses_by_external_id(addresses: list[dict]) -> dict[str, dict]:
    idx: dict[str, dict] = {}
    for a in addresses:
        ext = a.get("externalIds") or {}
        eid = ext.get("encompass_id") or ext.get("ENCOMPASS_ID") or ext.get("EncompassId")
        if eid:
            idx[str(eid)] = a
    return idx


def find_by_name(row_name: str, candidates: list[dict]) -> dict | None:
    """Return unique candidate whose normalized name matches ``row_name``."""

    if not row_name or not candidates:
        return None
    row_norm = normalize(row_name)
    matches = [a for a in candidates if normalize(a.get("name") or "") == row_norm]
    if len(matches) == 1:
        return matches[0]
    return None


def probable_match(
    row_name: str,
    row_addr: str,
    row_lat: float | None,
    row_lon: float | None,
    candidates: list[dict],
    distance_threshold_m: float = 25.0,
) -> dict | None:
    """
    Try to find a unique match among candidates by:
      1) exact match on normalized ``name`` + ``address``
      2) otherwise look for candidates within ``distance_threshold_m`` of
         ``row_lat``/``row_lon`` and pick the closest one. If multiple
         candidates are at the same minimum distance, prefer a candidate with
         a matching canonical address and then one with a matching normalized
         name.
    """
    if not candidates:
        return None
    row_key = f"{normalize(row_name)}|{canonical_address(row_addr)}"
    exact = [
        a
        for a in candidates
        if f"{normalize(a.get('name') or '')}|{canonical_address(a.get('formattedAddress') or '')}"
        == row_key
    ]
    if len(exact) == 1:
        return exact[0]
    if row_lat is not None and row_lon is not None:
        within: list[tuple[dict, float]] = []
        for a in candidates:
            g = a.get("geofence") or {}
            c = g.get("center") or {}
            lat = c.get("latitude")
            lon = c.get("longitude")
            if lat is None or lon is None:
                continue
            d = haversine_m(float(row_lat), float(row_lon), float(lat), float(lon))
            if d <= distance_threshold_m:
                within.append((a, d))
        if within:
            min_d = min(d for _, d in within)
            closest = [a for a, d in within if d == min_d]
            if len(closest) == 1:
                return closest[0]
            row_addr_norm = canonical_address(row_addr)
            addr_matches = [
                a
                for a in closest
                if canonical_address(a.get("formattedAddress") or "") == row_addr_norm
            ]
            if len(addr_matches) == 1:
                return addr_matches[0]
            row_name_norm = normalize(row_name)
            name_matches = [a for a in closest if normalize(a.get("name") or "") == row_name_norm]
            if len(name_matches) == 1:
                return name_matches[0]
    return None
