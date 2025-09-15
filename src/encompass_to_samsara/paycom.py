from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from collections.abc import Iterable, Mapping
from datetime import date, datetime
from typing import Any

import pytz
from pytz import BaseTzInfo, UnknownTimeZoneError

__all__ = [
    "compute_paycom_fingerprint",
    "norm_date",
    "norm_name",
    "norm_state",
    "norm_str",
    "norm_tz",
]

_RE_MULTI_SPACE = re.compile(r"\s+")
_RE_NAME_CHARS = re.compile(r"[^0-9a-z ]")
_RE_KEY_SANITIZE = re.compile(r"[^a-z0-9+-]+")
_TZ_OFFSET_RE = re.compile(
    r"^(?:(?:UTC|GMT)\s*)?([+-])\s*(\d{1,2})(?::?\s*(\d{2}))?$",
    re.IGNORECASE,
)


def norm_str(value: Any, *, lower: bool = True) -> str:
    """Return ``value`` stripped, de-accented, and normalized."""

    if value is None:
        return ""
    if isinstance(value, bytes):
        value = value.decode("utf-8", "ignore")
    s = str(value)
    s = unicodedata.normalize("NFKC", s)
    s = s.strip()
    if not s:
        return ""
    s = _RE_MULTI_SPACE.sub(" ", s)
    if lower:
        s = s.lower()
    return s


def norm_name(value: Any) -> str:
    """Normalize personal names for comparison/fingerprinting."""

    s = norm_str(value)
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = s.encode("ascii", "ignore").decode("ascii")
    s = _RE_NAME_CHARS.sub("", s)
    s = _RE_MULTI_SPACE.sub(" ", s).strip()
    return s


_STATE_ALIASES = {
    "alabama": "AL",
    "al": "AL",
    "alaska": "AK",
    "ak": "AK",
    "arizona": "AZ",
    "az": "AZ",
    "arkansas": "AR",
    "ar": "AR",
    "california": "CA",
    "ca": "CA",
    "colorado": "CO",
    "co": "CO",
    "connecticut": "CT",
    "ct": "CT",
    "delaware": "DE",
    "de": "DE",
    "district of col columbia": "DC",
    "district of columbia": "DC",
    "dc": "DC",
    "florida": "FL",
    "fl": "FL",
    "georgia": "GA",
    "ga": "GA",
    "hawaii": "HI",
    "hi": "HI",
    "idaho": "ID",
    "id": "ID",
    "illinois": "IL",
    "il": "IL",
    "indiana": "IN",
    "in": "IN",
    "iowa": "IA",
    "ia": "IA",
    "kansas": "KS",
    "ks": "KS",
    "kentucky": "KY",
    "ky": "KY",
    "louisiana": "LA",
    "la": "LA",
    "maine": "ME",
    "me": "ME",
    "maryland": "MD",
    "md": "MD",
    "massachusetts": "MA",
    "ma": "MA",
    "michigan": "MI",
    "mi": "MI",
    "minnesota": "MN",
    "mn": "MN",
    "mississippi": "MS",
    "ms": "MS",
    "missouri": "MO",
    "mo": "MO",
    "montana": "MT",
    "mt": "MT",
    "nebraska": "NE",
    "ne": "NE",
    "nevada": "NV",
    "nv": "NV",
    "new hampshire": "NH",
    "nh": "NH",
    "new jersey": "NJ",
    "nj": "NJ",
    "new mexico": "NM",
    "nm": "NM",
    "new york": "NY",
    "ny": "NY",
    "north carolina": "NC",
    "nc": "NC",
    "north dakota": "ND",
    "nd": "ND",
    "ohio": "OH",
    "oh": "OH",
    "oklahoma": "OK",
    "ok": "OK",
    "oregon": "OR",
    "or": "OR",
    "pennsylvania": "PA",
    "pa": "PA",
    "rhode island": "RI",
    "ri": "RI",
    "south carolina": "SC",
    "sc": "SC",
    "south dakota": "SD",
    "sd": "SD",
    "tennessee": "TN",
    "tn": "TN",
    "texas": "TX",
    "tx": "TX",
    "utah": "UT",
    "ut": "UT",
    "vermont": "VT",
    "vt": "VT",
    "virginia": "VA",
    "va": "VA",
    "washington": "WA",
    "wa": "WA",
    "west virginia": "WV",
    "wv": "WV",
    "wisconsin": "WI",
    "wi": "WI",
    "wyoming": "WY",
    "wy": "WY",
    "puerto rico": "PR",
    "pr": "PR",
    "guam": "GU",
    "gu": "GU",
    "american samoa": "AS",
    "as": "AS",
    "northern mariana islands": "MP",
    "mp": "MP",
    "virgin islands": "VI",
    "usvi": "VI",
    "vi": "VI",
}


def norm_state(value: Any) -> str:
    """Return value normalized to USPS two-letter codes when possible."""

    s = norm_str(value)
    if not s:
        return ""
    s = s.replace(".", "")
    s = _RE_MULTI_SPACE.sub(" ", s)
    return _STATE_ALIASES.get(s, s.upper())


_DATE_FORMATS = [
    "%Y-%m-%d",
    "%Y%m%d",
    "%Y/%m/%d",
    "%m/%d/%Y",
    "%m/%d/%y",
    "%m-%d-%Y",
    "%b %d %Y",
    "%b %d, %Y",
    "%B %d %Y",
    "%B %d, %Y",
    "%d %b %Y",
    "%d %b, %Y",
    "%d %B %Y",
    "%d %B, %Y",
]


def norm_date(value: Any) -> str:
    """Return ``value`` as ISO date (YYYY-MM-DD) if parsable."""

    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    s = str(value).strip()
    if not s:
        return ""
    lowered = s.lower()
    if lowered in {"na", "n/a", "none", "null", "n.a."}:
        return ""
    if s in {"0", "00000000", "0000-00-00"}:
        return ""
    # Try ISO first
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        dt = None
    if dt is not None:
        return dt.date().isoformat()
    cleaned = s.replace(",", " ").replace(".", "/").replace("\\", "/")
    cleaned = _RE_MULTI_SPACE.sub(" ", cleaned.strip())
    for fmt in _DATE_FORMATS:
        try:
            parsed = datetime.strptime(cleaned, fmt)
        except ValueError:
            continue
        return parsed.date().isoformat()
    return norm_str(cleaned)


def _clean_tz_key(value: str) -> str:
    value = unicodedata.normalize("NFKC", str(value))
    value = value.replace("#", " number ")
    value = value.replace("%", " percent ")
    value = value.replace("&", " and ")
    value = value.replace("@", " at ")
    value = value.lower()
    value = _RE_KEY_SANITIZE.sub("_", value)
    value = re.sub(r"_+", "_", value)
    return value.strip("_")


_TZ_ALIASES = {
    "central": "America/Chicago",
    "central_daylight_time": "America/Chicago",
    "central_standard_time": "America/Chicago",
    "cst": "America/Chicago",
    "cdt": "America/Chicago",
    "us_central": "US/Central",
    "eastern": "America/New_York",
    "eastern_daylight_time": "America/New_York",
    "eastern_standard_time": "America/New_York",
    "est": "America/New_York",
    "edt": "America/New_York",
    "us_eastern": "US/Eastern",
    "mountain": "America/Denver",
    "mountain_standard_time": "America/Denver",
    "mountain_daylight_time": "America/Denver",
    "mst": "America/Denver",
    "mdt": "America/Denver",
    "us_mountain": "US/Mountain",
    "pacific": "America/Los_Angeles",
    "pacific_daylight_time": "America/Los_Angeles",
    "pacific_standard_time": "America/Los_Angeles",
    "pst": "America/Los_Angeles",
    "pdt": "America/Los_Angeles",
    "us_pacific": "US/Pacific",
    "arizona": "America/Phoenix",
    "us_arizona": "US/Arizona",
    "alaska": "America/Anchorage",
    "hawaii": "Pacific/Honolulu",
    "hst": "Pacific/Honolulu",
    "utc": "UTC",
    "gmt": "UTC",
}

_TZ_BY_KEY = {}
for _name in pytz.all_timezones:
    _key = _clean_tz_key(_name)
    _TZ_BY_KEY.setdefault(_key, _name)


def _tz_from_offset(match: re.Match[str]) -> str:
    sign = match.group(1)
    hours = int(match.group(2))
    minutes = int(match.group(3) or 0)
    total = hours * 60 + minutes
    if sign == "-":
        total = -total
    sign_char = "+" if total >= 0 else "-"
    total = abs(total)
    hh, mm = divmod(total, 60)
    return f"UTC{sign_char}{hh:02d}:{mm:02d}"


def norm_tz(value: Any) -> str:
    """Normalize timezone names to IANA identifiers when possible."""

    if value is None:
        return ""
    if isinstance(value, BaseTzInfo):
        return value.zone
    if hasattr(value, "utcoffset"):
        try:
            offset = value.utcoffset(None)
        except Exception:  # pragma: no cover - guard
            offset = None
        if offset is not None:
            total = int(offset.total_seconds() // 60)
            sign = "+" if total >= 0 else "-"
            total = abs(total)
            hh, mm = divmod(total, 60)
            return f"UTC{sign}{hh:02d}:{mm:02d}"
    s = str(value).strip()
    if not s:
        return ""
    key = _clean_tz_key(s)
    if key in _TZ_BY_KEY:
        return _TZ_BY_KEY[key]
    if key in _TZ_ALIASES:
        alias = _TZ_ALIASES[key]
        alias_key = _clean_tz_key(alias)
        return _TZ_BY_KEY.get(alias_key, alias)
    try:
        tz = pytz.timezone(s)
    except UnknownTimeZoneError:
        tz = None
    if tz is None:
        alt = s.replace(" ", "_")
        try:
            tz = pytz.timezone(alt)
        except UnknownTimeZoneError:
            tz = None
    if tz is not None:
        return tz.zone
    match = _TZ_OFFSET_RE.match(s)
    if not match:
        match = _TZ_OFFSET_RE.match(s.replace(" ", ""))
    if match:
        return _tz_from_offset(match)
    return s.upper()


def _build_lookup(row: Mapping[str, Any] | Any) -> dict[str, Any]:
    if isinstance(row, Mapping):
        items = row.items()
    elif hasattr(row, "items"):
        items = row.items()
    elif hasattr(row, "__dict__"):
        items = row.__dict__.items()
    else:
        raise TypeError("row must be a mapping or have .items()/.__dict__")
    lookup: dict[str, Any] = {}
    for key, value in items:
        if key is None:
            continue
        norm_key = _clean_tz_key(str(key))
        if not norm_key:
            continue
        lookup.setdefault(norm_key, value)
    return lookup


def _get(lookup: dict[str, Any], *candidates: str) -> Any:
    for cand in candidates:
        norm_key = _clean_tz_key(cand)
        if not norm_key:
            continue
        if norm_key in lookup:
            value = lookup[norm_key]
            if value is None:
                continue
            if isinstance(value, str) and not value.strip():
                continue
            return value
    return None


def _unique_list(values: Iterable[Any]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []

    def _add(value: Any) -> None:
        if value is None:
            return
        if isinstance(value, list | tuple | set):
            for inner in value:
                _add(inner)
            return
        if isinstance(value, bytes):
            value = value.decode("utf-8", "ignore")
        s = str(value).strip()
        if not s:
            return
        if s not in seen:
            seen.add(s)
            out.append(s)

    for value in values:
        _add(value)
    return sorted(out)


def compute_paycom_fingerprint(
    row: Mapping[str, Any] | Any,
    tz: Any = None,
    peer_group_id: Any = None,
    tag_ids: Iterable[Any] | None = None,
) -> str:
    """Compute a fingerprint for a Paycom row."""

    lookup = _build_lookup(row)
    payload = {
        "address": {
            "city": norm_str(
                _get(lookup, "home_city", "city", "primary_city", "location_city")
            ),
            "country": norm_str(_get(lookup, "home_country", "country")),
            "line1": norm_str(
                _get(
                    lookup,
                    "home_address_line_1",
                    "home_address1",
                    "address_line_1",
                    "address1",
                    "street_address",
                )
            ),
            "line2": norm_str(
                _get(
                    lookup,
                    "home_address_line_2",
                    "home_address2",
                    "address_line_2",
                    "address2",
                    "apartment",
                    "suite",
                )
            ),
            "postalCode": norm_str(
                _get(
                    lookup,
                    "home_postal_code",
                    "postal_code",
                    "zip_code",
                    "zip",
                )
            ),
            "state": norm_state(_get(lookup, "home_state", "state")),
        },
        "contact": {
            "email": norm_str(
                _get(lookup, "work_email", "company_email", "email", "email_address")
            ),
            "personalEmail": norm_str(
                _get(lookup, "personal_email", "home_email", "alternate_email")
            ),
            "phoneHome": norm_str(_get(lookup, "home_phone", "phone_home")),
            "phoneMobile": norm_str(
                _get(lookup, "mobile_phone", "cell_phone", "mobile_number")
            ),
            "phoneWork": norm_str(
                _get(lookup, "work_phone", "business_phone", "phone")
            ),
        },
        "employment": {
            "department": norm_str(
                _get(lookup, "department", "home_department", "department_name")
            ),
            "division": norm_str(
                _get(lookup, "division", "business_unit", "company_division")
            ),
            "employeeId": norm_str(
                _get(
                    lookup,
                    "employee_id",
                    "employee_number",
                    "worker_id",
                    "paycom_id",
                    "employee_code",
                ),
                lower=False,
            ),
            "employmentStatus": norm_str(
                _get(lookup, "employment_status", "status", "employee_status")
            ),
            "employmentType": norm_str(
                _get(lookup, "employment_type", "employee_type", "status_type")
            ),
            "hireDate": norm_date(
                _get(
                    lookup,
                    "hire_date",
                    "original_hire_date",
                    "start_date",
                    "employment_start_date",
                )
            ),
            "location": norm_str(
                _get(lookup, "location", "primary_location", "work_location")
            ),
            "manager": norm_name(
                _get(lookup, "manager", "supervisor", "manager_name", "reports_to")
            ),
            "reHireDate": norm_date(
                _get(lookup, "rehire_date", "re_hire_date", "recent_rehire_date")
            ),
            "terminationDate": norm_date(
                _get(
                    lookup,
                    "termination_date",
                    "term_date",
                    "employment_end_date",
                    "end_date",
                )
            ),
            "title": norm_str(_get(lookup, "job_title", "position", "title")),
        },
        "identity": {
            "displayName": norm_name(_get(lookup, "display_name", "name")),
            "firstName": norm_name(
                _get(lookup, "first_name", "legal_first_name", "preferred_name_first")
            ),
            "lastName": norm_name(
                _get(lookup, "last_name", "legal_last_name", "preferred_name_last")
            ),
            "middleName": norm_name(_get(lookup, "middle_name", "mi")),
            "preferredFirstName": norm_name(
                _get(lookup, "preferred_first_name", "preferred_name")
            ),
            "preferredLastName": norm_name(_get(lookup, "preferred_last_name")),
            "prefix": norm_str(_get(lookup, "prefix", "name_prefix")),
            "suffix": norm_str(_get(lookup, "suffix", "name_suffix")),
        },
        "license": {
            "class": norm_str(
                _get(
                    lookup,
                    "drivers_license_class",
                    "license_class",
                    "driver_license_class",
                )
            ),
            "expiration": norm_date(
                _get(
                    lookup,
                    "drivers_license_expiration",
                    "license_expiration",
                    "license_expiration_date",
                )
            ),
            "number": norm_str(
                _get(
                    lookup,
                    "drivers_license_number",
                    "license_number",
                    "driver_license_number",
                ),
                lower=False,
            ),
            "state": norm_state(
                _get(
                    lookup,
                    "drivers_license_state",
                    "license_state",
                    "driver_license_state",
                )
            ),
        },
        "peerGroupId": norm_str(peer_group_id, lower=False),
        "tagIds": _unique_list(tag_ids or []),
        "timezone": norm_tz(tz),
    }
    json_payload = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(json_payload.encode("utf-8")).hexdigest()
