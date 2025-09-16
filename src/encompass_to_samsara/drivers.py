from __future__ import annotations

import csv
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Sequence

import pytz


LOG = logging.getLogger(__name__)


_HEADER_SANITIZE_RE = re.compile(r"[^a-z0-9]+")
_SPACE_RE = re.compile(r"\s+")
_TAG_SPLIT_RE = re.compile(r"[\s,;|]+")


def _normalize_header(value: str) -> str:
    return _HEADER_SANITIZE_RE.sub("", value.lower())


def _collapse_spaces(value: str | None) -> str:
    if not value:
        return ""
    return _SPACE_RE.sub(" ", str(value).strip())


def normalize_driver_name(value: str | None) -> str:
    """Return a canonical key for a driver's name."""

    collapsed = _collapse_spaces(value)
    return collapsed.lower()


def _find_column(fieldnames: Sequence[str] | None, *candidates: str) -> str | None:
    if not fieldnames:
        return None
    normalized = {name: _normalize_header(name) for name in fieldnames}
    candidate_norms = [_normalize_header(c) for c in candidates]

    # First try exact matches
    for cand in candidate_norms:
        for name, norm in normalized.items():
            if norm == cand:
                return name

    # Fallback to substring matching
    for cand in candidate_norms:
        if not cand:
            continue
        for name, norm in normalized.items():
            if cand in norm:
                return name
    return None


def _validate_timezone(value: str | None) -> str:
    tz = (value or "").strip()
    if not tz:
        return ""
    try:
        pytz.timezone(tz)
    except Exception:  # pragma: no cover - pytz raises multiple subclasses
        LOG.warning("Invalid timezone %r; defaulting to empty string", tz)
        return ""
    return tz


def _normalize_license_state(value: str | None) -> str:
    state = (value or "").strip()
    if len(state) == 2 and state.isalpha():
        return state.upper()
    if state:
        LOG.warning("Invalid license state %r; defaulting to empty string", state)
    return ""


def _parse_hire_date(value: str | None) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""

    # Replace ``T`` with space to help parsing
    candidate = raw.replace("T", " ").strip()
    # Remove trailing timezone designator if present
    for tz_sep in ("Z", "z"):
        if candidate.endswith(tz_sep):
            candidate = candidate[:-1]

    date_part = candidate.split()[0]

    # Try ISO parsing first
    try:
        dt = datetime.fromisoformat(candidate)
        return dt.date().isoformat()
    except ValueError:
        pass

    # Fallback formats commonly seen in spreadsheets
    formats = [
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%m/%d/%y",
        "%Y/%m/%d",
        "%m-%d-%Y",
        "%m-%d-%y",
        "%Y%m%d",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(date_part, fmt)
        except ValueError:
            continue
        return dt.date().isoformat()

    LOG.warning("Unparseable hire date %r; defaulting to empty string", raw)
    return ""


def _parse_tag_ids(value: str | None) -> list[str]:
    if not value:
        return []
    parts = [p.strip() for p in _TAG_SPLIT_RE.split(str(value)) if p.strip()]
    # Preserve as strings while deduping and sorting
    return sorted(set(parts))


@dataclass(slots=True)
class DriverTags:
    tagIds: list[str] = field(default_factory=list)
    licenseState: str = ""
    hireDate: str = ""


@dataclass(slots=True)
class DriverMetadata:
    timezone: str = ""
    peerGroup: str = ""
    licenseState: str = ""
    hireDate: str = ""
    tagIds: list[str] = field(default_factory=list)


def load_timezone_map(path: str | Path) -> dict[str, str]:
    """Return mapping of normalized driver name → timezone."""

    with open(Path(path), newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        name_field = _find_column(reader.fieldnames, "driver", "driver name", "name", "full name")
        tz_field = _find_column(reader.fieldnames, "timezone", "time zone", "tz")
        if not name_field or not tz_field:
            raise ValueError("timezone_map.csv must contain driver name and timezone columns")

        mapping: dict[str, str] = {}
        for row in reader:
            key = normalize_driver_name(row.get(name_field))
            if not key:
                continue
            mapping[key] = _validate_timezone(row.get(tz_field))
    return mapping


def load_peer_groups(path: str | Path) -> dict[str, str]:
    """Return mapping of normalized driver name → peer group."""

    with open(Path(path), newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        name_field = _find_column(reader.fieldnames, "driver", "driver name", "name", "full name")
        group_field = _find_column(reader.fieldnames, "peer group", "peergroup", "group")
        if not name_field or not group_field:
            raise ValueError("peer_groups.csv must contain driver name and peer group columns")

        mapping: dict[str, str] = {}
        for row in reader:
            key = normalize_driver_name(row.get(name_field))
            if not key:
                continue
            mapping[key] = _collapse_spaces(row.get(group_field))
    return mapping


def load_driver_tags(path: str | Path) -> dict[str, DriverTags]:
    """Return mapping of normalized driver name → :class:`DriverTags`."""

    with open(Path(path), newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        name_field = _find_column(reader.fieldnames, "driver", "driver name", "name", "full name")
        tags_field = _find_column(reader.fieldnames, "tag ids", "tagids", "tags")
        if not name_field:
            raise ValueError("driver_tags.csv must contain a driver name column")
        if not tags_field:
            raise ValueError("driver_tags.csv must contain a tag ids column")

        license_field = _find_column(reader.fieldnames, "license state", "licensestate")
        hire_field = _find_column(reader.fieldnames, "hire date", "hire_date", "hiredate")

        mapping: dict[str, DriverTags] = {}
        for row in reader:
            key = normalize_driver_name(row.get(name_field))
            if not key:
                continue
            tag_ids = _parse_tag_ids(row.get(tags_field))
            license_state = _normalize_license_state(row.get(license_field)) if license_field else ""
            hire_date = _parse_hire_date(row.get(hire_field)) if hire_field else ""
            mapping[key] = DriverTags(tagIds=tag_ids, licenseState=license_state, hireDate=hire_date)
    return mapping


def merge_driver_metadata(
    timezone_map: dict[str, str],
    peer_groups: dict[str, str],
    driver_tags: dict[str, DriverTags],
) -> dict[str, DriverMetadata]:
    """Merge disparate driver mappings into a single structure.

    Missing values default to ``""`` (or ``[]`` for ``tagIds``).
    """

    names: set[str] = set()
    names.update(timezone_map)
    names.update(peer_groups)
    names.update(driver_tags)

    combined: dict[str, DriverMetadata] = {}
    for name in names:
        tags = driver_tags.get(name)
        combined[name] = DriverMetadata(
            timezone=timezone_map.get(name, ""),
            peerGroup=peer_groups.get(name, ""),
            licenseState=tags.licenseState if tags else "",
            hireDate=tags.hireDate if tags else "",
            tagIds=list(tags.tagIds) if tags else [],
        )
    return combined


def load_driver_metadata(
    timezone_map_csv: str | Path,
    peer_groups_csv: str | Path,
    driver_tags_csv: str | Path,
) -> dict[str, DriverMetadata]:
    """Convenience wrapper to load and merge all driver metadata CSV files."""

    timezone_map = load_timezone_map(timezone_map_csv)
    peer_groups = load_peer_groups(peer_groups_csv)
    driver_tags = load_driver_tags(driver_tags_csv)
    return merge_driver_metadata(timezone_map, peer_groups, driver_tags)

