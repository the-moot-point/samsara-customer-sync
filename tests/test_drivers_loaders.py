from __future__ import annotations

from pathlib import Path

from encompass_to_samsara.drivers import (
    load_driver_metadata,
    load_driver_tags,
    load_peer_groups,
    load_timezone_map,
    merge_driver_metadata,
)


def _write_csv(tmp_path: Path, name: str, content: str) -> Path:
    path = tmp_path / name
    path.write_text(content, encoding="utf-8")
    return path


def test_load_timezone_map_normalizes_and_validates(tmp_path: Path) -> None:
    csv_content = (
        "Driver Name,Timezone\n"
        "  Jane   Doe  ,America/Chicago\n"
        "John Smith,Invalid/Zone\n"
        ",America/New_York\n"
    )
    path = _write_csv(tmp_path, "timezone_map.csv", csv_content)

    mapping = load_timezone_map(path)

    assert mapping == {
        "jane doe": "America/Chicago",
        "john smith": "",
    }


def test_load_peer_groups_trims_names_and_values(tmp_path: Path) -> None:
    csv_content = (
        "Name,Peer Group\n"
        " Alice   Example ,  Group A  \n"
        "Bob Example,Group B\n"
    )
    path = _write_csv(tmp_path, "peer_groups.csv", csv_content)

    mapping = load_peer_groups(path)

    assert mapping == {
        "alice example": "Group A",
        "bob example": "Group B",
    }


def test_load_driver_tags_normalizes_metadata(tmp_path: Path) -> None:
    csv_content = (
        "Full Name,TagIds,License_State,Hire_Date\n"
        " Ann   Example ,3 | 2 | 3,tx,1/2/2023\n"
        "Bob Example,,Texas,13/40/2020\n"
    )
    path = _write_csv(tmp_path, "driver_tags.csv", csv_content)

    mapping = load_driver_tags(path)

    assert mapping["ann example"].tagIds == ["2", "3"]
    assert mapping["ann example"].licenseState == "TX"
    assert mapping["ann example"].hireDate == "2023-01-02"

    assert mapping["bob example"].tagIds == []
    assert mapping["bob example"].licenseState == ""
    assert mapping["bob example"].hireDate == ""


def test_merge_driver_metadata_defaults_missing_fields(tmp_path: Path) -> None:
    tz_path = _write_csv(
        tmp_path,
        "timezone_map.csv",
        "Driver,Timezone\nJane Doe,America/Chicago\n",
    )
    groups_path = _write_csv(
        tmp_path,
        "peer_groups.csv",
        "Driver,Peer Group\nJane Doe,Group 1\n",
    )
    tags_path = _write_csv(
        tmp_path,
        "driver_tags.csv",
        "Driver,TagIds,License_State,Hire_Date\nJane Doe,1|2,IL,2024-05-01\nJohn Roe,3,,\n",
    )

    tz_map = load_timezone_map(tz_path)
    peer_groups = load_peer_groups(groups_path)
    tags = load_driver_tags(tags_path)

    merged = merge_driver_metadata(tz_map, peer_groups, tags)

    assert merged["jane doe"].timezone == "America/Chicago"
    assert merged["jane doe"].peerGroup == "Group 1"
    assert merged["jane doe"].tagIds == ["1", "2"]
    assert merged["jane doe"].licenseState == "IL"
    assert merged["jane doe"].hireDate == "2024-05-01"

    # John Roe only appears in driver_tags.csv → defaults for others
    assert merged["john roe"].timezone == ""
    assert merged["john roe"].peerGroup == ""
    assert merged["john roe"].tagIds == ["3"]

    # Convenience wrapper should return the same result
    merged_via_loader = load_driver_metadata(tz_path, groups_path, tags_path)
    assert merged_via_loader == merged
