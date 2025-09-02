import re

from encompass_to_samsara.transform import (
    SourceRow,
    compute_fingerprint,
    diff_address,
    clean_external_ids,
    normalize,
    sanitize_external_id_value,
    to_address_payload,
    validate_lat_lon,
    normalize_geofence,
)
from encompass_to_samsara.tags import build_tag_index


def test_normalize_and_fingerprint_stability():
    n1 = normalize("  Acme  Corp, Inc. ")
    n2 = normalize("Acme Corp Inc")
    assert n1 == n2
    f1 = compute_fingerprint("Store #1", "Active", "123 Main St, City, TX")
    f2 = compute_fingerprint("  Store  #1 ", "Active", "123 Main St City  TX")
    assert f1 == f2

def test_payload_includes_scope_and_tags():
    row = SourceRow(
        encompass_id="C123",
        name="Foo Store",
        status="Active",
        lat=30.1, lon=-97.7,
        address="742 Evergreen Terrace, Springfield",
        location="Austin",
        company="JECO",
        ctype="Retail",
    )
    tags = {"managedbyencompasssync":"1", "austin":"10", "jeco":"20", "candidatedelete":"2"}
    payload = to_address_payload(row, tags, radius_m=75)
    assert payload["externalIds"]["EncompassId"] == "C123"
    assert "fingerprint" in payload["externalIds"]
    assert set(payload["geofence"].keys()) == {"circle"}
    assert payload["geofence"]["circle"]["radiusMeters"] == 75
    assert payload["geofence"]["circle"]["latitude"] == 30.1
    assert payload["geofence"]["circle"]["longitude"] == -97.7
    assert set(payload.get("tagIds", [])) >= {"1","10","20"}


def test_missing_coordinates_defaults_geofence():
    row = SourceRow(
        encompass_id="C999",
        name="Invalid",
        status="Active",
        lat=None,
        lon=None,
        address="",
        location="",
        company="",
        ctype="Retail",
    )
    payload = to_address_payload(row, {})
    assert payload["geofence"] == {"circle": {"radiusMeters": 50}}
    assert normalize_geofence({"circle": {"radiusMeters": 50}}) == {
        "circle": {"radiusMeters": 50}
    }

def test_validate_lat_lon():
    assert validate_lat_lon(0,0)
    assert validate_lat_lon(90, 180)
    assert not validate_lat_lon(91, 0)
    assert not validate_lat_lon(0, 181)


def test_diff_address_normalizes_center_geofence():
    existing = {
        "geofence": {
            "radiusMeters": 50,
            "center": {"latitude": 10.0, "longitude": 20.0},
        }
    }
    desired = {
        "geofence": {
            "circle": {
                "radiusMeters": 50,
                "latitude": 10.0,
                "longitude": 20.0,
            }
        }
    }
    patch = diff_address(existing, desired)
    assert patch == {}


def test_hyphenated_location_maps_to_tag(monkeypatch, client):
    sample_tags = [{"id": "1", "name": "Austin North"}]
    monkeypatch.setattr(client, "list_tags", lambda: sample_tags)
    tag_index = build_tag_index(client)
    row = SourceRow(
        encompass_id="C1",
        name="Foo",
        status="Active",
        lat=None,
        lon=None,
        address="",
        location="Austin - North",
        company="",
        ctype="",
    )
    payload = to_address_payload(row, tag_index)
    assert payload["tagIds"] == ["1"]


def test_sanitize_external_id_value_strips(caplog):
    raw = "bad$$$" + "x" * 40
    with caplog.at_level("WARNING"):
        val = sanitize_external_id_value(raw)
    assert val == "bad" + "x" * 40
    assert any("invalid" in r.message for r in caplog.records)


def test_sanitize_external_id_value_allows_underscore():
    val = sanitize_external_id_value("foo_bar-123")
    assert val == "foo_bar-123"


def test_clean_external_ids_sanitizes_and_drops(caplog):
    ext = {"ENCOMPASS_ID": "id!!", "other": "good", "drop": "$$"}
    with caplog.at_level("WARNING"):
        cleaned = clean_external_ids(ext)
    assert cleaned["EncompassId"] == "id"
    assert cleaned["other"] == "good"
    assert "drop" not in cleaned


def test_clean_external_ids_sanitizes_keys(caplog):
    ext = {"BAD$KEY": "v1!", "__KEEP__": "ok", "$$": "drop"}
    with caplog.at_level("WARNING"):
        cleaned = clean_external_ids(ext)
    assert cleaned["badkey"] == "v1"
    assert cleaned["__keep__"] == "ok"
    assert "$$" not in cleaned


def test_to_address_payload_sanitizes_external_ids():
    row = SourceRow(
        encompass_id="ID$$$" + "1" * 40,
        name="Foo",
        status="Active!@#" + "2" * 40,
        lat=None,
        lon=None,
        address="",
        location="",
        company="",
        ctype="",
    )
    payload = to_address_payload(row, {})
    ext = payload["externalIds"]
    assert ext["EncompassId"] == "ID" + "1" * 40
    assert "encompassstatus" not in ext
    assert len(ext["fingerprint"]) == 64


def test_to_address_payload_produces_compliant_external_ids():
    row = SourceRow(
        encompass_id="ID_$1",
        name="Foo",
        status="Active$@",
        lat=None,
        lon=None,
        address="",
        location="",
        company="",
        ctype="",
    )
    payload = to_address_payload(row, {})
    ext = payload["externalIds"]
    assert set(ext.keys()) <= {"EncompassId", "fingerprint"}
    allowed = re.compile(r"^[A-Za-z0-9_.:-]+$")
    assert all(allowed.match(k) for k in ext)
    assert all(allowed.match(v) for v in ext.values())


def test_diff_address_sanitizes_external_ids():
    existing = {"externalIds": {"encompassid": "id!!"}}
    desired = {"externalIds": {"encompassid": "id@" + "1" * 40}}
    patch = diff_address(existing, desired)
    val = patch["externalIds"]["EncompassId"]
    assert val == "id" + "1" * 40


def test_diff_address_drops_or_renames_invalid_external_ids():
    existing = {"externalIds": {"ENCOMPASS_ID": "id@123", "bad$key": "v1", "$$": "drop"}}
    desired = {"externalIds": {}}
    patch = diff_address(existing, desired)
    ext = patch["externalIds"]
    allowed = re.compile(r"^[A-Za-z0-9_.:-]+$")
    assert set(ext.keys()) == {"EncompassId", "badkey"}
    assert all(allowed.match(k) for k in ext)
    assert all(allowed.match(v) for v in ext.values())
