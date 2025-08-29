from encompass_to_samsara.transform import (
    SourceRow,
    compute_fingerprint,
    diff_address,
    clean_external_ids,
    normalize,
    sanitize_external_id_value,
    to_address_payload,
    validate_lat_lon,
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
    assert payload["externalIds"]["encompassid"] == "C123"
    assert payload["externalIds"]["encompassmanaged"] == "1"
    assert "fingerprint" in payload["externalIds"]
    assert set(payload["geofence"].keys()) == {"circle"}
    assert payload["geofence"]["circle"]["radiusMeters"] == 75
    assert payload["geofence"]["circle"]["latitude"] == 30.1
    assert payload["geofence"]["circle"]["longitude"] == -97.7
    assert set(payload.get("tagIds", [])) >= {"1","10","20"}


def test_invalid_coordinates_skip_geofence():
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
    assert "geofence" not in payload

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


def test_sanitize_external_id_value_strips_and_truncates(caplog):
    raw = "bad$$$" + "x" * 40
    with caplog.at_level("WARNING"):
        val = sanitize_external_id_value(raw)
    assert val.startswith("bad")
    assert len(val) == 32
    assert any("invalid" in r.message for r in caplog.records)
    assert any("truncated" in r.message for r in caplog.records)


def test_clean_external_ids_sanitizes_and_drops(caplog):
    ext = {"ENCOMPASS_ID": "id!!", "other": "good", "drop": "$$"}
    with caplog.at_level("WARNING"):
        cleaned = clean_external_ids(ext)
    assert cleaned["encompassid"] == "id"
    assert cleaned["other"] == "good"
    assert "drop" not in cleaned


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
    assert ext["encompassid"] == "ID" + "1" * 30
    assert ext["encompassstatus"] == "Active" + "2" * 26
    assert len(ext["fingerprint"]) == 32


def test_diff_address_sanitizes_external_ids():
    existing = {"externalIds": {"encompassid": "id!!"}}
    desired = {"externalIds": {"encompassid": "id@" + "1" * 40}}
    patch = diff_address(existing, desired)
    val = patch["externalIds"]["encompassid"]
    assert val.startswith("id")
    assert len(val) == 32
