from encompass_to_samsara.transform import compute_fingerprint, normalize, to_address_payload, SourceRow, validate_lat_lon

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
    tags = {"managedby:encompasssync":"1", "austin":"10", "jeco":"20", "candidatedelete":"2"}
    payload = to_address_payload(row, tags, radius_m=75)
    assert payload["externalIds"]["encompass_id"] == "C123"
    assert payload["externalIds"]["ENCOMPASS_MANAGED"] == "1"
    assert "ENCOMPASS_FINGERPRINT" in payload["externalIds"]
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
