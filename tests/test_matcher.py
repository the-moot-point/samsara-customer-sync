from encompass_to_samsara.matcher import (
    index_addresses_by_external_id,
    probable_match,
    haversine_m,
)

def test_haversine_reasonable():
    d = haversine_m(30.2672, -97.7431, 30.2672, -97.7431)
    assert d == 0.0

def test_probable_match_by_name_address():
    row_name = "Foo Store"
    row_addr = "123 Main St"
    cand = [
        {"id":"1","name":"Foo Store","formattedAddress":"123 Main St"},
        {"id":"2","name":"Bar","formattedAddress":"456 Other"},
    ]
    m = probable_match(row_name, row_addr, None, None, cand)
    assert m and m["id"] == "1"

def test_probable_match_by_distance():
    row_name = "X"
    row_addr = "Y"
    cand = [
        {"id":"10","name":"A","formattedAddress":"Z","geofence":{"center":{"latitude":30.0,"longitude":-97.0}}},
        {"id":"11","name":"B","formattedAddress":"Q","geofence":{"center":{"latitude":30.0001,"longitude":-97.0001}}},
    ]
    m = probable_match(row_name, row_addr, 30.00009, -97.00009, cand, distance_threshold_m=25.0)
    assert m and m["id"] in ("10","11")


def test_probable_match_tie_break_by_address():
    """When distances tie, prefer candidate with matching address."""
    row_name = "Foo"
    row_addr = "123 Main St"
    cand = [
        {
            "id": "1",
            "name": "Other",
            "formattedAddress": "123 Main St",
            "geofence": {"center": {"latitude": 30.0, "longitude": -97.0}},
        },
        {
            "id": "2",
            "name": "Foo",
            "formattedAddress": "456 Other",
            "geofence": {"center": {"latitude": 30.0, "longitude": -97.0}},
        },
    ]
    m = probable_match(row_name, row_addr, 30.0, -97.0, cand, distance_threshold_m=25.0)
    assert m and m["id"] == "1"


def test_probable_match_tie_break_by_name():
    """When distances tie and no address matches, use name similarity."""
    row_name = "Foo"
    row_addr = "123 Main St"
    cand = [
        {
            "id": "1",
            "name": "Foo",
            "formattedAddress": "456 Other",
            "geofence": {"center": {"latitude": 30.0, "longitude": -97.0}},
        },
        {
            "id": "2",
            "name": "Bar",
            "formattedAddress": "789 Else",
            "geofence": {"center": {"latitude": 30.0, "longitude": -97.0}},
        },
    ]
    m = probable_match(row_name, row_addr, 30.0, -97.0, cand, distance_threshold_m=25.0)
    assert m and m["id"] == "1"


def test_index_addresses_by_external_id():
    addrs = [
        {"id": "a", "externalIds": {"encompass_id": "123"}},
        {"id": "b", "externalIds": {"ENCOMPASS_ID": "456"}},
        {"id": "c", "externalIds": {"other": "789"}},
    ]
    idx = index_addresses_by_external_id(addrs)
    assert idx["123"]["id"] == "a"
    assert idx["456"]["id"] == "b"
    assert "789" not in idx
    assert len(idx) == 2
