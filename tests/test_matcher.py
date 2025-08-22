from encompass_to_samsara.matcher import probable_match, haversine_m

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
