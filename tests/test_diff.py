import pytest


def test_diff_address_returns_only_changes():
    from encompass_to_samsara.transform import diff_address

    existing = {
        "name": "Old Name",
        "formattedAddress": "123 Main St",
        "geofence": {
            "circle": {
                "radiusMeters": 50,
                "center": {"latitude": 10.0, "longitude": 20.0},
            }
        },
        "tagIds": ["1", "2"],
        "externalIds": {
            "EncompassId": "abc",
            "ENCOMPASS_STATUS": "Active",
            "ENCOMPASS_MANAGED": "1",
            "ENCOMPASS_FINGERPRINT": "fp1",
            "OTHER": "keep",
        },
    }

    desired = {
        "name": "New Name",
        "formattedAddress": "456 Elm St",
        "geofence": {
            "circle": {
                "radiusMeters": 75,
                "center": {"latitude": 11.0, "longitude": 21.0},
            }
        },
        "tagIds": ["1", "3"],
        "externalIds": {
            "encompass_id": "abc",
            "ENCOMPASS_STATUS": "Inactive",
            "ENCOMPASS_MANAGED": "1",
            "ENCOMPASS_FINGERPRINT": "fp2",
            "ENCOMPASS_TYPE": "Retail",
            "OTHER": "keep",
        },
    }

    expected = {
        "name": "New Name",
        "formattedAddress": "456 Elm St",
        "geofence": {
            "circle": {
                "radiusMeters": 75,
                "center": {"latitude": 11.0, "longitude": 21.0},
            }
        },
        "tagIds": ["1", "3"],
        "externalIds": {
            "encompass_id": "abc",
            "ENCOMPASS_STATUS": "Inactive",
            "ENCOMPASS_MANAGED": "1",
            "ENCOMPASS_FINGERPRINT": "fp2",
            "ENCOMPASS_TYPE": "Retail",
            "OTHER": "keep",
        },
    }

    patch = diff_address(existing, desired)
    assert patch == expected


def test_diff_address_preserves_polygon_geofence():
    from encompass_to_samsara.transform import diff_address

    existing = {
        "name": "Old Name",
        "formattedAddress": "123 Main St",
        "geofence": {"polygon": {"type": "Polygon", "coordinates": []}},
    }
    desired = {
        "name": "New Name",
        "formattedAddress": "123 Main St",
        "geofence": {
            "circle": {
                "radiusMeters": 75,
                "center": {"latitude": 11.0, "longitude": 21.0},
            }
        },
    }
    patch = diff_address(existing, desired)
    assert patch == {"name": "New Name"}


def test_diff_address_handles_missing_circle_geofence():
    from encompass_to_samsara.transform import diff_address

    existing = {
        "name": "Same Name",
        "formattedAddress": "123 Main St",
        # malformed legacy geofence lacking "circle"
        "geofence": {
            "radiusMeters": 50,
            "center": {"latitude": 10.0, "longitude": 20.0},
        },
    }
    desired = {
        "name": "Same Name",
        "formattedAddress": "123 Main St",
        "geofence": {
            "circle": {
                "radiusMeters": 60,
                "center": {"latitude": 11.0, "longitude": 21.0},
            }
        },
    }

    patch = diff_address(existing, desired)
    assert patch == {"geofence": desired["geofence"]}


@pytest.mark.parametrize(
    "tag_data, desired_tags",
    [
        ({"tags": [{"id": "99", "name": "Updated Geofence"}]}, {"tagIds": ["99"]}),
        (
            {"tagIds": ["99"], "tagNames": {"99": "Updated Geofence"}},
            {"tagIds": ["99"]},
        ),
    ],
)
def test_diff_address_skips_geofence_with_updated_tag(tag_data, desired_tags):
    from encompass_to_samsara.transform import diff_address

    existing = {
        "name": "Old Name",
        "formattedAddress": "123 Main St",
        "geofence": {
            "circle": {
                "radiusMeters": 50,
                "center": {"latitude": 10.0, "longitude": 20.0},
            }
        },
        **tag_data,
    }
    desired = {
        "name": "New Name",
        "formattedAddress": "123 Main St",
        "geofence": {
            "circle": {
                "radiusMeters": 75,
                "center": {"latitude": 11.0, "longitude": 21.0},
            }
        },
        **desired_tags,
    }
    patch = diff_address(existing, desired)
    assert patch == {"name": "New Name"}
