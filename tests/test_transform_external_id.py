import re
from datetime import datetime

from encompass_to_samsara.transform import (
    DELETE_MARKER_KEY,
    build_delete_marker_value,
    sanitize_external_id_value,
    validate_external_id_key,
)


def test_delete_marker_key_is_alnum():
    assert validate_external_id_key(DELETE_MARKER_KEY) == DELETE_MARKER_KEY


def test_sanitize_value_allows_hyphen():
    assert sanitize_external_id_value("foo-bar") == "foo-bar"


def test_build_delete_marker_value_shape():
    now = datetime(2024, 8, 29, 12, 34, 56)
    val = build_delete_marker_value("123", now=now)
    assert val == "20240829T123456-123"
    assert re.fullmatch(r"\d{8}T\d{6}-123", val)
