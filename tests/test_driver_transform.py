import hashlib
import json

import pytest

from encompass_to_samsara.driver_transform import (
    compute_paycom_fingerprint,
    transform_driver_payload,
)


def test_compute_paycom_fingerprint_deterministic():
    payload = {
        "Employee_Code": " 12345 ",
        "First_Name": " Alice ",
        "Last_Name": "Smith",
        "nested": {"key": " value ", "list": [1, " 2 "]},
        "externalIds": {"existing": "keep"},
    }
    expected_serialized = json.dumps(
        {
            "Employee_Code": "12345",
            "First_Name": "Alice",
            "Last_Name": "Smith",
            "nested": {"key": "value", "list": [1, "2"]},
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    expected_hash = hashlib.sha256(expected_serialized.encode("utf-8")).hexdigest()

    assert compute_paycom_fingerprint(payload) == expected_hash


def test_compute_paycom_fingerprint_ignores_external_ids():
    base_payload = {
        "Employee_Code": "999",
        "First_Name": "Bob",
        "Last_Name": "Jones",
        "externalIds": {"foo": "bar"},
    }

    fingerprint1 = compute_paycom_fingerprint(base_payload)
    fingerprint2 = compute_paycom_fingerprint(
        {**base_payload, "externalIds": {"foo": "changed"}}
    )

    assert fingerprint1 == fingerprint2


def test_transform_driver_payload_sets_external_ids():
    raw_payload = {
        "Employee_Code": " 007 ",
        "name": "Agent",
        "externalIds": {"legacy": "ABC", "employeecode": "old"},
    }

    transformed = transform_driver_payload(raw_payload)

    external = transformed["externalIds"]
    assert external["employeeCode"] == "007"
    assert external["paycom_fingerprint"] == compute_paycom_fingerprint(raw_payload)
    assert external["legacy"] == "ABC"
    assert "Employee_Code" not in transformed


def test_transform_driver_payload_requires_employee_code():
    with pytest.raises(ValueError):
        transform_driver_payload({"name": "Missing"})


def test_transform_driver_payload_uses_fingerprint_source():
    payload = {"name": "Updated"}
    source = {
        "Employee_Code": "E42",
        "name": "Updated",
        "Work_Email": "user@example.com",
    }

    transformed = transform_driver_payload(payload, fingerprint_source=source)

    external = transformed["externalIds"]
    assert external["employeeCode"] == "E42"
    assert external["paycom_fingerprint"] == compute_paycom_fingerprint(source)
    assert "Employee_Code" not in transformed
    assert transformed["name"] == "Updated"
