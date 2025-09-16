from encompass_to_samsara.driver_sync import build_driver_patch, compute_paycom_fingerprint


def test_compute_paycom_fingerprint_ignores_contact_fields():
    base_row = {
        "Employee_ID": "123",
        "First_Name": "Ada",
        "Last_Name": "Lovelace",
        "Status": "Active",
        "Department": "Engineering",
        "Work_Email": "ada@example.com",
        "Primary_Phone": "555-000-1111",
    }

    fingerprint1 = compute_paycom_fingerprint(base_row)

    modified_contacts = {
        **base_row,
        "Work_Email": "ada@new.example.com",
        "Primary_Phone": "555-123-4567",
        "Secondary_Phone": "555-765-4321",
    }

    fingerprint2 = compute_paycom_fingerprint(modified_contacts)

    assert fingerprint1 == fingerprint2

    changed_core = {**base_row, "Status": "Inactive"}
    fingerprint3 = compute_paycom_fingerprint(changed_core)

    assert fingerprint3 != fingerprint1


def test_build_driver_patch_updates_external_id_and_contacts():
    row = {
        "Employee_ID": "123",
        "First_Name": "Ada",
        "Last_Name": "Lovelace",
        "Status": "Active",
        "Department": "Engineering",
        "Work_Email": "ada@new.example.com",
        "Primary_Phone": "555-123-4567",
        "Secondary_Phone": "555-765-4321",
    }

    samsara_driver = {
        "externalIds": {"paycom_fingerprint": "old"},
        "metadata": {
            "Work_Email": "ada@example.com",
            "Primary_Phone": "555-000-0000",
            "other": "keep",
        },
        "email": "ada@example.com",
        "primaryPhone": "555-000-0000",
    }

    patch = build_driver_patch(row, samsara_driver)

    expected_fp = compute_paycom_fingerprint(row)
    assert patch["externalIds"]["paycom_fingerprint"] == expected_fp
    assert patch["email"] == "ada@new.example.com"
    assert patch["primaryPhone"] == "555-123-4567"
    assert patch["secondaryPhone"] == "555-765-4321"
    assert patch["metadata"]["Work_Email"] == "ada@new.example.com"
    assert patch["metadata"]["other"] == "keep"


def test_build_driver_patch_contact_only_change():
    row = {
        "Employee_ID": "123",
        "First_Name": "Ada",
        "Last_Name": "Lovelace",
        "Status": "Active",
        "Department": "Engineering",
        "Work_Email": "ada@new.example.com",
        "Primary_Phone": "555-123-4567",
    }

    fingerprint = compute_paycom_fingerprint(row)

    samsara_driver = {
        "externalIds": {"paycom_fingerprint": fingerprint},
        "metadata": {
            "Work_Email": "ada@example.com",
            "Primary_Phone": "555-000-0000",
        },
        "email": "ada@example.com",
        "primaryPhone": "555-000-0000",
    }

    patch = build_driver_patch(row, samsara_driver)

    assert "externalIds" not in patch
    assert patch["email"] == "ada@new.example.com"
    assert patch["primaryPhone"] == "555-123-4567"
    assert patch["metadata"]["Work_Email"] == "ada@new.example.com"


def test_build_driver_patch_no_changes_returns_empty():
    row = {
        "Employee_ID": "123",
        "First_Name": "Ada",
        "Last_Name": "Lovelace",
        "Status": "Active",
        "Department": "Engineering",
        "Work_Email": "ada@example.com",
        "Primary_Phone": "555-000-0000",
    }

    fingerprint = compute_paycom_fingerprint(row)

    samsara_driver = {
        "externalIds": {"paycom_fingerprint": fingerprint},
        "metadata": {
            "Work_Email": "ada@example.com",
            "Primary_Phone": "555-000-0000",
        },
        "email": "ada@example.com",
        "primaryPhone": "555-000-0000",
    }

    patch = build_driver_patch(row, samsara_driver)

    assert patch == {}
