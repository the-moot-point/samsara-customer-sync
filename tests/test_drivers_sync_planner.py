from encompass_to_samsara.driver_transform import transform_driver_payload
from encompass_to_samsara.drivers_sync import plan_driver_actions


def _build_existing_driver(
    employee_code: str,
    email: str,
    *,
    tag_ids: list[str] | None = None,
    is_deactivated: bool = False,
) -> dict[str, object]:
    raw = {
        "Employee_Code": employee_code,
        "firstName": "Alice",
        "lastName": "Anderson",
        "email": email,
        "username": "aanderson",
        "isDeactivated": is_deactivated,
    }
    if tag_ids:
        raw["tagIds"] = list(tag_ids)
    transformed = transform_driver_payload(raw, fingerprint_source=raw)
    return {
        "id": f"driver-{employee_code}",
        "firstName": transformed.get("firstName"),
        "lastName": transformed.get("lastName"),
        "email": transformed.get("email"),
        "username": transformed.get("username"),
        "isDeactivated": transformed.get("isDeactivated"),
        "externalIds": transformed.get("externalIds"),
        "tagIds": transformed.get("tagIds"),
    }


def test_plan_create_driver_adds_scope_tag() -> None:
    rows = [
        {
            "Employee_Code": "E1",
            "First_Name": "Alice",
            "Last_Name": "Anderson",
            "Work_Email": "alice@example.com",
            "Status": "Active",
            "username": "aanderson",
        }
    ]

    plan = plan_driver_actions(rows, [], managed_tag_id="TAG1")

    assert len(plan) == 1
    action = plan[0]
    assert action.action == "create"
    assert action.employee_code == "E1"
    assert action.payload["tagIds"] == ["TAG1"]
    assert action.diff  # creating should record differences


def test_plan_update_on_fingerprint_change() -> None:
    existing = _build_existing_driver("E1", "alice@old.example.com", tag_ids=["TAG1"])

    rows = [
        {
            "Employee_Code": "E1",
            "First_Name": "Alice",
            "Last_Name": "Anderson",
            "Work_Email": "alice@new.example.com",
            "Status": "Active",
            "username": "aanderson",
        }
    ]

    plan = plan_driver_actions(rows, [existing], managed_tag_id="TAG1")

    assert len(plan) == 1
    action = plan[0]
    assert action.action == "update"
    assert action.diff["email"]["to"] == "alice@new.example.com"
    assert "externalIds.paycom_fingerprint" in action.diff


def test_plan_skip_when_no_changes() -> None:
    existing = _build_existing_driver("E1", "alice@example.com", tag_ids=["TAG1"])

    rows = [
        {
            "Employee_Code": "E1",
            "First_Name": "Alice",
            "Last_Name": "Anderson",
            "Work_Email": "alice@example.com",
            "Status": "Active",
            "username": "aanderson",
        }
    ]

    plan = plan_driver_actions(rows, [existing], managed_tag_id="TAG1")

    assert len(plan) == 1
    action = plan[0]
    assert action.action == "skip"
    assert action.diff == {}


def test_plan_reactivate_driver() -> None:
    existing = _build_existing_driver(
        "E1",
        "alice@example.com",
        tag_ids=["TAG1"],
        is_deactivated=True,
    )

    rows = [
        {
            "Employee_Code": "E1",
            "First_Name": "Alice",
            "Last_Name": "Anderson",
            "Work_Email": "alice@example.com",
            "Status": "Active",
            "username": "aanderson",
        }
    ]

    plan = plan_driver_actions(rows, [existing], managed_tag_id="TAG1")

    assert len(plan) == 1
    action = plan[0]
    assert action.action == "reactivate"
    assert action.patch["isDeactivated"] is False


def test_plan_deactivate_driver_from_status() -> None:
    existing = _build_existing_driver(
        "E1",
        "alice@example.com",
        tag_ids=["TAG1"],
        is_deactivated=False,
    )

    rows = [
        {
            "Employee_Code": "E1",
            "First_Name": "Alice",
            "Last_Name": "Anderson",
            "Work_Email": "alice@example.com",
            "Status": "Inactive",
            "username": "aanderson",
        }
    ]

    plan = plan_driver_actions(rows, [existing], managed_tag_id="TAG1")

    assert len(plan) == 1
    action = plan[0]
    assert action.action == "deactivate"
    assert action.patch["isDeactivated"] is True


def test_plan_deactivate_orphan_driver() -> None:
    existing = _build_existing_driver(
        "E99",
        "orphan@example.com",
        tag_ids=["TAG1"],
        is_deactivated=False,
    )

    plan = plan_driver_actions([], [existing], managed_tag_id="TAG1")

    assert len(plan) == 1
    action = plan[0]
    assert action.action == "deactivate"
    assert action.reason == "orphan"
    assert action.patch["isDeactivated"] is True
