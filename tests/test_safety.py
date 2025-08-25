import pytest
from datetime import datetime, timedelta, timezone

from encompass_to_samsara.safety import (
    load_warehouses,
    is_warehouse,
    is_managed,
    eligible_for_hard_delete,
)


@pytest.fixture
def warehouse_csv(tmp_path):
    content = """samsara_id,name
1,Main
, 
2,Second
"""
    path = tmp_path / "warehouses.csv"
    path.write_text(content)
    return str(path)


@pytest.fixture
def warehouse_yaml(tmp_path):
    content = """
warehouses:
  - samsara_id: "3"
    name: "Third"
  - samsara_id: 
    name:
"""
    path = tmp_path / "warehouses.yaml"
    path.write_text(content)
    return str(path)


@pytest.fixture
def address_samples():
    return {
        "warehouse_id": {"id": "1", "name": "Foo"},
        "warehouse_name": {"id": "999", "name": "Main"},
        "not_warehouse": {"id": "999", "name": "Unknown"},
        "managed_external": {"externalIds": {"encompass_id": "abc"}},
        "managed_tag": {"tags": [{"id": "M1"}]},
        "unmanaged": {"tags": [{"id": "X"}]},
    }


def test_load_warehouses_from_csv(warehouse_csv):
    ids, names = load_warehouses(warehouse_csv)
    assert ids == {"1", "2"}
    assert names == {"main", "second"}


def test_load_warehouses_from_yaml(warehouse_yaml):
    ids, names = load_warehouses(warehouse_yaml)
    assert ids == {"3"}
    assert names == {"third"}


def test_is_warehouse(address_samples, warehouse_csv):
    ids, names = load_warehouses(warehouse_csv)
    assert is_warehouse(address_samples["warehouse_id"], ids, names)
    assert is_warehouse(address_samples["warehouse_name"], ids, names)
    assert not is_warehouse(address_samples["not_warehouse"], ids, names)


def test_is_managed(address_samples):
    managed_tag_id = "M1"
    assert is_managed(address_samples["managed_external"], managed_tag_id)
    assert is_managed(address_samples["managed_tag"], managed_tag_id)
    assert not is_managed(address_samples["unmanaged"], managed_tag_id)


def test_eligible_for_hard_delete():
    now = datetime.utcnow().replace(tzinfo=timezone.utc)
    old_ts = (now - timedelta(days=31)).isoformat()
    recent_ts = (now - timedelta(days=1)).isoformat()
    state = {
        "candidate_deletes": {
            "old": old_ts,
            "recent": recent_ts,
            "bad": "not-a-date",
        }
    }
    assert eligible_for_hard_delete("old", state, 30)
    assert not eligible_for_hard_delete("recent", state, 30)
    assert not eligible_for_hard_delete("missing", state, 30)
    assert not eligible_for_hard_delete("bad", state, 30)
