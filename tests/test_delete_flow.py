from pathlib import Path

from encompass_to_samsara.sync_daily import run_daily
from encompass_to_samsara.transform import DELETE_MARKER_KEY


class DummyClient:
    def __init__(self, addresses):
        self._addresses = addresses
        self.patched = []
        self.deleted = []

    def list_tags(self):
        return [{"id": "1", "name": "ManagedBy:EncompassSync"}]

    def list_addresses(self):
        return self._addresses

    def patch_address(self, aid, payload):
        self.patched.append((aid, payload))

    def delete_address(self, aid):
        self.deleted.append(aid)


def write_csv(path: Path, rows):
    import csv

    with open(path, "w", encoding="utf-8", newline="") as f:
        if not rows:
            return
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        for r in rows:
            w.writerow(r)


def test_delete_direct_when_retention_zero(tmp_path, monkeypatch):
    delta_rows = [
        {
            "Customer ID": "C1",
            "Customer Name": "Foo",
            "Account Status": "Active",
            "Latitude": "30",
            "Longitude": "-97",
            "Report Address": "123",
            "Location": "Austin",
            "Company": "JECO",
            "Customer Type": "Retail",
            "Action": "delete",
        }
    ]
    d_csv = tmp_path / "delta.csv"
    write_csv(d_csv, delta_rows)
    wh_csv = tmp_path / "warehouses.csv"
    write_csv(wh_csv, [])
    out_dir = tmp_path / "out"

    addresses = [{"id": "300", "externalIds": {"EncompassId": "C1"}, "tagIds": ["1"]}]
    client = DummyClient(addresses)

    run_daily(
        client,
        encompass_delta=str(d_csv),
        warehouses_path=str(wh_csv),
        out_dir=str(out_dir),
        radius_m=50,
        apply=True,
        retention_days=0,
        confirm_delete=True,
    )

    assert client.deleted == ["300"]
    assert client.patched == []


def test_mark_delete_with_retention(tmp_path, monkeypatch):
    delta_rows = [
        {
            "Customer ID": "C1",
            "Customer Name": "Foo",
            "Account Status": "Active",
            "Latitude": "30",
            "Longitude": "-97",
            "Report Address": "123",
            "Location": "Austin",
            "Company": "JECO",
            "Customer Type": "Retail",
            "Action": "delete",
        }
    ]
    d_csv = tmp_path / "delta.csv"
    write_csv(d_csv, delta_rows)
    wh_csv = tmp_path / "warehouses.csv"
    write_csv(wh_csv, [])
    out_dir = tmp_path / "out"

    addresses = [{"id": "300", "externalIds": {"EncompassId": "C1"}, "tagIds": ["1"]}]
    client = DummyClient(addresses)

    monkeypatch.setattr(
        "encompass_to_samsara.sync_daily.build_delete_marker_value", lambda aid: "TS-" + aid
    )

    run_daily(
        client,
        encompass_delta=str(d_csv),
        warehouses_path=str(wh_csv),
        out_dir=str(out_dir),
        radius_m=50,
        apply=True,
        retention_days=5,
        confirm_delete=True,
    )

    assert client.deleted == []
    assert client.patched == [("300", {"externalIds": {DELETE_MARKER_KEY: "TS-300"}})]
    payload = client.patched[0][1]
    assert "EncompassId" not in payload["externalIds"]
