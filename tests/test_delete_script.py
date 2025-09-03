import csv
from pathlib import Path

from openpyxl import Workbook

from encompass_to_samsara.samsara_client import SamsaraClient
from encompass_to_samsara.scripts import delete_addresses


def _write_csv(path: Path, ids: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["ID"])
        w.writeheader()
        for i in ids:
            w.writerow({"ID": i})


def _write_xlsx(path: Path, ids: list[str]) -> None:
    wb = Workbook()
    ws = wb.active
    ws.append(["ID"])
    for i in ids:
        ws.append([i])
    wb.save(path)


def test_delete_from_csv(tmp_path, monkeypatch):
    ids = ["10", "20"]
    file_path = tmp_path / "ids.csv"
    _write_csv(file_path, ids)

    deleted: list[str] = []
    monkeypatch.setenv("SAMSARA_BEARER_TOKEN", "token")
    monkeypatch.setattr(SamsaraClient, "delete_address", lambda self, aid: deleted.append(aid))

    delete_addresses.main([str(file_path)])

    assert deleted == ids


def test_delete_from_excel(tmp_path, monkeypatch):
    ids = ["30", "40"]
    file_path = tmp_path / "ids.xlsx"
    _write_xlsx(file_path, ids)

    deleted: list[str] = []
    monkeypatch.setenv("SAMSARA_BEARER_TOKEN", "token")
    monkeypatch.setattr(SamsaraClient, "delete_address", lambda self, aid: deleted.append(aid))

    delete_addresses.main([str(file_path)])

    assert deleted == ids

