import csv
import json

import pytest
import responses

from encompass_to_samsara.samsara_client import SamsaraClient
from encompass_to_samsara.sync_daily import run_daily
from encompass_to_samsara.sync_full import run_full

API = "https://api.samsara.com"


def write_csv(path, rows):
    with open(path, "w", encoding="utf-8", newline="") as f:
        if not rows:
            return
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        for r in rows:
            w.writerow(r)


@pytest.mark.parametrize(
    "sync_func,src_arg",
    [
        (run_full, "encompass_csv"),
        (run_daily, "encompass_delta"),
    ],
)
def test_actions_jsonl_geofence_always_circle(
    tmp_path, token_env, base_responses, sync_func, src_arg
):
    row = {
        "Customer ID": "C1",
        "Customer Name": "Foo",
        "Account Status": "Active",
        "Latitude": "30.1",
        "Longitude": "-97.7",
        "Report Address": "123 A St",
        "Location": "Austin",
        "Company": "JECO",
        "Customer Type": "Retail",
    }
    if src_arg == "encompass_delta":
        row["Action"] = "upsert"
    src_csv = tmp_path / "src.csv"
    write_csv(src_csv, [row])

    wh_csv = tmp_path / "warehouses.csv"
    with open(wh_csv, "w", encoding="utf-8") as f:
        f.write("samsara_id,name\n")

    out_dir = tmp_path / "out"

    with base_responses as rsps:
        rsps.add(responses.GET, f"{API}/addresses", json={"addresses": []}, status=200)
        rsps.add(responses.POST, f"{API}/addresses", json={"id": "101"}, status=200)

        client = SamsaraClient(api_token="test-token")
        kwargs = {
            src_arg: str(src_csv),
            "warehouses_path": str(wh_csv),
            "out_dir": str(out_dir),
            "radius_m": 50,
            "apply": True,
            "retention_days": 30,
            "confirm_delete": False,
        }
        sync_func(client, **kwargs)

    with open(out_dir / "actions.jsonl", encoding="utf-8") as f:
        geos = [
            a["payload"]["geofence"]
            for a in (json.loads(line) for line in f)
            if a.get("payload") and a["payload"].get("geofence")
        ]
    assert geos and all("circle" in g and "center" not in g for g in geos)

