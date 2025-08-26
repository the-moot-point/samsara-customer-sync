import csv
import json
import os

import responses

from encompass_to_samsara.samsara_client import SamsaraClient
from encompass_to_samsara.sync_daily import run_daily
from encompass_to_samsara.transform import compute_fingerprint

API = "https://api.samsara.com"


def write_csv(path, rows):
    with open(path, "w", encoding="utf-8", newline="") as f:
        if not rows:
            return
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        for r in rows:
            w.writerow(r)


def test_daily_upsert_skip_when_unchanged(tmp_path, token_env, base_responses):
    # Delta CSV
    delta_rows = [
        {
            "Customer ID": "C1",
            "Customer Name": "Foo",
            "Account Status": "Active",
            "Latitude": "30.1",
            "Longitude": "-97.7",
            "Report Company Address": "123 A St",
            "Location": "Austin",
            "Company": "JECO",
            "Customer Type": "Retail",
            "Action": "upsert",
        },
    ]
    d_csv = tmp_path / "encompass_delta.csv"
    write_csv(d_csv, delta_rows)

    wh_csv = tmp_path / "warehouses.csv"
    with open(wh_csv, "w", encoding="utf-8") as f:
        f.write("samsara_id,name\n")

    out_dir = tmp_path / "out"

    # Existing matching address with same fingerprint
    # We'll compute via running once with create, then second run will skip
    fp = compute_fingerprint("Foo", "Active", "123 A St")
    samsara_addresses = [
        {
            "id": "300",
            "name": "Foo",
            "formattedAddress": "123 A St",
            "externalIds": {
                "encompass_id": "C1",
                "ENCOMPASS_STATUS": "Active",
                "ENCOMPASS_MANAGED": "1",
                "ENCOMPASS_FINGERPRINT": fp,
            },
        },
    ]

    with base_responses as rsps:
        rsps.add(
            responses.GET, f"{API}/addresses", json={"addresses": samsara_addresses}, status=200
        )
        # On update, no write should happen because we set the same fingerprint in state
        client = SamsaraClient(api_token="test-token")
        # Seed state
        os.makedirs(out_dir, exist_ok=True)
        with open(out_dir / "state.json", "w", encoding="utf-8") as f:
            json.dump({"fingerprints": {"300": fp}, "candidate_deletes": {}}, f)

        run_daily(
            client,
            encompass_delta=str(d_csv),
            warehouses_path=str(wh_csv),
            out_dir=str(out_dir),
            radius_m=50,
            apply=True,
            retention_days=30,
            confirm_delete=False,
        )

    # Verify actions includes skip
    with open(out_dir / "actions.jsonl", encoding="utf-8") as f:
        acts = [json.loads(line) for line in f]
    assert any(a["kind"] == "skip" and a["reason"] == "unchanged_fingerprint" for a in acts)


def test_daily_skip_inactive_status(tmp_path, token_env, base_responses):
    delta_rows = [
        {
            "Customer ID": "C1",
            "Customer Name": "Foo",
            "Account Status": "Inactive",
            "Latitude": "30.1",
            "Longitude": "-97.7",
            "Report Company Address": "123 A St",
            "Location": "Austin",
            "Company": "JECO",
            "Customer Type": "Retail",
            "Action": "upsert",
        }
    ]
    d_csv = tmp_path / "encompass_delta.csv"
    write_csv(d_csv, delta_rows)

    wh_csv = tmp_path / "warehouses.csv"
    with open(wh_csv, "w", encoding="utf-8") as f:
        f.write("samsara_id,name\n")

    out_dir = tmp_path / "out"

    with base_responses as rsps:
        rsps.add(responses.GET, f"{API}/addresses", json={"addresses": []}, status=200)
        client = SamsaraClient(api_token="test-token")
        run_daily(
            client,
            encompass_delta=str(d_csv),
            warehouses_path=str(wh_csv),
            out_dir=str(out_dir),
            radius_m=50,
            apply=True,
            retention_days=30,
            confirm_delete=False,
        )

    with open(out_dir / "actions.jsonl", encoding="utf-8") as f:
        acts = [json.loads(line) for line in f]
    assert any(a["kind"] == "skip" and a["reason"] == "inactive_status" for a in acts)
