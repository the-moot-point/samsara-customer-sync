import json
import os
import csv
import tempfile
import responses

from encompass_to_samsara.sync_full import run_full
from encompass_to_samsara.samsara_client import SamsaraClient

API = "https://api.samsara.com"

def write_csv(path, rows):
    with open(path, "w", encoding="utf-8", newline="") as f:
        if not rows:
            return
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        for r in rows:
            w.writerow(r)

def test_full_upsert_and_quarantine(tmp_path, token_env, base_responses):
    # Input CSV
    source_rows = [
        {"Customer ID":"C1","Customer Name":"Foo","Account Status":"Active","Latitude":"30.1","Longitude":"-97.7","Report Company Address":"123 A St","Location":"Austin","Company":"JECO","Customer Type":"Retail"},
        {"Customer ID":"C2","Customer Name":"Bar","Account Status":"Active","Latitude":"30.2","Longitude":"-97.8","Report Company Address":"456 B St","Location":"Austin","Company":"JECO","Customer Type":"Retail"},
    ]
    src_csv = tmp_path/"encompass_full.csv"
    write_csv(src_csv, source_rows)

    # Warehouses denylist (empty)
    wh_csv = tmp_path/"warehouses.csv"
    with open(wh_csv, "w", encoding="utf-8") as f:
        f.write("samsara_id,name\n")

    out_dir = tmp_path/"out"

    # Existing Samsara addresses: one managed orphan and one unmanaged candidate
    samsara_addresses = [
        {"id":"100","name":"Orphan1","formattedAddress":"XYZ","externalIds":{"EncompassId":"OLD"},"tagIds":["1"]},  # managed orphan
    ]

    with base_responses as rsps:
        rsps.add(responses.GET, f"{API}/addresses", json={"addresses": samsara_addresses}, status=200)
        # create two addresses
        rsps.add(responses.POST, f"{API}/addresses", json={"id":"201"}, status=200)
        rsps.add(responses.POST, f"{API}/addresses", json={"id":"202"}, status=200)
        # patch orphan to add CandidateDelete tag
        rsps.add(responses.PATCH, f"{API}/addresses/100", json={"id":"100"}, status=200)

        client = SamsaraClient(api_token="test-token")
        run_full(
            client,
            encompass_csv=str(src_csv),
            warehouses_path=str(wh_csv),
            out_dir=str(out_dir),
            radius_m=50,
            apply=True,
            retention_days=30,
            confirm_delete=False,
        )

    # Verify artifacts
    with open(out_dir/"actions.jsonl","r",encoding="utf-8") as f:
        acts = [json.loads(l) for l in f]
    kinds = [a["kind"] for a in acts]
    assert kinds.count("create") == 2
    assert "quarantine" in kinds
    # Report exists
    assert (out_dir/"sync_report.csv").exists()
    assert (out_dir/"state.json").exists()
