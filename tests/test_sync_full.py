import csv
import json

import responses

from encompass_to_samsara.samsara_client import SamsaraClient
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


def test_full_upsert_and_quarantine(tmp_path, token_env, base_responses):
    # Input CSV
    source_rows = [
        {
            "Customer ID": "C1",
            "Customer Name": "Foo",
            "Account Status": "Active",
            "Latitude": "30.1",
            "Longitude": "-97.7",
            "Report Address": "123 A St",
            "Location": "Austin",
            "Company": "JECO",
            "Customer Type": "Retail",
        },
        {
            "Customer ID": "C2",
            "Customer Name": "Bar",
            "Account Status": "Active",
            "Latitude": "30.2",
            "Longitude": "-97.8",
            "Report Address": "456 B St",
            "Location": "Austin",
            "Company": "JECO",
            "Customer Type": "Retail",
        },
    ]
    src_csv = tmp_path / "encompass_full.csv"
    write_csv(src_csv, source_rows)

    # Warehouses denylist (empty)
    wh_csv = tmp_path / "warehouses.csv"
    with open(wh_csv, "w", encoding="utf-8") as f:
        f.write("samsara_id,name\n")

    out_dir = tmp_path / "out"

    # Existing Samsara addresses: one managed orphan and one unmanaged candidate
    samsara_addresses = [
        {
            "id": "100",
            "name": "Orphan1",
            "formattedAddress": "XYZ",
            "externalIds": {"EncompassId": "OLD"},
            "tagIds": ["1"],
        },  # managed orphan
    ]

    with base_responses as rsps:
        rsps.add(
            responses.GET, f"{API}/addresses", json={"addresses": samsara_addresses}, status=200
        )
        # create two addresses
        rsps.add(responses.POST, f"{API}/addresses", json={"id": "201"}, status=200)
        rsps.add(responses.POST, f"{API}/addresses", json={"id": "202"}, status=200)
        # patch orphan to add CandidateDelete tag
        rsps.add(responses.PATCH, f"{API}/addresses/100", json={"id": "100"}, status=200)

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
    with open(out_dir / "actions.jsonl", encoding="utf-8") as f:
        acts = [json.loads(line) for line in f]
    kinds = [a["kind"] for a in acts]
    assert kinds.count("create") == 2
    assert "quarantine" in kinds
    geos = [
        a["payload"]["geofence"]
        for a in acts
        if a.get("payload") and a["payload"].get("geofence")
    ]
    assert geos and all("circle" in g and "center" not in g for g in geos)
    # Report exists
    assert (out_dir / "sync_report.csv").exists()
    assert (out_dir / "state.json").exists()


def test_reuse_address_by_name(tmp_path, token_env, base_responses):
    source_rows = [
        {
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
    ]
    src_csv = tmp_path / "encompass_full.csv"
    write_csv(src_csv, source_rows)

    wh_csv = tmp_path / "warehouses.csv"
    with open(wh_csv, "w", encoding="utf-8") as f:
        f.write("samsara_id,name\n")

    out_dir = tmp_path / "out"

    samsara_addresses = [
        {
            "id": "101",
            "name": "Foo",
            "formattedAddress": "Old",
            "geofence": {
                "radiusMeters": 50,
                "center": {"latitude": 0.0, "longitude": 0.0},
            },
        }
    ]

    with base_responses as rsps:
        rsps.add(
            responses.GET,
            f"{API}/addresses",
            json={"addresses": samsara_addresses},
            status=200,
        )
        rsps.add(
            responses.PATCH,
            f"{API}/addresses/101",
            json={"id": "101"},
            status=200,
        )

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

        patch_calls = [c for c in rsps.calls if c.request.method == "PATCH"]
        post_calls = [c for c in rsps.calls if c.request.method == "POST"]
        assert len(post_calls) == 0
        assert len(patch_calls) == 1
        req_body = patch_calls[0].request.body
        if isinstance(req_body, bytes):
            req_body = req_body.decode()
        body = json.loads(req_body)
        assert body["externalIds"]["encompass_id"] == "C1"

    with open(out_dir / "actions.jsonl", encoding="utf-8") as f:
        acts = [json.loads(line) for line in f]
    assert acts[0]["kind"] == "update"
    geos = [
        a["payload"]["geofence"]
        for a in acts
        if a.get("payload") and a["payload"].get("geofence")
    ]
    assert geos and all("circle" in g and "center" not in g for g in geos)


def test_full_skip_inactive_status(tmp_path, token_env, base_responses):
    source_rows = [
        {
            "Customer ID": "C1",
            "Customer Name": "Foo",
            "Account Status": "Inactive",
            "Latitude": "30.1",
            "Longitude": "-97.7",
            "Report Address": "123 A St",
            "Location": "Austin",
            "Company": "JECO",
            "Customer Type": "Retail",
        }
    ]
    src_csv = tmp_path / "encompass_full.csv"
    write_csv(src_csv, source_rows)

    wh_csv = tmp_path / "warehouses.csv"
    with open(wh_csv, "w", encoding="utf-8") as f:
        f.write("samsara_id,name\n")

    out_dir = tmp_path / "out"

    with base_responses as rsps:
        rsps.add(responses.GET, f"{API}/addresses", json={"addresses": []}, status=200)
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

    with open(out_dir / "actions.jsonl", encoding="utf-8") as f:
        acts = [json.loads(line) for line in f]
    assert acts[0]["kind"] == "skip" and acts[0]["reason"] == "inactive_status"
    assert all(c.request.method == "GET" for c in rsps.calls)


def test_actions_radius_meters_int(tmp_path, token_env, base_responses):
    source_rows = [
        {
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
    ]
    src_csv = tmp_path / "encompass_full.csv"
    write_csv(src_csv, source_rows)

    wh_csv = tmp_path / "warehouses.csv"
    with open(wh_csv, "w", encoding="utf-8") as f:
        f.write("samsara_id,name\n")

    out_dir = tmp_path / "out"

    with base_responses as rsps:
        rsps.add(responses.GET, f"{API}/addresses", json={"addresses": []}, status=200)
        rsps.add(responses.POST, f"{API}/addresses", json={"id": "201"}, status=200)
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

    with open(out_dir / "actions.jsonl", encoding="utf-8") as f:
        acts = [json.loads(line) for line in f]
    create_act = next(a for a in acts if a["kind"] == "create")
    radius = create_act["payload"]["geofence"]["circle"]["radiusMeters"]
    assert isinstance(radius, int)
