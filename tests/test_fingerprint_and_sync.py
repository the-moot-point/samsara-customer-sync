import copy
import csv
import json
from datetime import datetime
from pathlib import Path

import pytest
import responses

from encompass_to_samsara.state import load_state
from encompass_to_samsara.tags import build_tag_index, resolve_tag_id
from encompass_to_samsara.transform import (
    build_delete_marker_value,
    compute_fingerprint,
    normalize,
)
from encompass_to_samsara.sync_full import run_full


@pytest.fixture
def sample_encompass_row() -> dict:
    data_path = Path(__file__).resolve().parents[1] / "data" / "encompass_delta_example.csv"
    with open(data_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        row = next(reader)
    return row


def test_sample_fingerprint_deterministic(sample_encompass_row):
    name = sample_encompass_row["Customer Name"]
    status = sample_encompass_row["Account Status"]
    addr = sample_encompass_row["Report Address"]

    expected = "3dc1844f01f2ab519f9e33341741e1d128d49195eb8a339247ce81276dda60e5"
    first = compute_fingerprint(name, status, addr)
    second = compute_fingerprint(f"  {name.lower()}  ", status.lower(), addr.replace(", ", " , "))

    assert first == expected
    assert second == expected


class _DummyClient:
    def __init__(self, tags_payload: dict):
        self._payload = tags_payload

    def list_tags(self):
        return self._payload["tags"]


def test_normalization_rules(sample_state_file, tags_payload):
    assert normalize("  ACME, Inc.  ") == "acme inc"
    assert normalize(" Active ") == "active"

    state = load_state(str(sample_state_file))
    assert state["fingerprints"] == {}
    assert state["candidate_deletes"] == {}

    tag_index = build_tag_index(_DummyClient(tags_payload))
    assert resolve_tag_id(tag_index, "ManagedBy:EncompassSync") == "1"
    assert resolve_tag_id(tag_index, "austin") == "10"

    marker = build_delete_marker_value(
        "C 123", now=datetime(2024, 8, 21, 15, 26, 5)
    )
    assert marker == "20240821T152605-C123"


def _write_encompass_csv(path: Path, row: dict) -> None:
    fieldnames = [
        "Customer ID",
        "Customer Name",
        "Account Status",
        "Latitude",
        "Longitude",
        "Report Address",
        "Location",
        "Company",
    ]
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow({k: row.get(k, "") for k in fieldnames})


def test_sync_patches_when_fingerprint_changes(
    tmp_path,
    client,
    warehouses_csv,
    base_responses,
    sample_encompass_row,
    sample_samsara_address,
):
    src_csv = tmp_path / "encompass_full.csv"
    _write_encompass_csv(src_csv, sample_encompass_row)
    out_dir = tmp_path / "out"

    expected_fp = compute_fingerprint(
        sample_encompass_row["Customer Name"],
        sample_encompass_row["Account Status"],
        sample_encompass_row["Report Address"],
    )

    existing = copy.deepcopy(sample_samsara_address)
    existing["externalIds"]["EncompassId"] = sample_encompass_row["Customer ID"]
    existing["externalIds"]["fingerprint"] = "outdated"
    existing["formattedAddress"] = "Legacy Address"
    existing["name"] = "Legacy Store"

    with base_responses as rsps:
        rsps.add(
            responses.GET,
            f"{client.base_url}/addresses",
            json={"addresses": [existing]},
            status=200,
        )
        rsps.add(
            responses.PATCH,
            f"{client.base_url}/addresses/{existing['id']}",
            json={"id": existing["id"]},
            status=200,
        )

        run_full(
            client,
            encompass_csv=str(src_csv),
            warehouses_path=str(warehouses_csv),
            out_dir=str(out_dir),
            radius_m=50,
            apply=True,
            retention_days=30,
            confirm_delete=False,
        )

        patch_calls = [c for c in rsps.calls if c.request.method == "PATCH"]
        assert patch_calls, "Expected an update PATCH when fingerprint changes"
        body = json.loads(patch_calls[0].request.body)
        assert body["externalIds"]["fingerprint"] == expected_fp
        assert body["formattedAddress"] == sample_encompass_row["Report Address"]

    actions_path = out_dir / "actions.jsonl"
    assert actions_path.exists()
    with open(actions_path, encoding="utf-8") as f:
        actions = [json.loads(line) for line in f]

    assert any(a["kind"] == "update" for a in actions)
    update_payloads = [a["payload"] for a in actions if a.get("kind") == "update"]
    assert any(p["externalIds"]["fingerprint"] == expected_fp for p in update_payloads)
    assert any(
        p["formattedAddress"] == sample_encompass_row["Report Address"]
        for p in update_payloads
    )
