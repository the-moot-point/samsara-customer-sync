import csv
import json
from click.testing import CliRunner
import responses

from encompass_to_samsara.cli import cli

API = "https://api.samsara.com"


def write_csv(path, rows):
    with open(path, "w", encoding="utf-8", newline="") as f:
        if not rows:
            return
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        for r in rows:
            w.writerow(r)


def test_cli_full_actions_no_delta(tmp_path, base_responses):
    """Invoke the CLI full subcommand and ensure action reasons are clean."""
    # Prepare source CSV with one active row
    source_rows = [{
        "Customer ID": "C1",
        "Customer Name": "Foo",
        "Account Status": "Active",
        "Latitude": "30.1",
        "Longitude": "-97.7",
        "Report Address": "123 A St",
        "Location": "Austin",
        "Company": "JECO",
        "Customer Type": "Retail",
    }]
    src_csv = tmp_path / "encompass_full.csv"
    write_csv(src_csv, source_rows)

    # Warehouses denylist (empty)
    wh_csv = tmp_path / "warehouses.csv"
    with open(wh_csv, "w", encoding="utf-8") as f:
        f.write("samsara_id,name\n")

    out_dir = tmp_path / "out"
    runner = CliRunner()

    with base_responses as rsps:
        rsps.add(responses.GET, f"{API}/addresses", json={"addresses": []}, status=200)
        result = runner.invoke(
            cli,
            [
                "full",
                "--encompass-csv",
                str(src_csv),
                "--warehouses",
                str(wh_csv),
                "--out-dir",
                str(out_dir),
            ],
            env={"SAMSARA_BEARER_TOKEN": "test-token"},
        )

    assert result.exit_code == 0, result.output

    # Ensure actions reasons do not start with delta_
    with open(out_dir / "actions.jsonl", encoding="utf-8") as f:
        for line in f:
            reason = json.loads(line)["reason"]
            assert not reason.startswith("delta_"), reason
