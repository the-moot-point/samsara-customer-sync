import json

from click.testing import CliRunner

from encompass_to_samsara.cli import cli


def test_cli_passes_rate_config(tmp_path, monkeypatch):
    cfg = tmp_path / 'rate.json'
    cfg.write_text(json.dumps({'min_interval': 0.5}))
    # required files for CLI
    enc_csv = tmp_path / 'encompass.csv'
    enc_csv.write_text('header\n')
    wh_csv = tmp_path / 'warehouses.csv'
    wh_csv.write_text('samsara_id,name\n')
    out_dir = tmp_path / 'out'

    captured = {}

    class DummyClient:
        def __init__(self, **kwargs):
            captured['kwargs'] = kwargs

    def fake_run_full(client, **kwargs):
        return None

    monkeypatch.setattr('encompass_to_samsara.cli.SamsaraClient', DummyClient)
    monkeypatch.setattr('encompass_to_samsara.cli.run_full', fake_run_full)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            '--api-rate-config',
            str(cfg),
            'full',
            '--encompass-csv',
            str(enc_csv),
            '--warehouses',
            str(wh_csv),
            '--out-dir',
            str(out_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    assert captured['kwargs'].get('rate_limits') == {'min_interval': 0.5}
