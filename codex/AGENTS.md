# AGENTS: Encompass → Samsara Sync

## Mission
Keep Samsara customer addresses in lockstep with Encompass (SoT). Implement and maintain `sync-e2s` CLI.

## Constraints / Guardrails
- Never modify or delete records unless `--apply` is set.
- Only act on addresses with encompassid or tag `ManagedBy:EncompassSync`.
- Respect warehouses denylist (data/warehouses*.csv).
- All timestamps/logs = UTC. Never print secrets.

## Run Commands
- Full (dry):  make full-dry
- Full (apply): make full-apply
- Daily (dry): make daily-dry
- Daily (apply): make daily-apply
- Tests: make test
- Lint/format: make lint fmt

## Repo Facts
- Python 3.11+, deps via pyproject.
- Reports written to ./output
- State persists in ./output/state.json

## Acceptance Checks (quick)
- Re-run on unchanged input → 0 API writes.
- Each Encompass Customer ID ↔ exactly one Samsara address with encompassid + scope markers.
- Company/Location tags match via tag IDs.
