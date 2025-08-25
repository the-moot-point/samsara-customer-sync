# Samsara Customer Sync

A production-ready Python 3.11+ CLI that keeps **Samsara Addresses** in lockstep with **Encompass**
(Encompass is the source of truth). Supports a **one-time full refresh** and **daily incremental**
sync. Safe-by-default (dry-run), idempotent (fingerprints), auditable (reports + JSONL actions).

> **APIs used**
>
> - **Samsara**: `GET /addresses`, `GET /addresses/{id}`, `POST /addresses`,
>   `PATCH /addresses/{id}`, `DELETE /addresses/{id}`, `GET /tags`.

---

## Quick start

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
cp .env.example .env   # set SAMSARA_BEARER_TOKEN
make test              # run tests
make full-dry          # dry-run full refresh using sample paths
# If the diff looks good:
make full-apply        # applies changes (and allows deletes per flags)
```

### Environment

Create a `.env` or export environment variables:

- `SAMSARA_BEARER_TOKEN` (required): API token for Samsara.
- `E2S_DEFAULT_RADIUS_METERS` (optional, default 50): geofence radius for new/updated addresses.

### Export current Samsara addresses

```bash
export SAMSARA_BEARER_TOKEN=your_token
python scripts/export_addresses.py
```

The script writes all existing Samsara addresses to `addresses.json`.

### CLI

```
# Full refresh
sync-e2s full   --encompass-csv data/encompass_full.csv   --warehouses data/warehouses.csv   --out-dir output   [--radius-m 50] [--retention-days 30] [--confirm-delete] [--apply]

# Daily incremental
sync-e2s daily   --encompass-delta data/encompass_delta.csv   --warehouses data/warehouses.csv   --out-dir output   [--radius-m 50] [--retention-days 30] [--confirm-delete] [--apply]
```

- **Dry-run by default**: The tool writes planned actions to `output/actions.jsonl` and diffs
  to `dry_run_diff.csv` without calling write endpoints unless `--apply` is passed.
- **Deletes are gated** with a two-step process:
  - Orphans are tagged **CandidateDelete** (quarantine).
  - Hard delete only when **both** `--confirm-delete` and retention window elapsed.

### Inputs

1. **Full Encompass CSV** with columns:
   `Customer ID, Customer Name, Account Status, Latitude, Longitude, Report Company Address, Location, Company, Customer Type`
2. **Daily delta CSV** (same columns, optional `Action` = `upsert|delete`)
3. **`warehouses.csv`** (or `.yaml`) list of Samsara IDs/names never to modify/delete.

### Mapping

- `Customer ID` → `externalIds.encompass_id` (required)
- `Customer Name` → `name`
- `Report Company Address` → `formattedAddress` (composed if needed)
- `Latitude`,`Longitude` → geofence center (default radius 50 m; configurable)
- `Account Status` → `externalIds.ENCOMPASS_STATUS`
- `Location` → **Tag** (resolved via List Tags)
- `Company` → **Tag** (resolved via List Tags)
- `Customer Type` → ignored (optional: `externalIds.ENCOMPASS_TYPE`)
- Scope markers: Tag **ManagedBy:EncompassSync** **and** `externalIds.ENCOMPASS_MANAGED="1"`
- Fingerprint: `externalIds.ENCOMPASS_FINGERPRINT = sha256(normalize(name) + "|" + normalize(account_status) + "|" + normalize(formattedAddress))`

### Safety rails

- Only touch addresses with `externalIds.encompass_id` or tag `ManagedBy:EncompassSync`.
- Two-step delete: tag `CandidateDelete`; hard-delete only with `--confirm-delete` **and**
  after retention window.
- Never touch entries in `warehouses.csv` (denylist of Samsara IDs/names).
- Retries/backoff on 429/5xx with exponential backoff + jitter; UTC timestamps; logs retries.

### Reports (written to `./output/`)

- `dry_run_diff.csv` – field-by-field planned changes
- `sync_report.csv` – summary counts and timing
- `actions.jsonl` – one JSON object per action (create/update/delete/quarantine/skip/error)
- `errors.csv` – structured error list
- `duplicates.csv` – duplicate `Customer ID` in source, or duplicate `encompass_id` in Samsara
- `state.json` – persistent state (`id → fingerprint`, candidate-delete timestamps)

### Acceptance

- Re-running `full`/`daily` with unchanged inputs → **zero** API writes (fingerprint idempotence)
- All managed Samsara addresses carry `encompass_id` and scope markers
- Company/Location tags resolved to correct IDs
- Orphans quarantined or deleted only under explicit flags; warehouses never modified

---

## Using Codex

Install Codex (cloud version of OpenAI Codex):

```bash
npm i -g @openai/codex
```

From repo root, you can run the full refresh dry and summarize the diff:

```bash
codex exec --full-auto "run the full refresh in dry-run and summarize the diff"
```

Codex automatically merges agent guidance from the files in `codex/` and your
`~/.codex/config.toml`.

---

## Development

- Formatting & linting: `make fmt` and `make lint`
- Tests (mocked HTTP via `responses`): `make test`

> **Note**: The tag **CandidateDelete** is used to quarantine orphans.
> Ensure a Tag named exactly `CandidateDelete` exists in Samsara (the CLI cannot create tags
> – only List Tags is allowed by requirements). If missing, the tool falls back to setting
> `externalIds.ENCOMPASS_DELETE_CANDIDATE="1"` and logs a warning.
