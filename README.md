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
python -m encompass_to_samsara.scripts.export_addresses
```

The script writes all existing Samsara addresses to `addresses.json`.

### Delete Samsara addresses

Provide a CSV or Excel file with an `ID` column listing the address IDs to remove.

```bash
export SAMSARA_BEARER_TOKEN=your_token
python -m encompass_to_samsara.scripts.delete_addresses path/to/ids.csv
```

The script iterates through the IDs and calls the Samsara Delete Address endpoint for
each.

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

### Rate limiting configuration

Use `--api-rate-config <file>` to supply a JSON file that tunes client-side throttling.
The file may specify a global `min_interval` (seconds between any two requests) and
per-endpoint limits in requests per second keyed by "METHOD /path".

Example `rate_limits.json`:

```json
{
  "min_interval": 0.2,
  "GET /addresses": 3,
  "POST /addresses": 1
}
```

Run the CLI with the config:

```bash
sync-e2s --api-rate-config rate_limits.json full --encompass-csv data/encompass_full.csv --warehouses data/warehouses.csv --out-dir output
```

Delays introduced by these limits are logged at DEBUG with the HTTP method and path.

### Inputs

1. **Full Encompass CSV** with columns:
   `Customer ID, Customer Name, Account Status, Latitude, Longitude, Report Address, Location, Company, Customer Type`
   - `Report Address` values should exclude company names
2. **Daily delta CSV** (same columns, optional `Action` = `upsert|delete`)
3. **`warehouses.csv`** (or `.yaml`) list of Samsara IDs/names never to modify/delete.

### Mapping

- `Customer ID` → `externalIds.encompassid` (required)
- `Customer Name` → `name`
- `Report Address` (no company name) → `formattedAddress` (composed if needed)
- `Latitude`,`Longitude` → geofence circle (default radius 50 m; configurable)
- `Account Status` → used to compute a fingerprint (not stored)
- `Location` → **Tag** (resolved via List Tags)
- `Company` → **Tag** (resolved via List Tags)
- `Customer Type` → ignored (optional: `externalIds.ENCOMPASS_TYPE`)
- Scope marker: Tag **ManagedBy:EncompassSync**
- Fingerprint: `externalIds.fingerprint = sha256(normalize(name) + "|" + normalize(account_status) + "|" + normalize(formattedAddress))`

### External ID requirements

Samsara restricts each `externalIds` key and value to **32 characters** from the set
`[A-Za-z0-9_.:-]`. Values outside this set should be sanitized or replaced before
sending to the API.

Canonical keys used by this tool:

- `encompassid` – primary customer identifier
- `fingerprint`

Sanitization & backward compatibility:

- The CLI normalizes legacy keys `EncompassId` and `ENCOMPASS_ID` to the canonical
  `encompassid` and preserves other external IDs intact.
- When indexing existing addresses, all three key variants are recognized so older
  records remain discoverable.

Usage example for a compliant external ID:

```python
import re

raw_id = "ACME Store #1"
safe_id = re.sub(r"[^A-Za-z0-9_.:-]", "_", raw_id)[:32]
payload = {
    "externalIds": {
        "encompassid": safe_id,
    }
}
```

### Paycom driver external IDs

The Paycom → Samsara driver sync stores two additional external identifiers on
every driver record so we can look up existing employees and detect field-level
changes without diffing the entire payload:

- `employeeCode` – the unique Paycom employee code after being sanitized with
  the same rules used for other `externalIds` values (invalid characters are
  dropped and the value is truncated to 32 characters).
- `paycom_fingerprint` – a 64-character SHA-256 hex digest computed from the
  normalized Paycom row. This is the idempotency key that keeps repeated runs
  from issuing redundant PATCH requests when nothing material has changed.

#### Field coverage and normalization

The fingerprint covers the core identity, contact, compliance, and routing
fields we receive from Paycom. Each field is normalized before being added to
the fingerprint payload so cosmetic differences (extra spaces, punctuation, or
mixed casing) do not trigger unnecessary updates.

| Paycom column                 | Normalization rule                                                                              |
| ----------------------------- | ----------------------------------------------------------------------------------------------- |
| `First Name`, `Preferred Name`, `Last Name` | `normalize(...)` – lower-case, trim, strip punctuation, collapse repeated whitespace.                |
| `Employment Status`          | `normalize(...)`; used to flip `isDeactivated` when the status is anything other than `active`. |
| `Position`                   | `normalize(...)`; also drives tag assignment via `data/position_mapping.csv`.                   |
| `Work_Location`              | `normalize(...)`; used for both tags and the time zone lookup in `data/location_mapping.csv`.    |
| `Work Email`                 | `value.strip().lower()`; empty strings collapse to `""`.                                        |
| `Mobile Phone`, `Work Phone` | Digits-only (``"".join(ch for ch in value if ch.isdigit())``) to remove formatting characters. |
| `Driver License Number`      | `normalize(...)`                                                                                 |
| `Driver License State`       | `normalize(...)`                                                                                 |
| `Supervisor Employee Code`   | `sanitize_external_id_value(...)`                                                               |

Blank or missing values contribute empty strings so the field order remains
stable. Because the hash is deterministic, any change to the normalized values
above regenerates `paycom_fingerprint` and forces a PATCH the next time the
sync runs.

#### Patch triggers

During reconciliation the sync evaluates the current Samsara driver alongside
the freshly computed payload and issues a `PATCH /fleet/drivers/{id}` when any
of the following are true:

- `externalIds.employeeCode` is missing or differs from the sanitized Paycom
  value.
- `externalIds.paycom_fingerprint` is missing or does not match the newly
  computed fingerprint.
- Location or position derived tags/time zone disagree with the current driver
  record (for example, a driver transferring depots or changing job family).
- The `Employment Status` flip requires toggling `isDeactivated`.

If none of those checks fail, the record is skipped and no API write is issued.

#### Example fingerprint + payload

```python
from hashlib import sha256

from encompass_to_samsara.transform import normalize, sanitize_external_id_value


def normalize_phone(value: str | None) -> str:
    return "".join(ch for ch in (value or "") if ch.isdigit())


def compute_paycom_fingerprint(row: dict[str, str]) -> str:
    parts = [
        normalize(row.get("First Name")),
        normalize(row.get("Preferred Name")),
        normalize(row.get("Last Name")),
        normalize(row.get("Employment Status")),
        normalize(row.get("Position")),
        normalize(row.get("Work_Location")),
        (row.get("Work Email") or "").strip().lower(),
        normalize_phone(row.get("Mobile Phone")),
        normalize_phone(row.get("Work Phone")),
        normalize(row.get("Driver License Number")),
        normalize(row.get("Driver License State")),
        sanitize_external_id_value(row.get("Supervisor Employee Code")) or "",
    ]
    return sha256("|".join(parts).encode("utf-8")).hexdigest()


location_index = {
    "Austin": {"tag_id": "2762148", "tz": "America/Chicago"},
    # ... additional rows loaded from data/location_mapping.csv
}
position_tags = {
    "Delivery Driver": "4134370",
    # ... additional mappings from data/position_mapping.csv
}


def build_driver_payload(row: dict[str, str]) -> dict[str, object]:
    loc_info = location_index.get(row.get("Work_Location"))
    tag_ids = []
    if loc_info and loc_info.get("tag_id"):
        tag_ids.append(loc_info["tag_id"])
    if tag := position_tags.get(row.get("Position")):
        tag_ids.append(tag)
    payload = {
        "firstName": row.get("First Name") or "",
        "lastName": row.get("Last Name") or "",
        "email": (row.get("Work Email") or "").strip().lower() or None,
        "phone": normalize_phone(row.get("Mobile Phone")) or None,
        "timeZone": (loc_info or {}).get("tz", "America/Chicago"),
        "tagIds": tag_ids,
        "isDeactivated": normalize(row.get("Employment Status")) != "active",
        "externalIds": {
            "employeeCode": sanitize_external_id_value(row.get("Employee Code")) or None,
            "paycom_fingerprint": compute_paycom_fingerprint(row),
        },
    }
    return payload
```

### Safety rails

- Only touch addresses with `externalIds.encompassid` or tag `ManagedBy:EncompassSync`.
- Customers with `Account Status` `INACTIVE` are ignored unless explicitly deleted.
- Two-step delete: tag `CandidateDelete`; hard-delete only with `--confirm-delete` **and**
  after retention window.
- Never touch entries in `warehouses.csv` (denylist of Samsara IDs/names).
- Retries/backoff on 429/5xx with exponential backoff + jitter; UTC timestamps; logs retries.
- Configurable per-endpoint rate limiting via `--api-rate-config`; delays are logged at DEBUG with the HTTP method and path.

### Reports (written to `./output/`)

- `dry_run_diff.csv` – field-by-field planned changes
- `sync_report.csv` – summary counts and timing
- `actions.jsonl` – one JSON object per action (create/update/delete/quarantine/skip/error)
- `errors.csv` – structured error list
- `duplicates.csv` – duplicate `Customer ID` in source, or duplicate `encompassid` in Samsara
- `state.json` – persistent state (`id → fingerprint`, candidate-delete timestamps)

### Acceptance

- Re-running `full`/`daily` with unchanged inputs → **zero** API writes (fingerprint idempotence)
- All managed Samsara addresses carry `encompassid` and scope markers
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
> `externalIds.DeleteCandidateAt="<timestamp>-<addressId>"` and logs a warning.
