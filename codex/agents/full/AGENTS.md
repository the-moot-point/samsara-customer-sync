# AGENT TASK: Full Refresh
1) Read data/encompass_full.csv; validate lat/lon.
2) Fetch Samsara addresses + tags; index tags by name → id.
3) Upsert by encompass_id; attach id on unique probable matches; create missing.
4) Quarantine orphans with CandidateDelete (skip warehouses).
5) Write dry_run_diff.csv, sync_report.csv, actions.jsonl.
6) If tests pass and diff looks good, run apply when asked.

Shell to use: `make full-dry` → optionally `make full-apply`.
