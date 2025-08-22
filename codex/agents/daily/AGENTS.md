# AGENT TASK: Daily Incremental
1) Read data/encompass_delta.csv (Action optional).
2) For upserts: skip when fingerprint unchanged; else PATCH/CREATE.
3) For deletes: CandidateDelete or hard-delete per flags.
4) Update output/state.json; write reports.

Shell to use: `make daily-dry` → optionally `make daily-apply`.
