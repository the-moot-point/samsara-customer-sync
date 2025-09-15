.PHONY: full-dry full-apply daily-dry daily-apply test lint fmt
full-dry:   ; python -m encompass_to_samsara.cli full  --encompass-csv data/encompass_full.csv  --warehouses data/warehouses.csv --out-dir output
full-apply: ; python -m encompass_to_samsara.cli full  --encompass-csv data/encompass_full.csv  --warehouses data/warehouses.csv --out-dir output --confirm-delete --apply
daily-dry:  ; python -m encompass_to_samsara.cli daily --encompass-delta data/encompass_delta.csv --warehouses data/warehouses.csv --out-dir output
daily-apply:; python -m encompass_to_samsara.cli daily --encompass-delta data/encompass_delta.csv --warehouses data/warehouses.csv --out-dir output --apply
test:       ; pytest -q
lint:       ; ruff check --config ruff.toml .
fmt:        ; ruff format --config ruff.toml .
