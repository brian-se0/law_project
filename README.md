# Shadow Trading in U.S. Options Markets

This repository builds a reproducible law-and-economics pipeline for studying abnormal pre-disclosure options activity in economically linked firms around U.S. M&A announcements. The project is designed for interpretability, legal relevance, auditability, and honest limits rather than trading or accusation.

## Current status

The repo now includes the Milestone 0 and Milestone 1 foundations plus the first live Milestone 3 and Milestone 4 implementation slices:

- project scaffold, config loading, and test wiring
- vendor-aligned option EOD schema normalization
- read-only ingestion of zipped daily CSV archives from the vendor raw directory
- partitioned Parquet output for derived options tables
- deduplicated underlying-daily table construction with consistency flags
- SEC M&A event-universe builder with official SEC submissions/raw-filing parsing
- explicit target/acquirer extraction plus canonical `source_firm_id` fields in the SEC candidate/event tables
- linkage-table build script, dated `gvkey <-> underlying_symbol` bridge, QC wiring, and unit tests for lagged TNIC/VTNIC ingestion
- QC report generation with provenance notes
- unit tests for schema handling, QC logic, and event-date alignment

Event-universe construction is now materially underway, including a broader 2020-2021 validation slice. The linkage-table build is implemented in code but still awaits raw TNIC/VTNIC input files under `data/external/linkages/`.

## Quick start

1. Confirm `configs/paths.yaml` points at the immutable raw directory.
2. Run the first pipeline step on a small slice:

```powershell
python scripts/ingest_options.py --project-root . --limit-files 1
```

3. Build the underlying-daily table from processed options data:

```powershell
python scripts/build_underlying_daily.py --project-root .
```

4. Build the SEC M&A event universe:

```powershell
python scripts/build_mna_event_universe.py --project-root . --limit-companies 25
```

5. Build the dated gvkey bridge plus lagged linkage tables once raw TNIC/VTNIC files are placed in `data/external/linkages/`:

```powershell
python scripts/build_linkages.py --project-root .
```

6. Run tests:

```powershell
pytest -q
```

If `make` is available in your environment, the repo also provides `make ingest-options`, `make build-linkages`, and `make test`.

## Guardrails

- `D:\Options Data` is treated as immutable input only.
- The ingestion pipeline reads zipped CSVs directly and never rewrites raw archives.
- SEC pulls are cached under `data/external/sec/`; raw SEC records are not edited in place.
- Raw TNIC/VTNIC linkage files should be placed under `data/external/linkages/`; the build step ignores packaged readme files, stages the open `gvkey_ciks` seed when needed, and fails loudly if the actual raw linkage inputs are missing.
- Processed tables are written to Parquet under `data/processed/`.
- `open_interest` is treated as start-of-day only.
- Zero or crossed quotes are flagged and excluded from spread calculations.
- Suspicious footprints are not described as proof of illegal trading.
