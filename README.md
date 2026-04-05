# Shadow Trading in U.S. Options Markets

This repository now centers the primary paper around a single legal-empirical case study:

`MDVN -> INCY` around Pfizer's August 22, 2016 announcement that it would acquire Medivation.

The broad SEC event-universe and linkage infrastructure still remains in the repo as reusable support code, but it is no longer the critical path for the first paper-quality deliverable. The main objective is to reconstruct the related-security setting highlighted by `SEC v. Panuwat`, quantify abnormal pre-disclosure options footprints in `INCY` and ex ante linked firms, and translate that evidence into a related-securities watchlist framework.

## Current repo direction

Implemented and reusable:

- vendor-aligned options ingestion and QC
- deduplicated underlying-daily table construction
- SEC M&A candidate/event pipeline
- lagged TNIC/VTNIC linkage build plus dated gvkey bridge
- MDVN-only case-study config surface
- frozen-case, bucketing, abnormal-metric, case-study, and output-generation runners

Current practical limit in this checkout:

- the checked-in processed slice is still mostly a 2020-2021 validation sample plus small 2004 coverage
- the MDVN case-study code is implemented, but a full live MDVN run still requires the relevant 2016 options partitions and SEC event coverage in the local data

## Case-study run order

1. Confirm `configs/paths.yaml` points at the immutable raw and processed data locations.
2. Ingest or stage the needed options quote dates.
3. Build the underlying-daily table.
4. Build the SEC event universe.
5. Build lagged linkages.
6. Freeze the MDVN case event.
7. Build option buckets and exact-contract tables.
8. Run the MDVN case study.
9. Generate scripted figures and tables.

PowerShell examples:

```powershell
python scripts/ingest_options.py --project-root .
python scripts/build_underlying_daily.py --project-root .
python scripts/build_mna_event_universe.py --project-root . --start-date 2016-08-01 --end-date 2016-08-31 --symbols MDVN
python scripts/build_linkages.py --project-root .
python scripts/freeze_mdvn_case_event.py --project-root .
python scripts/build_option_buckets.py --project-root .
python scripts/run_mdvn_case_study.py --project-root .
python scripts/make_mdvn_outputs.py --project-root .
```

If `make` is available:

```powershell
make freeze-case
make build-buckets
make main-study
make paper
make package
make release
```

## Guardrails

- `data/raw/` is immutable input only.
- Processed tables are written to Parquet under `data/processed/`.
- Heavy case-study filtering reads the processed options partitions and uses DuckDB.
- `open_interest` is treated as start-of-day only.
- Opening-demand inference uses next-day open-interest change and is explicitly documented as an approximation.
- Zero or crossed quotes are excluded from spread calculations rather than treated as clean prices.
- The repo is not a trading-strategy or accusation engine.
- Abnormal activity is not described as proof of illegality or liability.

## Release Discipline

- Generate any final assessment bundle from the current commit with `python scripts/package_mdvn_assessment.py --project-root .` or `make package`.
- Put legal and timeline claims behind primary SEC or court sources.
- Put data-layout and field-availability claims behind the Cboe Option EOD Summary vendor schema.
