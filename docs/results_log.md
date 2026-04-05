# Results Log

## 2026-04-04

No empirical results yet. Repo scaffolded, the vendor-aligned options ingestion/QC pipeline was implemented, the deduplicated underlying-daily table pipeline was added, and a first SEC M&A event-universe builder was implemented using official SEC submissions/raw-filing data with audit tables and QC outputs.

Additional pipeline validation on 2026-04-04:

- Expanded the processed options slice to four real 2020-2021 deal windows for `VAR`, `XLNX`, `FLIR`, and `COHR` and rebuilt `underlying_daily.parquet` across 41 processed quote dates.
- Re-ran the SEC event builder on that broader slice. The enriched parser produced 19 candidate filings and 1 deduplicated event for the 2021 Coherent process, with explicit target/acquirer fields and a canonical `source_firm_id`.
- Added an SEC-native historical ticker-to-CIK bridge that combines full-text symbol searches, targeted exchange/symbol queries, display-name evidence, and EDGAR company-search pages.
- Re-ran the same 2020-2021 validation slice after the bridge change. The builder now matches all 4 requested symbols, including 3 historical SEC resolutions for `XLNX`, `VAR`, and `FLIR`.
- The broader post-bridge run currently produces 93 candidate filings and 9 deduplicated candidate events across `COHR`, `XLNX`, `VAR`, and `FLIR`.
- The bridge improvement fixes the undercoverage problem, but the expanded event universe remains intentionally conservative and manual-review heavy: all 9 events are still flagged because some clusters and acquirer-name extractions remain noisy. The bridge should therefore be treated as a schema-and-coverage win, not yet a finalized clean-sample result.

## 2026-04-05

No new empirical MDVN result has been logged yet, but the repo's production path has been re-centered around the `MDVN -> INCY` case study:

- added a dedicated `case_study` config block for the `mdvn_panuwat_2016` case
- added a freeze-case step that writes a one-row reviewed case-event table from the generic SEC event universe
- added exact-contract, bucketing, abnormal-metric, case-study, and scripted-output runners
- added matched-control and linkage-rank summary tables for the case-study layer
- updated the repo build targets and documentation so the public code path matches the MDVN-only paper direction

This is still an infrastructure and reproducibility milestone rather than a substantive empirical finding. A live MDVN result still depends on loading the relevant 2016 option partitions and event coverage into the local processed data slice.

Follow-up pipeline validation on 2026-04-05:

- Ran the live 2016 `MDVN` slice through ingest, underlying build, SEC event build, linkage build, case freeze, bucket build, and case-study summary on the local data.
- Fixed a historical SEC resolver bug where the bridge could stop on the first plausible symbol hit instead of scoring the full targeted hit set, which was causing the MDVN freeze step to miss the Medivation/Pfizer event.
- Fixed the case-study extraction transfer path so the live DuckDB slice can be materialized into Polars without a brittle schema-inference failure.
- Wired matched controls through their own option extraction and bucket construction path so the control benchmark is no longer structurally null in the case-study summary.
- Added scripted memo outputs for the watchlist/compliance translation and limitations note, alongside escaped markdown tables for the exact-contract inventory.

This is still not a logged substantive empirical result. It is a live-run reproducibility milestone showing that the MDVN-only scripted path now executes on the local 2016 slice and emits the expected processed artifacts.
