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
- Tightened the linkage-rank output so percentile ranks are now computed against the full relevant link set rather than the retained display subset.
- Added a scripted exact-contract window summary that leads the paper-facing MDVN outputs with pooled complaint-named `INCY` evidence, while demoting the short-dated OTM bucket layer to a benchmark and sensitivity role.
- Removed the case-study fallback from delta buckets to strike-vs-spot moneyness. The canonical MDVN path now hard-fails if required Calcs-backed fields are missing.
- Re-ran the live 2016 MDVN outputs after the evidence-alignment changes. The pooled complaint-named `INCY` contracts now provide the headline result: in the pre-event window `[-5,-1]`, pooled exact-series mean abnormal volume is `3.9680`, pooled mean abnormal premium is `2.1255`, pooled mean abnormal lead open-interest change is `9.4876`, pooled raw volume is `1,197` contracts, pooled premium is `$274,631.81`, and the three exact contracts represent `34.3%` of all INCY call volume and `68.1%` of same-expiry INCY call volume.
- The terminal exact-series window `[-2,-1]` remains strong, with pooled abnormal volume `4.1335`, pooled abnormal premium `1.9409`, pooled raw volume `754`, and `69.9%` of same-expiry INCY call volume.
- The broad short-dated OTM bucket benchmark remains weaker for this single case: `INCY` pre-event bucket mean abnormal volume is `-0.3173`, while the terminal bucket mean turns positive at `0.5638`. This confirms that the best evidence in this repo is the exact-series layer rather than the generic bucket layer.

This is now a substantive single-case empirical result rather than only a reproducibility milestone. The remaining work is primarily paper polish, citation integration, and any final presentation cleanups that improve interpretability without changing the canonical MDVN specification.
