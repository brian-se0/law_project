# Implementation Plan

## Status snapshot (updated 2026-04-05)

Completed:
- Milestone 0 — Repo and environment
- Milestone 1 — Options ingestion and QC
- Milestone 2 — Underlying daily table
- Milestone 4 — Linkages
  - lagged linkage build script, QC layer, config surface, and tests implemented
  - raw linkage inputs are unpacked under `data/external/linkages/tnic3_data/tnic3_data.txt` and `data/external/linkages/VertNetwork_10gran/VertNetwork_10gran.txt`
  - project-scoped dated `gvkey <-> underlying_symbol <-> event_year` bridge implemented from the open `farr::gvkey_ciks` seed plus SEC issuer evidence
  - linkage discovery ignores packaged readmes, gvkey-pair TNIC/VTNIC files resolve live, and vertical `vertscore` is accepted as `link_score`
  - latest live build writes `11,018` bridge rows, `4,952` linkage rows, and `51,814` control candidates over the current 2020-2021 validation slice

In progress:
- Milestone 3 — Event universe
  - official SEC submissions/raw-filing pipeline implemented
  - richer target/acquirer extraction and canonical `source_firm_id` fields implemented
  - SEC-native historical ticker-to-CIK bridge implemented
  - broader 2020-2021 validation slice now matches `COHR`, `XLNX`, `VAR`, and `FLIR`
  - latest validated slice (`2020-07-27` to `2021-03-12`) yields 93 candidate filings, 9 deduplicated candidate events, and 3 historical SEC resolutions
  - broader validation remains manual-review heavy because some candidate clusters and acquirer extractions are still noisy

Strategic reset:
- The project is no longer a broad multi-event M&A paper as the primary deliverable.
- The primary paper is now a one-source-symbol legal-empirical case study centered on `MDVN` (`Medivation, Inc.`).
- The legally focal traded security is `INCY` (`Incyte Corporation`) because that is the related security named in the SEC’s shadow-trading case.
- The broad SEC event-universe builder and the broad linkage stack remain valuable infrastructure, but they are no longer the critical path for the first paper-quality result.
- The critical path is now:
  1. freeze the canonical `MDVN` case event,
  2. extract the exact `INCY` contracts named in the SEC complaint,
  3. build bucketed and abnormal metrics for `INCY` plus ex ante linked firms,
  4. translate the result into a related-securities compliance/watchlist framework.

Public-repo alignment gaps to close next:
- `README.md` is still framed as a broad multi-event pipeline rather than an MDVN-only case study.
- `Makefile` still has placeholder targets for `build-buckets`, `replicate`, `main-study`, `policy-text`, and `paper`.
- `configs/research_params.yaml` has no case-study section yet.
- `pyproject.toml` does not yet include `duckdb`.
- There is no case-study-specific runner or bucket/abnormal/plot module yet.
- Broad 2020-2021 event clustering cleanup is still consuming attention even though it is no longer the main bottleneck for the paper.

## 1. Project title

From Medivation to Incyte: Quantifying Related-Security Materiality After *SEC v. Panuwat*

## 2. Core objective

Build a reproducible law-and-economics case-study pipeline that reconstructs the Medivation/Incyte shadow-trading problem using daily options data, ex ante economic linkages, and SEC-source legal facts, then translates the resulting evidence into a concrete related-securities compliance/watchlist rule.

## 3. One-sentence claim

If confidential information about Medivation’s impending acquisition was economically relevant to Incyte and similarly situated linked firms, then the options market of `INCY`—especially the exact short-dated OTM call series named in the SEC complaint—should display measurable pre-disclosure footprints before the public announcement of Pfizer’s acquisition of Medivation.

## 4. Why this is a law project, not just a finance project

- The central question is not alpha generation. It is whether information about one issuer can be material to a different issuer’s securities.
- The project is organized around the legal and compliance problem highlighted by `SEC v. Panuwat`.
- The output is a related-securities compliance/watchlist framework, not a trading strategy.
- The project is explicitly designed to distinguish suspicious public-market footprints from proof of illegality.
- The writing, exhibits, and deliverables must be understandable to a legally trained reader.

## 5. Scope decisions

Primary scope:
- Source firm fixed to one symbol: `MDVN`
- Source event fixed to one public announcement: Pfizer’s acquisition announcement for Medivation on `2016-08-22`
- Primary legally focal traded security: `INCY`
- Comparison traded firms:
  - `INCY` first
  - then the ex ante linked-firm set from lagged `TNIC-3`
  - then the ex ante linked-firm set from lagged `VTNIC 10%`
- Main market: single-name U.S. equity options only
- Main public event window: `[-5,-1]` trading days before the public announcement
- Main case-specific terminal window: `[-2,-1]` trading days, corresponding to `2016-08-18` and `2016-08-19`
- Main link year: `2015` only, to preserve ex ante linkage

Primary legal framing:
- Quantitative reconstruction of the litigated related-security setting in `SEC v. Panuwat`
- Focus on related-security materiality, comparable-company logic, and compliance design
- Use the broad ex ante peer network to contextualize `INCY`, not to replace it

Secondary scope:
- Own-target `MDVN` options activity as a benchmark, not the main result
- Ex ante linked-firm cross section around the same source event
- Announcement-day stock-return reactions in linked firms as an ex post materiality proxy
- Rumor/timeline sensitivity appendix using SEC-source chronology

Out of scope for the primary paper:
- Multi-event panel estimation as the main design
- Acquirer-source events as a main design
- ETF or index options
- Full-granularity VTNIC as a main design
- Policy-text corpus mining across many issuers
- Account-level attribution
- Criminal or individual accusations
- Intraday trade classification
- Black-box ML
- Market-manipulation claims

## 6. Research questions

1. Do the exact `INCY` option series named in the SEC complaint show abnormal pre-disclosure public-market footprints before the `MDVN` announcement?
2. Does `INCY` more broadly show abnormal options activity relative to its own history, matched controls, and other ex ante linked firms?
3. Where does `INCY` rank within Medivation’s ex ante `TNIC-3` and `VTNIC 10%` related-firm network?
4. Do stronger ex ante linked firms show stronger announcement-day stock reactions and/or pre-disclosure options footprints?
5. Would a defensible ex ante related-securities watchlist have captured `INCY` before the public Medivation announcement?
6. How should a public-company insider-trading policy define “related securities” after `Panuwat`?

## 7. Hypotheses

H1. The exact `INCY` call contracts named in the SEC complaint exhibit abnormal activity in the case terminal window `[-2,-1]`.

H2. `INCY` exhibits stronger short-dated OTM call activity than most matched non-linked controls in the pre-announcement window.

H3. `INCY` ranks near the upper end of Medivation’s ex ante comparable-company set under lagged `TNIC-3`, and possibly also under lagged `VTNIC 10%`.

H4. Across Medivation’s ex ante linked firms, stronger linkage is associated with stronger announcement-day reactions and/or stronger pre-disclosure options footprints.

H5. A compliance rule that combines explicit comparable-company logic with lagged ex ante linkages would have flagged `INCY` before the public announcement.

H6. Even if some results are null or mixed, a transparent reconstruction of the `MDVN -> INCY` case can still clarify how related-security materiality should be analyzed in securities-law compliance.

## 8. Data sources

Required:
1. Vendor options EOD summary files
2. SEC EDGAR filings / APIs for issuer, event, and timeline evidence
3. SEC complaint and SEC litigation materials in `SEC v. Panuwat`
4. Official Pfizer press release announcing the Medivation acquisition
5. Medivation tender-offer / 14D-9 materials for rumor and timeline context
6. Hoberg-Phillips `TNIC-3`
7. Hoberg-Phillips `VTNIC 10%`
8. `farr::gvkey_ciks` as the free seed bridge for `gvkey` / `iid` / `cik` resolution

Recommended public additions:
9. SEC Company Facts / submissions data for issuer metadata
10. SEC historical company-index and cumulative CIK-name files for auditability
11. Public legal commentary only as secondary background, never as the primary factual source

Optional if institutionally available:
12. CRSP daily returns for cleaner announcement-day market-adjusted returns
13. Compustat fundamentals for richer controls
14. SDC / CapIQ / FactSet only if the project later scales beyond the single-case design

Provenance note:
- `farr::gvkey_ciks` should be described honestly as a free, openly distributed derived bridge with partially proprietary upstream provenance, not as a purely public-raw-data reconstruction.

## 9. Canonical units of observation

- Exact contract-day
- Underlying-day
- Firm-day-bucket
- Linked-firm case observation
- Matched-control case observation
- Case-document observation

## 10. Canonical identifiers

- `case_id = mdvn_panuwat_2016`
- `source_symbol = MDVN`
- `primary_related_symbol = INCY`
- `source_firm_id`
- `linked_firm_id`
- `cik`
- `gvkey`
- `iid`
- `quote_date`
- `event_id`
- `series_id = underlying_symbol + root + expiration + strike + option_type`
- `link_type in {horizontal_tnic, vertical_vtnic}`
- `litigated_contract_flag`
- `linked_rank_within_source`
- `control_group_id`

## 11. Core derived variables

### 11.1 Underlying and quote variables
- `S_1545 = midpoint(underlying_bid_1545, underlying_ask_1545)` when available; else `active_underlying_price_1545` if present
- `S_eod = midpoint(underlying_bid_eod, underlying_ask_eod)`
- `mid_1545 = midpoint(bid_1545, ask_1545)`
- `mid_eod = midpoint(bid_eod, ask_eod)`
- `rel_spread_1545 = (ask_1545 - bid_1545) / mid_1545`
- `rel_spread_eod = (ask_eod - bid_eod) / mid_eod`
- `dte_cal = expiration - quote_date in calendar days`

### 11.2 Contract classification
- `tenor_bucket = {0_7, 8_30, 31_90, 91_plus}`
- if calcs exist:
  - `call_otm` if `0.10 <= delta_1545 <= 0.40`
  - `call_atm` if `0.40 < delta_1545 < 0.60`
  - `put_otm` if `-0.40 <= delta_1545 <= -0.10`
  - `put_atm` if `-0.60 < delta_1545 < -0.40`
- if calcs do not exist:
  - use strike-vs-spot moneyness bands

### 11.3 Case-specific focal contracts
The primary litigated-series inventory is:
- `INCY 2016-09-16 C 80.0`
- `INCY 2016-09-16 C 82.5`
- `INCY 2016-09-16 C 85.0`

Case-specific flags:
- `litigated_contract_flag`
- `primary_related_symbol_flag`
- `case_terminal_window_flag`
- `case_pre_event_window_flag`
- `announcement_window_flag`

### 11.4 Firm-day-bucket features
- `volume_bucket = sum(trade_volume)`
- `premium_bucket = sum(trade_volume * 100 * vwap)`
- `delta_notional_bucket = sum(trade_volume * 100 * abs(delta_1545) * S_1545)` when delta exists
- `iv_bucket = volume-weighted mean implied_volatility_1545` when available
- `spread_bucket = volume-weighted mean rel_spread_1545`
- `lead_oi_change_bucket = sum(open_interest_t+1 - open_interest_t)` across continuing series
- `vol_to_oi_bucket = volume_bucket / max(1, sum(open_interest))`

### 11.5 Exact-series features
- `contract_volume`
- `contract_premium = trade_volume * 100 * vwap`
- `contract_lead_oi_change = open_interest_t+1 - open_interest_t`
- `contract_rel_spread_1545`
- `contract_iv_1545`
- `contract_volume_share_of_underlying_call_volume`
- `contract_volume_share_of_same_expiry_call_volume`

### 11.6 Abnormal measures
Relative to estimation window `[-120,-20]`:
- `z_volume = z-score of log(1 + volume_bucket)`
- `z_premium = z-score of log(1 + premium_bucket)`
- `z_delta_notional`
- `z_iv`
- `z_spread`
- `z_lead_oi`

Exact-series abnormal measures:
- `z_contract_volume`
- `z_contract_premium`
- `z_contract_lead_oi`
- `z_contract_iv`

### 11.7 Linkage-context variables
- `linkage_strength_horizontal = TNIC-3 score`
- `linkage_strength_vertical = VTNIC 10% score`
- `linked_rank_within_source`
- `linked_percentile_within_source`
- `primary_related_pair_flag` for `MDVN -> INCY`

### 11.8 Ex post materiality proxies
- `linked_firm_return_0_1`
- `linked_firm_return_0`
- `source_return_0_1`
- `linked_firm_abnormal_return_0_1` when market adjustment is available

## 12. Event and timeline rules

Canonical public event:
- `source_public_announcement_date = 2016-08-22`
- `t = 0` is the first trading day aligned to the public announcement

Case-context date:
- `case_private_context_date = 2016-08-18`
- this date is used as legal context only
- do not redefine `t = 0` using a private-information date

Windows:
- estimation window = `[-120,-20]`
- pre-event window = `[-5,-1]`
- case terminal window = `[-2,-1]`
- announcement window = `[0,+1]`

Rumor/timeline handling:
- maintain a separate rumor/timeline appendix based on SEC-source chronology
- never treat rumors as equivalent to the final public announcement
- sensitivity analysis may exclude earlier rumor-heavy dates, but the canonical event remains the public acquisition announcement

## 13. Sample construction rules

Primary case sample:
- one source event only: Medivation acquisition announcement
- source firm must have listed single-name options in the vendor data
- `INCY` must be evaluated even if it later ranks below a generic top-k filter, because it is the litigated related-security pair
- ex ante linked-firm comparison set must use only lagged `2015` linkage information
- linked firms must have single-name options and enough history for estimation-window calculations

Comparison set construction:
- `INCY` as the legally focal pair
- top-k horizontal peers by lagged `TNIC-3` score
- available vertical peers from lagged `VTNIC 10%`
- matched non-linked controls on the same event date using options-liquidity and volatility similarity

Exclusions:
- contracts with non-positive or crossed quotes
- zero-IV rows when IV is used
- ultra-illiquid firms with insufficient nonzero days in the estimation window
- ETFs and indices
- firms with confounding major firm-specific announcements in the same `[-5,+1]` window, if they can be identified reliably
- any ex post linkage selection

Honesty rule:
- if `INCY` cannot be cleanly recovered through the generic ex ante linkage bridge, keep `INCY` as a legally focal case pair and document the linkage-resolution issue explicitly rather than forcing a false merge.

## 14. Primary empirical design

### Phase A — Freeze the canonical case event
1. Run the existing SEC event builder on a tight 2016 window centered on `MDVN`.
2. Freeze a human-reviewed `mdvn_case_event` record.
3. Preserve the evidence trail:
   - source filing / release
   - target/acquirer fields
   - timestamp logic
   - source and acquirer identifiers
   - review notes

### Phase B — Exact-contract reconstruction
1. Extract the three `INCY` call series named in the SEC complaint.
2. Build daily series-level metrics for those contracts around `2016-08-18` to `2016-08-22`.
3. Compare those contracts to:
   - their own estimation-window history
   - other `INCY` call contracts with similar tenor
   - same-expiry `INCY` calls
4. Produce an explicit missingness note if any named contract is absent or has no usable quote data.

### Phase C — Underlying-level related-security footprint
1. Build `INCY` firm-day-bucket features.
2. Compute abnormal short-dated OTM call activity for `INCY`.
3. Compare `INCY` with:
   - other ex ante linked firms
   - matched non-linked controls
   - Medivation itself as an own-target benchmark

### Phase D — Ex ante network context
1. Build Medivation’s lagged `TNIC-3` and `VTNIC 10%` related-firm set using `2015` linkages.
2. Rank `INCY` within that ex ante network.
3. Show whether stronger link scores correspond to stronger day-0/day-1 stock reactions and/or stronger pre-event option footprints across linked firms.

### Phase E — Compliance translation
1. Convert the empirical results into an ex ante related-securities watchlist rule.
2. Explicitly ask:
   - would `INCY` have been included?
   - would the top-k lagged peers have been included?
   - should derivatives of those names also have been covered?
3. Produce a short, board- and compliance-readable policy memo.

### Optional Phase F — Rumor/timeline sensitivity appendix
1. Use SEC-source chronology to mark rumor-sensitive periods.
2. Recompute the case using:
   - full `[-5,-1]`
   - terminal `[-2,-1]`
3. Document how sensitive the interpretation is to earlier public rumor background.

## 15. Statistical rules

- Primary evidence should be transparent and case-study oriented.
- Do not use a multi-event regression as the main proof.
- Use:
  - event-study-style abnormal metrics
  - percentile and rank comparisons
  - matched-control comparisons
  - placebo-date checks
  - optional permutation/randomization checks as supplements
- Keep component measures more prominent than composites.
- Use composite scores only as secondary summaries.
- Never equate abnormal trading with legal liability.
- Distinguish:
  - public-market footprint
  - materiality proxy
  - legal conclusion

## 16. Current repo assessment and what changes because of the MDVN-only pivot

What is already strong:
- immutable raw-data handling
- options ingestion and QC
- underlying-daily construction
- SEC candidate/event pipeline
- SEC-native historical bridge
- lagged linkage build
- gvkey bridge
- tests and QC writing
- clean pipeline-style architecture through `src/shadow_trading/pipelines.py`

What is no longer the best use of time:
- spending the next sprint on general 2020-2021 cluster cleanup
- trying to finish every broad multi-event milestone before freezing the Medivation case
- planning the paper as if it will be a many-event panel before the case-study core exists

What the MDVN-only pivot changes:
- broad event-universe cleanup becomes a support task, not the primary blocker
- the primary blocker is now the absence of a case-study pipeline and bucket/abnormal/output code
- the repo should be re-centered around one auditable case event while keeping the broader infrastructure reusable

## 17. Repo layout: current observed state and planned additions

### 17.1 Current observed public-repo layout
- `configs/`
  - `paths.example.yaml`
  - `research_params.yaml`
- `docs/`
  - `assumptions_log.md`
  - `data_dictionary.md`
  - `results_log.md`
- `scripts/`
  - `ingest_options.py`
  - `build_underlying_daily.py`
  - `build_mna_event_universe.py`
  - `build_linkages.py`
- `src/shadow_trading/`
  - `__init__.py`
  - `calendars.py`
  - `config.py`
  - `io.py`
  - `linkages.py`
  - `options_clean.py`
  - `pipelines.py`
  - `schema.py`
  - `sec_events.py`
  - `sec_party.py`
  - `underlyings.py`
- `tests/`
  - `test_event_alignment.py`
  - `test_ingest_pipeline.py`
  - `test_linkages.py`
  - `test_schema.py`
  - `test_sec_events.py`
  - `test_underlying_daily.py`

### 17.2 Planned additions for the MDVN-only direction
New scripts:
- `scripts/build_option_buckets.py`
- `scripts/freeze_mdvn_case_event.py`
- `scripts/run_mdvn_case_study.py`
- `scripts/make_mdvn_outputs.py`

New source modules:
- `src/shadow_trading/case_study.py`
- `src/shadow_trading/buckets.py`
- `src/shadow_trading/abnormal.py`
- `src/shadow_trading/plots.py`

New tests:
- `tests/test_case_study_config.py`
- `tests/test_mdvn_case_event.py`
- `tests/test_mdvn_exact_contracts.py`
- `tests/test_bucketing.py`
- `tests/test_abnormal_metrics.py`

New processed outputs:
- `data/processed/case_studies/mdvn_case_event.parquet`
- `data/processed/case_studies/mdvn_related_firms.parquet`
- `data/processed/case_studies/mdvn_exact_contracts.parquet`
- `data/processed/case_studies/mdvn_bucket_features.parquet`
- `data/processed/case_studies/mdvn_abnormal_metrics.parquet`
- `outputs/qc/mdvn_case_qc.json`
- `outputs/qc/mdvn_case_qc.md`
- `outputs/figures/mdvn_*`
- `outputs/tables/mdvn_*`

## 18. Concrete codebase-alignment tasks

### Task A — Add a case-study config surface
Extend `configs/research_params.yaml` with a dedicated case-study block rather than hard-coding `MDVN` inside scripts.

Recommended initial block:

```yaml
case_study:
  mode: mdvn_only
  case_id: mdvn_panuwat_2016
  source_symbol: MDVN
  source_name: Medivation, Inc.
  source_role: target
  acquirer_symbol: PFE
  acquirer_name: Pfizer Inc.
  primary_related_symbol: INCY
  primary_related_name: Incyte Corporation
  public_announcement_date: 2016-08-22
  case_private_context_date: 2016-08-18
  link_year: 2015
  horizontal_link_source: TNIC-3
  vertical_link_source: VTNIC_10
  horizontal_top_k: 10
  include_primary_related_symbol_even_if_not_top_k: true
  exact_contracts:
    - underlying_symbol: INCY
      expiration: 2016-09-16
      strike: 80.0
      option_type: C
    - underlying_symbol: INCY
      expiration: 2016-09-16
      strike: 82.5
      option_type: C
    - underlying_symbol: INCY
      expiration: 2016-09-16
      strike: 85.0
      option_type: C
  windows:
    estimation: [-120, -20]
    pre_event: [-5, -1]
    terminal_case: [-2, -1]
    announcement: [0, 1]
````

Implementation rule:

* The project is MDVN-only by configuration, not by an untested one-off code branch.
* The code should remain reusable for future case studies even though the paper does not require them.

### Task B — Freeze the canonical case event

Add a new pipeline step that:

1. reads the generic SEC event outputs,
2. filters to the MDVN case window,
3. writes a single reviewed case-event table,
4. records the evidence used for freezing the event.

New artifacts:

* `data/processed/case_studies/mdvn_case_event.parquet`
* `outputs/qc/mdvn_case_event_qc.{json,md}`

Minimum required fields:

* `case_id`
* `event_id`
* `source_firm_id`
* `source_symbol`
* `source_name`
* `target_cik`
* `target_gvkey` if available
* `acquirer_symbol`
* `acquirer_cik`
* `first_public_disclosure_dt`
* `event_trading_date`
* `case_private_context_date`
* `review_status`
* `review_note`
* `evidence_source`

### Task C — Add DuckDB for heavy aggregation

Current code is Polars-only.
For the MDVN-only case this is optional, but the next build stage is bucket aggregation over large contract tables and is exactly where DuckDB becomes useful.

Add to `pyproject.toml`:

* `duckdb>=1.1`

Use DuckDB for:

* contract-window filtering
* exact-series extraction
* firm-day-bucket aggregation
* writing intermediate case-study tables

Keep final outputs in Parquet.

### Task D — Implement bucketing and abnormal metrics

Add generic modules:

* `buckets.py`
* `abnormal.py`

Add script:

* `scripts/build_option_buckets.py`

Required behaviors:

* accept case-study config
* extract exact case-study series and broader linked-firm universe
* compute firm-day-bucket tables
* compute exact-series and bucket-level abnormal metrics
* write processed outputs and QC markdown

### Task E — Implement the MDVN case-study runner

Add:

* `src/shadow_trading/case_study.py`
* `scripts/run_mdvn_case_study.py`

The runner should:

1. load frozen `mdvn_case_event`
2. load lagged linkages
3. ensure `INCY` is preserved as the primary related symbol
4. extract exact complaint-named contracts
5. compute `INCY`-level bucketed metrics
6. compute linked-firm and control comparisons
7. write one case-study QC bundle

### Task F — Implement output generation

Add:

* `src/shadow_trading/plots.py`
* `scripts/make_mdvn_outputs.py`

Minimum required outputs:

* timeline figure
* exact-contract figure
* `INCY` abnormal-activity figure
* `INCY` linkage-rank figure
* watchlist-compliance schematic
* tables written to `outputs/tables/`

### Task G — Update Makefile

Replace placeholders with actual working targets.

Recommended target set:

* `make init`
* `make ingest-options`
* `make build-underlyings`
* `make build-events`
* `make build-linkages`
* `make freeze-case`
* `make build-buckets`
* `make main-study`
* `make paper`
* `make test`

Recommended mappings:

* `freeze-case` -> `python scripts/freeze_mdvn_case_event.py --project-root .`
* `build-buckets` -> `python scripts/build_option_buckets.py --project-root .`
* `main-study` -> `python scripts/run_mdvn_case_study.py --project-root .`
* `paper` -> `python scripts/make_mdvn_outputs.py --project-root .`

### Task H — Update README and AGENTS

`README.md` should:

* state that the primary paper is now an MDVN-only legal-empirical case study
* keep the broad pipeline description as reusable infrastructure
* explain that the public repo may still support broader event-universe construction, but that it is not the main paper target
* include the case-study run order

`AGENTS.md` should:

* set `MDVN` as the default current case-study objective
* prioritize exact-contract reconstruction, `INCY` linkage context, and compliance translation
* demote broad multi-event scaling to deferred work

### Task I — Add case-study tests

Required tests:

* config loads the MDVN case block correctly
* exact litigated `INCY` contracts are parsed into stable `series_id` values
* frozen case event writes one row only
* bucketing works on a synthetic case-study slice
* abnormal metrics are stable on a deterministic fixture
* the case-study runner fails loudly if the frozen case event is missing

### Task J — Reframe broad event-universe cleanup

Do not delete the broad SEC event-universe and linkage infrastructure.
Instead:

* keep it as reusable support code
* keep current 2020-2021 slice as a regression-test fixture
* stop treating full broad-sample cleanup as a gating dependency for the first paper draft

## 19. Milestones and build order

### Milestone 0 — Repo and environment `[Done]`

Acceptance criteria:

* package imports cleanly
* config loads
* smoke tests pass

### Milestone 1 — Options ingestion and QC `[Done]`

Acceptance criteria:

* processed options dataset exists
* QC report exists

### Milestone 2 — Underlying daily table `[Done]`

Acceptance criteria:

* unique `(underlying_symbol, quote_date)` table exists
* consistency checks documented

### Milestone 3 — Broad SEC event universe `[In progress, now secondary]`

Acceptance criteria:

* generic candidate/event pipeline continues to run
* no need to finish full historical cleanup before the case study

### Milestone 4 — Linkages `[Done]`

Acceptance criteria:

* lagged linkages build live
* gvkey bridge works
* QC report exists

### Milestone 5 — Freeze MDVN case event `[Next]`

Goal:

* produce one reviewed `MDVN` case-event record
  Acceptance criteria:
* exactly one frozen case-event row
* evidence trail saved
* no symbol-specific hotfixes in the pipeline code

### Milestone 6 — Bucketed option features `[Pending]`

Goal:

* build exact-series and firm-day-bucket features for the MDVN case
  Acceptance criteria:
* exact `INCY` litigated-series table exists
* firm-day-bucket features exist for `INCY` plus linked firms

### Milestone 7 — Exact-contract reconstruction `[Pending]`

Goal:

* characterize public-market footprints in the three complaint-named `INCY` calls
  Acceptance criteria:
* one exact-contract figure
* one exact-contract table
* explicit note if any series is absent or unusable

### Milestone 8 — Related-security case study `[Pending]`

Goal:

* compare `INCY` to linked firms and matched controls
  Acceptance criteria:
* one main `INCY` abnormal-activity figure
* one linkage-rank figure
* one linked-firm comparison table

### Milestone 9 — Compliance translation `[Pending]`

Goal:

* convert results into a related-securities watchlist rule
  Acceptance criteria:
* one short memo section or table translating the evidence into policy design

### Milestone 10 — Paper and polish `[Pending]`

Goal:

* finalize writing and reproducibility
  Acceptance criteria:
* README updated
* Makefile targets are real
* final figures/tables generated from scripts
* paper draft complete

## 20. MDVN-only acceptance criteria

The MDVN-only case-study phase passes only if all of the following are true:

* `MDVN` resolves to one frozen source event with `event_id`, `source_firm_id`, target/acquirer identifiers, raw announcement timestamp logic, and mapped trading date.
* `INCY` resolves as the primary related security in the case config and flows through the pipeline without a code edit.
* the exact complaint-named `INCY` call series are either:

  * successfully extracted and analyzed, or
  * explicitly documented as missing/unusable with a written QC note.
* the pipeline builds abnormal metrics for:

  * the exact `INCY` series,
  * `INCY` at the underlying level,
  * the ex ante linked-firm set,
  * matched controls.
* one scripted run generates:

  * a case-event QC table,
  * an exclusions note,
  * an exact-contract output,
  * one main `INCY` abnormal-activity figure,
  * one watchlist/compliance memo.
* the run is reproducible from a clean checkout with config changes only.
* the public docs and build targets are updated to reflect the MDVN-only direction.

## 21. Make targets

Required targets:

* `make init`
* `make ingest-options`
* `make build-underlyings`
* `make build-events`
* `make build-linkages`
* `make freeze-case`
* `make build-buckets`
* `make main-study`
* `make paper`
* `make test`

Legacy placeholder targets:

* remove or repurpose `replicate` only if it is mapped to a real MDVN benchmark step
* do not leave placeholder echo targets once the case-study runner exists

## 22. Figure plan

Figure 1. Medivation / Pfizer / Incyte case timeline
Figure 2. Own-target `MDVN` abnormal option activity before the announcement
Figure 3. Exact complaint-named `INCY` call-series activity around `2016-08-18` to `2016-08-22`
Figure 4. `INCY` abnormal short-dated OTM call activity vs matched controls
Figure 5. `INCY` rank within Medivation’s ex ante linked-firm set
Figure 6. Related-securities watchlist framework after `Panuwat`

## 23. Table plan

Table 1. Case chronology and identifier audit trail
Table 2. Exact complaint-named `INCY` contract inventory and metrics
Table 3. `INCY` vs linked firms vs matched controls
Table 4. Ex ante linkage ranking for `INCY` and top peers
Table 5. Compliance/watchlist translation matrix

Optional appendix table:

* rumor/timeline sensitivity results

## 24. Writing outline

1. Introduction: why `MDVN -> INCY` matters for shadow trading
2. Legal background: misappropriation, related securities, and `Panuwat`
3. Data and case construction
4. Exact-contract reconstruction
5. Ex ante comparable-company and supply-chain context
6. Related-security options-footprint results
7. Compliance/watchlist implications
8. Limitations
9. Conclusion

## 25. Key limitations to state explicitly

* This is a one-case design, not a broad-sample estimate.
* Daily summary data do not identify traders.
* Aggregate volume does not reveal account-level trade direction.
* Open interest is start-of-day, so opening-demand inference is approximate.
* The case-specific private-information date is legal context, not a public event timestamp.
* Rumor contamination is possible in parts of the Medivation sale process.
* The data can show suspicious public-market footprints, not proof of liability.
* A null or mixed result does not invalidate the legal contribution if the reconstruction remains honest and interpretable.

## 26. Final success criteria

The project is ready for résumé use when all of the following are true:

* the MDVN case is reproducible from a clean checkout
* the exact `INCY` litigated-series logic is implemented and audited
* the paper clearly explains related-security materiality using quantitative evidence
* the public repo docs match the actual code path
* the limitations section is honest
* the final output is strong even if the empirical result is mixed or null

## 27. Reference list

Primary legal and case materials:

* SEC. 2021. Complaint in `SEC v. Matthew Panuwat`.
* SEC. 2024. Litigation release announcing jury verdict in `SEC v. Matthew Panuwat`.
* Pfizer. 2016. Press release announcing the acquisition of Medivation.
* Medivation. 2016. Tender-offer / 14D-9 materials.

Research literature:

* Augustin, Patrick, Menachem Brenner, and Marti G. Subrahmanyam. 2019. "Informed Options Trading Prior to Takeover Announcements: Insider Trading?" Management Science 65(12): 5697-5720.
* Augustin, Patrick, and Marti G. Subrahmanyam. 2020. "Informed Options Trading Before Corporate Events." Annual Review of Financial Economics 12: 327-355.
* Du, Mingzhi, and Jimmy E. Hilliard. 2025. "Informed Option Trading of Target Firms' Rivals Prior to M&A Announcements." Journal of Futures Markets 45(10): 1683-1692.
* Enriques, Luca, Yoon-Ho Alex Lee, and Alessandro Romano. 2025. "The Placebo Effect of Insider Dealing Regulation." Oxford Journal of Legal Studies 45(3): 753-774.
* Mehta, Mihir N., David M. Reeb, and Wanli Zhao. 2021. "Shadow Trading." The Accounting Review 96(4): 367-404.
* Tookes, Heather E. 2008. "Information, Trading, and Product Market Interactions: Cross-Sectional Implications of Informed Trading." Journal of Finance 63(1): 379-413.
* Weinbaum, David, Andy Fodor, Dmitriy Muravyev, and Martijn Cremers. 2023. "Option Trading Activity, News Releases, and Stock Return Predictability." Management Science 69(8): 4810-4827.

```
