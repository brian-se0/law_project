# Implementation Plan

## Status snapshot (updated 2026-04-05)

Completed:
- Milestone 0 — Repo and environment
- Milestone 1 — Options ingestion and QC
- Milestone 2 — Underlying daily table
- Milestone 4 — Linkages
  - lagged linkage build script, QC layer, config surface, and tests implemented
  - raw linkage inputs are now unpacked under `data/external/linkages/tnic3_data/tnic3_data.txt` and `data/external/linkages/VertNetwork_10gran/VertNetwork_10gran.txt`
  - project-scoped dated `gvkey <-> underlying_symbol <-> event_year` bridge implemented from the open `farr::gvkey_ciks` seed plus SEC issuer evidence
  - linkage discovery now ignores packaged readmes, gvkey-pair TNIC/VTNIC files resolve live, and vertical `vertscore` is accepted as `link_score`
  - latest live build writes `11,018` bridge rows, `4,952` linkage rows, and `51,814` control candidates over the current 2020-2021 validation slice

In progress:
- Milestone 3 — Event universe
  - official SEC submissions/raw-filing pipeline implemented
  - richer target/acquirer extraction and canonical `source_firm_id` fields implemented
  - SEC-native historical ticker-to-CIK bridge implemented; broader 2020-2021 validation slice now matches `COHR`, `XLNX`, `VAR`, and `FLIR`
  - latest validated slice (`2020-07-27` to `2021-03-12`) now yields 93 candidate filings, 9 deduplicated candidate events, and 3 historical fallback matches
  - broader validation remains manual-review heavy because some candidate clusters and acquirer extractions are still noisy

Current systems-MVP decision:
- GPT-5.4 Pro recommends a temporary one-source-event systems MVP before scaling
- define the MVP as one source symbol plus the full fan-out of its ex ante linked firms, not one ticker in isolation
- first system-MVP symbol: `RHT` (`Red Hat, Inc.`)
- rationale: clean official weekend announcement timing for `t=0`, distinctive historical target symbol, strong auditability, and a good stress test of the historical bridge without the rumor/competing-bid complexity of `XLNX` or `COHR`
- fallback order if `RHT` creates a fresh blocker within one working session: `VAR` second, `FLIR` third
- `XLNX` and `COHR` should be treated as later adversarial tests rather than first-pass MVP cases

Next planned implementation step:
- formalize the one-source-event systems MVP around `RHT` now that the linkage bridge is operational
- if `RHT` is still outside the currently staged event slice, use `VAR` as the immediate systems check while keeping `RHT` as the first full historical MVP target
- tighten event clustering and counterparty cleanup on the expanded historical sample

Current handoff note:
- The repo can now ingest immutable vendor zip archives from `D:\Options Data`, rebuild the underlying-daily table over broader slices, produce SEC candidate/event tables with explicit target/acquirer fields, resolve historical symbols through an SEC-native bridge, build a dated `gvkey <-> underlying_symbol` bridge, and run the live linkage build end-to-end.
- `python scripts/build_linkages.py --project-root . --overwrite` now succeeds and writes:
  - `data/processed/gvkey_underlying_bridge.parquet`
  - `data/processed/linkages.parquet`
  - `data/processed/linkage_control_candidates.parquet`
  - `outputs/qc/linkages_qc.{json,md}`
- The current no-pay bridge path is now implemented as:
  - use `farr::gvkey_ciks` as an openly distributed seed table for `gvkey` / `iid` / `cik`
  - retain `iid` in the audit bridge while collapsing to unique firm-year gvkey paths for downstream linkage joins
  - join that seed to the repo's SEC issuer layer on dated `cik`
  - intersect the resulting ticker candidates with observed options `underlying_symbol` presence near the event date
  - prefer source-event SEC evidence over weaker competing issuer evidence when the same historical symbol maps to multiple CIKs in the noisy event slice
  - keep only unique high-confidence matches and push the remainder to manual review
- This bridge should still be described honestly in the repo as a free, openly distributed derived bridge with partially proprietary upstream provenance, not as a fully public-raw-data reconstruction of Compustat history.
- The clean next pickup is:
  1. pick one clean source event for the systems MVP and freeze its event record, with `RHT` still preferred and `VAR` as the immediate fallback if the staged slice remains limited
  2. inspect the new live linkage outputs source-by-source and preserve a short exclusions note for any dropped or ambiguous source event-years
  3. tighten event clustering and counterparty cleanup so the current `COHR` / `FLIR` / duplicate-cluster noise is reduced before the main study
  4. move into bucketed option features for the systems MVP path once the source event is frozen

## 1. Project title
Shadow Trading in U.S. Options Markets: An Empirical Standard for Related-Security Insider Trading

## 2. Core objective
Build a reproducible law-and-economics research pipeline that measures whether confidential information about a source firm appears to spill into the options market of economically linked firms before public M&A announcements, then translate the results into a compliance/watchlist framework.

## 3. One-sentence claim
If source-firm M&A information is economically fungible to related firms, then ex ante linked firms should exhibit abnormal, leveraged, pre-disclosure options activity—especially in short-dated OTM contracts—before the source-firm announcement.

## 4. Why this is a law project, not just a finance project
- The central question is materiality to a different issuer's security.
- The output is a related-securities compliance standard, not a trading strategy.
- The paper will distinguish suspicious pre-disclosure footprints from socially useful price discovery.
- The writing must be understandable to a legally trained reader.

## 5. Scope decisions
Primary scope:
- Source firm = public U.S. target firm in announced M&A deal
- Traded firm = ex ante linked public firm with listed single-name options
- Event type = unscheduled M&A announcement
- Linkage types = horizontal (TNIC) first, vertical (VTNIC) second
- Main market = single-name equity options
- Main window = [-5,-1] trading days before first public disclosure

Secondary scope:
- Acquirer-source events
- ETF options extension
- Policy-text extension using Exhibit 19.1 insider trading policies

Out of scope for the systems MVP:
- Account-level attribution
- Criminal/individual accusations
- Intraday trade classification
- Market-manipulation claims
- Black-box-first modeling

## 6. Research questions
1. Do linked firms show abnormal options activity before source-firm M&A announcements?
2. Is that activity stronger when linkage is stronger?
3. Is the activity concentrated in short-dated OTM options?
4. Does pre-disclosure options activity predict ex post linked-firm announcement-day return reactions?
5. Can the evidence be translated into an administrable related-securities compliance/watchlist rule?
6. Optional: Are explicit cross-issuer insider-trading policy bans associated with weaker linked-firm pre-disclosure footprints?

## 7. Hypotheses
H1. Linked firms exhibit positive abnormal pre-announcement options activity relative to matched non-linked firms.
H2. The effect is increasing in TNIC/VTNIC linkage strength.
H3. Horizontal peers show a bullish footprint concentrated in short-dated OTM calls.
H4. Vertical links show a stronger unsigned footprint (calls and/or puts, depending on spillover direction).
H5. The strongest pre-disclosure footprints occur in linked firms that later experience the largest announcement-day price reactions.
H6. Optional: Source firms with explicit cross-issuer policy prohibitions show weaker linked-firm footprints after policy filing.

## 8. Data sources
Required:
1. Vendor options EOD summary files
2. SEC EDGAR filings / APIs for M&A event collection and timestamps
3. Hoberg-Phillips TNIC data
4. Hoberg-Phillips VTNIC data

Recommended public additions:
5. SEC Company Facts / submissions data for issuer metadata and controls
6. Exhibit 19 / 19.1 insider trading policies for the policy-text extension
7. `farr::gvkey_ciks` as a free seed bridge for `gvkey` / `iid` / `cik` resolution
   - use with an explicit provenance note: openly distributed and free to obtain, but derived from partially proprietary upstream sources rather than built purely from public raw data

Nice-to-have if institutionally available:
8. CRSP daily stock returns
9. Compustat fundamentals
10. SDC Platinum / Capital IQ / FactSet M&A datasets
11. FactSet Revere or similar supply-chain data

## 9. Canonical units of observation
- Contract-day
- Firm-day-bucket
- Linked-firm event
- Source event
- Source x linked-firm-year policy observation (optional extension)

## 10. Canonical identifiers
- source_firm_id
- linked_firm_id
- cik
- ticker
- quote_date
- event_id
- series_id = underlying_symbol + root + expiration + strike + option_type
- link_type in {horizontal_tnic, vertical_vtnic, coarse_industry}
- policy_doc_id (optional)

## 11. Core derived variables
Underlying and pricing:
- S_1545 = midpoint(underlying_bid_1545, underlying_ask_1545) when available; else active_underlying_price_1545 if present
- S_eod = midpoint(underlying_bid_eod, underlying_ask_eod)
- mid_1545 = midpoint(bid_1545, ask_1545)
- mid_eod = midpoint(bid_eod, ask_eod)
- rel_spread_1545 = (ask_1545 - bid_1545) / mid_1545
- rel_spread_eod = (ask_eod - bid_eod) / mid_eod
- dte_cal = expiration - quote_date in calendar days

Contract classification:
- tenor_bucket = {0_7, 8_30, 31_90, 91_plus}
- if calcs exist:
  - call_otm if 0.10 <= delta_1545 <= 0.40
  - call_atm if 0.40 < delta_1545 < 0.60
  - put_otm if -0.40 <= delta_1545 <= -0.10
  - put_atm if -0.60 < delta_1545 < -0.40
- if calcs do not exist:
  - use strike vs S_1545 to define moneyness bands

Firm-day-bucket features:
- volume_bucket = sum(trade_volume)
- premium_bucket = sum(trade_volume * 100 * vwap)
- delta_notional_bucket = sum(trade_volume * 100 * abs(delta_1545) * S_1545) when delta exists
- iv_bucket = volume-weighted mean implied_volatility_1545 when available
- spread_bucket = volume-weighted mean rel_spread_1545
- lead_oi_change_bucket = sum(open_interest_t+1 - open_interest_t) across continuing series
- vol_to_oi_bucket = volume_bucket / max(1, sum(open_interest))

Abnormal measures:
- z_volume = z-score of log(1 + volume_bucket) vs estimation window [-120,-20]
- z_premium = z-score of log(1 + premium_bucket) vs estimation window
- z_delta_notional = z-score vs estimation window
- z_iv = z-score vs estimation window
- z_spread = z-score vs estimation window
- z_lead_oi = z-score vs estimation window

Event-level primary metrics:
- call_short_otm_score = sum(z_volume for call_otm x tenor_bucket in {0_7, 8_30} over t in [-5,-1])
- put_short_otm_score = sum(z_volume for put_otm x tenor_bucket in {0_7, 8_30} over t in [-5,-1])
- horizontal_bullish_footprint = mean(z_volume, z_premium, z_lead_oi, z_iv) for short OTM calls
- vertical_unsigned_footprint = max(abs(call_short_otm_score), abs(put_short_otm_score)) or unsigned composite
- linked_firm_event_return = linked firm's day 0 to +1 raw or market-adjusted return around source event
- source_event_materiality = source firm's announcement-day return or deal premium when available
- linkage_strength_horizontal = TNIC similarity score / percentile
- linkage_strength_vertical = VTNIC score / percentile

Optional policy-text variables:
- policy_cross_issuer_ban
- policy_mentions_other_companies
- policy_mentions_competitors
- policy_mentions_suppliers_customers
- policy_mentions_etfs_or_funds
- policy_mentions_derivatives
- policy_requires_preclearance

## 12. Event timestamp rules
- event_time_0 = first public disclosure time
- if disclosure occurs after 4:00 pm ET or on a non-trading day, next trading day is t = 0
- if disclosure occurs during market hours, pre-event window ends at t = -1
- maintain both filing timestamp and announcement trading date in the event table

## 13. Sample construction rules
Primary sample:
- U.S. public targets with announced M&A deals
- source firm must have listed single-name options in the vendor data
- at least one ex ante linked firm must also have listed single-name options
- linked-firm option buckets must have enough history for estimation window calculations

Exclusions:
- overlapping events for the same linked firm within the same pre-event window
- contracts with non-positive or crossed quotes
- zero-IV rows when IV is used
- ultra-illiquid firms with insufficient nonzero days in estimation window
- indices and ETFs in the MVP

Matched controls:
- same event date
- similar options-liquidity decile
- similar size decile or realized-volatility decile
- not in top linkage threshold

## 14. Primary empirical design
Phase A: Validation / replication
1. Reproduce abnormal own-target option activity before M&A announcements.
2. Show concentration in short-dated OTM calls.

Phase B: Novel shadow-trading test
1. Build ex ante linked-firm sets using TNIC.
2. Estimate abnormal pre-event option activity in linked firms.
3. Compare linked firms vs matched non-linked controls.
4. Estimate gradient by linkage strength decile.
5. Repeat with VTNIC.

Phase C: Materiality validation
1. Relate pre-event linked-firm option footprints to linked-firm announcement-day returns.
2. Test whether larger linked-firm returns are preceded by stronger footprints.

Phase D: Compliance translation
1. Construct a related-securities watchlist rule using ex ante linkage only.
2. Construct an enforcement-triage score using linkage + pre-disclosure footprint.
3. Evaluate precision/coverage trade-offs.

Optional Phase E: Policy text
1. Parse filed insider trading policies.
2. Classify breadth of cross-issuer prohibitions.
3. Test whether broader policies correlate with weaker linked-firm footprints.

## 15. Statistical rules
- Primary results must use transparent event-study or linear/logit models before any ML.
- Use firm and/or event clustered standard errors where appropriate.
- Use placebo dates and matched controls.
- Separate preregistered primary outcomes from secondary exploratory outcomes.
- Use multiple-testing correction for secondary bucket families.
- Document all exclusions and transformations.

## 16. Systems MVP vs full version
Systems MVP:
- one-source-event systems validation, not a substantive research sample
- first source symbol = `RHT`
- source event plus the full fan-out of its ex ante linked firms
- M&A events only
- TNIC horizontal peers required
- vertical loader success required, but a zero-match vertical set is acceptable for the first event
- single primary measure = abnormal short-dated OTM call volume
- one matched-control design
- one scripted audit bundle:
  - linkage audit table
  - exclusions table
  - one main linked-firm abnormal-activity figure
  - one one-page watchlist/compliance memo
- no notebook-only logic
- no policy-text extension
- no symbol-specific code branches
- switching config from `RHT` to one backup symbol (`VAR` or `FLIR`) must require no code edits

Full version:
- add VTNIC
- add composite footprint score
- add linked-firm materiality validation
- add policy-text extension
- add commercial data refinements if available

## 17. Deliverables
Required:
- reproducible repo
- cleaned event table
- cleaned linkage table
- bucketed options feature table
- replication memo
- main results memo
- 4-6 paper-quality figures
- 3-5 main tables
- 1 final research paper draft
- 1 résumé-ready project description

Nice-to-have:
- policy-text appendix
- slide deck
- short legal memo for admissions writing sample adaptation

## 18. Proposed repo layout
.
├── AGENTS.md
├── IMPLEMENTATION_PLAN.md
├── README.md
├── pyproject.toml
├── configs/
│   ├── paths.example.yaml
│   ├── research_params.yaml
│   └── logging.yaml
├── data/
│   ├── raw/           # never commit
│   ├── external/      # TNIC, VTNIC, SEC pulls
│   ├── interim/
│   └── processed/
├── docs/
│   ├── literature_review.md
│   ├── legal_theory_notes.md
│   ├── data_dictionary.md
│   ├── assumptions_log.md
│   └── results_log.md
├── notebooks/
│   ├── 00_schema_checks.ipynb
│   ├── 01_event_universe.ipynb
│   ├── 02_linkages.ipynb
│   ├── 03_replication.ipynb
│   └── 04_main_results.ipynb
├── outputs/
│   ├── figures/
│   ├── tables/
│   └── qc/
├── scripts/
│   ├── ingest_options.py
│   ├── build_underlying_daily.py
│   ├── build_mna_event_universe.py
│   ├── build_linkages.py
│   ├── build_option_buckets.py
│   ├── run_replication.py
│   ├── run_shadow_study.py
│   ├── parse_policies.py
│   └── make_paper_outputs.py
├── src/
│   └── shadow_trading/
│       ├── __init__.py
│       ├── config.py
│       ├── calendars.py
│       ├── schema.py
│       ├── io.py
│       ├── sec_events.py
│       ├── linkages.py
│       ├── options_clean.py
│       ├── underlyings.py
│       ├── buckets.py
│       ├── abnormal.py
│       ├── models.py
│       ├── policy_text.py
│       └── plots.py
└── tests/
    ├── test_schema.py
    ├── test_event_alignment.py
    ├── test_ingest_pipeline.py
    ├── test_underlying_daily.py
    ├── test_sec_events.py
    ├── test_bucketing.py
    ├── test_abnormal_metrics.py
    └── test_policy_parser.py

## 19. Milestones and build order
Milestone 0 — Repo and environment `[Done]`
Goal:
- [x] initialize repo, config, linting, tests, docs skeleton
Acceptance criteria:
- [x] package imports cleanly
- [x] config loads
- [x] smoke tests pass

Milestone 1 — Options ingestion and QC `[Done]`
Goal:
- [x] ingest vendor zip CSVs to partitioned parquet
- [x] create `series_id` and schema validation
- [x] generate row-count and missingness reports
Acceptance criteria:
- [x] one command produces processed options parquet
- [x] QC report saved to `outputs/qc/`

Milestone 2 — Underlying daily table `[Done]`
Goal:
- [x] deduplicate repeated underlying quote fields into one daily underlying table per symbol/date
- [x] compute raw daily returns
Acceptance criteria:
- [x] unique `(underlying_symbol, quote_date)` table exists
- [x] consistency checks documented

Milestone 3 — Event universe `[In progress]`
Goal:
- [x] collect candidate M&A announcement filings from official SEC data
- [x] timestamp first public disclosure from SEC acceptance datetime
- [~] deduplicate multi-filing same-deal announcements with an interpretable baseline clustering rule
Acceptance criteria:
- [x] baseline event table exists with `event_id`, `source_cik`, `first_public_disclosure_dt`, `event_trading_date`, and `deal_type`
- [~] target/acquirer extraction, canonical `source_firm_id`, and the SEC-native historical ticker-to-CIK bridge are now in place, but the broader historical sample still needs cleaner clustering/counterparty resolution before this milestone can be treated as final

Milestone 4 — Linkages `[Done]`
Goal:
- build TNIC and VTNIC link tables using lagged yearly data
- define matched control candidates
Acceptance criteria:
- [x] code path implemented: script, config, QC, tests, and a dated gvkey bridge now produce lagged link tables and control candidates on synthetic inputs
- [x] first live build now succeeds against the staged raw TNIC/VTNIC files in `data/external/linkages`

Milestone 5 — Bucketed option features `[Pending]`
Goal:
- aggregate contract-day data to firm-day-bucket tables
- compute abnormal metrics versus estimation windows
Acceptance criteria:
- one table per linked-firm event with pre-event bucket features

Milestone 6 — Replication `[Pending]`
Goal:
- reproduce abnormal target option activity before own M&A announcements
Acceptance criteria:
- at least one figure and one table match expected qualitative pattern

Milestone 7 — Main shadow-trading study `[Pending]`
Goal:
- estimate linked-firm abnormal activity vs controls
- estimate linkage-strength gradient
Acceptance criteria:
- main event-study figure
- main regression table
- sensitivity analyses saved

Milestone 8 — Materiality validation `[Pending]`
Goal:
- relate pre-event footprints to linked-firm announcement-day returns
Acceptance criteria:
- one figure showing footprint vs ex post linked-firm reaction
- one regression table

Milestone 9 — Compliance translation `[Pending]`
Goal:
- create ex ante watchlist rule and ex post triage score
Acceptance criteria:
- short memo section translating results into compliance policy

Milestone 10 — Optional policy-text extension `[Pending]`
Goal:
- parse insider trading policies and classify cross-issuer prohibitions
Acceptance criteria:
- labeled policy table
- one exploratory table/figure

Milestone 11 — Paper and polish `[Pending]`
Goal:
- finalize results, writing, and reproducibility
Acceptance criteria:
- README complete
- final figures/tables generated from scripts
- paper draft complete

## 19. Systems MVP acceptance criteria
The `RHT` systems MVP passes only if all of the following are true:
- `RHT` resolves to one unique source event with frozen `event_id`, `source_firm_id`, `target_cik`, `acquirer_cik`, raw announcement timestamp, and mapped trading date, with no symbol-specific code path
- the historical bridge resolves `RHT` to a dated issuer identity and at least one dated `gvkey` path, and writes an audit table with method and evidence fields
- the horizontal linkage build produces at least 5 ex ante linked firms with listed single-name options surviving optionability and history filters
- the options pipeline produces contract-level buckets, firm-day-bucket features, abnormal measures for estimation `[-120,-20]`, pre-event `[-5,-1]`, and event `[0,+1]` via scripts only
- one scripted run generates a linkage audit table, an exclusions table, one main linked-firm abnormal-activity figure, and one one-page watchlist/compliance memo
- the full run is reproducible from a clean checkout with config changes only
- switching the config from `RHT` to `VAR` or `FLIR` requires no code edits

## 20. Make targets to create
- [x] `make init`
- [x] `make ingest-options`
- [x] `make build-underlyings`
- [x] `make build-events`
- [x] `make build-linkages`
- [ ] `make build-buckets`
- [ ] `make replicate`
- [ ] `make main-study`
- [ ] `make policy-text`
- [ ] `make paper`
- [x] `make test`

## 21. Figure plan
Figure 1. Event-time abnormal option activity in source targets before M&A announcement
Figure 2. Event-time abnormal activity in linked firms vs matched controls
Figure 3. Linked-firm footprint by TNIC/VTNIC linkage decile
Figure 4. Pre-event footprint vs linked-firm announcement-day return
Figure 5. Compliance watchlist precision vs coverage
Figure 6. Optional policy-language prevalence and association with footprints

## 22. Table plan
Table 1. Sample construction and summary statistics
Table 2. Replication of own-target option activity
Table 3. Linked-firm abnormal option activity before source-firm M&A
Table 4. Linkage-strength gradient
Table 5. Footprint and ex post linked-firm materiality
Table 6. Optional policy-text extension

## 23. Writing outline
1. Introduction
2. Legal background on related-security insider trading
3. Literature and hypotheses
4. Data and variable construction
5. Validation / replication
6. Main results
7. Materiality and compliance translation
8. Optional policy-text results
9. Limitations
10. Conclusion

## 24. Key limitations to state explicitly
- Daily summary data do not identify traders.
- Aggregate volume does not reveal trade direction at the account level.
- Open interest is start-of-day, so net opening demand must be approximated with next-day change.
- Rumor contamination is possible in some deals.
- Options data capture suspicious footprints, not legal liability.

## 25. Final success criteria
The project is ready for résumé use when all of the following are true:
- replication works
- linked-firm effect appears in at least one primary design
- the paper has a clear legal contribution
- the repo is reproducible
- the limitations section is honest and precise

## 26. Reference list
- Augustin, Patrick, Menachem Brenner, and Marti G. Subrahmanyam. 2019. "Informed Options Trading Prior to Takeover Announcements: Insider Trading?" Management Science 65(12): 5697-5720.
- Augustin, Patrick, and Marti G. Subrahmanyam. 2020. "Informed Options Trading Before Corporate Events." Annual Review of Financial Economics 12: 327-355.
- Cao, Jie, Amit Goyal, Sai Ke, and Xintong Zhan. 2024. "Options Trading and Stock Price Informativeness." Journal of Financial and Quantitative Analysis 59(4): 1516-1540.
- Deuskar, Prachi, Aditi Khatri, and Jayanthi Sunder. 2024. "Insider Trading Restrictions and Informed Trading in Peer Stocks." Management Science.
- Du, Mingzhi, and Jimmy E. Hilliard. 2025. "Informed Option Trading of Target Firms' Rivals Prior to M&A Announcements." Journal of Futures Markets 45(10): 1683-1692.
- Enriques, Luca, Yoon-Ho Alex Lee, and Alessandro Romano. 2025. "The Placebo Effect of Insider Dealing Regulation." Oxford Journal of Legal Studies 45(3): 753-774.
- Frésard, Laurent, Gerard Hoberg, and Gordon M. Phillips. 2020. "Innovation Activities and Integration through Vertical Acquisitions." Review of Financial Studies 33(7): 2937-2976.
- Hoberg, Gerard, and Gordon Phillips. 2010. "Product Market Synergies and Competition in Mergers and Acquisitions: A Text-Based Analysis." Review of Financial Studies 23(10): 3773-3811.
- Hoberg, Gerard, and Gordon Phillips. 2016. "Text-Based Network Industries and Endogenous Product Differentiation." Journal of Political Economy 124(5): 1423-1465.
- Mehta, Mihir N., David M. Reeb, and Wanli Zhao. 2021. "Shadow Trading." The Accounting Review 96(4): 367-404.
- Tookes, Heather E. 2008. "Information, Trading, and Product Market Interactions: Cross-Sectional Implications of Informed Trading." Journal of Finance 63(1): 379-413.
- Weinbaum, David, Andy Fodor, Dmitriy Muravyev, and Martijn Cremers. 2023. "Option Trading Activity, News Releases, and Stock Return Predictability." Management Science 69(8): 4810-4827.
