# AGENTS.md

## Mission
Build a reproducible law-and-economics project on shadow trading in U.S. options markets. The objective is to measure pre-disclosure options footprints in economically linked firms around source-firm M&A announcements and translate the evidence into a compliance/watchlist framework.

## Current default objective
The default current objective is the `MDVN -> INCY` case study tied to `SEC v. Panuwat`.

Unless the user explicitly redirects the work, prioritize:
1. freezing the canonical `MDVN` case event
2. reconstructing the exact `INCY` contracts named in the SEC complaint
3. placing `INCY` inside Medivation's lagged ex ante linkage context
4. translating the empirical result into a related-securities watchlist rule

## Primary priorities
1. Reproducibility
2. Interpretability
3. Legal relevance
4. Auditability
5. Honest limits

## What this repo is NOT
- not a trading-strategy repo
- not a market-manipulation repo
- not an individual accusation engine
- not a black-box benchmark project

## Scope order
Always work in this order unless the user instructs otherwise:
1. schema and QC
2. event universe
3. linkage tables
4. freeze the canonical MDVN case event
5. exact-contract reconstruction
6. MDVN-only shadow-trading results
7. materiality validation
8. compliance translation
9. optional policy-text extension
10. polish

## Non-negotiable research rules
- Use ex ante linkages only. No ex post peer cherry-picking.
- Main sample = source firm is an M&A target; traded firms are linked firms with single-name options.
- Prioritize single-name stock options before ETFs or indices.
- Do not describe any pattern as illegal trading or proof of insider trading.
- Preferred language: "abnormal pre-disclosure activity", "shadow-trading risk", "suspicious footprint", "related-securities watchlist".
- State every limitation that matters.
- Treat the own-target `MDVN` activity as a benchmark, but the legally focal traded security is `INCY`.
- Preserve `INCY` as the primary related security even if generic linkage recovery is imperfect; document the linkage issue rather than forcing a false merge.
- No ML until baseline event studies and regressions are complete.

## Data-specific rules
- Treat `data/raw` as immutable. Never edit raw files in place.
- Store heavy intermediate tables as Parquet or DuckDB tables.
- Use a unique `series_id` based on `underlying_symbol`, `root`, `expiration`, `strike`, and `option_type`.
- Deduplicate repeated underlying quote fields into a separate underlying daily table.
- Treat `open_interest` as start-of-day only.
- For any opening-demand proxy, use next-day open-interest change and document the approximation.
- Treat the `1545` snapshot as the early-close snapshot on early-close days.
- If calcs fields are unavailable, skip Greek/IV-dependent features and use strike-vs-spot moneyness.
- Drop or flag rows with zero or crossed quotes before spread calculations.
- Freeze linkage tables using lagged yearly data.

## Event-alignment rules
- Use first public disclosure timestamp, not just filing date.
- If an announcement is after market close or on a non-trading day, next trading day is `t = 0`.
- If an announcement occurs during market hours, the pre-event window ends at `t = -1`.
- Preserve both raw timestamp and mapped trading date in the event table.
- Deduplicate multiple filings referring to the same deal; never silently overwrite conflicting event records.

## Modeling rules
- Primary window: estimation `[-120,-20]`, pre-event `[-5,-1]`, announcement `[0,+1]`.
- For the MDVN case study, also preserve the terminal-case window `[-2,-1]`.
- Primary buckets: `option_type x tenor_bucket x moneyness_bucket`.
- Horizontal peer hypothesis is bullish and call-heavy; vertical hypothesis is unsigned unless sign is justified.
- Primary outputs:
  - abnormal short-dated OTM volume
  - abnormal premium / delta-notional
  - lead open-interest change
  - implied-volatility and spread changes when available
- The primary legally focal exact-series inventory is:
  - `INCY 2016-09-16 C 80.0`
  - `INCY 2016-09-16 C 82.5`
  - `INCY 2016-09-16 C 85.0`
- Use matched controls and placebo dates before exploring more complex models.
- Use clustered standard errors when feasible.
- Treat composite scores as secondary to interpretable component measures.

## Coding rules
- Prefer DuckDB for big joins and aggregations.
- Prefer Polars for medium-large transforms.
- Use pandas only when the job is small or existing library interfaces truly require it.
- Use typed Python where reasonable.
- Keep functions small and deterministic.
- Do not hard-code data paths; use config files.
- Do not introduce a new dependency if the repo already has a working equivalent.
- Do not leave notebook-only logic in final workflows; productionize any notebook logic used in final results.

## Documentation rules
- Update `docs/data_dictionary.md` whenever a schema or variable definition changes.
- Update `docs/assumptions_log.md` whenever a methodological choice changes.
- Update `docs/results_log.md` when a new main result or robustness check is produced.
- Keep a short provenance note for each derived table.
- Save paper figures and tables via scripts, not manual notebook exports.

## Validation checklist for any non-trivial change
Before marking work complete:
1. run lint / format checks if configured
2. run unit tests if configured
3. run the smallest relevant pipeline step affected by the change
4. inspect at least one output artifact
5. update docs if schema, assumptions, or results changed

Minimum preferred command order when available:
- `ruff check .`
- `black --check .`
- `pytest -q`
- `make test`
- `make <relevant-target>`

If a referenced command does not exist yet, create the smallest sensible version and document it.

## Writing rules for memos and paper drafts
- Write for a legally trained but quantitatively literate reader.
- Define jargon before using it.
- Keep normative claims separate from empirical claims.
- Never equate abnormal trading with liability.
- Use "materiality proxy" rather than "materiality" unless legally justified.
- Explain why each variable matters for doctrine, compliance, or market integrity.
- Prefer clear charts and small tables over dense model farms.

## Source discipline
- Prefer peer-reviewed papers and official SEC / exchange / data-library sources.
- Use SSRN or practitioner materials only as secondary background or gap-spotting.
- For any current or potentially changed fact, verify before writing.
- Keep bibliographic details in `docs/literature_review.md` or a BibTeX file.

## When stuck
- first reduce scope to the MVP
- then confirm the MDVN case-study pipeline still works
- then leave a short note explaining the blocker, the assumptions tried, and the next best step
- do not silently change the research question to make the code easier

## Good default next action
Unless the user says otherwise, the next best action is usually:
1. get the schema right
2. freeze the MDVN case event
3. build the linkage context
4. reconstruct the exact `INCY` contracts
5. run the MDVN case study
