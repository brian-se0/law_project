# Data Dictionary

## `data/processed/options_eod_summary/`

Derived from the vendor's Option EOD Summary zipped CSV archives in `D:\Options Data`.

The ingestion pipeline currently writes one Parquet file per quote date under:

- `data/processed/options_eod_summary/quote_date=YYYY-MM-DD/options_eod_summary.parquet`

Canonical fields preserved or derived at ingestion:

- `underlying_symbol`
- `quote_date`
- `root`
- `expiration`
- `strike`
- `option_type`
- `open`
- `high`
- `low`
- `close`
- `trade_volume`
- `bid_size_1545`
- `bid_1545`
- `ask_size_1545`
- `ask_1545`
- `underlying_bid_1545`
- `underlying_ask_1545`
- `implied_underlying_price_1545`
- `active_underlying_price_1545` when calcs are included
- `implied_volatility_1545` when calcs are included
- `delta_1545` when calcs are included
- `gamma_1545` when calcs are included
- `theta_1545` when calcs are included
- `vega_1545` when calcs are included
- `rho_1545` when calcs are included
- `bid_size_eod`
- `bid_eod`
- `ask_size_eod`
- `ask_eod`
- `underlying_bid_eod`
- `underlying_ask_eod`
- `vwap`
- `open_interest`
- `delivery_code`
- `series_id`
- `mid_1545`
- `mid_eod`
- `rel_spread_1545`
- `rel_spread_eod`
- `s_1545`
- `s_eod`
- `dte_cal`
- `has_valid_1545_quote`
- `has_valid_eod_quote`
- `has_valid_underlying_1545_quote`
- `has_valid_underlying_eod_quote`
- `has_calcs`

## `outputs/qc/options_ingest_qc.json`

Structured QC artifact with:

- source archive list
- processed output dataset location
- per-file row and quote-date counts
- aggregate row counts and unique-series counts
- invalid/crossed quote counts
- missingness by canonical column
- provenance note for the derived dataset

## `data/processed/underlying_daily.parquet`

Deduplicated underlying-level daily table derived from the processed options dataset.

Primary fields:

- `quote_date`
- `underlying_symbol`
- `option_series_count`: number of option-series rows collapsed into the underlying-day row
- `underlying_bid_1545`
- `underlying_ask_1545`
- `implied_underlying_price_1545`
- `active_underlying_price_1545`
- `s_1545`
- `underlying_bid_eod`
- `underlying_ask_eod`
- `s_eod`
- `has_valid_underlying_1545_quote`
- `has_valid_underlying_eod_quote`
- `distinct_underlying_bid_1545_count`
- `distinct_underlying_ask_1545_count`
- `distinct_s_1545_count`
- `distinct_underlying_bid_eod_count`
- `distinct_underlying_ask_eod_count`
- `distinct_s_eod_count`
- `distinct_1545_snapshot_count`
- `distinct_eod_snapshot_count`
- `has_inconsistent_1545_snapshot`
- `has_inconsistent_eod_snapshot`
- `prior_s_eod`
- `raw_return`: close-to-close percentage change using `s_eod`

## `outputs/qc/underlying_daily_qc.json`

Structured QC artifact with:

- processed options input dataset location
- source partition count
- collapsed source option row count
- underlying-daily row count
- unique underlying count
- quote-date range
- inconsistent underlying snapshot counts
- missingness in `s_1545`, `s_eod`, and `raw_return`
- provenance note for the derived table

## `data/processed/sec_mna_candidates.parquet`

Auditable candidate-filing table built from official SEC submissions JSON plus raw filing text.

Primary fields:

- `matched_symbol`
- `matched_cik`
- `matched_company_name`
- `matched_ticker`
- `accession_number`
- `filing_date`
- `acceptance_datetime_utc`
- `form`
- `items`
- `primary_document`
- `primary_doc_description`
- `raw_filing_url`
- `filer_name`
- `filer_cik`
- `filed_by_name`
- `filed_by_cik`
- `subject_company_name`
- `subject_company_cik`
- `source_firm_id`
- `source_cik`
- `source_name`
- `source_ticker`
- `source_underlying_symbol`
- `source_resolution`
- `source_has_option_data`
- `target_firm_id`
- `target_cik`
- `target_name`
- `target_ticker`
- `target_underlying_symbol`
- `target_resolution`
- `target_has_option_data`
- `acquirer_firm_id`
- `acquirer_cik`
- `acquirer_name`
- `acquirer_ticker`
- `acquirer_underlying_symbol`
- `acquirer_resolution`
- `acquirer_has_option_data`
- `counterparty_name`
- `counterparty_slug`
- `deal_type`
- `mna_match_score`
- `matched_keyword_count`
- `matched_keywords`
- `exclusion_hits`
- `text_excerpt`
- `is_mna_candidate`
- `is_target_side`
- `requires_manual_review`

## `data/processed/sec_mna_event_universe.parquet`

Deduplicated event table keyed to first public SEC disclosure.

Primary fields:

- `event_id`
- `source_firm_id`
- `source_cik`
- `source_name`
- `source_ticker`
- `source_underlying_symbol`
- `target_firm_id`
- `target_cik`
- `target_name`
- `target_ticker`
- `target_underlying_symbol`
- `acquirer_firm_id`
- `acquirer_cik`
- `acquirer_name`
- `acquirer_ticker`
- `acquirer_underlying_symbol`
- `first_public_disclosure_dt`
- `first_public_disclosure_filing_date`
- `event_trading_date`
- `pre_event_window_end`
- `announcement_form`
- `announcement_accession_number`
- `announcement_filing_url`
- `deal_type`
- `counterparty_name`
- `counterparty_slug`
- `source_resolution`
- `target_resolution`
- `acquirer_resolution`
- `candidate_filing_count`
- `candidate_forms`
- `candidate_accessions`
- `max_match_score`
- `requires_manual_review`
- `has_conflicting_counterparties`
- `has_conflicting_acquirers`
- `cluster_start_dt`
- `cluster_end_dt`

## `outputs/qc/sec_mna_event_universe_qc.json`

Structured QC artifact with:

- option-universe size used for matching
- matched SEC company count
- companies scanned
- candidate-filing count
- classified M&A-candidate count
- target-side candidate count
- final event count
- manual-review count
- form counts
- provenance note for the SEC event build

## `data/processed/linkages.parquet`

Lagged yearly linkage table derived from raw TNIC/VTNIC-style files placed under `data/external/linkages/`.

The live build now supports both direct ticker-pair files and gvkey-pair files. When the raw linkage inputs are gvkey-based, the pipeline first builds a dated `gvkey <-> underlying_symbol <-> event_year` bridge from the open `gvkey_ciks` seed plus SEC issuer evidence observed in the repo.

## `data/processed/gvkey_underlying_bridge.parquet`

Project-scoped dated bridge between Hoberg-Phillips gvkeys and option-underlying symbols, restricted to event years that actually appear in the event universe.

Primary fields:

- `gvkey`
- `iid`
- `cik`
- `event_year`
- `underlying_symbol`
- `firm_id`
- `observed_start_date`
- `observed_end_date`
- `option_obs_count`
- `seed_first_date`
- `seed_last_date`
- `issuer_ticker`
- `issuer_name`
- `issuer_sources`
- `evidence_event_ids`
- `bridge_method`
- `bridge_confidence`

Primary fields:

- `source_firm_id`
- `linked_firm_id`
- `link_type`
- `link_year`
- `link_score`
- `link_rank`
- `source_ticker`
- `linked_ticker`
- `source_gvkey`
- `linked_gvkey`
- `source_name`
- `linked_name`

## `data/processed/linkage_control_candidates.parquet`

Source-event-year control universe derived from the option-firm universe after excluding retained linked firms.

Primary fields:

- `source_firm_id`
- `event_year`
- `link_year`
- `link_type`
- `control_firm_id`

## `outputs/qc/linkages_qc.json`

Structured QC artifact with:

- discovered raw linkage input files
- source event-year count used for the lagged join
- option firm-year count
- source-event gvkey count
- gvkey-underlying bridge row count
- final linkage-row count
- control-candidate count
- linkage-type counts
- bridge-method counts
- provenance note for the linkage build

## `data/processed/case_studies/mdvn_case_event.parquet`

Frozen one-row case-event table for the `MDVN -> INCY` case study.

Primary fields:

- `case_id`
- `event_id`
- `source_firm_id`
- `source_symbol`
- `source_name`
- `target_cik`
- `target_gvkey`
- `acquirer_symbol`
- `acquirer_cik`
- `first_public_disclosure_dt`
- `event_trading_date`
- `case_private_context_date`
- `review_status`
- `review_note`
- `evidence_source`

## `data/processed/case_studies/mdvn_related_firms.parquet`

Lagged ex ante related-firm table retained for the case study.

Primary fields:

- `case_id`
- `source_symbol`
- `source_firm_id`
- `linked_firm_id`
- `link_type`
- `link_year`
- `link_score`
- `linked_rank_within_source`
- `linked_percentile_within_source`
- `primary_related_pair_flag`

## `data/processed/case_studies/mdvn_exact_contracts.parquet`

Daily exact-series table for the complaint-named `INCY` contracts.

Primary fields:

- `quote_date`
- `relative_day`
- `series_id`
- `underlying_symbol`
- `expiration`
- `strike`
- `option_type`
- `contract_volume`
- `contract_premium`
- `contract_lead_oi_change`
- `contract_rel_spread_1545`
- `contract_iv_1545`
- `contract_volume_share_of_underlying_call_volume`
- `contract_volume_share_of_same_expiry_call_volume`
- `z_contract_volume`
- `z_contract_premium`
- `z_contract_lead_oi`
- `z_contract_iv`

## `data/processed/case_studies/mdvn_bucket_features.parquet`

Firm-day-bucket table for case-study symbols around the frozen event.

Primary fields:

- `quote_date`
- `relative_day`
- `underlying_symbol`
- `option_type`
- `tenor_bucket`
- `moneyness_bucket`
- `volume_bucket`
- `premium_bucket`
- `delta_notional_bucket`
- `iv_bucket`
- `spread_bucket`
- `lead_oi_change_bucket`
- `vol_to_oi_bucket`
- `z_volume`
- `z_premium`
- `z_delta_notional`
- `z_iv`
- `z_spread`
- `z_lead_oi`

## `data/processed/case_studies/mdvn_abnormal_metrics.parquet`

Case-study comparison table spanning linked firms, the source benchmark, and matched controls.

Primary fields:

- `case_id`
- `comparison_role`
- `underlying_symbol`
- `primary_related_pair_flag`
- `link_type`
- `link_score`
- `linked_rank_within_source`
- `linked_percentile_within_source`
- `match_rank`
- `match_distance`
- `pre_event_short_dated_otm_call_z_volume_mean`
- `pre_event_short_dated_otm_call_z_premium_mean`
- `pre_event_short_dated_otm_call_z_delta_notional_mean`
- `pre_event_short_dated_otm_call_z_lead_oi_mean`
- `terminal_case_short_dated_otm_call_z_volume_mean`
- `terminal_case_short_dated_otm_call_z_premium_mean`
- `announcement_short_dated_otm_call_z_volume_mean`
- `announcement_short_dated_otm_call_z_premium_mean`
- `return_0`
- `return_0_1`
- `source_return_0_1`
