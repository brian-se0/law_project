from __future__ import annotations

from dataclasses import replace
from datetime import date

import pytest
import polars as pl

from shadow_trading.case_study import (
    build_case_study_paths,
    build_related_firms,
    run_case_study,
    summarize_exact_contract_windows,
)

from case_study_fixtures import build_case_study_config, seed_case_study_inputs


def test_case_study_runner_fails_loudly_if_frozen_case_event_is_missing(tmp_path) -> None:
    config = seed_case_study_inputs(tmp_path, include_frozen_event=False)

    with pytest.raises(FileNotFoundError, match="freeze_mdvn_case_event.py"):
        run_case_study(config, overwrite=True)


def test_case_study_runner_builds_non_null_matched_control_metrics(tmp_path) -> None:
    config = seed_case_study_inputs(tmp_path, include_frozen_event=True)

    run_case_study(config, overwrite=True)
    paths = build_case_study_paths(config)

    control_matches = pl.read_parquet(paths.control_matches_file)
    abnormal_metrics = pl.read_parquet(paths.abnormal_metrics_file)
    matched_controls = abnormal_metrics.filter(pl.col("comparison_role") == "matched_control")

    assert control_matches.height == 1
    assert control_matches.row(0, named=True)["match_distance"] is not None
    assert control_matches.row(0, named=True)["estimation_mean_daily_option_volume"] is not None
    assert matched_controls.height == 1
    assert (
        matched_controls.row(0, named=True)["pre_event_short_dated_otm_call_z_volume_mean"]
        is not None
    )


def test_build_related_firms_keeps_force_retained_primary_percentile_in_unit_interval(
    tmp_path,
) -> None:
    config = build_case_study_config(tmp_path)
    config = replace(config, case_study=replace(config.case_study, horizontal_top_k=1))

    horizontal_rows = [
        {
            "source_firm_id": "MDVN",
            "linked_firm_id": f"PEER{rank:02d}",
            "link_type": "horizontal_tnic",
            "link_year": 2015,
            "link_score": 1.0 - rank / 100,
            "source_ticker": "MDVN",
            "linked_ticker": f"PEER{rank:02d}",
            "source_gvkey": "1111",
            "linked_gvkey": f"{2000 + rank}",
            "source_name": "Medivation, Inc.",
            "linked_name": f"Peer {rank}",
            "link_rank": rank,
        }
        for rank in range(1, 32)
    ]
    linkages = pl.DataFrame(
        horizontal_rows
        + [
            {
                "source_firm_id": "MDVN",
                "linked_firm_id": "INCY",
                "link_type": "horizontal_tnic",
                "link_year": 2015,
                "link_score": 0.42,
                "source_ticker": "MDVN",
                "linked_ticker": "INCY",
                "source_gvkey": "1111",
                "linked_gvkey": "2222",
                "source_name": "Medivation, Inc.",
                "linked_name": "Incyte Corporation",
                "link_rank": 32,
            }
        ]
    )

    related_firms = build_related_firms(
        config=config,
        linkages=linkages,
        event_trading_date=date(2016, 8, 22),
    )
    incy_row = related_firms.filter(pl.col("linked_firm_id") == "INCY").row(0, named=True)

    assert incy_row["linked_rank_within_source"] == 32
    assert 0.0 <= incy_row["linked_percentile_within_source"] <= 1.0


def test_exact_contract_window_summary_has_non_null_pooled_pre_event_metrics(tmp_path) -> None:
    config = seed_case_study_inputs(tmp_path, include_frozen_event=True)

    run_case_study(config, overwrite=True)
    paths = build_case_study_paths(config)
    exact_contracts = pl.read_parquet(paths.exact_contracts_file)
    summary = summarize_exact_contract_windows(
        exact_contracts=exact_contracts,
        expected_exact_contracts=config.case_study.exact_contracts,
        windows=config.case_study.windows,
    )
    pooled_pre_event = summary.filter(
        (pl.col("summary_scope") == "pooled") & (pl.col("window_label") == "pre_event")
    ).row(0, named=True)

    assert pooled_pre_event["mean_z_contract_volume"] is not None
    assert pooled_pre_event["mean_z_contract_premium"] is not None
    assert pooled_pre_event["pooled_contract_volume"] > 0
