from __future__ import annotations

import pytest
import polars as pl

from shadow_trading.case_study import build_case_study_paths, run_case_study

from case_study_fixtures import seed_case_study_inputs


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
