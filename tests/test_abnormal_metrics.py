from __future__ import annotations

from math import isclose, log

import polars as pl

from shadow_trading.abnormal import compute_bucket_abnormal_metrics


def test_abnormal_metrics_are_stable_on_deterministic_fixture() -> None:
    frame = pl.DataFrame(
        {
            "quote_date": [None, None, None, None],
            "relative_day": [-3, -2, -1, 0],
            "underlying_symbol": ["INCY"] * 4,
            "option_type": ["C"] * 4,
            "tenor_bucket": ["8_30"] * 4,
            "moneyness_bucket": ["call_otm"] * 4,
            "case_pre_event_window_flag": [False, True, True, False],
            "case_terminal_window_flag": [False, False, True, False],
            "announcement_window_flag": [False, False, False, True],
            "volume_bucket": [1, 3, 5, 7],
            "premium_bucket": [10.0, 20.0, 30.0, 40.0],
            "delta_notional_bucket": [5.0, 10.0, 15.0, 30.0],
            "iv_bucket": [0.20, 0.25, 0.30, 0.50],
            "spread_bucket": [0.05, 0.06, 0.07, 0.10],
            "lead_oi_change_bucket": [1, 2, 3, 6],
            "vol_to_oi_bucket": [0.1, 0.2, 0.3, 0.4],
            "series_count_bucket": [1, 1, 1, 1],
        }
    )

    result = compute_bucket_abnormal_metrics(frame, estimation_window=(-3, -1))
    row = result.filter(pl.col("relative_day") == 0).row(0, named=True)

    transformed = [log(1 + value) for value in [1, 3, 5]]
    mean = sum(transformed) / 3
    variance = sum((value - mean) ** 2 for value in transformed) / 2
    expected_z = (log(8) - mean) / variance**0.5

    assert isclose(float(row["z_volume"]), expected_z, rel_tol=1e-6)
    assert row["estimation_obs_count"] == 3
