from __future__ import annotations

import polars as pl

from shadow_trading.case_study import build_case_buckets, build_case_study_paths

from case_study_fixtures import seed_case_study_inputs


def test_bucketing_works_on_synthetic_case_slice(tmp_path) -> None:
    config = seed_case_study_inputs(tmp_path, include_frozen_event=True)

    build_case_buckets(config, overwrite=True)

    paths = build_case_study_paths(config)
    bucket_features = pl.read_parquet(paths.bucket_features_file)

    assert bucket_features.height > 0
    assert "z_volume" in bucket_features.columns
    assert (
        bucket_features.filter(
            (pl.col("underlying_symbol") == "INCY")
            & (pl.col("moneyness_bucket") == "call_otm")
            & pl.col("tenor_bucket").is_in(["0_7", "8_30"])
        ).height
        > 0
    )
