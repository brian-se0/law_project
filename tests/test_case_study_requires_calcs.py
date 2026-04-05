from __future__ import annotations

from datetime import date

import pytest
import polars as pl

from shadow_trading.case_study import build_case_buckets

from case_study_fixtures import seed_case_study_inputs


def test_build_case_buckets_requires_calcs_backed_rows(tmp_path) -> None:
    config = seed_case_study_inputs(tmp_path, include_frozen_event=True)

    _rewrite_case_partition(
        config,
        quote_date=date(2016, 8, 18),
        mutated_column="has_calcs",
        mutated_value=False,
        symbol="INCY",
        strike=80.0,
    )

    with pytest.raises(ValueError, match="has_calcs"):
        build_case_buckets(config, overwrite=True)


def test_build_case_buckets_requires_delta_for_bucket_classification(tmp_path) -> None:
    config = seed_case_study_inputs(tmp_path, include_frozen_event=True)

    _rewrite_case_partition(
        config,
        quote_date=date(2016, 8, 18),
        mutated_column="delta_1545",
        mutated_value=None,
        symbol="PEER",
        strike=55.0,
    )

    with pytest.raises(ValueError, match="delta_1545"):
        build_case_buckets(config, overwrite=True)


def test_build_case_buckets_requires_exact_contract_iv(tmp_path) -> None:
    config = seed_case_study_inputs(tmp_path, include_frozen_event=True)

    _rewrite_case_partition(
        config,
        quote_date=date(2016, 8, 18),
        mutated_column="implied_volatility_1545",
        mutated_value=None,
        symbol="INCY",
        strike=80.0,
    )

    with pytest.raises(ValueError, match="implied_volatility_1545"):
        build_case_buckets(config, overwrite=True)


def _rewrite_case_partition(
    config,
    *,
    quote_date: date,
    mutated_column: str,
    mutated_value: object,
    symbol: str,
    strike: float,
) -> None:
    partition_path = (
        config.paths.processed_dir
        / config.ingest_options.output_dataset_dir
        / f"quote_date={quote_date.isoformat()}"
        / "options_eod_summary.parquet"
    )
    frame = pl.read_parquet(partition_path)
    target = (pl.col("underlying_symbol") == symbol) & (pl.col("strike") == strike)
    frame = frame.with_columns(
        pl.when(target)
        .then(pl.lit(mutated_value, dtype=frame.schema[mutated_column]))
        .otherwise(pl.col(mutated_column))
        .alias(mutated_column)
    )
    frame.write_parquet(partition_path, compression="zstd")
