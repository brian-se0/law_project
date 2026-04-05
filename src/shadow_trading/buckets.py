from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import polars as pl

from shadow_trading.config import CaseStudyWindowConfig, ExactContractConfig


@dataclass(frozen=True)
class CaseWindowDates:
    event_trading_date: date
    estimation_dates: tuple[date, ...]
    pre_event_dates: tuple[date, ...]
    terminal_case_dates: tuple[date, ...]
    announcement_dates: tuple[date, ...]
    extraction_dates: tuple[date, ...]
    date_map: pl.DataFrame


def build_case_window_dates(
    *,
    underlyings_path: Path,
    event_trading_date: date,
    windows: CaseStudyWindowConfig,
) -> CaseWindowDates:
    if not underlyings_path.exists():
        raise FileNotFoundError(
            f"{underlyings_path} does not exist. Build the underlying-daily table first."
        )

    trading_dates = (
        pl.read_parquet(underlyings_path, columns=["quote_date"])
        .select("quote_date")
        .unique()
        .sort("quote_date")
        .get_column("quote_date")
        .to_list()
    )
    if not trading_dates:
        raise ValueError(f"No trading dates were found in {underlyings_path}.")

    try:
        event_index = trading_dates.index(event_trading_date)
    except ValueError as exc:
        raise ValueError(
            f"Event trading date {event_trading_date.isoformat()} is not present in "
            f"{underlyings_path}. Load the relevant quote-date slice before running the case study."
        ) from exc

    min_offset = min(
        windows.estimation[0],
        windows.pre_event[0],
        windows.terminal_case[0],
        windows.announcement[0],
    )
    max_offset = max(
        windows.estimation[1],
        windows.pre_event[1],
        windows.terminal_case[1],
        windows.announcement[1],
    )
    if event_index + min_offset < 0:
        raise ValueError(
            "The processed data does not contain enough pre-event history for the configured "
            f"estimation window ending at {event_trading_date.isoformat()}."
        )
    if event_index + max_offset >= len(trading_dates):
        raise ValueError(
            "The processed data does not contain enough post-event history for the configured "
            f"announcement window ending at {event_trading_date.isoformat()}."
        )

    relative_offsets = list(range(min_offset, max_offset + 1))
    base_dates = [trading_dates[event_index + offset] for offset in relative_offsets]
    extraction_dates = list(base_dates)
    if event_index + max_offset + 1 < len(trading_dates):
        extraction_dates.append(trading_dates[event_index + max_offset + 1])

    date_map = pl.DataFrame(
        {
            "quote_date": base_dates,
            "relative_day": relative_offsets,
            "next_trading_date": [
                (
                    trading_dates[event_index + offset + 1]
                    if event_index + offset + 1 < len(trading_dates)
                    else None
                )
                for offset in relative_offsets
            ],
        }
    ).with_columns(
        [
            pl.col("relative_day")
            .is_between(windows.pre_event[0], windows.pre_event[1], closed="both")
            .alias("case_pre_event_window_flag"),
            pl.col("relative_day")
            .is_between(windows.terminal_case[0], windows.terminal_case[1], closed="both")
            .alias("case_terminal_window_flag"),
            pl.col("relative_day")
            .is_between(windows.announcement[0], windows.announcement[1], closed="both")
            .alias("announcement_window_flag"),
        ]
    )

    return CaseWindowDates(
        event_trading_date=event_trading_date,
        estimation_dates=_slice_offsets(trading_dates, event_index, windows.estimation),
        pre_event_dates=_slice_offsets(trading_dates, event_index, windows.pre_event),
        terminal_case_dates=_slice_offsets(trading_dates, event_index, windows.terminal_case),
        announcement_dates=_slice_offsets(trading_dates, event_index, windows.announcement),
        extraction_dates=tuple(extraction_dates),
        date_map=date_map,
    )


def extract_case_option_slice(
    *,
    options_dataset_dir: Path,
    symbols: list[str],
    quote_dates: tuple[date, ...],
) -> pl.DataFrame:
    if not options_dataset_dir.exists():
        raise FileNotFoundError(
            f"{options_dataset_dir} does not exist. Run the options ingest step first."
        )

    partition_paths = _resolve_option_partition_paths(
        options_dataset_dir=options_dataset_dir,
        quote_dates=quote_dates,
    )
    if not partition_paths:
        raise FileNotFoundError(
            "No processed option partitions were found for the configured case-study dates. "
            "Load the required quote dates before running bucket construction."
        )

    normalized_symbols = sorted({symbol.strip().upper() for symbol in symbols})
    return _extract_option_slice_duckdb(partition_paths, normalized_symbols)


def extract_symbol_daily_option_volume(
    *,
    options_dataset_dir: Path,
    symbols: list[str],
    quote_dates: tuple[date, ...],
) -> pl.DataFrame:
    if not options_dataset_dir.exists():
        raise FileNotFoundError(
            f"{options_dataset_dir} does not exist. Run the options ingest step first."
        )

    partition_paths = _resolve_option_partition_paths(
        options_dataset_dir=options_dataset_dir,
        quote_dates=quote_dates,
    )
    if not partition_paths:
        raise FileNotFoundError(
            "No processed option partitions were found for the configured case-study dates. "
            "Load the required quote dates before running bucket construction."
        )

    normalized_symbols = sorted({symbol.strip().upper() for symbol in symbols})
    if not normalized_symbols:
        return pl.DataFrame(
            schema={
                "quote_date": pl.Date,
                "underlying_symbol": pl.String,
                "daily_total_volume": pl.Int64,
            }
        )
    return _extract_symbol_daily_option_volume_duckdb(partition_paths, normalized_symbols)


def enrich_case_option_rows(
    *,
    options_frame: pl.DataFrame,
    window_dates: CaseWindowDates,
    exact_contracts: tuple[ExactContractConfig, ...],
    primary_related_symbol: str,
) -> pl.DataFrame:
    if options_frame.height == 0:
        return _empty_option_row_frame()

    exact_contract_lookup = pl.DataFrame(
        {
            "series_id": [contract.series_id for contract in exact_contracts],
            "litigated_contract_flag": [True] * len(exact_contracts),
        }
    )
    premium_proxy = pl.coalesce([pl.col("vwap"), pl.col("mid_1545"), pl.col("mid_eod")])
    with_windows = (
        options_frame.join(window_dates.date_map, on="quote_date", how="inner")
        .sort(["series_id", "quote_date"])
        .with_columns(
            [
                pl.col("quote_date").shift(-1).over("series_id").alias("__next_quote_date"),
                pl.col("open_interest").shift(-1).over("series_id").alias("__next_open_interest"),
            ]
        )
        .with_columns(
            [
                pl.when(pl.col("__next_quote_date") == pl.col("next_trading_date"))
                .then(pl.col("__next_open_interest") - pl.col("open_interest"))
                .otherwise(None)
                .alias("lead_open_interest_change"),
                (pl.col("underlying_symbol") == primary_related_symbol).alias(
                    "primary_related_symbol_flag"
                ),
                premium_proxy.alias("__premium_proxy"),
            ]
        )
        .join(exact_contract_lookup, on="series_id", how="left")
        .with_columns(
            [
                pl.col("litigated_contract_flag").fill_null(False),
                _tenor_bucket_expr().alias("tenor_bucket"),
                _moneyness_bucket_expr().alias("moneyness_bucket"),
                (pl.col("trade_volume") * 100 * pl.col("__premium_proxy")).alias("premium_proxy"),
            ]
        )
    )
    return with_windows.drop(
        [
            column
            for column in ["__next_quote_date", "__next_open_interest", "__premium_proxy"]
            if column in with_windows.columns
        ]
    )


def build_exact_contract_features(
    option_rows: pl.DataFrame,
) -> pl.DataFrame:
    if option_rows.height == 0:
        return _empty_exact_contract_frame()

    call_volume = (
        option_rows.filter(pl.col("option_type") == "C")
        .group_by(["underlying_symbol", "quote_date"])
        .agg(pl.col("trade_volume").sum().alias("underlying_call_volume"))
    )
    same_expiry_call_volume = (
        option_rows.filter(pl.col("option_type") == "C")
        .group_by(["underlying_symbol", "quote_date", "expiration"])
        .agg(pl.col("trade_volume").sum().alias("same_expiry_call_volume"))
    )
    exact_rows = option_rows.filter(pl.col("litigated_contract_flag"))
    if exact_rows.height == 0:
        return _empty_exact_contract_frame()

    return (
        exact_rows.join(call_volume, on=["underlying_symbol", "quote_date"], how="left")
        .join(
            same_expiry_call_volume,
            on=["underlying_symbol", "quote_date", "expiration"],
            how="left",
        )
        .with_columns(
            [
                pl.col("trade_volume").alias("contract_volume"),
                pl.col("premium_proxy").alias("contract_premium"),
                pl.col("lead_open_interest_change").alias("contract_lead_oi_change"),
                pl.col("rel_spread_1545").alias("contract_rel_spread_1545"),
                pl.col("implied_volatility_1545").alias("contract_iv_1545"),
                pl.col("underlying_call_volume"),
                pl.col("same_expiry_call_volume"),
                pl.when(pl.col("underlying_call_volume") > 0)
                .then(pl.col("trade_volume") / pl.col("underlying_call_volume"))
                .otherwise(None)
                .alias("contract_volume_share_of_underlying_call_volume"),
                pl.when(pl.col("same_expiry_call_volume") > 0)
                .then(pl.col("trade_volume") / pl.col("same_expiry_call_volume"))
                .otherwise(None)
                .alias("contract_volume_share_of_same_expiry_call_volume"),
            ]
        )
        .select(
            [
                "quote_date",
                "relative_day",
                "series_id",
                "underlying_symbol",
                "root",
                "expiration",
                "strike",
                "option_type",
                "contract_volume",
                "contract_premium",
                "contract_lead_oi_change",
                "contract_rel_spread_1545",
                "contract_iv_1545",
                "underlying_call_volume",
                "same_expiry_call_volume",
                "contract_volume_share_of_underlying_call_volume",
                "contract_volume_share_of_same_expiry_call_volume",
                "litigated_contract_flag",
                "primary_related_symbol_flag",
                "case_pre_event_window_flag",
                "case_terminal_window_flag",
                "announcement_window_flag",
            ]
        )
        .sort(["series_id", "quote_date"])
    )


def validate_case_study_calcs(option_rows: pl.DataFrame) -> None:
    if option_rows.height == 0:
        return

    missing_calcs = option_rows.filter(~pl.col("has_calcs").fill_null(False))
    if missing_calcs.height:
        raise ValueError(
            "Case-study rows require Calcs-backed fields; found rows without `has_calcs` for "
            f"{_sample_series_ids(missing_calcs)}."
        )

    missing_delta = option_rows.filter(pl.col("delta_1545").is_null())
    if missing_delta.height:
        raise ValueError(
            "Case-study rows require non-null `delta_1545` for delta-based moneyness buckets; "
            f"found null delta rows for {_sample_series_ids(missing_delta)}."
        )

    missing_exact_iv = option_rows.filter(
        pl.col("litigated_contract_flag") & pl.col("implied_volatility_1545").is_null()
    )
    if missing_exact_iv.height:
        raise ValueError(
            "Case-study exact-contract rows require non-null `implied_volatility_1545`; found "
            f"missing IV for {_sample_series_ids(missing_exact_iv)}."
        )


def build_bucket_features(option_rows: pl.DataFrame) -> pl.DataFrame:
    if option_rows.height == 0:
        return _empty_bucket_frame()

    working = option_rows.with_columns(
        [
            pl.when(
                pl.col("delta_1545").is_not_null()
                & (pl.col("s_1545") > 0)
                & (pl.col("trade_volume") > 0)
            )
            .then(pl.col("trade_volume") * 100 * pl.col("delta_1545").abs() * pl.col("s_1545"))
            .otherwise(None)
            .alias("__delta_notional_component"),
            pl.when(pl.col("implied_volatility_1545").is_not_null() & (pl.col("trade_volume") > 0))
            .then(pl.col("trade_volume"))
            .otherwise(None)
            .alias("__iv_weight"),
            pl.when(pl.col("implied_volatility_1545").is_not_null() & (pl.col("trade_volume") > 0))
            .then(pl.col("trade_volume") * pl.col("implied_volatility_1545"))
            .otherwise(None)
            .alias("__iv_weighted"),
            pl.when(pl.col("rel_spread_1545").is_not_null() & (pl.col("trade_volume") > 0))
            .then(pl.col("trade_volume"))
            .otherwise(None)
            .alias("__spread_weight"),
            pl.when(pl.col("rel_spread_1545").is_not_null() & (pl.col("trade_volume") > 0))
            .then(pl.col("trade_volume") * pl.col("rel_spread_1545"))
            .otherwise(None)
            .alias("__spread_weighted"),
        ]
    )
    aggregated = (
        working.group_by(
            [
                "quote_date",
                "relative_day",
                "underlying_symbol",
                "option_type",
                "tenor_bucket",
                "moneyness_bucket",
                "case_pre_event_window_flag",
                "case_terminal_window_flag",
                "announcement_window_flag",
            ]
        )
        .agg(
            [
                pl.col("trade_volume").sum().alias("volume_bucket"),
                pl.col("premium_proxy").drop_nulls().sum().alias("premium_bucket"),
                pl.col("__delta_notional_component")
                .drop_nulls()
                .sum()
                .alias("__delta_notional_sum"),
                pl.col("__delta_notional_component")
                .drop_nulls()
                .len()
                .alias("__delta_notional_count"),
                pl.col("__iv_weighted").drop_nulls().sum().alias("__iv_weighted_sum"),
                pl.col("__iv_weight").drop_nulls().sum().alias("__iv_weight_sum"),
                pl.col("__spread_weighted").drop_nulls().sum().alias("__spread_weighted_sum"),
                pl.col("__spread_weight").drop_nulls().sum().alias("__spread_weight_sum"),
                pl.col("lead_open_interest_change").drop_nulls().sum().alias("__lead_oi_sum"),
                pl.col("lead_open_interest_change").drop_nulls().len().alias("__lead_oi_count"),
                pl.col("open_interest").drop_nulls().sum().alias("__open_interest_sum"),
                pl.col("series_id").n_unique().alias("series_count_bucket"),
            ]
        )
        .with_columns(
            [
                pl.when(pl.col("__delta_notional_count") > 0)
                .then(pl.col("__delta_notional_sum"))
                .otherwise(None)
                .alias("delta_notional_bucket"),
                pl.when(pl.col("__iv_weight_sum") > 0)
                .then(pl.col("__iv_weighted_sum") / pl.col("__iv_weight_sum"))
                .otherwise(None)
                .alias("iv_bucket"),
                pl.when(pl.col("__spread_weight_sum") > 0)
                .then(pl.col("__spread_weighted_sum") / pl.col("__spread_weight_sum"))
                .otherwise(None)
                .alias("spread_bucket"),
                pl.when(pl.col("__lead_oi_count") > 0)
                .then(pl.col("__lead_oi_sum"))
                .otherwise(None)
                .alias("lead_oi_change_bucket"),
                (
                    pl.col("volume_bucket")
                    / pl.max_horizontal(pl.lit(1.0), pl.col("__open_interest_sum").cast(pl.Float64))
                ).alias("vol_to_oi_bucket"),
            ]
        )
        .drop(
            [
                "__delta_notional_sum",
                "__delta_notional_count",
                "__iv_weighted_sum",
                "__iv_weight_sum",
                "__spread_weighted_sum",
                "__spread_weight_sum",
                "__lead_oi_sum",
                "__lead_oi_count",
                "__open_interest_sum",
            ]
        )
        .sort(
            [
                "underlying_symbol",
                "quote_date",
                "option_type",
                "tenor_bucket",
                "moneyness_bucket",
            ]
        )
    )
    return aggregated


def summarize_bucket_build(
    *,
    option_rows: pl.DataFrame,
    exact_contracts: pl.DataFrame,
    bucket_features: pl.DataFrame,
    related_symbols: list[str],
    expected_exact_contracts: tuple[ExactContractConfig, ...],
    window_dates: CaseWindowDates,
) -> dict[str, Any]:
    expected_series_ids = {contract.series_id for contract in expected_exact_contracts}
    observed_series_ids = (
        set(exact_contracts.get_column("series_id").unique().to_list())
        if exact_contracts.height
        else set()
    )
    missing_series_ids = sorted(expected_series_ids - observed_series_ids)
    return {
        "quote_date_min": min(window_dates.extraction_dates).isoformat(),
        "quote_date_max": max(window_dates.extraction_dates).isoformat(),
        "symbol_count": len(set(related_symbols)),
        "filtered_option_row_count": option_rows.height,
        "exact_contract_row_count": exact_contracts.height,
        "bucket_row_count": bucket_features.height,
        "expected_exact_series_count": len(expected_series_ids),
        "observed_exact_series_count": len(observed_series_ids),
        "missing_exact_series_ids": missing_series_ids,
        "missing_exact_series_count": len(missing_series_ids),
        "provenance_note": (
            "Built from the processed options parquet partitions for the case-study event window. "
            "The extraction step filters the large contract tables with DuckDB, "
            "computes next-day open-interest changes only when the exact next trading day is "
            "observed, and requires Calcs-backed rows for delta-based moneyness and exact-series "
            "abnormal metrics."
        ),
    }


def render_bucket_qc_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# MDVN Case Bucket QC",
        "",
        f"- Quote-date range: {report['quote_date_min']} to {report['quote_date_max']}",
        f"- Symbols extracted: {report['symbol_count']:,}",
        f"- Filtered option rows: {report['filtered_option_row_count']:,}",
        f"- Exact-contract rows: {report['exact_contract_row_count']:,}",
        f"- Bucket rows: {report['bucket_row_count']:,}",
        f"- Expected exact series: {report['expected_exact_series_count']:,}",
        f"- Observed exact series: {report['observed_exact_series_count']:,}",
        f"- Missing exact series: {report['missing_exact_series_count']:,}",
        "",
        "## Missing Exact Series",
        "",
    ]
    if report["missing_exact_series_ids"]:
        for series_id in report["missing_exact_series_ids"]:
            lines.append(f"- {series_id}")
    else:
        lines.append("- None")
    lines.extend(["", "## Provenance", "", report["provenance_note"], ""])
    return "\n".join(lines)


def _extract_option_slice_duckdb(
    partition_paths: list[Path],
    symbols: list[str],
) -> pl.DataFrame:
    path_literals = ", ".join(_sql_string_literal(str(path)) for path in partition_paths)
    symbol_literals = ", ".join(_sql_string_literal(symbol) for symbol in symbols)
    query = f"""
        SELECT *
        FROM read_parquet([{path_literals}])
        WHERE underlying_symbol IN ({symbol_literals})
        ORDER BY quote_date, underlying_symbol, series_id
    """
    connection = duckdb.connect()
    try:
        relation = connection.sql(query)
        rows = relation.fetchall()
        return pl.DataFrame(
            rows,
            schema=relation.columns,
            orient="row",
            infer_schema_length=None,
        )
    finally:
        connection.close()


def _extract_symbol_daily_option_volume_duckdb(
    partition_paths: list[Path],
    symbols: list[str],
) -> pl.DataFrame:
    path_literals = ", ".join(_sql_string_literal(str(path)) for path in partition_paths)
    symbol_literals = ", ".join(_sql_string_literal(symbol) for symbol in symbols)
    query = f"""
        SELECT
            quote_date,
            underlying_symbol,
            SUM(trade_volume) AS daily_total_volume
        FROM read_parquet([{path_literals}])
        WHERE underlying_symbol IN ({symbol_literals})
        GROUP BY quote_date, underlying_symbol
        ORDER BY underlying_symbol, quote_date
    """
    connection = duckdb.connect()
    try:
        relation = connection.sql(query)
        rows = relation.fetchall()
        return pl.DataFrame(
            rows,
            schema=relation.columns,
            orient="row",
            infer_schema_length=None,
        )
    finally:
        connection.close()


def _resolve_option_partition_paths(
    *,
    options_dataset_dir: Path,
    quote_dates: tuple[date, ...],
) -> list[Path]:
    return [
        options_dataset_dir / f"quote_date={quote_date.isoformat()}" / "options_eod_summary.parquet"
        for quote_date in quote_dates
        if (
            options_dataset_dir
            / f"quote_date={quote_date.isoformat()}"
            / "options_eod_summary.parquet"
        ).exists()
    ]


def _slice_offsets(
    trading_dates: list[date],
    event_index: int,
    bounds: tuple[int, int],
) -> tuple[date, ...]:
    start, end = bounds
    return tuple(trading_dates[event_index + offset] for offset in range(start, end + 1))


def _tenor_bucket_expr() -> pl.Expr:
    return (
        pl.when(pl.col("dte_cal") <= 7)
        .then(pl.lit("0_7"))
        .when(pl.col("dte_cal") <= 30)
        .then(pl.lit("8_30"))
        .when(pl.col("dte_cal") <= 90)
        .then(pl.lit("31_90"))
        .otherwise(pl.lit("91_plus"))
    )


def _moneyness_bucket_expr() -> pl.Expr:
    delta_bucket = (
        pl.when((pl.col("option_type") == "C") & pl.col("delta_1545").is_between(0.10, 0.40))
        .then(pl.lit("call_otm"))
        .when(
            (pl.col("option_type") == "C")
            & (pl.col("delta_1545") > 0.40)
            & (pl.col("delta_1545") < 0.60)
        )
        .then(pl.lit("call_atm"))
        .when((pl.col("option_type") == "P") & pl.col("delta_1545").is_between(-0.40, -0.10))
        .then(pl.lit("put_otm"))
        .when(
            (pl.col("option_type") == "P")
            & (pl.col("delta_1545") > -0.60)
            & (pl.col("delta_1545") < -0.40)
        )
        .then(pl.lit("put_atm"))
        .otherwise(None)
    )
    return delta_bucket.fill_null("other")


def _sql_string_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _sample_series_ids(frame: pl.DataFrame, *, limit: int = 3) -> str:
    series_ids = frame.get_column("series_id").drop_nulls().unique().sort().head(limit).to_list()
    if not series_ids:
        return "the selected case-study rows"
    suffix = "..." if frame.get_column("series_id").drop_nulls().unique().len() > limit else ""
    return ", ".join(str(series_id) for series_id in series_ids) + suffix


def _empty_option_row_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "quote_date": pl.Date,
            "relative_day": pl.Int64,
            "series_id": pl.String,
            "underlying_symbol": pl.String,
            "root": pl.String,
            "expiration": pl.Date,
            "strike": pl.Float64,
            "option_type": pl.String,
            "trade_volume": pl.Int64,
            "open_interest": pl.Int64,
            "vwap": pl.Float64,
            "mid_1545": pl.Float64,
            "mid_eod": pl.Float64,
            "rel_spread_1545": pl.Float64,
            "implied_volatility_1545": pl.Float64,
            "delta_1545": pl.Float64,
            "s_1545": pl.Float64,
            "dte_cal": pl.Int64,
            "lead_open_interest_change": pl.Int64,
            "tenor_bucket": pl.String,
            "moneyness_bucket": pl.String,
            "premium_proxy": pl.Float64,
            "litigated_contract_flag": pl.Boolean,
            "primary_related_symbol_flag": pl.Boolean,
            "case_pre_event_window_flag": pl.Boolean,
            "case_terminal_window_flag": pl.Boolean,
            "announcement_window_flag": pl.Boolean,
        }
    )


def _empty_exact_contract_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "quote_date": pl.Date,
            "relative_day": pl.Int64,
            "series_id": pl.String,
            "underlying_symbol": pl.String,
            "root": pl.String,
            "expiration": pl.Date,
            "strike": pl.Float64,
            "option_type": pl.String,
            "contract_volume": pl.Int64,
            "contract_premium": pl.Float64,
            "contract_lead_oi_change": pl.Int64,
            "contract_rel_spread_1545": pl.Float64,
            "contract_iv_1545": pl.Float64,
            "contract_volume_share_of_underlying_call_volume": pl.Float64,
            "contract_volume_share_of_same_expiry_call_volume": pl.Float64,
            "litigated_contract_flag": pl.Boolean,
            "primary_related_symbol_flag": pl.Boolean,
            "case_pre_event_window_flag": pl.Boolean,
            "case_terminal_window_flag": pl.Boolean,
            "announcement_window_flag": pl.Boolean,
        }
    )


def _empty_bucket_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "quote_date": pl.Date,
            "relative_day": pl.Int64,
            "underlying_symbol": pl.String,
            "option_type": pl.String,
            "tenor_bucket": pl.String,
            "moneyness_bucket": pl.String,
            "case_pre_event_window_flag": pl.Boolean,
            "case_terminal_window_flag": pl.Boolean,
            "announcement_window_flag": pl.Boolean,
            "volume_bucket": pl.Int64,
            "premium_bucket": pl.Float64,
            "delta_notional_bucket": pl.Float64,
            "iv_bucket": pl.Float64,
            "spread_bucket": pl.Float64,
            "lead_oi_change_bucket": pl.Int64,
            "vol_to_oi_bucket": pl.Float64,
            "series_count_bucket": pl.UInt32,
        }
    )
