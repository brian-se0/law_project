from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Sequence

import polars as pl

REQUIRED_UNDERLYING_SOURCE_COLUMNS = {
    "quote_date",
    "underlying_symbol",
    "underlying_bid_1545",
    "underlying_ask_1545",
    "implied_underlying_price_1545",
    "active_underlying_price_1545",
    "underlying_bid_eod",
    "underlying_ask_eod",
    "s_1545",
    "s_eod",
    "has_valid_underlying_1545_quote",
    "has_valid_underlying_eod_quote",
}


def build_underlying_daily_frame(options_frame: pl.DataFrame) -> pl.DataFrame:
    working = _ensure_source_columns(options_frame)

    return (
        working.group_by(["quote_date", "underlying_symbol"])
        .agg(
            [
                pl.len().alias("option_series_count"),
                _first_non_null("underlying_bid_1545").alias("underlying_bid_1545"),
                _first_non_null("underlying_ask_1545").alias("underlying_ask_1545"),
                _first_non_null("implied_underlying_price_1545").alias(
                    "implied_underlying_price_1545"
                ),
                _first_non_null("active_underlying_price_1545").alias(
                    "active_underlying_price_1545"
                ),
                _first_non_null("s_1545").alias("s_1545"),
                _first_non_null("underlying_bid_eod").alias("underlying_bid_eod"),
                _first_non_null("underlying_ask_eod").alias("underlying_ask_eod"),
                _first_non_null("s_eod").alias("s_eod"),
                pl.col("has_valid_underlying_1545_quote")
                .cast(pl.Boolean)
                .any()
                .alias("has_valid_underlying_1545_quote"),
                pl.col("has_valid_underlying_eod_quote")
                .cast(pl.Boolean)
                .any()
                .alias("has_valid_underlying_eod_quote"),
                _non_null_n_unique("underlying_bid_1545").alias(
                    "distinct_underlying_bid_1545_count"
                ),
                _non_null_n_unique("underlying_ask_1545").alias(
                    "distinct_underlying_ask_1545_count"
                ),
                _non_null_n_unique("s_1545").alias("distinct_s_1545_count"),
                _non_null_n_unique("underlying_bid_eod").alias("distinct_underlying_bid_eod_count"),
                _non_null_n_unique("underlying_ask_eod").alias("distinct_underlying_ask_eod_count"),
                _non_null_n_unique("s_eod").alias("distinct_s_eod_count"),
                _snapshot_distinct_count(
                    [
                        "underlying_bid_1545",
                        "underlying_ask_1545",
                    ]
                ).alias("distinct_1545_snapshot_count"),
                _snapshot_distinct_count(
                    [
                        "underlying_bid_eod",
                        "underlying_ask_eod",
                        "s_eod",
                    ]
                ).alias("distinct_eod_snapshot_count"),
            ]
        )
        .with_columns(
            [
                (pl.col("distinct_1545_snapshot_count") > 1).alias(
                    "has_inconsistent_1545_snapshot"
                ),
                (pl.col("distinct_eod_snapshot_count") > 1).alias("has_inconsistent_eod_snapshot"),
            ]
        )
        .sort(["underlying_symbol", "quote_date"])
    )


def add_underlying_raw_returns(frame: pl.DataFrame) -> pl.DataFrame:
    return (
        frame.sort(["underlying_symbol", "quote_date"])
        .with_columns(
            [
                pl.col("s_eod").shift(1).over("underlying_symbol").alias("prior_s_eod"),
            ]
        )
        .with_columns(
            [
                pl.when((pl.col("s_eod") > 0) & (pl.col("prior_s_eod") > 0))
                .then(pl.col("s_eod") / pl.col("prior_s_eod") - 1)
                .otherwise(None)
                .alias("raw_return")
            ]
        )
    )


def build_underlying_daily_qc_report(
    frame: pl.DataFrame,
    source_partition_files: Sequence[Path],
    input_dataset_dir: Path,
    output_path: Path,
) -> dict[str, Any]:
    return {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "input_dataset_dir": str(input_dataset_dir),
        "processed_output": str(output_path),
        "source_partition_count": len(source_partition_files),
        "source_partition_files": [str(path) for path in source_partition_files],
        "underlying_daily_row_count": frame.height,
        "source_option_row_count": int(frame.get_column("option_series_count").sum()),
        "unique_underlying_symbol_count": frame.get_column("underlying_symbol").n_unique(),
        "quote_date_min": str(frame.get_column("quote_date").min()),
        "quote_date_max": str(frame.get_column("quote_date").max()),
        "groups_with_inconsistent_1545_snapshot": frame.filter(
            pl.col("has_inconsistent_1545_snapshot")
        ).height,
        "groups_with_inconsistent_eod_snapshot": frame.filter(
            pl.col("has_inconsistent_eod_snapshot")
        ).height,
        "missing_s_1545_rows": frame.get_column("s_1545").null_count(),
        "missing_s_eod_rows": frame.get_column("s_eod").null_count(),
        "missing_raw_return_rows": frame.get_column("raw_return").null_count(),
        "provenance_note": (
            "Derived from the processed options dataset by collapsing repeated underlying quote fields "
            "to one row per underlying_symbol and quote_date. Repeated-field disagreements are flagged "
            "rather than suppressed, and raw_return is computed as close-to-close percentage change "
            "using s_eod."
        ),
    }


def render_underlying_daily_qc_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Underlying Daily QC",
        "",
        f"- Generated at (UTC): {report['generated_at_utc']}",
        f"- Input dataset: `{report['input_dataset_dir']}`",
        f"- Output file: `{report['processed_output']}`",
        f"- Source partitions: {report['source_partition_count']:,}",
        f"- Underlying-daily rows: {report['underlying_daily_row_count']:,}",
        f"- Source option rows collapsed: {report['source_option_row_count']:,}",
        f"- Unique underlying symbols: {report['unique_underlying_symbol_count']:,}",
        f"- Quote-date range: {report['quote_date_min']} to {report['quote_date_max']}",
        f"- Inconsistent 15:45 groups: {report['groups_with_inconsistent_1545_snapshot']:,}",
        f"- Inconsistent EOD groups: {report['groups_with_inconsistent_eod_snapshot']:,}",
        f"- Missing s_1545 rows: {report['missing_s_1545_rows']:,}",
        f"- Missing s_eod rows: {report['missing_s_eod_rows']:,}",
        f"- Missing raw_return rows: {report['missing_raw_return_rows']:,}",
        "",
        "## Provenance",
        "",
        report["provenance_note"],
        "",
    ]
    return "\n".join(lines)


def _ensure_source_columns(frame: pl.DataFrame) -> pl.DataFrame:
    missing = [
        column for column in REQUIRED_UNDERLYING_SOURCE_COLUMNS if column not in frame.columns
    ]
    if not missing:
        return frame
    expressions = [pl.lit(None).alias(column) for column in missing]
    return frame.with_columns(expressions)


def _first_non_null(column: str) -> pl.Expr:
    return pl.col(column).drop_nulls().first()


def _non_null_n_unique(column: str) -> pl.Expr:
    return pl.col(column).drop_nulls().n_unique()


def _snapshot_distinct_count(columns: Sequence[str]) -> pl.Expr:
    available_columns = [pl.col(column) for column in columns]
    return pl.struct(available_columns).drop_nulls().n_unique()
