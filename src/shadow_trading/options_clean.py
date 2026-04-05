from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Sequence

import polars as pl

from shadow_trading.schema import (
    CONTRACT_DAY_KEY,
    DATE_COLUMNS,
    FLOAT_COLUMNS,
    INTEGER_COLUMNS,
    STRING_COLUMNS,
    VENDOR_EXPECTED_COLUMNS,
    canonicalize_columns,
    missing_core_columns,
)


def normalize_option_frame(frame: pl.DataFrame) -> pl.DataFrame:
    renamed = frame.rename(
        dict(zip(frame.columns, canonicalize_columns(frame.columns), strict=True))
    )
    missing = missing_core_columns(renamed.columns)
    if missing:
        missing_cols = ", ".join(missing)
        raise ValueError(f"Option archive is missing required columns: {missing_cols}")

    working = _cast_columns(renamed)
    working = working.with_columns(
        [
            pl.col("underlying_symbol").cast(pl.String).str.strip_chars().str.to_uppercase(),
            pl.col("root").cast(pl.String).str.strip_chars().str.to_uppercase(),
            _normalized_option_type_expr().alias("option_type"),
        ]
    )

    option_quote_valid_1545 = _valid_quote_expr("bid_1545", "ask_1545")
    option_quote_valid_eod = _valid_quote_expr("bid_eod", "ask_eod")
    underlying_quote_valid_1545 = _valid_quote_expr("underlying_bid_1545", "underlying_ask_1545")
    underlying_quote_valid_eod = _valid_quote_expr("underlying_bid_eod", "underlying_ask_eod")

    mid_1545 = _midpoint_expr("bid_1545", "ask_1545")
    mid_eod = _midpoint_expr("bid_eod", "ask_eod")
    underlying_mid_1545 = _midpoint_expr("underlying_bid_1545", "underlying_ask_1545")
    underlying_mid_eod = _midpoint_expr("underlying_bid_eod", "underlying_ask_eod")
    strike_string = pl.col("strike").round(4).cast(pl.String)
    has_calcs_expr = _has_calcs_expr(working.columns)

    return working.with_columns(
        [
            option_quote_valid_1545.alias("has_valid_1545_quote"),
            option_quote_valid_eod.alias("has_valid_eod_quote"),
            underlying_quote_valid_1545.alias("has_valid_underlying_1545_quote"),
            underlying_quote_valid_eod.alias("has_valid_underlying_eod_quote"),
            has_calcs_expr.alias("has_calcs"),
            pl.when(option_quote_valid_1545).then(mid_1545).otherwise(None).alias("mid_1545"),
            pl.when(option_quote_valid_eod).then(mid_eod).otherwise(None).alias("mid_eod"),
            pl.when(option_quote_valid_1545 & (mid_1545 > 0))
            .then((pl.col("ask_1545") - pl.col("bid_1545")) / mid_1545)
            .otherwise(None)
            .alias("rel_spread_1545"),
            pl.when(option_quote_valid_eod & (mid_eod > 0))
            .then((pl.col("ask_eod") - pl.col("bid_eod")) / mid_eod)
            .otherwise(None)
            .alias("rel_spread_eod"),
            pl.when(underlying_quote_valid_1545)
            .then(underlying_mid_1545)
            .when(pl.col("active_underlying_price_1545") > 0)
            .then(pl.col("active_underlying_price_1545"))
            .otherwise(None)
            .alias("s_1545"),
            pl.when(underlying_quote_valid_eod)
            .then(underlying_mid_eod)
            .otherwise(None)
            .alias("s_eod"),
            (pl.col("expiration") - pl.col("quote_date")).dt.total_days().alias("dte_cal"),
            pl.concat_str(
                [
                    pl.col("underlying_symbol"),
                    pl.col("root"),
                    pl.col("expiration").cast(pl.String),
                    strike_string,
                    pl.col("option_type"),
                ],
                separator="|",
            ).alias("series_id"),
        ]
    ).sort(["quote_date", "underlying_symbol", "root", "expiration", "strike", "option_type"])


def build_frame_qc_report(
    frame: pl.DataFrame,
    source_archive: Path,
    source_member: str,
    output_path: Path,
) -> dict[str, Any]:
    duplicate_contract_day_rows = frame.height - frame.unique(subset=CONTRACT_DAY_KEY).height
    missing_counts = {
        column: int(frame.get_column(column).null_count()) for column in frame.columns
    }

    return {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "source_archive": str(source_archive),
        "source_member": source_member,
        "processed_output": str(output_path),
        "quote_date": str(frame.get_column("quote_date").min()),
        "row_count": frame.height,
        "unique_series_id_count": frame.get_column("series_id").n_unique(),
        "unique_underlying_symbol_count": frame.get_column("underlying_symbol").n_unique(),
        "duplicate_contract_day_rows": duplicate_contract_day_rows,
        "rows_with_calcs": frame.filter(pl.col("has_calcs")).height,
        "invalid_option_quote_1545_rows": frame.filter(~pl.col("has_valid_1545_quote")).height,
        "invalid_option_quote_eod_rows": frame.filter(~pl.col("has_valid_eod_quote")).height,
        "invalid_underlying_quote_1545_rows": frame.filter(
            ~pl.col("has_valid_underlying_1545_quote")
        ).height,
        "invalid_underlying_quote_eod_rows": frame.filter(
            ~pl.col("has_valid_underlying_eod_quote")
        ).height,
        "missing_vendor_columns": sorted(VENDOR_EXPECTED_COLUMNS - set(frame.columns)),
        "unexpected_columns": sorted(
            set(frame.columns) - VENDOR_EXPECTED_COLUMNS - _derived_columns()
        ),
        "missing_counts": missing_counts,
    }


def build_aggregate_qc_report(
    file_reports: Sequence[dict[str, Any]],
    dataset_output_dir: Path,
    source_archives: Sequence[Path],
) -> dict[str, Any]:
    aggregate_row_count = sum(int(report["row_count"]) for report in file_reports)
    aggregate_unique_series = sum(int(report["unique_series_id_count"]) for report in file_reports)
    invalid_1545 = sum(int(report["invalid_option_quote_1545_rows"]) for report in file_reports)
    invalid_eod = sum(int(report["invalid_option_quote_eod_rows"]) for report in file_reports)

    return {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "dataset_output_dir": str(dataset_output_dir),
        "source_archive_count": len(source_archives),
        "source_archives": [str(path) for path in source_archives],
        "processed_quote_dates": [report["quote_date"] for report in file_reports],
        "file_count": len(file_reports),
        "aggregate_row_count": aggregate_row_count,
        "aggregate_unique_series_id_count": aggregate_unique_series,
        "aggregate_invalid_option_quote_1545_rows": invalid_1545,
        "aggregate_invalid_option_quote_eod_rows": invalid_eod,
        "file_reports": list(file_reports),
        "provenance_note": (
            "Derived from immutable vendor zip archives in D:\\Options Data. "
            "Archives were read in place, CSV columns were canonicalized to the vendor layout, "
            "rows were normalized per trading date, and partitioned Parquet outputs were written "
            "under data/processed without modifying the raw archive set."
        ),
    }


def render_qc_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Options Ingest QC",
        "",
        f"- Generated at (UTC): {report['generated_at_utc']}",
        f"- Dataset output: `{report['dataset_output_dir']}`",
        f"- Source archive count: {report['source_archive_count']:,}",
        f"- Processed quote dates: {report['file_count']:,}",
        f"- Aggregate row count: {report['aggregate_row_count']:,}",
        f"- Aggregate unique series IDs: {report['aggregate_unique_series_id_count']:,}",
        f"- Aggregate invalid 15:45 option quotes: {report['aggregate_invalid_option_quote_1545_rows']:,}",
        f"- Aggregate invalid EOD option quotes: {report['aggregate_invalid_option_quote_eod_rows']:,}",
        "",
        "## Provenance",
        "",
        report["provenance_note"],
        "",
        "## Files",
        "",
    ]

    for file_report in report["file_reports"]:
        lines.append(
            "- "
            f"{file_report['quote_date']}: {file_report['row_count']:,} rows from "
            f"`{Path(file_report['source_archive']).name}`"
        )

    return "\n".join(lines)


def _cast_columns(frame: pl.DataFrame) -> pl.DataFrame:
    expressions: list[pl.Expr] = []
    for column in DATE_COLUMNS:
        if column in frame.columns:
            expressions.append(
                pl.col(column).cast(pl.String).str.strptime(pl.Date, strict=False).alias(column)
            )
    for column in INTEGER_COLUMNS:
        if column in frame.columns:
            expressions.append(pl.col(column).cast(pl.Int64, strict=False).alias(column))
    for column in FLOAT_COLUMNS:
        if column in frame.columns:
            expressions.append(pl.col(column).cast(pl.Float64, strict=False).alias(column))
    for column in STRING_COLUMNS:
        if column in frame.columns:
            expressions.append(pl.col(column).cast(pl.String).alias(column))
    if expressions:
        return frame.with_columns(expressions)
    return frame


def _normalized_option_type_expr() -> pl.Expr:
    cleaned = pl.col("option_type").cast(pl.String).str.strip_chars().str.to_uppercase()
    return (
        pl.when(cleaned.is_in(["CALL", "C"]))
        .then(pl.lit("C"))
        .when(cleaned.is_in(["PUT", "P"]))
        .then(pl.lit("P"))
        .otherwise(cleaned)
    )


def _valid_quote_expr(bid_column: str, ask_column: str) -> pl.Expr:
    return (
        pl.col(bid_column).is_not_null()
        & pl.col(ask_column).is_not_null()
        & (pl.col(bid_column) > 0)
        & (pl.col(ask_column) > 0)
        & (pl.col(ask_column) >= pl.col(bid_column))
    )


def _midpoint_expr(bid_column: str, ask_column: str) -> pl.Expr:
    return (pl.col(bid_column) + pl.col(ask_column)) / 2


def _has_calcs_expr(columns: Sequence[str]) -> pl.Expr:
    candidate_columns = [
        column
        for column in (
            "active_underlying_price_1545",
            "implied_volatility_1545",
            "delta_1545",
            "gamma_1545",
            "theta_1545",
            "vega_1545",
            "rho_1545",
        )
        if column in columns
    ]
    if not candidate_columns:
        return pl.lit(False)
    return pl.any_horizontal([pl.col(column).is_not_null() for column in candidate_columns])


def _derived_columns() -> set[str]:
    return {
        "series_id",
        "mid_1545",
        "mid_eod",
        "rel_spread_1545",
        "rel_spread_eod",
        "s_1545",
        "s_eod",
        "dte_cal",
        "has_valid_1545_quote",
        "has_valid_eod_quote",
        "has_valid_underlying_1545_quote",
        "has_valid_underlying_eod_quote",
        "has_calcs",
    }
