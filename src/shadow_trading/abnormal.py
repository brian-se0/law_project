from __future__ import annotations

from dataclasses import dataclass

import polars as pl


@dataclass(frozen=True)
class AbnormalMetricSpec:
    source_column: str
    output_column: str
    transform: str = "identity"


BUCKET_ABNORMAL_METRICS = (
    AbnormalMetricSpec("volume_bucket", "z_volume", "log1p"),
    AbnormalMetricSpec("premium_bucket", "z_premium", "log1p"),
    AbnormalMetricSpec("delta_notional_bucket", "z_delta_notional", "log1p"),
    AbnormalMetricSpec("iv_bucket", "z_iv", "identity"),
    AbnormalMetricSpec("spread_bucket", "z_spread", "identity"),
    AbnormalMetricSpec("lead_oi_change_bucket", "z_lead_oi", "identity"),
)

EXACT_CONTRACT_ABNORMAL_METRICS = (
    AbnormalMetricSpec("contract_volume", "z_contract_volume", "log1p"),
    AbnormalMetricSpec("contract_premium", "z_contract_premium", "log1p"),
    AbnormalMetricSpec("contract_lead_oi_change", "z_contract_lead_oi", "identity"),
    AbnormalMetricSpec("contract_iv_1545", "z_contract_iv", "identity"),
)


def compute_abnormal_metrics(
    frame: pl.DataFrame,
    *,
    group_keys: list[str],
    estimation_window: tuple[int, int],
    metric_specs: tuple[AbnormalMetricSpec, ...],
    relative_day_column: str = "relative_day",
) -> pl.DataFrame:
    if frame.height == 0:
        return _empty_with_metric_columns(frame, metric_specs)

    working = frame
    transform_columns: list[str] = []
    for spec in metric_specs:
        transform_column = f"__{spec.output_column}_transformed"
        transform_columns.append(transform_column)
        working = working.with_columns(
            _metric_transform_expr(spec.source_column, spec.transform).alias(transform_column)
        )

    estimation_start, estimation_end = estimation_window
    baseline = (
        working.filter(
            pl.col(relative_day_column).is_between(estimation_start, estimation_end, closed="both")
        )
        .group_by(group_keys)
        .agg([pl.len().alias("estimation_obs_count")] + _baseline_aggregation_exprs(metric_specs))
    )
    if baseline.height == 0:
        return _empty_with_metric_columns(frame, metric_specs)

    result = working.join(baseline, on=group_keys, how="left")
    for spec in metric_specs:
        transform_column = f"__{spec.output_column}_transformed"
        mean_column = f"__{spec.output_column}_mean"
        std_column = f"__{spec.output_column}_std"
        result = result.with_columns(
            pl.when(pl.col(std_column) > 0)
            .then((pl.col(transform_column) - pl.col(mean_column)) / pl.col(std_column))
            .otherwise(None)
            .alias(spec.output_column)
        )

    drop_columns = (
        transform_columns
        + [f"__{spec.output_column}_mean" for spec in metric_specs]
        + [f"__{spec.output_column}_std" for spec in metric_specs]
    )
    return result.drop([column for column in drop_columns if column in result.columns])


def compute_bucket_abnormal_metrics(
    frame: pl.DataFrame,
    *,
    estimation_window: tuple[int, int],
) -> pl.DataFrame:
    return compute_abnormal_metrics(
        frame,
        group_keys=["underlying_symbol", "option_type", "tenor_bucket", "moneyness_bucket"],
        estimation_window=estimation_window,
        metric_specs=BUCKET_ABNORMAL_METRICS,
    )


def compute_exact_contract_abnormal_metrics(
    frame: pl.DataFrame,
    *,
    estimation_window: tuple[int, int],
) -> pl.DataFrame:
    return compute_abnormal_metrics(
        frame,
        group_keys=["series_id"],
        estimation_window=estimation_window,
        metric_specs=EXACT_CONTRACT_ABNORMAL_METRICS,
    )


def _baseline_aggregation_exprs(metric_specs: tuple[AbnormalMetricSpec, ...]) -> list[pl.Expr]:
    expressions: list[pl.Expr] = []
    for spec in metric_specs:
        transformed_column = f"__{spec.output_column}_transformed"
        expressions.extend(
            [
                pl.col(transformed_column).mean().alias(f"__{spec.output_column}_mean"),
                pl.col(transformed_column).std(ddof=1).alias(f"__{spec.output_column}_std"),
            ]
        )
    return expressions


def _metric_transform_expr(column: str, transform: str) -> pl.Expr:
    if transform == "identity":
        return pl.col(column)
    if transform == "log1p":
        return pl.when(pl.col(column) >= 0).then((pl.col(column) + 1).log()).otherwise(None)
    raise ValueError(f"Unsupported abnormal-metric transform: {transform}")


def _empty_with_metric_columns(
    frame: pl.DataFrame,
    metric_specs: tuple[AbnormalMetricSpec, ...],
) -> pl.DataFrame:
    result = frame
    for spec in metric_specs:
        if spec.output_column not in result.columns:
            result = result.with_columns(pl.lit(None, dtype=pl.Float64).alias(spec.output_column))
    if "estimation_obs_count" not in result.columns:
        result = result.with_columns(pl.lit(None, dtype=pl.UInt32).alias("estimation_obs_count"))
    return result
