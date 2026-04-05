from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import polars as pl

from shadow_trading.abnormal import (
    compute_bucket_abnormal_metrics,
    compute_exact_contract_abnormal_metrics,
)
from shadow_trading.buckets import (
    CaseWindowDates,
    build_bucket_features,
    build_case_window_dates,
    build_exact_contract_features,
    enrich_case_option_rows,
    extract_case_option_slice,
    render_bucket_qc_markdown,
    summarize_bucket_build,
)
from shadow_trading.config import ProjectConfig
from shadow_trading.io import ensure_directories, write_json, write_text

HORIZONTAL_LINK_TYPE = "horizontal_tnic"
VERTICAL_LINK_TYPE = "vertical_vtnic"
PRIMARY_RELATED_LINK_TYPE = "primary_related_case"


@dataclass(frozen=True)
class CaseStudyPaths:
    case_dir: Path
    case_event_file: Path
    related_firms_file: Path
    exact_contracts_file: Path
    bucket_features_file: Path
    abnormal_metrics_file: Path
    control_matches_file: Path
    case_event_qc_json_file: Path
    case_event_qc_markdown_file: Path
    bucket_qc_json_file: Path
    bucket_qc_markdown_file: Path
    case_qc_json_file: Path
    case_qc_markdown_file: Path


@dataclass(frozen=True)
class FreezeCaseArtifacts:
    case_event_file: Path
    qc_json_file: Path
    qc_markdown_file: Path
    row_count: int


@dataclass(frozen=True)
class BuildBucketsArtifacts:
    related_firms_file: Path
    exact_contracts_file: Path
    bucket_features_file: Path
    qc_json_file: Path
    qc_markdown_file: Path
    related_firm_row_count: int
    exact_contract_row_count: int
    bucket_row_count: int


@dataclass(frozen=True)
class CaseStudyArtifacts:
    related_firms_file: Path
    exact_contracts_file: Path
    bucket_features_file: Path
    abnormal_metrics_file: Path
    control_matches_file: Path
    qc_json_file: Path
    qc_markdown_file: Path
    abnormal_metric_row_count: int
    control_match_row_count: int


def build_case_study_paths(config: ProjectConfig) -> CaseStudyPaths:
    case_stem = config.case_study.source_symbol.lower()
    case_dir = config.paths.processed_dir / "case_studies"
    return CaseStudyPaths(
        case_dir=case_dir,
        case_event_file=case_dir / f"{case_stem}_case_event.parquet",
        related_firms_file=case_dir / f"{case_stem}_related_firms.parquet",
        exact_contracts_file=case_dir / f"{case_stem}_exact_contracts.parquet",
        bucket_features_file=case_dir / f"{case_stem}_bucket_features.parquet",
        abnormal_metrics_file=case_dir / f"{case_stem}_abnormal_metrics.parquet",
        control_matches_file=case_dir / f"{case_stem}_control_matches.parquet",
        case_event_qc_json_file=config.paths.qc_dir / f"{case_stem}_case_event_qc.json",
        case_event_qc_markdown_file=config.paths.qc_dir / f"{case_stem}_case_event_qc.md",
        bucket_qc_json_file=config.paths.qc_dir / f"{case_stem}_bucket_qc.json",
        bucket_qc_markdown_file=config.paths.qc_dir / f"{case_stem}_bucket_qc.md",
        case_qc_json_file=config.paths.qc_dir / f"{case_stem}_case_qc.json",
        case_qc_markdown_file=config.paths.qc_dir / f"{case_stem}_case_qc.md",
    )


def freeze_case_event(
    config: ProjectConfig,
    *,
    overwrite: bool = False,
) -> FreezeCaseArtifacts:
    ensure_directories(config.paths)
    paths = build_case_study_paths(config)
    if paths.case_event_file.exists() and not overwrite:
        raise FileExistsError(
            f"{paths.case_event_file} already exists. Re-run with overwrite enabled."
        )

    events_path = config.paths.processed_dir / config.build_events.events_file_name
    if not events_path.exists():
        raise FileNotFoundError(
            f"{events_path} does not exist. Build the SEC event universe before freezing the case."
        )

    events = pl.read_parquet(events_path)
    source_symbol = config.case_study.source_symbol
    public_date = config.case_study.public_announcement_date.isoformat()
    filtered = (
        events.with_columns(
            [
                pl.coalesce(
                    [
                        pl.col("source_underlying_symbol"),
                        pl.col("source_ticker"),
                        pl.col("source_firm_id"),
                    ]
                )
                .cast(pl.String)
                .str.to_uppercase()
                .alias("__source_symbol"),
                pl.coalesce(
                    [
                        pl.col("acquirer_underlying_symbol"),
                        pl.col("acquirer_ticker"),
                    ]
                )
                .cast(pl.String)
                .str.to_uppercase()
                .alias("__acquirer_symbol"),
                pl.col("first_public_disclosure_dt")
                .cast(pl.String)
                .str.slice(0, 10)
                .alias("__disclosure_date"),
            ]
        )
        .filter(pl.col("__source_symbol") == source_symbol)
        .filter(
            (pl.col("event_trading_date") == public_date)
            | (pl.col("__disclosure_date") == public_date)
        )
    )
    base_match_count = filtered.height
    if config.case_study.acquirer_symbol:
        acquirer_filtered = filtered.filter(
            pl.col("__acquirer_symbol") == config.case_study.acquirer_symbol
        )
        if acquirer_filtered.height:
            filtered = acquirer_filtered

    if filtered.height == 0:
        raise ValueError(
            "No SEC event row matched the configured MDVN case. Rebuild the event universe with a "
            f"2016 window that includes {config.case_study.source_symbol} before freezing the case."
        )
    if filtered.height > 1:
        event_ids = ", ".join(filtered.get_column("event_id").to_list())
        raise ValueError(
            "Freezing the case would be ambiguous because multiple SEC event rows matched the case "
            f"filters: {event_ids}"
        )

    selected = filtered.drop(
        [
            column
            for column in ["__source_symbol", "__acquirer_symbol", "__disclosure_date"]
            if column in filtered.columns
        ]
    )
    selected_row = selected.row(0, named=True)
    event_trading_date = date.fromisoformat(str(selected_row["event_trading_date"]))
    target_gvkey = _resolve_case_target_gvkey(config, event_trading_date=event_trading_date)
    frozen_event = pl.DataFrame(
        [
            {
                "case_id": config.case_study.case_id,
                "event_id": selected_row["event_id"],
                "source_firm_id": selected_row["source_firm_id"],
                "source_symbol": config.case_study.source_symbol,
                "source_name": selected_row.get("source_name") or config.case_study.source_name,
                "target_cik": selected_row.get("target_cik") or selected_row.get("source_cik"),
                "target_gvkey": target_gvkey,
                "acquirer_symbol": selected_row.get("acquirer_underlying_symbol")
                or selected_row.get("acquirer_ticker")
                or config.case_study.acquirer_symbol,
                "acquirer_cik": selected_row.get("acquirer_cik"),
                "first_public_disclosure_dt": selected_row["first_public_disclosure_dt"],
                "event_trading_date": event_trading_date,
                "case_private_context_date": config.case_study.case_private_context_date,
                "review_status": "frozen",
                "review_note": (
                    "Frozen from the generic SEC event universe using the configured source symbol, "
                    "public announcement date, and acquirer filter when available."
                ),
                "evidence_source": selected_row.get("announcement_filing_url"),
                "announcement_form": selected_row.get("announcement_form"),
                "announcement_accession_number": selected_row.get("announcement_accession_number"),
                "candidate_filing_count": selected_row.get("candidate_filing_count"),
                "candidate_forms": selected_row.get("candidate_forms"),
                "requires_manual_review": selected_row.get("requires_manual_review"),
            }
        ]
    )
    paths.case_dir.mkdir(parents=True, exist_ok=True)
    frozen_event.write_parquet(paths.case_event_file, compression="zstd")

    qc_report = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "case_id": config.case_study.case_id,
        "source_symbol": config.case_study.source_symbol,
        "public_announcement_date": public_date,
        "base_match_count": base_match_count,
        "selected_event_id": selected_row["event_id"],
        "selected_event_trading_date": event_trading_date.isoformat(),
        "selected_first_public_disclosure_dt": selected_row["first_public_disclosure_dt"],
        "selected_announcement_form": selected_row.get("announcement_form"),
        "selected_announcement_accession_number": selected_row.get("announcement_accession_number"),
        "selected_evidence_source": selected_row.get("announcement_filing_url"),
        "target_gvkey": target_gvkey,
        "case_event_output": str(paths.case_event_file),
        "provenance_note": (
            "Frozen from the generic SEC event universe rather than hand-entered. The case-event "
            "record preserves the SEC-derived announcement timestamp, trading-date alignment, and "
            "review metadata needed for the MDVN-only case-study pipeline."
        ),
    }
    write_json(paths.case_event_qc_json_file, qc_report)
    write_text(paths.case_event_qc_markdown_file, render_case_event_qc_markdown(qc_report))
    return FreezeCaseArtifacts(
        case_event_file=paths.case_event_file,
        qc_json_file=paths.case_event_qc_json_file,
        qc_markdown_file=paths.case_event_qc_markdown_file,
        row_count=frozen_event.height,
    )


def load_frozen_case_event(config: ProjectConfig) -> pl.DataFrame:
    paths = build_case_study_paths(config)
    if not paths.case_event_file.exists():
        raise FileNotFoundError(
            f"{paths.case_event_file} does not exist. Run scripts/freeze_mdvn_case_event.py first."
        )
    return pl.read_parquet(paths.case_event_file)


def build_case_buckets(
    config: ProjectConfig,
    *,
    overwrite: bool = False,
) -> BuildBucketsArtifacts:
    ensure_directories(config.paths)
    paths = build_case_study_paths(config)
    outputs = [paths.related_firms_file, paths.exact_contracts_file, paths.bucket_features_file]
    if any(path.exists() for path in outputs) and not overwrite:
        raise FileExistsError(
            "One or more case-study bucket outputs already exist. Re-run with overwrite enabled."
        )

    frozen_event = load_frozen_case_event(config)
    event_row = frozen_event.row(0, named=True)
    event_trading_date = _coerce_date(event_row["event_trading_date"])
    linkages_path = config.paths.processed_dir / config.build_linkages.output_file_name
    if not linkages_path.exists():
        raise FileNotFoundError(
            f"{linkages_path} does not exist. Build lagged linkages before building case buckets."
        )
    linkages = pl.read_parquet(linkages_path)
    related_firms = build_related_firms(
        config=config,
        linkages=linkages,
        event_trading_date=event_trading_date,
    )

    window_dates = build_case_window_dates(
        underlyings_path=config.paths.processed_dir / config.build_underlyings.output_file_name,
        event_trading_date=event_trading_date,
        windows=config.case_study.windows,
    )
    related_symbols = sorted(
        {
            config.case_study.source_symbol,
            config.case_study.primary_related_symbol,
            *[
                str(symbol)
                for symbol in related_firms.get_column("linked_firm_id").drop_nulls().to_list()
            ],
        }
    )
    option_rows = extract_case_option_slice(
        options_dataset_dir=config.paths.processed_dir / config.ingest_options.output_dataset_dir,
        symbols=related_symbols,
        quote_dates=window_dates.extraction_dates,
    )
    enriched_rows = enrich_case_option_rows(
        options_frame=option_rows,
        window_dates=window_dates,
        exact_contracts=config.case_study.exact_contracts,
        primary_related_symbol=config.case_study.primary_related_symbol,
    )
    exact_contracts = compute_exact_contract_abnormal_metrics(
        build_exact_contract_features(enriched_rows),
        estimation_window=config.case_study.windows.estimation,
    )
    bucket_features = compute_bucket_abnormal_metrics(
        build_bucket_features(enriched_rows),
        estimation_window=config.case_study.windows.estimation,
    )

    paths.case_dir.mkdir(parents=True, exist_ok=True)
    related_firms.write_parquet(paths.related_firms_file, compression="zstd")
    exact_contracts.write_parquet(paths.exact_contracts_file, compression="zstd")
    bucket_features.write_parquet(paths.bucket_features_file, compression="zstd")

    qc_report = summarize_bucket_build(
        option_rows=enriched_rows,
        exact_contracts=exact_contracts,
        bucket_features=bucket_features,
        related_symbols=related_symbols,
        expected_exact_contracts=config.case_study.exact_contracts,
        window_dates=window_dates,
    )
    qc_report.update(
        {
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "case_id": config.case_study.case_id,
            "event_trading_date": event_trading_date.isoformat(),
            "related_firm_row_count": related_firms.height,
            "related_firms_output": str(paths.related_firms_file),
            "exact_contracts_output": str(paths.exact_contracts_file),
            "bucket_features_output": str(paths.bucket_features_file),
        }
    )
    write_json(paths.bucket_qc_json_file, qc_report)
    write_text(paths.bucket_qc_markdown_file, render_bucket_qc_markdown(qc_report))
    return BuildBucketsArtifacts(
        related_firms_file=paths.related_firms_file,
        exact_contracts_file=paths.exact_contracts_file,
        bucket_features_file=paths.bucket_features_file,
        qc_json_file=paths.bucket_qc_json_file,
        qc_markdown_file=paths.bucket_qc_markdown_file,
        related_firm_row_count=related_firms.height,
        exact_contract_row_count=exact_contracts.height,
        bucket_row_count=bucket_features.height,
    )


def run_case_study(
    config: ProjectConfig,
    *,
    overwrite: bool = False,
) -> CaseStudyArtifacts:
    ensure_directories(config.paths)
    paths = build_case_study_paths(config)
    outputs = [paths.abnormal_metrics_file, paths.control_matches_file]
    if any(path.exists() for path in outputs) and not overwrite:
        raise FileExistsError(
            "One or more case-study outputs already exist. Re-run with overwrite enabled."
        )

    if (
        not paths.related_firms_file.exists()
        or not paths.exact_contracts_file.exists()
        or not paths.bucket_features_file.exists()
    ):
        build_case_buckets(config, overwrite=overwrite)

    frozen_event = load_frozen_case_event(config)
    related_firms = pl.read_parquet(paths.related_firms_file)
    exact_contracts = pl.read_parquet(paths.exact_contracts_file)
    bucket_features = pl.read_parquet(paths.bucket_features_file)
    controls_candidates = _load_controls_candidates(config)
    event_row = frozen_event.row(0, named=True)
    event_trading_date = _coerce_date(event_row["event_trading_date"])
    window_dates = build_case_window_dates(
        underlyings_path=config.paths.processed_dir / config.build_underlyings.output_file_name,
        event_trading_date=event_trading_date,
        windows=config.case_study.windows,
    )
    underlyings = pl.read_parquet(
        config.paths.processed_dir / config.build_underlyings.output_file_name
    )

    control_matches = select_primary_related_controls(
        config=config,
        controls_candidates=controls_candidates,
        underlyings=underlyings,
        bucket_features=bucket_features,
        window_dates=window_dates,
    )
    control_matches.write_parquet(paths.control_matches_file, compression="zstd")

    abnormal_metrics = build_case_abnormal_summary(
        config=config,
        related_firms=related_firms,
        control_matches=control_matches,
        bucket_features=bucket_features,
        underlyings=underlyings,
        window_dates=window_dates,
    )
    abnormal_metrics.write_parquet(paths.abnormal_metrics_file, compression="zstd")

    expected_series_ids = {contract.series_id for contract in config.case_study.exact_contracts}
    observed_series_ids = (
        set(exact_contracts.get_column("series_id").unique().to_list())
        if exact_contracts.height
        else set()
    )
    qc_report = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "case_id": config.case_study.case_id,
        "event_trading_date": event_trading_date.isoformat(),
        "related_firm_row_count": related_firms.height,
        "exact_contract_row_count": exact_contracts.height,
        "bucket_row_count": bucket_features.height,
        "abnormal_metric_row_count": abnormal_metrics.height,
        "control_match_row_count": control_matches.height,
        "primary_related_symbol": config.case_study.primary_related_symbol,
        "primary_related_in_linkages": bool(
            related_firms.filter(
                pl.col("linked_firm_id") == config.case_study.primary_related_symbol
            ).height
        ),
        "missing_exact_series_ids": sorted(expected_series_ids - observed_series_ids),
        "related_firms_output": str(paths.related_firms_file),
        "exact_contracts_output": str(paths.exact_contracts_file),
        "bucket_features_output": str(paths.bucket_features_file),
        "abnormal_metrics_output": str(paths.abnormal_metrics_file),
        "control_matches_output": str(paths.control_matches_file),
        "provenance_note": (
            "The case-study summary keeps component bucket and exact-series measures primary, uses "
            "lagged ex ante linkage context, and constructs matched non-linked controls for the "
            "primary related symbol from the pre-event options and return history."
        ),
    }
    write_json(paths.case_qc_json_file, qc_report)
    write_text(paths.case_qc_markdown_file, render_case_qc_markdown(qc_report))
    return CaseStudyArtifacts(
        related_firms_file=paths.related_firms_file,
        exact_contracts_file=paths.exact_contracts_file,
        bucket_features_file=paths.bucket_features_file,
        abnormal_metrics_file=paths.abnormal_metrics_file,
        control_matches_file=paths.control_matches_file,
        qc_json_file=paths.case_qc_json_file,
        qc_markdown_file=paths.case_qc_markdown_file,
        abnormal_metric_row_count=abnormal_metrics.height,
        control_match_row_count=control_matches.height,
    )


def build_related_firms(
    *,
    config: ProjectConfig,
    linkages: pl.DataFrame,
    event_trading_date: date,
) -> pl.DataFrame:
    case = config.case_study
    relevant = linkages.filter(
        (pl.col("source_firm_id") == case.source_symbol) & (pl.col("link_year") == case.link_year)
    )
    horizontal = relevant.filter(
        (pl.col("link_type") == HORIZONTAL_LINK_TYPE)
        & (pl.col("link_rank") <= case.horizontal_top_k)
    )
    vertical = relevant.filter(pl.col("link_type") == VERTICAL_LINK_TYPE)
    frames = [frame for frame in [horizontal, vertical] if frame.height]
    retained = pl.concat(frames, how="vertical_relaxed") if frames else _empty_related_firm_frame()

    primary_from_all = relevant.filter(pl.col("linked_firm_id") == case.primary_related_symbol)
    if case.include_primary_related_symbol_even_if_not_top_k:
        primary_in_retained = retained.filter(
            pl.col("linked_firm_id") == case.primary_related_symbol
        ).height
        if primary_in_retained == 0:
            if primary_from_all.height:
                retained = pl.concat([retained, primary_from_all], how="vertical_relaxed")
            else:
                retained = pl.concat(
                    [
                        retained,
                        pl.DataFrame(
                            [
                                {
                                    "source_firm_id": case.source_symbol,
                                    "linked_firm_id": case.primary_related_symbol,
                                    "link_type": PRIMARY_RELATED_LINK_TYPE,
                                    "link_year": case.link_year,
                                    "link_score": None,
                                    "source_ticker": case.source_symbol,
                                    "linked_ticker": case.primary_related_symbol,
                                    "source_gvkey": None,
                                    "linked_gvkey": None,
                                    "source_name": case.source_name,
                                    "linked_name": case.primary_related_name,
                                    "link_rank": None,
                                }
                            ]
                        ),
                    ],
                    how="vertical_relaxed",
                )

    rank_denominators = (
        retained.filter(pl.col("link_type").is_in([HORIZONTAL_LINK_TYPE, VERTICAL_LINK_TYPE]))
        .group_by("link_type")
        .agg(pl.len().alias("__link_count"))
    )
    retained = retained.join(rank_denominators, on="link_type", how="left").with_columns(
        [
            pl.lit(case.case_id).alias("case_id"),
            pl.lit(case.source_symbol).alias("source_symbol"),
            pl.lit(event_trading_date.year).alias("event_year"),
            (pl.col("linked_firm_id") == case.primary_related_symbol).alias(
                "primary_related_pair_flag"
            ),
            pl.col("link_rank").alias("linked_rank_within_source"),
            pl.when(pl.col("__link_count") > 1)
            .then(1 - ((pl.col("link_rank") - 1) / (pl.col("__link_count") - 1)))
            .when(pl.col("__link_count") == 1)
            .then(pl.lit(1.0))
            .otherwise(None)
            .alias("linked_percentile_within_source"),
        ]
    )
    return retained.drop("__link_count").sort(
        ["primary_related_pair_flag", "link_type", "linked_rank_within_source", "linked_firm_id"],
        descending=[True, False, False, False],
    )


def select_primary_related_controls(
    *,
    config: ProjectConfig,
    controls_candidates: pl.DataFrame,
    underlyings: pl.DataFrame,
    bucket_features: pl.DataFrame,
    window_dates: CaseWindowDates,
) -> pl.DataFrame:
    if controls_candidates.height == 0:
        return _empty_control_matches_frame()

    candidates = controls_candidates.filter(
        (pl.col("source_firm_id") == config.case_study.source_symbol)
        & (pl.col("link_year") == config.case_study.link_year)
        & (pl.col("link_type") == HORIZONTAL_LINK_TYPE)
    )
    if candidates.height == 0:
        return _empty_control_matches_frame()

    symbols = [
        config.case_study.primary_related_symbol,
        *candidates.get_column("control_firm_id").to_list(),
    ]
    estimation_dates = list(window_dates.estimation_dates)
    return_features = (
        underlyings.filter(pl.col("quote_date").is_in(estimation_dates))
        .filter(pl.col("underlying_symbol").is_in(symbols))
        .group_by("underlying_symbol")
        .agg(
            [
                pl.col("raw_return").drop_nulls().std(ddof=1).alias("estimation_return_std"),
                pl.col("raw_return").drop_nulls().abs().mean().alias("estimation_abs_return_mean"),
                pl.col("quote_date").n_unique().alias("estimation_observed_days"),
            ]
        )
    )
    daily_volume = (
        bucket_features.group_by(["underlying_symbol", "quote_date"])
        .agg(pl.col("volume_bucket").sum().alias("daily_total_volume"))
        .filter(pl.col("quote_date").is_in(estimation_dates))
        .group_by("underlying_symbol")
        .agg(pl.col("daily_total_volume").mean().alias("estimation_mean_daily_option_volume"))
    )
    features = return_features.join(daily_volume, on="underlying_symbol", how="left")
    target = features.filter(
        pl.col("underlying_symbol") == config.case_study.primary_related_symbol
    )
    if target.height == 0:
        return _empty_control_matches_frame()

    target_row = target.row(0, named=True)
    control_features = candidates.join(
        features.rename({"underlying_symbol": "control_firm_id"}),
        on="control_firm_id",
        how="inner",
    ).drop_nulls(
        subset=[
            "estimation_return_std",
            "estimation_abs_return_mean",
            "estimation_observed_days",
        ]
    )
    if control_features.height == 0:
        return _empty_control_matches_frame()

    metric_names = [
        "estimation_return_std",
        "estimation_abs_return_mean",
        "estimation_observed_days",
        "estimation_mean_daily_option_volume",
    ]
    scales = _metric_scales(control_features, metric_names)
    distance_expr = None
    for metric_name in metric_names:
        target_value = target_row.get(metric_name)
        if target_value is None:
            continue
        component = (pl.col(metric_name) - pl.lit(float(target_value))).abs() / pl.lit(
            scales.get(metric_name, 1.0)
        )
        distance_expr = component if distance_expr is None else distance_expr + component

    if distance_expr is None:
        return _empty_control_matches_frame()

    selected = control_features.with_columns(distance_expr.alias("match_distance")).sort(
        "match_distance"
    )
    selected = selected.head(config.case_study.horizontal_top_k).with_columns(
        [
            pl.lit(config.case_study.case_id).alias("case_id"),
            pl.lit(config.case_study.source_symbol).alias("source_symbol"),
            pl.lit(config.case_study.primary_related_symbol).alias("primary_related_symbol"),
            pl.int_range(1, pl.len() + 1).alias("match_rank"),
        ]
    )
    return selected.select(
        [
            "case_id",
            "source_symbol",
            "primary_related_symbol",
            "control_firm_id",
            "match_rank",
            "match_distance",
            "estimation_return_std",
            "estimation_abs_return_mean",
            "estimation_observed_days",
            "estimation_mean_daily_option_volume",
        ]
    )


def build_case_abnormal_summary(
    *,
    config: ProjectConfig,
    related_firms: pl.DataFrame,
    control_matches: pl.DataFrame,
    bucket_features: pl.DataFrame,
    underlyings: pl.DataFrame,
    window_dates: CaseWindowDates,
) -> pl.DataFrame:
    bucket_summary = summarize_focal_bucket_activity(bucket_features)
    return_summary = summarize_announcement_returns(
        underlyings=underlyings,
        symbols=sorted(
            {
                config.case_study.source_symbol,
                config.case_study.primary_related_symbol,
                *related_firms.get_column("linked_firm_id").drop_nulls().to_list(),
                *control_matches.get_column("control_firm_id").drop_nulls().to_list(),
            }
        ),
        announcement_dates=window_dates.announcement_dates,
    )
    source_return = return_summary.filter(
        pl.col("underlying_symbol") == config.case_study.source_symbol
    ).select("return_0_1")
    source_return_value = (
        source_return.row(0, named=True)["return_0_1"] if source_return.height else None
    )

    linked_summary = (
        related_firms.rename({"linked_firm_id": "underlying_symbol"})
        .join(bucket_summary, on="underlying_symbol", how="left")
        .join(return_summary, on="underlying_symbol", how="left")
        .with_columns(
            [
                pl.lit("linked_firm").alias("comparison_role"),
                pl.lit(source_return_value).alias("source_return_0_1"),
                pl.lit(None, dtype=pl.UInt32).alias("match_rank"),
                pl.lit(None, dtype=pl.Float64).alias("match_distance"),
            ]
        )
    )
    controls_summary = (
        control_matches.rename({"control_firm_id": "underlying_symbol"})
        .join(bucket_summary, on="underlying_symbol", how="left")
        .join(return_summary, on="underlying_symbol", how="left")
        .with_columns(
            [
                pl.lit("matched_control").alias("comparison_role"),
                pl.lit(config.case_study.case_id).alias("case_id"),
                pl.lit(config.case_study.source_symbol).alias("source_symbol"),
                pl.lit(False).alias("primary_related_pair_flag"),
                pl.lit(None, dtype=pl.String).alias("link_type"),
                pl.lit(None, dtype=pl.Float64).alias("link_score"),
                pl.lit(None, dtype=pl.UInt32).alias("linked_rank_within_source"),
                pl.lit(None, dtype=pl.Float64).alias("linked_percentile_within_source"),
                pl.lit(source_return_value).alias("source_return_0_1"),
            ]
        )
    )
    source_summary = (
        bucket_summary.filter(pl.col("underlying_symbol") == config.case_study.source_symbol)
        .join(return_summary, on="underlying_symbol", how="left")
        .with_columns(
            [
                pl.lit(config.case_study.case_id).alias("case_id"),
                pl.lit(config.case_study.source_symbol).alias("source_symbol"),
                pl.lit("source_benchmark").alias("comparison_role"),
                pl.lit(False).alias("primary_related_pair_flag"),
                pl.lit(None, dtype=pl.String).alias("link_type"),
                pl.lit(None, dtype=pl.Float64).alias("link_score"),
                pl.lit(None, dtype=pl.UInt32).alias("linked_rank_within_source"),
                pl.lit(None, dtype=pl.Float64).alias("linked_percentile_within_source"),
                pl.lit(None, dtype=pl.UInt32).alias("match_rank"),
                pl.lit(None, dtype=pl.Float64).alias("match_distance"),
                pl.lit(source_return_value).alias("source_return_0_1"),
            ]
        )
    )
    linked_summary = _align_case_summary_frame(linked_summary)
    controls_summary = _align_case_summary_frame(controls_summary)
    source_summary = _align_case_summary_frame(source_summary)
    combined = pl.concat(
        [frame for frame in [linked_summary, controls_summary, source_summary] if frame.height],
        how="vertical_relaxed",
    )
    return combined.sort(
        [
            "comparison_role",
            "primary_related_pair_flag",
            "match_rank",
            "linked_rank_within_source",
            "underlying_symbol",
        ],
        descending=[False, True, False, False, False],
    )


def summarize_focal_bucket_activity(bucket_features: pl.DataFrame) -> pl.DataFrame:
    focal = bucket_features.filter(
        (pl.col("option_type") == "C")
        & (pl.col("moneyness_bucket") == "call_otm")
        & pl.col("tenor_bucket").is_in(["0_7", "8_30"])
    )
    if focal.height == 0:
        return pl.DataFrame(schema=_abnormal_summary_schema())

    return (
        focal.group_by("underlying_symbol")
        .agg(
            [
                _window_mean_expr("case_pre_event_window_flag", "z_volume").alias(
                    "pre_event_short_dated_otm_call_z_volume_mean"
                ),
                _window_mean_expr("case_pre_event_window_flag", "z_premium").alias(
                    "pre_event_short_dated_otm_call_z_premium_mean"
                ),
                _window_mean_expr("case_pre_event_window_flag", "z_delta_notional").alias(
                    "pre_event_short_dated_otm_call_z_delta_notional_mean"
                ),
                _window_mean_expr("case_pre_event_window_flag", "z_lead_oi").alias(
                    "pre_event_short_dated_otm_call_z_lead_oi_mean"
                ),
                _window_mean_expr("case_terminal_window_flag", "z_volume").alias(
                    "terminal_case_short_dated_otm_call_z_volume_mean"
                ),
                _window_mean_expr("case_terminal_window_flag", "z_premium").alias(
                    "terminal_case_short_dated_otm_call_z_premium_mean"
                ),
                _window_mean_expr("announcement_window_flag", "z_volume").alias(
                    "announcement_short_dated_otm_call_z_volume_mean"
                ),
                _window_mean_expr("announcement_window_flag", "z_premium").alias(
                    "announcement_short_dated_otm_call_z_premium_mean"
                ),
                pl.col("quote_date").n_unique().alias("focal_bucket_observed_days"),
            ]
        )
        .sort("underlying_symbol")
    )


def summarize_announcement_returns(
    *,
    underlyings: pl.DataFrame,
    symbols: list[str],
    announcement_dates: tuple[date, ...],
) -> pl.DataFrame:
    if not announcement_dates:
        return _empty_return_summary_frame()

    filtered = underlyings.filter(pl.col("underlying_symbol").is_in(symbols)).filter(
        pl.col("quote_date").is_in(list(announcement_dates))
    )
    if filtered.height == 0:
        return _empty_return_summary_frame()

    day0 = announcement_dates[0]
    day1 = announcement_dates[-1]
    return (
        filtered.group_by("underlying_symbol")
        .agg(
            [
                pl.col("raw_return")
                .filter(pl.col("quote_date") == day0)
                .drop_nulls()
                .first()
                .alias("return_0"),
                (
                    (
                        pl.col("raw_return")
                        .filter(pl.col("quote_date") == day0)
                        .drop_nulls()
                        .first()
                        .fill_null(0)
                        + 1
                    )
                    * (
                        pl.col("raw_return")
                        .filter(pl.col("quote_date") == day1)
                        .drop_nulls()
                        .first()
                        .fill_null(0)
                        + 1
                    )
                    - 1
                ).alias("return_0_1"),
            ]
        )
        .sort("underlying_symbol")
    )


def render_case_event_qc_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# MDVN Case Event QC",
        "",
        f"- Generated at (UTC): {report['generated_at_utc']}",
        f"- Case ID: {report['case_id']}",
        f"- Source symbol: {report['source_symbol']}",
        f"- Public announcement date: {report['public_announcement_date']}",
        f"- Base match count: {report['base_match_count']:,}",
        f"- Selected event ID: {report['selected_event_id']}",
        f"- Selected event trading date: {report['selected_event_trading_date']}",
        f"- Selected first public disclosure: {report['selected_first_public_disclosure_dt']}",
        f"- Announcement form: {report['selected_announcement_form']}",
        f"- Announcement accession: {report['selected_announcement_accession_number']}",
        f"- Target gvkey: {report['target_gvkey']}",
        f"- Evidence source: {report['selected_evidence_source']}",
        f"- Frozen case event: `{report['case_event_output']}`",
        "",
        "## Provenance",
        "",
        report["provenance_note"],
        "",
    ]
    return "\n".join(lines)


def render_case_qc_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# MDVN Case Study QC",
        "",
        f"- Generated at (UTC): {report['generated_at_utc']}",
        f"- Case ID: {report['case_id']}",
        f"- Event trading date: {report['event_trading_date']}",
        f"- Related-firm rows: {report['related_firm_row_count']:,}",
        f"- Exact-contract rows: {report['exact_contract_row_count']:,}",
        f"- Bucket rows: {report['bucket_row_count']:,}",
        f"- Abnormal-summary rows: {report['abnormal_metric_row_count']:,}",
        f"- Control matches: {report['control_match_row_count']:,}",
        f"- Primary related symbol: {report['primary_related_symbol']}",
        f"- Primary related in linkages: {report['primary_related_in_linkages']}",
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


def _resolve_case_target_gvkey(
    config: ProjectConfig,
    *,
    event_trading_date: date,
) -> str | None:
    bridge_path = config.paths.processed_dir / config.build_linkages.bridge_output_file_name
    if not bridge_path.exists():
        return None
    bridge = pl.read_parquet(bridge_path)
    matches = bridge.filter(
        (pl.col("firm_id") == config.case_study.source_symbol)
        & (pl.col("event_year") == event_trading_date.year)
    ).select("gvkey")
    return matches.row(0, named=True)["gvkey"] if matches.height == 1 else None


def _load_controls_candidates(config: ProjectConfig) -> pl.DataFrame:
    path = config.paths.processed_dir / config.build_linkages.controls_file_name
    if not path.exists():
        return _empty_control_candidates_frame()
    return pl.read_parquet(path)


def _metric_scales(frame: pl.DataFrame, metric_names: list[str]) -> dict[str, float]:
    scales: dict[str, float] = {}
    for metric_name in metric_names:
        if metric_name not in frame.columns:
            continue
        value = frame.select(pl.col(metric_name).std(ddof=1).alias("scale")).row(0, named=True)[
            "scale"
        ]
        scales[metric_name] = float(value) if value not in {None, 0.0} else 1.0
    return scales


def _window_mean_expr(flag_column: str, value_column: str) -> pl.Expr:
    return pl.col(value_column).filter(pl.col(flag_column)).drop_nulls().mean()


def _coerce_date(value: Any) -> date:
    return value if isinstance(value, date) else date.fromisoformat(str(value))


def _empty_related_firm_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "source_firm_id": pl.String,
            "linked_firm_id": pl.String,
            "link_type": pl.String,
            "link_year": pl.Int64,
            "link_score": pl.Float64,
            "source_ticker": pl.String,
            "linked_ticker": pl.String,
            "source_gvkey": pl.String,
            "linked_gvkey": pl.String,
            "source_name": pl.String,
            "linked_name": pl.String,
            "link_rank": pl.UInt32,
            "case_id": pl.String,
            "source_symbol": pl.String,
            "event_year": pl.Int64,
            "primary_related_pair_flag": pl.Boolean,
            "linked_rank_within_source": pl.UInt32,
            "linked_percentile_within_source": pl.Float64,
        }
    )


def _empty_control_candidates_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "source_firm_id": pl.String,
            "event_year": pl.Int64,
            "link_year": pl.Int64,
            "link_type": pl.String,
            "control_firm_id": pl.String,
        }
    )


def _empty_control_matches_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "case_id": pl.String,
            "source_symbol": pl.String,
            "primary_related_symbol": pl.String,
            "control_firm_id": pl.String,
            "match_rank": pl.Int64,
            "match_distance": pl.Float64,
            "estimation_return_std": pl.Float64,
            "estimation_abs_return_mean": pl.Float64,
            "estimation_observed_days": pl.UInt32,
            "estimation_mean_daily_option_volume": pl.Float64,
        }
    )


def _empty_return_summary_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "underlying_symbol": pl.String,
            "return_0": pl.Float64,
            "return_0_1": pl.Float64,
        }
    )


def _abnormal_summary_schema() -> dict[str, pl.DataType]:
    return {
        "underlying_symbol": pl.String,
        "pre_event_short_dated_otm_call_z_volume_mean": pl.Float64,
        "pre_event_short_dated_otm_call_z_premium_mean": pl.Float64,
        "pre_event_short_dated_otm_call_z_delta_notional_mean": pl.Float64,
        "pre_event_short_dated_otm_call_z_lead_oi_mean": pl.Float64,
        "terminal_case_short_dated_otm_call_z_volume_mean": pl.Float64,
        "terminal_case_short_dated_otm_call_z_premium_mean": pl.Float64,
        "announcement_short_dated_otm_call_z_volume_mean": pl.Float64,
        "announcement_short_dated_otm_call_z_premium_mean": pl.Float64,
        "focal_bucket_observed_days": pl.UInt32,
    }


def _case_summary_schema() -> dict[str, pl.DataType]:
    return {
        "case_id": pl.String,
        "source_symbol": pl.String,
        "comparison_role": pl.String,
        "underlying_symbol": pl.String,
        "primary_related_pair_flag": pl.Boolean,
        "link_type": pl.String,
        "link_score": pl.Float64,
        "linked_rank_within_source": pl.UInt32,
        "linked_percentile_within_source": pl.Float64,
        "match_rank": pl.UInt32,
        "match_distance": pl.Float64,
        "pre_event_short_dated_otm_call_z_volume_mean": pl.Float64,
        "pre_event_short_dated_otm_call_z_premium_mean": pl.Float64,
        "pre_event_short_dated_otm_call_z_delta_notional_mean": pl.Float64,
        "pre_event_short_dated_otm_call_z_lead_oi_mean": pl.Float64,
        "terminal_case_short_dated_otm_call_z_volume_mean": pl.Float64,
        "terminal_case_short_dated_otm_call_z_premium_mean": pl.Float64,
        "announcement_short_dated_otm_call_z_volume_mean": pl.Float64,
        "announcement_short_dated_otm_call_z_premium_mean": pl.Float64,
        "focal_bucket_observed_days": pl.UInt32,
        "return_0": pl.Float64,
        "return_0_1": pl.Float64,
        "source_return_0_1": pl.Float64,
    }


def _align_case_summary_frame(frame: pl.DataFrame) -> pl.DataFrame:
    schema = _case_summary_schema()
    missing_columns = [column for column in schema if column not in frame.columns]
    if missing_columns:
        frame = frame.with_columns(
            [pl.lit(None, dtype=schema[column]).alias(column) for column in missing_columns]
        )
    return frame.select(
        [pl.col(column).cast(schema[column], strict=False).alias(column) for column in schema]
    )
