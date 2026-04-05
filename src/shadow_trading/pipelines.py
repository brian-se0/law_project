from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from zipfile import ZipFile

import polars as pl

from shadow_trading.config import ProjectConfig
from shadow_trading.io import discover_input_archives, ensure_directories, write_json, write_text
from shadow_trading.case_study import (
    BuildBucketsArtifacts as CaseBuildBucketsArtifacts,
    CaseStudyArtifacts as MdvnCaseStudyArtifacts,
    FreezeCaseArtifacts as MdvnFreezeCaseArtifacts,
    build_case_buckets,
    freeze_case_event,
    run_case_study,
)
from shadow_trading.linkages import (
    build_gvkey_underlying_bridge,
    build_linkage_qc_report,
    build_linkage_tables,
    render_linkage_qc_markdown,
)
from shadow_trading.options_clean import (
    build_aggregate_qc_report,
    build_frame_qc_report,
    normalize_option_frame,
    render_qc_markdown,
)
from shadow_trading.sec_events import (
    build_sec_event_candidates,
    build_sec_event_qc_report,
    build_sec_event_universe,
    render_sec_event_qc_markdown,
)
from shadow_trading.underlyings import (
    REQUIRED_UNDERLYING_SOURCE_COLUMNS,
    add_underlying_raw_returns,
    build_underlying_daily_frame,
    build_underlying_daily_qc_report,
    render_underlying_daily_qc_markdown,
)
from shadow_trading.plots import OutputArtifacts as CaseOutputArtifacts, make_case_study_outputs

ARCHIVE_DATE_PATTERN = re.compile(r"_(\d{4}-\d{2}-\d{2})\.zip$", re.IGNORECASE)


@dataclass(frozen=True)
class IngestRunOptions:
    start_date: date | None = None
    end_date: date | None = None
    limit_files: int | None = None
    overwrite: bool = False


@dataclass(frozen=True)
class IngestArtifacts:
    dataset_output_dir: Path
    qc_json_file: Path
    qc_markdown_file: Path
    processed_file_count: int
    processed_row_count: int
    skipped_existing_outputs: int


@dataclass(frozen=True)
class BuildUnderlyingsRunOptions:
    start_date: date | None = None
    end_date: date | None = None
    overwrite: bool = False


@dataclass(frozen=True)
class BuildUnderlyingsArtifacts:
    output_file: Path
    qc_json_file: Path
    qc_markdown_file: Path
    row_count: int
    source_partition_count: int


@dataclass(frozen=True)
class BuildEventsRunOptions:
    start_date: date | None = None
    end_date: date | None = None
    limit_companies: int | None = None
    symbols: tuple[str, ...] | None = None
    overwrite: bool = False
    refresh_cache: bool = False


@dataclass(frozen=True)
class BuildEventsArtifacts:
    candidates_file: Path
    events_file: Path
    qc_json_file: Path
    qc_markdown_file: Path
    candidate_row_count: int
    event_row_count: int


@dataclass(frozen=True)
class BuildLinkagesRunOptions:
    overwrite: bool = False


@dataclass(frozen=True)
class BuildLinkagesArtifacts:
    bridge_file: Path
    linkages_file: Path
    controls_file: Path
    qc_json_file: Path
    qc_markdown_file: Path
    bridge_row_count: int
    linkage_row_count: int
    control_row_count: int


@dataclass(frozen=True)
class FreezeCaseRunOptions:
    overwrite: bool = False


@dataclass(frozen=True)
class BuildBucketsRunOptions:
    overwrite: bool = False


@dataclass(frozen=True)
class RunCaseStudyRunOptions:
    overwrite: bool = False


@dataclass(frozen=True)
class MakeOutputsRunOptions:
    pass


def run_options_ingest(
    config: ProjectConfig,
    run_options: IngestRunOptions | None = None,
) -> IngestArtifacts:
    options = run_options or IngestRunOptions()
    ensure_directories(config.paths)

    archives = discover_input_archives(config.paths.raw_dir, config.ingest_options.file_glob)
    selected_archives = _filter_archives(
        archives=archives,
        start_date=options.start_date,
        end_date=options.end_date,
        limit_files=options.limit_files,
    )
    if not selected_archives:
        raise FileNotFoundError(
            f"No archives matching {config.ingest_options.file_glob!r} were found in {config.paths.raw_dir}."
        )

    dataset_output_dir = config.paths.processed_dir / config.ingest_options.output_dataset_dir
    dataset_output_dir.mkdir(parents=True, exist_ok=True)

    file_reports: list[dict[str, object]] = []
    processed_archives: list[Path] = []
    skipped_existing_outputs = 0

    for archive_path in selected_archives:
        raw_frame, member_name = _read_archive_csv(archive_path)
        normalized = normalize_option_frame(raw_frame)
        quote_date = str(normalized.get_column("quote_date").min())
        output_file = (
            dataset_output_dir / f"quote_date={quote_date}" / "options_eod_summary.parquet"
        )
        if output_file.exists() and not options.overwrite:
            skipped_existing_outputs += 1
            continue

        output_file.parent.mkdir(parents=True, exist_ok=True)
        normalized.write_parquet(output_file, compression="zstd")

        file_reports.append(
            build_frame_qc_report(normalized, archive_path, member_name, output_file)
        )
        processed_archives.append(archive_path)

    qc_json_file = config.paths.qc_dir / f"{config.ingest_options.qc_report_stem}.json"
    qc_markdown_file = config.paths.qc_dir / f"{config.ingest_options.qc_report_stem}.md"
    if not file_reports:
        return IngestArtifacts(
            dataset_output_dir=dataset_output_dir,
            qc_json_file=qc_json_file,
            qc_markdown_file=qc_markdown_file,
            processed_file_count=0,
            processed_row_count=0,
            skipped_existing_outputs=skipped_existing_outputs,
        )

    aggregate_report = build_aggregate_qc_report(
        file_reports, dataset_output_dir, processed_archives
    )
    write_json(qc_json_file, aggregate_report)
    write_text(qc_markdown_file, render_qc_markdown(aggregate_report))

    return IngestArtifacts(
        dataset_output_dir=dataset_output_dir,
        qc_json_file=qc_json_file,
        qc_markdown_file=qc_markdown_file,
        processed_file_count=len(file_reports),
        processed_row_count=sum(int(report["row_count"]) for report in file_reports),
        skipped_existing_outputs=skipped_existing_outputs,
    )


def run_underlying_daily_build(
    config: ProjectConfig,
    run_options: BuildUnderlyingsRunOptions | None = None,
) -> BuildUnderlyingsArtifacts:
    options = run_options or BuildUnderlyingsRunOptions()
    ensure_directories(config.paths)

    input_dataset_dir = config.paths.processed_dir / config.build_underlyings.input_dataset_dir
    partition_files = _discover_processed_option_partitions(input_dataset_dir)
    selected_partition_files = _filter_processed_partitions(
        partition_files=partition_files,
        start_date=options.start_date,
        end_date=options.end_date,
    )
    if not selected_partition_files:
        raise FileNotFoundError(
            f"No processed options partitions were found in {input_dataset_dir}. "
            "Run the options ingest step first."
        )

    output_file = config.paths.processed_dir / config.build_underlyings.output_file_name
    if output_file.exists() and not options.overwrite:
        raise FileExistsError(f"{output_file} already exists. Re-run with overwrite enabled.")

    daily_frames = [
        build_underlying_daily_frame(
            pl.read_parquet(path, columns=sorted(REQUIRED_UNDERLYING_SOURCE_COLUMNS))
        )
        for path in selected_partition_files
    ]
    combined = add_underlying_raw_returns(pl.concat(daily_frames, how="vertical_relaxed"))

    output_file.parent.mkdir(parents=True, exist_ok=True)
    combined.write_parquet(output_file, compression="zstd")

    qc_json_file = config.paths.qc_dir / f"{config.build_underlyings.qc_report_stem}.json"
    qc_markdown_file = config.paths.qc_dir / f"{config.build_underlyings.qc_report_stem}.md"
    qc_report = build_underlying_daily_qc_report(
        frame=combined,
        source_partition_files=selected_partition_files,
        input_dataset_dir=input_dataset_dir,
        output_path=output_file,
    )
    write_json(qc_json_file, qc_report)
    write_text(qc_markdown_file, render_underlying_daily_qc_markdown(qc_report))

    return BuildUnderlyingsArtifacts(
        output_file=output_file,
        qc_json_file=qc_json_file,
        qc_markdown_file=qc_markdown_file,
        row_count=combined.height,
        source_partition_count=len(selected_partition_files),
    )


def run_sec_event_universe_build(
    config: ProjectConfig,
    run_options: BuildEventsRunOptions | None = None,
) -> BuildEventsArtifacts:
    options = run_options or BuildEventsRunOptions()
    ensure_directories(config.paths)

    underlyings_path = config.paths.processed_dir / config.build_events.input_underlyings_file_name
    if not underlyings_path.exists():
        raise FileNotFoundError(
            f"{underlyings_path} does not exist. Build the underlying-daily table before building events."
        )

    inferred_start_date, inferred_end_date = _infer_date_bounds_from_underlyings(underlyings_path)
    start_date = options.start_date or inferred_start_date
    end_date = options.end_date or inferred_end_date
    cache_root = config.paths.external_dir / config.build_events.cache_dir
    cache_root.mkdir(parents=True, exist_ok=True)

    candidates_file = config.paths.processed_dir / config.build_events.candidates_file_name
    events_file = config.paths.processed_dir / config.build_events.events_file_name
    if (candidates_file.exists() or events_file.exists()) and not options.overwrite:
        raise FileExistsError(
            f"{candidates_file} or {events_file} already exists. Re-run with overwrite enabled."
        )

    candidates, metadata = build_sec_event_candidates(
        underlyings_path=underlyings_path,
        cache_root=cache_root,
        user_agent=config.build_events.user_agent,
        request_spacing_seconds=config.build_events.request_spacing_seconds,
        start_date=start_date,
        end_date=end_date,
        candidate_forms=config.build_events.candidate_forms,
        limit_companies=options.limit_companies,
        symbol_filter=options.symbols,
        refresh_cache=options.refresh_cache,
    )
    events = build_sec_event_universe(candidates)

    candidates.write_parquet(candidates_file, compression="zstd")
    events.write_parquet(events_file, compression="zstd")

    qc_json_file = config.paths.qc_dir / f"{config.build_events.qc_report_stem}.json"
    qc_markdown_file = config.paths.qc_dir / f"{config.build_events.qc_report_stem}.md"
    qc_report = build_sec_event_qc_report(
        candidates=candidates,
        events=events,
        candidates_output_path=candidates_file,
        events_output_path=events_file,
        metadata=metadata,
    )
    write_json(qc_json_file, qc_report)
    write_text(qc_markdown_file, render_sec_event_qc_markdown(qc_report))

    return BuildEventsArtifacts(
        candidates_file=candidates_file,
        events_file=events_file,
        qc_json_file=qc_json_file,
        qc_markdown_file=qc_markdown_file,
        candidate_row_count=candidates.height,
        event_row_count=events.height,
    )


def run_linkage_build(
    config: ProjectConfig,
    run_options: BuildLinkagesRunOptions | None = None,
) -> BuildLinkagesArtifacts:
    options = run_options or BuildLinkagesRunOptions()
    ensure_directories(config.paths)

    underlyings_path = (
        config.paths.processed_dir / config.build_linkages.input_underlyings_file_name
    )
    events_path = config.paths.processed_dir / config.build_linkages.input_events_file_name
    if not underlyings_path.exists():
        raise FileNotFoundError(
            f"{underlyings_path} does not exist. Build the underlying-daily table before building linkages."
        )
    if not events_path.exists():
        raise FileNotFoundError(
            f"{events_path} does not exist. Build the SEC event universe before building linkages."
        )

    linkages_file = config.paths.processed_dir / config.build_linkages.output_file_name
    controls_file = config.paths.processed_dir / config.build_linkages.controls_file_name
    bridge_file = config.paths.processed_dir / config.build_linkages.bridge_output_file_name
    if (linkages_file.exists() or controls_file.exists()) and not options.overwrite:
        raise FileExistsError(
            f"{linkages_file} or {controls_file} already exists. Re-run with overwrite enabled."
        )

    underlyings = pl.read_parquet(underlyings_path, columns=["quote_date", "underlying_symbol"])
    events = pl.read_parquet(
        events_path,
        columns=[
            "event_id",
            "event_trading_date",
            "first_public_disclosure_dt",
            "source_firm_id",
            "source_cik",
            "source_name",
            "source_ticker",
            "source_underlying_symbol",
            "target_cik",
            "target_name",
            "target_ticker",
            "target_underlying_symbol",
            "acquirer_cik",
            "acquirer_name",
            "acquirer_ticker",
            "acquirer_underlying_symbol",
        ],
    )
    bridge, bridge_metadata = build_gvkey_underlying_bridge(
        underlyings=underlyings,
        events=events,
        cache_root=config.paths.external_dir / config.build_events.cache_dir,
        user_agent=config.build_events.user_agent,
        request_spacing_seconds=config.build_events.request_spacing_seconds,
        seed_path=(
            config.paths.external_dir
            / config.build_linkages.raw_linkages_dir
            / config.build_linkages.bridge_seed_file_name
        ),
    )
    bridge.write_parquet(bridge_file, compression="zstd")
    linkages, controls, metadata = build_linkage_tables(
        events=events,
        underlyings=underlyings,
        raw_linkages_dir=config.paths.external_dir / config.build_linkages.raw_linkages_dir,
        gvkey_underlying_bridge=bridge,
    )
    metadata.update(bridge_metadata)
    metadata["bridge_output"] = str(bridge_file)

    linkages.write_parquet(linkages_file, compression="zstd")
    controls.write_parquet(controls_file, compression="zstd")

    qc_json_file = config.paths.qc_dir / f"{config.build_linkages.qc_report_stem}.json"
    qc_markdown_file = config.paths.qc_dir / f"{config.build_linkages.qc_report_stem}.md"
    qc_report = build_linkage_qc_report(
        linkages=linkages,
        controls=controls,
        linkages_output_path=linkages_file,
        controls_output_path=controls_file,
        metadata=metadata,
    )
    write_json(qc_json_file, qc_report)
    write_text(qc_markdown_file, render_linkage_qc_markdown(qc_report))

    return BuildLinkagesArtifacts(
        bridge_file=bridge_file,
        linkages_file=linkages_file,
        controls_file=controls_file,
        qc_json_file=qc_json_file,
        qc_markdown_file=qc_markdown_file,
        bridge_row_count=bridge.height,
        linkage_row_count=linkages.height,
        control_row_count=controls.height,
    )


def run_case_event_freeze(
    config: ProjectConfig,
    run_options: FreezeCaseRunOptions | None = None,
) -> MdvnFreezeCaseArtifacts:
    options = run_options or FreezeCaseRunOptions()
    return freeze_case_event(config, overwrite=options.overwrite)


def run_case_bucket_build(
    config: ProjectConfig,
    run_options: BuildBucketsRunOptions | None = None,
) -> CaseBuildBucketsArtifacts:
    options = run_options or BuildBucketsRunOptions()
    return build_case_buckets(config, overwrite=options.overwrite)


def run_mdvn_case_study(
    config: ProjectConfig,
    run_options: RunCaseStudyRunOptions | None = None,
) -> MdvnCaseStudyArtifacts:
    options = run_options or RunCaseStudyRunOptions()
    return run_case_study(config, overwrite=options.overwrite)


def run_case_output_build(
    config: ProjectConfig,
    run_options: MakeOutputsRunOptions | None = None,
) -> CaseOutputArtifacts:
    _ = run_options or MakeOutputsRunOptions()
    return make_case_study_outputs(config)


def _filter_archives(
    archives: list[Path],
    start_date: date | None,
    end_date: date | None,
    limit_files: int | None,
) -> list[Path]:
    selected: list[Path] = []
    for archive in archives:
        archive_date = _archive_date(archive)
        if start_date is not None and archive_date < start_date:
            continue
        if end_date is not None and archive_date > end_date:
            continue
        selected.append(archive)
    if limit_files is not None:
        return selected[:limit_files]
    return selected


def _archive_date(archive_path: Path) -> date:
    match = ARCHIVE_DATE_PATTERN.search(archive_path.name)
    if not match:
        raise ValueError(f"Archive filename does not contain a quote date: {archive_path.name}")
    return date.fromisoformat(match.group(1))


def _discover_processed_option_partitions(dataset_dir: Path) -> list[Path]:
    if not dataset_dir.exists():
        return []
    return sorted(path.resolve() for path in dataset_dir.rglob("options_eod_summary.parquet"))


def _filter_processed_partitions(
    partition_files: list[Path],
    start_date: date | None,
    end_date: date | None,
) -> list[Path]:
    selected: list[Path] = []
    for partition_file in partition_files:
        partition_date = _processed_partition_date(partition_file)
        if start_date is not None and partition_date < start_date:
            continue
        if end_date is not None and partition_date > end_date:
            continue
        selected.append(partition_file)
    return selected


def _processed_partition_date(partition_file: Path) -> date:
    parent_name = partition_file.parent.name
    if not parent_name.startswith("quote_date="):
        raise ValueError(f"Expected quote_date partition parent, found {partition_file.parent}")
    return date.fromisoformat(parent_name.split("=", maxsplit=1)[1])


def _infer_date_bounds_from_underlyings(underlyings_path: Path) -> tuple[date, date]:
    frame = pl.read_parquet(underlyings_path, columns=["quote_date"])
    min_date = frame.get_column("quote_date").min()
    max_date = frame.get_column("quote_date").max()
    if min_date is None or max_date is None:
        raise ValueError(f"Unable to infer date bounds from {underlyings_path}.")
    return min_date, max_date


def _read_archive_csv(archive_path: Path) -> tuple[pl.DataFrame, str]:
    with ZipFile(archive_path) as archive:
        csv_members = [name for name in archive.namelist() if name.lower().endswith(".csv")]
        if len(csv_members) != 1:
            raise ValueError(
                f"Expected exactly one CSV member in {archive_path.name}, found {len(csv_members)}."
            )
        member_name = csv_members[0]
        with archive.open(member_name) as handle:
            frame = pl.read_csv(
                handle,
                null_values=["", "NULL", "null", "NA", "N/A"],
                infer_schema_length=10_000,
                try_parse_dates=False,
            )
    return frame, member_name
