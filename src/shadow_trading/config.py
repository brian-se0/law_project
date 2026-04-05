from __future__ import annotations

from dataclasses import dataclass
from datetime import time
from pathlib import Path
from typing import Any

import yaml

DEFAULT_PATHS_FILE = Path("configs/paths.yaml")
DEFAULT_PATHS_FALLBACK_FILE = Path("configs/paths.example.yaml")
DEFAULT_RESEARCH_FILE = Path("configs/research_params.yaml")


@dataclass(frozen=True)
class PathsConfig:
    project_root: Path
    raw_dir: Path
    external_dir: Path
    interim_dir: Path
    processed_dir: Path
    outputs_dir: Path
    qc_dir: Path


@dataclass(frozen=True)
class IngestOptionsConfig:
    file_glob: str
    output_dataset_dir: str
    qc_report_stem: str


@dataclass(frozen=True)
class BuildUnderlyingsConfig:
    input_dataset_dir: str
    output_file_name: str
    qc_report_stem: str


@dataclass(frozen=True)
class BuildEventsConfig:
    input_underlyings_file_name: str
    cache_dir: str
    candidates_file_name: str
    events_file_name: str
    qc_report_stem: str
    user_agent: str
    request_spacing_seconds: float
    candidate_forms: tuple[str, ...]


@dataclass(frozen=True)
class BuildLinkagesConfig:
    input_underlyings_file_name: str
    input_events_file_name: str
    raw_linkages_dir: str
    bridge_seed_file_name: str
    bridge_output_file_name: str
    output_file_name: str
    controls_file_name: str
    qc_report_stem: str


@dataclass(frozen=True)
class MarketConfig:
    timezone: str
    regular_open: time
    regular_close: time


@dataclass(frozen=True)
class WindowConfig:
    estimation: tuple[int, int]
    pre_event: tuple[int, int]
    announcement: tuple[int, int]


@dataclass(frozen=True)
class ProjectConfig:
    paths: PathsConfig
    ingest_options: IngestOptionsConfig
    build_underlyings: BuildUnderlyingsConfig
    build_events: BuildEventsConfig
    build_linkages: BuildLinkagesConfig
    market: MarketConfig
    windows: WindowConfig


def load_project_config(
    project_root: Path | str,
    paths_file: Path | str | None = None,
    research_file: Path | str | None = None,
) -> ProjectConfig:
    root = Path(project_root).resolve()
    resolved_paths_file = _resolve_config_path(
        project_root=root,
        provided=paths_file,
        default_relative=DEFAULT_PATHS_FILE,
        fallback_relative=DEFAULT_PATHS_FALLBACK_FILE,
    )
    resolved_research_file = _resolve_config_path(
        project_root=root,
        provided=research_file,
        default_relative=DEFAULT_RESEARCH_FILE,
    )

    paths_payload = _load_yaml(resolved_paths_file).get("paths", {})
    research_payload = _load_yaml(resolved_research_file)

    paths = PathsConfig(
        project_root=root,
        raw_dir=_resolve_project_path(root, paths_payload.get("raw_dir", "data/raw")),
        external_dir=_resolve_project_path(
            root, paths_payload.get("external_dir", "data/external")
        ),
        interim_dir=_resolve_project_path(root, paths_payload.get("interim_dir", "data/interim")),
        processed_dir=_resolve_project_path(
            root,
            paths_payload.get("processed_dir", "data/processed"),
        ),
        outputs_dir=_resolve_project_path(root, paths_payload.get("outputs_dir", "outputs")),
        qc_dir=_resolve_project_path(root, paths_payload.get("qc_dir", "outputs/qc")),
    )

    ingest_payload = research_payload.get("ingest_options", {})
    build_underlyings_payload = research_payload.get("build_underlyings", {})
    build_events_payload = research_payload.get("build_events", {})
    build_linkages_payload = research_payload.get("build_linkages", {})
    market_payload = research_payload.get("market", {})
    windows_payload = research_payload.get("windows", {})

    return ProjectConfig(
        paths=paths,
        ingest_options=IngestOptionsConfig(
            file_glob=str(ingest_payload.get("file_glob", "UnderlyingOptionsEODCalcs_*.zip")),
            output_dataset_dir=str(ingest_payload.get("output_dataset_dir", "options_eod_summary")),
            qc_report_stem=str(ingest_payload.get("qc_report_stem", "options_ingest_qc")),
        ),
        build_underlyings=BuildUnderlyingsConfig(
            input_dataset_dir=str(
                build_underlyings_payload.get("input_dataset_dir", "options_eod_summary")
            ),
            output_file_name=str(
                build_underlyings_payload.get("output_file_name", "underlying_daily.parquet")
            ),
            qc_report_stem=str(
                build_underlyings_payload.get("qc_report_stem", "underlying_daily_qc")
            ),
        ),
        build_events=BuildEventsConfig(
            input_underlyings_file_name=str(
                build_events_payload.get("input_underlyings_file_name", "underlying_daily.parquet")
            ),
            cache_dir=str(build_events_payload.get("cache_dir", "sec")),
            candidates_file_name=str(
                build_events_payload.get("candidates_file_name", "sec_mna_candidates.parquet")
            ),
            events_file_name=str(
                build_events_payload.get("events_file_name", "sec_mna_event_universe.parquet")
            ),
            qc_report_stem=str(
                build_events_payload.get("qc_report_stem", "sec_mna_event_universe_qc")
            ),
            user_agent=str(
                build_events_payload.get(
                    "user_agent",
                    "law_project_research research@example.com",
                )
            ),
            request_spacing_seconds=float(build_events_payload.get("request_spacing_seconds", 0.2)),
            candidate_forms=tuple(
                str(form)
                for form in build_events_payload.get(
                    "candidate_forms",
                    [
                        "8-K",
                        "8-K/A",
                        "DEFA14A",
                        "DEFM14A",
                        "PREM14A",
                        "DEFA14C",
                        "DEFM14C",
                        "PREM14C",
                        "425",
                        "S-4",
                        "S-4/A",
                        "F-4",
                        "F-4/A",
                        "SC TO-T",
                        "SC TO-T/A",
                        "SC TO-C",
                        "14D9",
                        "14D9/A",
                    ],
                )
            ),
        ),
        build_linkages=BuildLinkagesConfig(
            input_underlyings_file_name=str(
                build_linkages_payload.get(
                    "input_underlyings_file_name", "underlying_daily.parquet"
                )
            ),
            input_events_file_name=str(
                build_linkages_payload.get(
                    "input_events_file_name", "sec_mna_event_universe.parquet"
                )
            ),
            raw_linkages_dir=str(build_linkages_payload.get("raw_linkages_dir", "linkages")),
            bridge_seed_file_name=str(
                build_linkages_payload.get("bridge_seed_file_name", "gvkey_ciks_seed.csv")
            ),
            bridge_output_file_name=str(
                build_linkages_payload.get(
                    "bridge_output_file_name", "gvkey_underlying_bridge.parquet"
                )
            ),
            output_file_name=str(
                build_linkages_payload.get("output_file_name", "linkages.parquet")
            ),
            controls_file_name=str(
                build_linkages_payload.get(
                    "controls_file_name", "linkage_control_candidates.parquet"
                )
            ),
            qc_report_stem=str(build_linkages_payload.get("qc_report_stem", "linkages_qc")),
        ),
        market=MarketConfig(
            timezone=str(market_payload.get("timezone", "America/New_York")),
            regular_open=_parse_time(market_payload.get("regular_open", "09:30")),
            regular_close=_parse_time(market_payload.get("regular_close", "16:00")),
        ),
        windows=WindowConfig(
            estimation=_tuple_of_ints(windows_payload.get("estimation", [-120, -20]), "estimation"),
            pre_event=_tuple_of_ints(windows_payload.get("pre_event", [-5, -1]), "pre_event"),
            announcement=_tuple_of_ints(
                windows_payload.get("announcement", [0, 1]),
                "announcement",
            ),
        ),
    )


def _resolve_config_path(
    project_root: Path,
    provided: Path | str | None,
    default_relative: Path,
    fallback_relative: Path | None = None,
) -> Path:
    if provided is not None:
        candidate = Path(provided)
        return candidate if candidate.is_absolute() else (project_root / candidate).resolve()

    default_path = (project_root / default_relative).resolve()
    if default_path.exists():
        return default_path

    if fallback_relative is not None:
        fallback_path = (project_root / fallback_relative).resolve()
        if fallback_path.exists():
            return fallback_path

    raise FileNotFoundError(f"Missing configuration file: {default_path}")


def _resolve_project_path(project_root: Path, raw_path: str | Path) -> Path:
    path = Path(raw_path)
    return path if path.is_absolute() else (project_root / path).resolve()


def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, dict):
        raise TypeError(f"Expected a mapping in config file: {path}")
    return payload


def _parse_time(value: str | time) -> time:
    if isinstance(value, time):
        return value
    return time.fromisoformat(str(value))


def _tuple_of_ints(value: Any, field_name: str) -> tuple[int, int]:
    if not isinstance(value, list | tuple) or len(value) != 2:
        raise ValueError(f"{field_name} must be a 2-item list or tuple.")
    return int(value[0]), int(value[1])
