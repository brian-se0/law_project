from __future__ import annotations

from dataclasses import dataclass
from datetime import date, time
from pathlib import Path
from typing import Any

import yaml

DEFAULT_PATHS_FILE = Path("configs/paths.yaml")
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
class CaseStudyWindowConfig:
    estimation: tuple[int, int]
    pre_event: tuple[int, int]
    terminal_case: tuple[int, int]
    announcement: tuple[int, int]


@dataclass(frozen=True)
class ExactContractConfig:
    underlying_symbol: str
    expiration: date
    strike: float
    option_type: str
    root: str | None = None

    @property
    def normalized_root(self) -> str:
        return _normalize_symbol(self.root or self.underlying_symbol)

    @property
    def series_id(self) -> str:
        return (
            f"{_normalize_symbol(self.underlying_symbol)}|"
            f"{self.normalized_root}|"
            f"{self.expiration.isoformat()}|"
            f"{float(self.strike)}|"
            f"{_normalize_option_type(self.option_type)}"
        )


@dataclass(frozen=True)
class CaseStudyConfig:
    mode: str
    case_id: str
    source_symbol: str
    source_name: str
    source_role: str
    acquirer_symbol: str | None
    acquirer_name: str | None
    primary_related_symbol: str
    primary_related_name: str
    public_announcement_date: date
    case_private_context_date: date | None
    link_year: int
    horizontal_link_source: str
    vertical_link_source: str
    horizontal_top_k: int
    include_primary_related_symbol_even_if_not_top_k: bool
    exact_contracts: tuple[ExactContractConfig, ...]
    windows: CaseStudyWindowConfig


@dataclass(frozen=True)
class ProjectConfig:
    paths: PathsConfig
    ingest_options: IngestOptionsConfig
    build_underlyings: BuildUnderlyingsConfig
    build_events: BuildEventsConfig
    build_linkages: BuildLinkagesConfig
    market: MarketConfig
    windows: WindowConfig
    case_study: CaseStudyConfig


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
    case_study_payload = research_payload.get("case_study", {})

    default_windows = {
        "estimation": windows_payload.get("estimation", [-120, -20]),
        "pre_event": windows_payload.get("pre_event", [-5, -1]),
        "announcement": windows_payload.get("announcement", [0, 1]),
    }
    case_windows_payload = case_study_payload.get("windows", {})
    case_exact_contract_payload = case_study_payload.get(
        "exact_contracts",
        [
            {
                "underlying_symbol": "INCY",
                "expiration": "2016-09-16",
                "strike": 80.0,
                "option_type": "C",
            },
            {
                "underlying_symbol": "INCY",
                "expiration": "2016-09-16",
                "strike": 82.5,
                "option_type": "C",
            },
            {
                "underlying_symbol": "INCY",
                "expiration": "2016-09-16",
                "strike": 85.0,
                "option_type": "C",
            },
        ],
    )

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
        case_study=CaseStudyConfig(
            mode=str(case_study_payload.get("mode", "mdvn_only")),
            case_id=str(case_study_payload.get("case_id", "mdvn_panuwat_2016")),
            source_symbol=_normalize_symbol(case_study_payload.get("source_symbol", "MDVN")),
            source_name=str(case_study_payload.get("source_name", "Medivation, Inc.")),
            source_role=str(case_study_payload.get("source_role", "target")),
            acquirer_symbol=_normalize_optional_symbol(
                case_study_payload.get("acquirer_symbol", "PFE")
            ),
            acquirer_name=_optional_string(case_study_payload.get("acquirer_name", "Pfizer Inc.")),
            primary_related_symbol=_normalize_symbol(
                case_study_payload.get("primary_related_symbol", "INCY")
            ),
            primary_related_name=str(
                case_study_payload.get("primary_related_name", "Incyte Corporation")
            ),
            public_announcement_date=_parse_date(
                case_study_payload.get("public_announcement_date", "2016-08-22")
            ),
            case_private_context_date=_optional_date(
                case_study_payload.get("case_private_context_date", "2016-08-18")
            ),
            link_year=int(case_study_payload.get("link_year", 2015)),
            horizontal_link_source=str(case_study_payload.get("horizontal_link_source", "TNIC-3")),
            vertical_link_source=str(case_study_payload.get("vertical_link_source", "VTNIC_10")),
            horizontal_top_k=int(case_study_payload.get("horizontal_top_k", 10)),
            include_primary_related_symbol_even_if_not_top_k=bool(
                case_study_payload.get(
                    "include_primary_related_symbol_even_if_not_top_k",
                    True,
                )
            ),
            exact_contracts=tuple(
                ExactContractConfig(
                    underlying_symbol=_normalize_symbol(contract.get("underlying_symbol", "INCY")),
                    expiration=_parse_date(contract.get("expiration", "2016-09-16")),
                    strike=float(contract.get("strike", 0.0)),
                    option_type=_normalize_option_type(contract.get("option_type", "C")),
                    root=_normalize_optional_symbol(contract.get("root")),
                )
                for contract in case_exact_contract_payload
            ),
            windows=CaseStudyWindowConfig(
                estimation=_tuple_of_ints(
                    case_windows_payload.get("estimation", default_windows["estimation"]),
                    "case_study.windows.estimation",
                ),
                pre_event=_tuple_of_ints(
                    case_windows_payload.get("pre_event", default_windows["pre_event"]),
                    "case_study.windows.pre_event",
                ),
                terminal_case=_tuple_of_ints(
                    case_windows_payload.get("terminal_case", [-2, -1]),
                    "case_study.windows.terminal_case",
                ),
                announcement=_tuple_of_ints(
                    case_windows_payload.get(
                        "announcement",
                        default_windows["announcement"],
                    ),
                    "case_study.windows.announcement",
                ),
            ),
        ),
    )


def _resolve_config_path(
    project_root: Path,
    provided: Path | str | None,
    default_relative: Path,
) -> Path:
    if provided is not None:
        candidate = Path(provided)
        return candidate if candidate.is_absolute() else (project_root / candidate).resolve()

    default_path = (project_root / default_relative).resolve()
    if default_path.exists():
        return default_path

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


def _parse_date(value: Any) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _optional_date(value: Any) -> date | None:
    if value in {None, ""}:
        return None
    return _parse_date(value)


def _optional_string(value: Any) -> str | None:
    if value in {None, ""}:
        return None
    return str(value)


def _normalize_symbol(value: Any) -> str:
    return str(value).strip().upper().replace("/", ".").replace("-", ".")


def _normalize_optional_symbol(value: Any) -> str | None:
    if value in {None, ""}:
        return None
    return _normalize_symbol(value)


def _normalize_option_type(value: Any) -> str:
    normalized = str(value).strip().upper()
    if normalized == "CALL":
        return "C"
    if normalized == "PUT":
        return "P"
    return normalized
