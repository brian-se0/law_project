from __future__ import annotations

import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

import polars as pl

from shadow_trading.sec_events import SecClient, fetch_company_ticker_frame

SUPPORTED_LINKAGE_EXTENSIONS = {".csv", ".tsv", ".txt", ".parquet"}
GVKEY_CIK_SEED_URL = "https://iangow.r-universe.dev/farr/data/gvkey_ciks/csv"

LINKAGE_COLUMN_ALIASES = {
    "link_year": ["year", "link_year", "fyear", "calyear"],
    "source_ticker": ["ticker1", "tic1", "source_ticker", "firm1_ticker", "focal_ticker"],
    "linked_ticker": ["ticker2", "tic2", "linked_ticker", "firm2_ticker", "peer_ticker"],
    "source_name": ["name1", "firm1_name", "source_name"],
    "linked_name": ["name2", "firm2_name", "linked_name"],
    "source_gvkey": ["gvkey1", "source_gvkey", "firm1_gvkey"],
    "linked_gvkey": ["gvkey2", "linked_gvkey", "firm2_gvkey"],
    "link_score": [
        "score",
        "link_score",
        "similarity",
        "weight",
        "tnic3",
        "tnic_score",
        "vtnic_score",
        "vertscore",
    ],
}


def build_gvkey_underlying_bridge(
    *,
    underlyings: pl.DataFrame,
    events: pl.DataFrame,
    cache_root: Path,
    user_agent: str,
    request_spacing_seconds: float,
    seed_path: Path,
) -> tuple[pl.DataFrame, dict[str, Any]]:
    if events.height == 0:
        return _empty_gvkey_underlying_bridge_frame(), {
            "bridge_seed_file": str(seed_path),
            "bridge_seed_url": GVKEY_CIK_SEED_URL,
            "bridge_row_count": 0,
            "bridge_method_counts": {},
            "bridge_issuer_source_counts": {},
        }

    event_years = (
        events.select(pl.col("event_trading_date").str.to_date().dt.year().alias("event_year"))
        .drop_nulls()
        .unique()
        .sort("event_year")
    )
    option_symbol_years = _build_option_symbol_years(
        underlyings=underlyings,
        event_years=event_years.get_column("event_year").to_list(),
    )
    if option_symbol_years.height == 0:
        return _empty_gvkey_underlying_bridge_frame(), {
            "bridge_seed_file": str(seed_path),
            "bridge_seed_url": GVKEY_CIK_SEED_URL,
            "bridge_row_count": 0,
            "bridge_method_counts": {},
            "bridge_issuer_source_counts": {},
        }

    client = SecClient(
        user_agent=user_agent,
        request_spacing_seconds=request_spacing_seconds,
        cache_root=cache_root,
        refresh_cache=False,
    )
    current_issuer_evidence = (
        fetch_company_ticker_frame(client)
        .select(
            [
                _normalize_cik_expr("cik").alias("cik"),
                pl.col("ticker").cast(pl.String).alias("issuer_ticker"),
                pl.col("name").cast(pl.String).alias("issuer_name"),
                pl.col("normalized_symbol").cast(pl.String).alias("firm_id"),
                pl.lit(None, dtype=pl.String).alias("underlying_symbol"),
                pl.lit("current_sec_company_tickers").alias("issuer_source"),
                pl.lit(None, dtype=pl.Date).alias("evidence_date"),
                pl.lit(None, dtype=pl.String).alias("evidence_event_id"),
            ]
        )
        .filter(pl.col("cik").is_not_null() & pl.col("firm_id").is_not_null())
    )
    event_issuer_evidence = _build_event_issuer_evidence(events)
    issuer_evidence = pl.concat(
        [current_issuer_evidence, event_issuer_evidence],
        how="vertical_relaxed",
    ).unique(subset=["cik", "firm_id", "issuer_source", "evidence_date", "evidence_event_id"])

    gvkey_seed = _load_gvkey_cik_seed(seed_path=seed_path, user_agent=user_agent)
    issuer_candidates = (
        option_symbol_years.join(
            issuer_evidence.select(
                [
                    "cik",
                    "firm_id",
                    "issuer_ticker",
                    "issuer_name",
                    "issuer_source",
                    "evidence_date",
                    "evidence_event_id",
                ]
            ),
            on="firm_id",
            how="left",
        )
        .filter(pl.col("cik").is_not_null())
        .with_columns(_issuer_priority_expr("issuer_source").alias("issuer_priority"))
    )
    preferred_issuer_candidates = issuer_candidates.join(
        issuer_candidates.group_by(["underlying_symbol", "event_year"]).agg(
            pl.col("issuer_priority").min().alias("best_issuer_priority")
        ),
        on=["underlying_symbol", "event_year"],
        how="inner",
    ).filter(pl.col("issuer_priority") == pl.col("best_issuer_priority"))
    candidate_bridge = (
        preferred_issuer_candidates.drop("best_issuer_priority")
        .join(gvkey_seed, on="cik", how="inner")
        .filter(
            (pl.col("observed_start_date") <= pl.col("seed_last_date"))
            & (pl.col("observed_end_date") >= pl.col("seed_first_date"))
        )
    )

    if candidate_bridge.height == 0:
        return _empty_gvkey_underlying_bridge_frame(), {
            "bridge_seed_file": str(seed_path),
            "bridge_seed_url": GVKEY_CIK_SEED_URL,
            "bridge_seed_row_count": gvkey_seed.height,
            "bridge_option_symbol_year_count": option_symbol_years.height,
            "bridge_candidate_row_count": 0,
            "bridge_row_count": 0,
            "bridge_method_counts": {},
            "bridge_issuer_source_counts": _count_rows_by_type(
                event_issuer_evidence, "issuer_source"
            ),
        }

    symbol_year_counts = candidate_bridge.group_by(["underlying_symbol", "event_year"]).agg(
        [
            pl.col("gvkey").n_unique().alias("gvkey_candidate_count"),
            pl.col("cik").n_unique().alias("cik_candidate_count"),
        ]
    )
    accepted = candidate_bridge.join(
        symbol_year_counts,
        on=["underlying_symbol", "event_year"],
        how="inner",
    ).filter((pl.col("gvkey_candidate_count") == 1) & (pl.col("cik_candidate_count") == 1))

    gvkey_year_counts = accepted.group_by(["gvkey", "event_year"]).agg(
        pl.col("firm_id").n_unique().alias("firm_id_candidate_count")
    )
    accepted = accepted.join(
        gvkey_year_counts,
        on=["gvkey", "event_year"],
        how="inner",
    ).filter(pl.col("firm_id_candidate_count") == 1)

    if accepted.height == 0:
        return _empty_gvkey_underlying_bridge_frame(), {
            "bridge_seed_file": str(seed_path),
            "bridge_seed_url": GVKEY_CIK_SEED_URL,
            "bridge_seed_row_count": gvkey_seed.height,
            "bridge_option_symbol_year_count": option_symbol_years.height,
            "bridge_candidate_row_count": candidate_bridge.height,
            "bridge_row_count": 0,
            "bridge_method_counts": {},
            "bridge_issuer_source_counts": _count_rows_by_type(issuer_evidence, "issuer_source"),
        }

    bridge = (
        accepted.group_by(["gvkey", "iid", "cik", "event_year", "underlying_symbol", "firm_id"])
        .agg(
            [
                pl.col("observed_start_date").min().alias("observed_start_date"),
                pl.col("observed_end_date").max().alias("observed_end_date"),
                pl.col("option_obs_count").max().alias("option_obs_count"),
                pl.col("seed_first_date").min().alias("seed_first_date"),
                pl.col("seed_last_date").max().alias("seed_last_date"),
                pl.col("issuer_ticker").drop_nulls().first().alias("issuer_ticker"),
                pl.col("issuer_name").drop_nulls().first().alias("issuer_name"),
                _concat_unique_values_expr("issuer_source").alias("issuer_sources"),
                _concat_unique_values_expr("evidence_event_id").alias("evidence_event_ids"),
                pl.col("issuer_source")
                .cast(pl.String)
                .str.starts_with("sec_event_")
                .any()
                .alias("has_event_evidence"),
            ]
        )
        .with_columns(
            [
                pl.when(pl.col("has_event_evidence"))
                .then(pl.lit("sec_event_evidence_plus_gvkey_cik_seed"))
                .otherwise(pl.lit("current_sec_ticker_plus_gvkey_cik_seed"))
                .alias("bridge_method"),
                pl.lit("high").alias("bridge_confidence"),
            ]
        )
        .drop("has_event_evidence")
        .sort(["event_year", "underlying_symbol", "gvkey", "iid"])
    )

    metadata = {
        "bridge_seed_file": str(seed_path),
        "bridge_seed_url": GVKEY_CIK_SEED_URL,
        "bridge_seed_row_count": gvkey_seed.height,
        "bridge_option_symbol_year_count": option_symbol_years.height,
        "bridge_candidate_row_count": candidate_bridge.height,
        "bridge_row_count": bridge.height,
        "bridge_accepted_symbol_year_count": (
            bridge.select(["underlying_symbol", "event_year"]).unique().height
        ),
        "bridge_ambiguous_symbol_year_count": symbol_year_counts.filter(
            (pl.col("gvkey_candidate_count") != 1) | (pl.col("cik_candidate_count") != 1)
        ).height,
        "bridge_method_counts": _count_rows_by_type(bridge, "bridge_method"),
        "bridge_issuer_source_counts": _count_rows_by_type(issuer_evidence, "issuer_source"),
    }
    return bridge, metadata


def build_linkage_tables(
    *,
    events: pl.DataFrame,
    underlyings: pl.DataFrame,
    raw_linkages_dir: Path,
    gvkey_underlying_bridge: pl.DataFrame | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame, dict[str, Any]]:
    linkage_files = discover_linkage_files(raw_linkages_dir)
    if not linkage_files:
        raise FileNotFoundError(
            f"No TNIC/VTNIC files were found in {raw_linkages_dir}. "
            "Place raw linkage files there before running the build."
        )

    source_event_years = (
        events.select(
            [
                "source_firm_id",
                pl.col("event_trading_date").str.to_date().dt.year().alias("event_year"),
            ]
        )
        .with_columns((pl.col("event_year") - 1).alias("link_year"))
        .unique()
    )
    event_years = source_event_years.select("event_year").unique()
    option_firm_years = (
        underlyings.with_columns(
            [
                _normalize_symbol_expr("underlying_symbol").alias("firm_id"),
                pl.col("quote_date").dt.year().alias("event_year"),
            ]
        )
        .join(event_years, on="event_year", how="inner")
        .select(["firm_id", "event_year"])
        .unique()
    )

    source_event_gvkeys = _build_source_event_gvkeys(
        source_event_years=source_event_years,
        gvkey_underlying_bridge=gvkey_underlying_bridge,
    )
    link_years = set(source_event_years.get_column("link_year").to_list())
    source_gvkeys = (
        set(source_event_gvkeys.get_column("source_gvkey").to_list())
        if source_event_gvkeys.height
        else set()
    )

    linkage_frames: list[pl.DataFrame] = []
    for link_type, path in linkage_files.items():
        standardized = standardize_linkage_file(
            path=path,
            link_type=link_type,
            link_years=link_years,
            source_gvkeys=source_gvkeys,
        )
        if standardized.height == 0:
            continue

        symmetrized = _symmetrize_linkage_pairs(standardized)
        resolved_frames: list[pl.DataFrame] = []

        ticker_rows = symmetrized.filter(
            pl.col("source_ticker").is_not_null() & pl.col("linked_ticker").is_not_null()
        )
        if ticker_rows.height:
            resolved_frames.append(
                _resolve_ticker_linkages(
                    frame=ticker_rows,
                    source_event_years=source_event_years,
                    option_firm_years=option_firm_years,
                )
            )

        gvkey_rows = symmetrized.filter(
            pl.col("source_gvkey").is_not_null() & pl.col("linked_gvkey").is_not_null()
        )
        if gvkey_rows.height and gvkey_underlying_bridge is not None:
            resolved_frames.append(
                _resolve_gvkey_linkages(
                    frame=gvkey_rows,
                    source_event_gvkeys=source_event_gvkeys,
                    option_firm_years=option_firm_years,
                    gvkey_underlying_bridge=gvkey_underlying_bridge,
                )
            )

        resolved_frames = [frame for frame in resolved_frames if frame.height]
        if resolved_frames:
            linkage_frames.append(pl.concat(resolved_frames, how="vertical_relaxed"))

    if not linkage_frames:
        linkages = _empty_linkage_frame()
    else:
        linkages = (
            pl.concat(linkage_frames, how="vertical_relaxed")
            .with_columns(
                pl.col("link_score")
                .rank(method="ordinal", descending=True)
                .over(["source_firm_id", "link_type", "link_year"])
                .alias("link_rank")
            )
            .sort(["source_firm_id", "link_type", "link_year", "link_rank"])
        )

    controls = build_control_candidates(
        source_event_years=source_event_years,
        option_firm_years=option_firm_years,
        linkages=linkages,
    )
    metadata = {
        "raw_files": {link_type: str(path) for link_type, path in linkage_files.items()},
        "source_event_year_count": source_event_years.height,
        "option_firm_year_count": option_firm_years.height,
        "source_event_gvkey_count": source_event_gvkeys.height,
        "bridge_row_count": (
            gvkey_underlying_bridge.height if gvkey_underlying_bridge is not None else 0
        ),
    }
    return linkages, controls, metadata


def discover_linkage_files(raw_linkages_dir: Path) -> dict[str, Path]:
    if not raw_linkages_dir.exists():
        return {}

    matches: dict[str, list[Path]] = {"horizontal_tnic": [], "vertical_vtnic": []}
    for path in sorted(raw_linkages_dir.rglob("*")):
        if not path.is_file() or path.suffix.lower() not in SUPPORTED_LINKAGE_EXTENSIONS:
            continue
        name = path.name.lower()
        if "readme" in name:
            continue
        if "vtnic" in name or "vertnetwork" in name:
            matches["vertical_vtnic"].append(path.resolve())
        elif "tnic" in name:
            matches["horizontal_tnic"].append(path.resolve())

    discovered: dict[str, Path] = {}
    for link_type, paths in matches.items():
        if not paths:
            continue
        if len(paths) > 1:
            raise ValueError(
                f"Expected one {link_type} file in {raw_linkages_dir}, found {len(paths)}."
            )
        discovered[link_type] = paths[0]
    return discovered


def standardize_linkage_file(
    *,
    path: Path,
    link_type: str,
    link_years: set[int] | None = None,
    source_gvkeys: set[str] | None = None,
) -> pl.DataFrame:
    frame = _scan_tabular_file(path)
    columns = frame.collect_schema().names()
    frame = frame.rename({column: _canonicalize_column(column) for column in columns})
    normalized_columns = frame.collect_schema().names()

    selected: dict[str, str] = {}
    for canonical, aliases in LINKAGE_COLUMN_ALIASES.items():
        column = _first_present(normalized_columns, aliases)
        if column is not None:
            selected[canonical] = column

    if "link_year" not in selected or "link_score" not in selected:
        raise ValueError(f"{path} is missing a year or score column after normalization.")

    has_tickers = "source_ticker" in selected and "linked_ticker" in selected
    has_gvkeys = "source_gvkey" in selected and "linked_gvkey" in selected
    if not has_tickers and not has_gvkeys:
        raise ValueError(
            f"{path} must expose either ticker columns (ticker1/ticker2) or gvkey columns "
            "(gvkey1/gvkey2)."
        )

    if link_years:
        frame = frame.filter(pl.col(selected["link_year"]).cast(pl.Int64).is_in(sorted(link_years)))
    if has_gvkeys and source_gvkeys is not None:
        frame = frame.filter(
            _normalize_gvkey_expr(selected["source_gvkey"]).is_in(sorted(source_gvkeys))
            | _normalize_gvkey_expr(selected["linked_gvkey"]).is_in(sorted(source_gvkeys))
        )

    expressions = [
        pl.col(selected["link_year"]).cast(pl.Int64).alias("link_year"),
        pl.col(selected["link_score"]).cast(pl.Float64).alias("link_score"),
        (
            pl.col(selected["source_ticker"]).cast(pl.String).alias("source_ticker")
            if "source_ticker" in selected
            else pl.lit(None, dtype=pl.String).alias("source_ticker")
        ),
        (
            pl.col(selected["linked_ticker"]).cast(pl.String).alias("linked_ticker")
            if "linked_ticker" in selected
            else pl.lit(None, dtype=pl.String).alias("linked_ticker")
        ),
        (
            _normalize_gvkey_expr(selected["source_gvkey"]).alias("source_gvkey")
            if "source_gvkey" in selected
            else pl.lit(None, dtype=pl.String).alias("source_gvkey")
        ),
        (
            _normalize_gvkey_expr(selected["linked_gvkey"]).alias("linked_gvkey")
            if "linked_gvkey" in selected
            else pl.lit(None, dtype=pl.String).alias("linked_gvkey")
        ),
    ]
    for optional in ("source_name", "linked_name"):
        if optional in selected:
            expressions.append(pl.col(selected[optional]).cast(pl.String).alias(optional))
        else:
            expressions.append(pl.lit(None, dtype=pl.String).alias(optional))

    standardized = (
        frame.select(expressions)
        .with_columns(pl.lit(link_type).alias("link_type"))
        .filter(
            (pl.col("source_ticker").is_not_null() & pl.col("linked_ticker").is_not_null())
            | (pl.col("source_gvkey").is_not_null() & pl.col("linked_gvkey").is_not_null())
        )
        .collect()
    )
    if "source_ticker" in standardized.columns:
        standardized = standardized.with_columns(
            [
                pl.when(pl.col("source_ticker").is_not_null())
                .then(_normalize_symbol_expr("source_ticker"))
                .otherwise(pl.lit(None, dtype=pl.String))
                .alias("source_ticker"),
                pl.when(pl.col("linked_ticker").is_not_null())
                .then(_normalize_symbol_expr("linked_ticker"))
                .otherwise(pl.lit(None, dtype=pl.String))
                .alias("linked_ticker"),
            ]
        )
    return standardized


def build_control_candidates(
    *,
    source_event_years: pl.DataFrame,
    option_firm_years: pl.DataFrame,
    linkages: pl.DataFrame,
) -> pl.DataFrame:
    if linkages.height == 0:
        return _empty_control_frame()

    link_types = linkages.select("link_type").unique()
    linked_source_years = (
        source_event_years.join(
            linkages.select(
                ["source_firm_id", "linked_firm_id", "link_type", "link_year"]
            ).unique(),
            on=["source_firm_id", "link_year"],
            how="inner",
        )
        .select(["source_firm_id", "event_year", "link_year", "link_type", "linked_firm_id"])
        .unique()
    )
    universe = (
        source_event_years.join(link_types, how="cross")
        .join(
            option_firm_years.rename({"firm_id": "control_firm_id"}), on="event_year", how="inner"
        )
        .filter(pl.col("source_firm_id") != pl.col("control_firm_id"))
        .join(
            linked_source_years.rename({"linked_firm_id": "control_firm_id"}),
            on=["source_firm_id", "event_year", "link_year", "link_type", "control_firm_id"],
            how="anti",
        )
        .sort(["source_firm_id", "link_type", "event_year", "control_firm_id"])
    )
    return universe


def build_linkage_qc_report(
    *,
    linkages: pl.DataFrame,
    controls: pl.DataFrame,
    linkages_output_path: Path,
    controls_output_path: Path,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    type_counts = (
        linkages.group_by("link_type").len().sort("link_type").iter_rows(named=True)
        if linkages.height
        else []
    )
    return {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "raw_files": metadata["raw_files"],
        "source_event_year_count": metadata["source_event_year_count"],
        "option_firm_year_count": metadata["option_firm_year_count"],
        "source_event_gvkey_count": metadata.get("source_event_gvkey_count", 0),
        "gvkey_underlying_bridge_row_count": metadata.get("bridge_row_count", 0),
        "gvkey_underlying_bridge_output": metadata.get("bridge_output"),
        "gvkey_underlying_bridge_seed_file": metadata.get("bridge_seed_file"),
        "linkage_row_count": linkages.height,
        "control_candidate_count": controls.height,
        "linkages_output": str(linkages_output_path),
        "controls_output": str(controls_output_path),
        "link_type_counts": {row["link_type"]: int(row["len"]) for row in type_counts},
        "bridge_method_counts": metadata.get("bridge_method_counts", {}),
        "provenance_note": (
            "Built from raw TNIC/VTNIC-style yearly linkage files, frozen to event year minus one, "
            "restricted to firms that appear in the options universe, and resolved from gvkey-pair files "
            "through a project-scoped dated bridge that combines the open gvkey_ciks seed with SEC issuer "
            "evidence. Vertical raw files are converted into an unsigned watchlist relation by symmetrizing "
            "pairs and retaining the stronger score when both directions are present."
        ),
    }


def render_linkage_qc_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Linkage Build QC",
        "",
        f"- Generated at (UTC): {report['generated_at_utc']}",
        f"- Source event-years: {report['source_event_year_count']:,}",
        f"- Option firm-years: {report['option_firm_year_count']:,}",
        f"- Source event gvkey rows: {report['source_event_gvkey_count']:,}",
        f"- GVKEY bridge rows: {report['gvkey_underlying_bridge_row_count']:,}",
        f"- Linkage rows: {report['linkage_row_count']:,}",
        f"- Control candidates: {report['control_candidate_count']:,}",
        f"- Linkage table: `{report['linkages_output']}`",
        f"- Control table: `{report['controls_output']}`",
    ]
    if report.get("gvkey_underlying_bridge_output"):
        lines.append(f"- GVKEY bridge table: `{report['gvkey_underlying_bridge_output']}`")
    if report.get("gvkey_underlying_bridge_seed_file"):
        lines.append(f"- GVKEY bridge seed file: `{report['gvkey_underlying_bridge_seed_file']}`")
    lines.extend(["", "## Raw Files", ""])
    for link_type, path in report["raw_files"].items():
        lines.append(f"- {link_type}: `{path}`")
    lines.extend(["", "## Link Types", ""])
    for link_type, count in report["link_type_counts"].items():
        lines.append(f"- {link_type}: {count:,}")
    if report["bridge_method_counts"]:
        lines.extend(["", "## Bridge Methods", ""])
        for bridge_method, count in report["bridge_method_counts"].items():
            lines.append(f"- {bridge_method}: {count:,}")
    lines.extend(["", "## Provenance", "", report["provenance_note"], ""])
    return "\n".join(lines)


def _build_option_symbol_years(
    *,
    underlyings: pl.DataFrame,
    event_years: list[int],
) -> pl.DataFrame:
    return (
        underlyings.with_columns(
            [
                _normalize_symbol_expr("underlying_symbol").alias("firm_id"),
                pl.col("quote_date").dt.year().alias("event_year"),
            ]
        )
        .filter(pl.col("event_year").is_in(event_years))
        .group_by(["underlying_symbol", "firm_id", "event_year"])
        .agg(
            [
                pl.col("quote_date").min().alias("observed_start_date"),
                pl.col("quote_date").max().alias("observed_end_date"),
                pl.len().alias("option_obs_count"),
            ]
        )
        .sort(["event_year", "underlying_symbol"])
    )


def _build_event_issuer_evidence(events: pl.DataFrame) -> pl.DataFrame:
    disclosure_date = pl.col("first_public_disclosure_dt").str.slice(0, 10).str.to_date()
    role_specs = (
        ("source", "source_firm_id"),
        ("target", "target_underlying_symbol"),
        ("acquirer", "acquirer_underlying_symbol"),
    )
    frames: list[pl.DataFrame] = []
    for role, preferred_symbol_column in role_specs:
        frame = (
            events.select(
                [
                    _normalize_cik_expr(f"{role}_cik").alias("cik"),
                    pl.col(f"{role}_ticker").cast(pl.String).alias("issuer_ticker"),
                    pl.col(f"{role}_name").cast(pl.String).alias("issuer_name"),
                    pl.coalesce(
                        [
                            pl.col(preferred_symbol_column),
                            pl.col(f"{role}_ticker"),
                        ]
                    )
                    .cast(pl.String)
                    .alias("underlying_symbol"),
                    pl.lit(f"sec_event_{role}").alias("issuer_source"),
                    disclosure_date.alias("evidence_date"),
                    pl.col("event_id").cast(pl.String).alias("evidence_event_id"),
                ]
            )
            .filter(pl.col("cik").is_not_null() & pl.col("underlying_symbol").is_not_null())
            .with_columns(_normalize_symbol_expr("underlying_symbol").alias("firm_id"))
            .select(
                [
                    "cik",
                    "issuer_ticker",
                    "issuer_name",
                    "firm_id",
                    "underlying_symbol",
                    "issuer_source",
                    "evidence_date",
                    "evidence_event_id",
                ]
            )
        )
        if frame.height:
            frames.append(frame)

    if not frames:
        return _empty_issuer_evidence_frame()
    return pl.concat(frames, how="vertical_relaxed").unique(
        subset=["cik", "firm_id", "issuer_source", "evidence_date", "evidence_event_id"]
    )


def _load_gvkey_cik_seed(*, seed_path: Path, user_agent: str) -> pl.DataFrame:
    if not seed_path.exists():
        seed_path.parent.mkdir(parents=True, exist_ok=True)
        request = Request(GVKEY_CIK_SEED_URL, headers={"User-Agent": user_agent})
        with urlopen(request, timeout=120) as response:
            seed_path.write_text(response.read().decode("utf-8"), encoding="utf-8")

    return (
        pl.read_csv(seed_path, null_values=["", "NA", "NULL"])
        .select(
            [
                _normalize_gvkey_expr("gvkey").alias("gvkey"),
                pl.col("iid").cast(pl.String).alias("iid"),
                _normalize_cik_expr("cik").alias("cik"),
                pl.col("first_date").str.to_date().alias("seed_first_date"),
                pl.col("last_date").str.to_date().alias("seed_last_date"),
            ]
        )
        .filter(pl.col("gvkey").is_not_null() & pl.col("cik").is_not_null())
    )


def _resolve_ticker_linkages(
    *,
    frame: pl.DataFrame,
    source_event_years: pl.DataFrame,
    option_firm_years: pl.DataFrame,
) -> pl.DataFrame:
    return (
        frame.with_columns(
            [
                _normalize_symbol_expr("source_ticker").alias("source_firm_id"),
                _normalize_symbol_expr("linked_ticker").alias("linked_firm_id"),
            ]
        )
        .join(source_event_years, on=["source_firm_id", "link_year"], how="inner")
        .join(
            option_firm_years.rename({"firm_id": "linked_firm_id"}),
            on=["linked_firm_id", "event_year"],
            how="inner",
        )
        .filter(pl.col("source_firm_id") != pl.col("linked_firm_id"))
        .select(
            [
                "source_firm_id",
                "linked_firm_id",
                "link_type",
                "link_year",
                "link_score",
                "source_ticker",
                "linked_ticker",
                "source_gvkey",
                "linked_gvkey",
                "source_name",
                "linked_name",
            ]
        )
        .group_by(["source_firm_id", "linked_firm_id", "link_type", "link_year"])
        .agg(
            [
                pl.col("link_score").max().alias("link_score"),
                pl.col("source_ticker").drop_nulls().first(),
                pl.col("linked_ticker").drop_nulls().first(),
                pl.col("source_gvkey").drop_nulls().first(),
                pl.col("linked_gvkey").drop_nulls().first(),
                pl.col("source_name").drop_nulls().first(),
                pl.col("linked_name").drop_nulls().first(),
            ]
        )
    )


def _resolve_gvkey_linkages(
    *,
    frame: pl.DataFrame,
    source_event_gvkeys: pl.DataFrame,
    option_firm_years: pl.DataFrame,
    gvkey_underlying_bridge: pl.DataFrame,
) -> pl.DataFrame:
    if source_event_gvkeys.height == 0 or gvkey_underlying_bridge.height == 0:
        return _empty_linkage_frame().drop("link_rank")

    linked_bridge = gvkey_underlying_bridge.select(
        [
            "event_year",
            pl.col("gvkey").alias("linked_gvkey"),
            pl.col("firm_id").alias("linked_firm_id"),
            pl.col("underlying_symbol").alias("linked_ticker"),
            pl.col("issuer_name").alias("linked_name"),
        ]
    ).unique()
    return (
        frame.join(source_event_gvkeys, on=["source_gvkey", "link_year"], how="inner")
        .join(linked_bridge, on=["linked_gvkey", "event_year"], how="inner")
        .join(
            option_firm_years.rename({"firm_id": "linked_firm_id"}),
            on=["linked_firm_id", "event_year"],
            how="inner",
        )
        .filter(pl.col("source_firm_id") != pl.col("linked_firm_id"))
        .select(
            [
                "source_firm_id",
                "linked_firm_id",
                "link_type",
                "link_year",
                "link_score",
                "source_ticker",
                "linked_ticker",
                "source_gvkey",
                "linked_gvkey",
                "source_name",
                "linked_name",
            ]
        )
        .group_by(["source_firm_id", "linked_firm_id", "link_type", "link_year"])
        .agg(
            [
                pl.col("link_score").max().alias("link_score"),
                pl.col("source_ticker").drop_nulls().first(),
                pl.col("linked_ticker").drop_nulls().first(),
                pl.col("source_gvkey").drop_nulls().first(),
                pl.col("linked_gvkey").drop_nulls().first(),
                pl.col("source_name").drop_nulls().first(),
                pl.col("linked_name").drop_nulls().first(),
            ]
        )
    )


def _build_source_event_gvkeys(
    *,
    source_event_years: pl.DataFrame,
    gvkey_underlying_bridge: pl.DataFrame | None,
) -> pl.DataFrame:
    if gvkey_underlying_bridge is None or gvkey_underlying_bridge.height == 0:
        return _empty_source_event_gvkey_frame()

    return source_event_years.join(
        gvkey_underlying_bridge.select(
            [
                pl.col("firm_id").alias("source_firm_id"),
                "event_year",
                pl.col("gvkey").alias("source_gvkey"),
                pl.col("underlying_symbol").alias("source_ticker"),
                pl.col("issuer_name").alias("source_name"),
            ]
        ).unique(),
        on=["source_firm_id", "event_year"],
        how="inner",
    ).unique()


def _symmetrize_linkage_pairs(frame: pl.DataFrame) -> pl.DataFrame:
    reversed_frame = frame.select(
        [
            "link_year",
            pl.col("linked_ticker").alias("source_ticker"),
            pl.col("source_ticker").alias("linked_ticker"),
            pl.col("linked_gvkey").alias("source_gvkey"),
            pl.col("source_gvkey").alias("linked_gvkey"),
            pl.col("linked_name").alias("source_name"),
            pl.col("source_name").alias("linked_name"),
            "link_score",
            "link_type",
        ]
    )
    return pl.concat([frame, reversed_frame.select(frame.columns)], how="vertical_relaxed")


def _scan_tabular_file(path: Path) -> pl.LazyFrame:
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return pl.scan_parquet(path)
    separator = "\t" if suffix in {".tsv", ".txt"} else ","
    return pl.scan_csv(
        path,
        separator=separator,
        infer_schema_length=10_000,
        null_values=["", "NA", "NULL"],
    )


def _concat_unique_values_expr(column: str) -> pl.Expr:
    return pl.col(column).drop_nulls().cast(pl.String).unique().sort().implode().list.join(";")


def _count_rows_by_type(frame: pl.DataFrame, column: str) -> dict[str, int]:
    if frame.height == 0 or column not in frame.columns:
        return {}
    rows = frame.group_by(column).len().sort(column).iter_rows(named=True)
    return {
        str(row[column]): int(row["len"])
        for row in rows
        if row[column] is not None and str(row[column]) != ""
    }


def _issuer_priority_expr(column: str) -> pl.Expr:
    return (
        pl.when(pl.col(column) == "sec_event_source")
        .then(pl.lit(1))
        .when(pl.col(column) == "sec_event_target")
        .then(pl.lit(2))
        .when(pl.col(column) == "sec_event_acquirer")
        .then(pl.lit(3))
        .otherwise(pl.lit(4))
    )


def _canonicalize_column(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")


def _normalize_symbol_expr(column: str) -> pl.Expr:
    return (
        pl.col(column)
        .cast(pl.String)
        .str.strip_chars()
        .str.to_uppercase()
        .str.replace_all("/", ".", literal=True)
        .str.replace_all("-", ".", literal=True)
    )


def _normalize_gvkey_expr(column: str) -> pl.Expr:
    return pl.col(column).cast(pl.Int64, strict=False).cast(pl.String)


def _normalize_cik_expr(column: str) -> pl.Expr:
    return pl.col(column).cast(pl.Int64, strict=False).cast(pl.String).str.pad_start(10, "0")


def _first_present(columns: list[str], candidates: list[str]) -> str | None:
    for candidate in candidates:
        if candidate in columns:
            return candidate
    return None


def _empty_issuer_evidence_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "cik": pl.String,
            "issuer_ticker": pl.String,
            "issuer_name": pl.String,
            "firm_id": pl.String,
            "underlying_symbol": pl.String,
            "issuer_source": pl.String,
            "evidence_date": pl.Date,
            "evidence_event_id": pl.String,
        }
    )


def _empty_gvkey_underlying_bridge_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "gvkey": pl.String,
            "iid": pl.String,
            "cik": pl.String,
            "event_year": pl.Int64,
            "underlying_symbol": pl.String,
            "firm_id": pl.String,
            "observed_start_date": pl.Date,
            "observed_end_date": pl.Date,
            "option_obs_count": pl.UInt32,
            "seed_first_date": pl.Date,
            "seed_last_date": pl.Date,
            "issuer_ticker": pl.String,
            "issuer_name": pl.String,
            "issuer_sources": pl.String,
            "evidence_event_ids": pl.String,
            "bridge_method": pl.String,
            "bridge_confidence": pl.String,
        }
    )


def _empty_source_event_gvkey_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "source_firm_id": pl.String,
            "event_year": pl.Int64,
            "link_year": pl.Int64,
            "source_gvkey": pl.String,
            "source_ticker": pl.String,
            "source_name": pl.String,
        }
    )


def _empty_linkage_frame() -> pl.DataFrame:
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
        }
    )


def _empty_control_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "source_firm_id": pl.String,
            "event_year": pl.Int64,
            "link_year": pl.Int64,
            "link_type": pl.String,
            "control_firm_id": pl.String,
        }
    )
