from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import polars as pl

from shadow_trading.case_study import build_case_study_paths
from shadow_trading.config import ProjectConfig
from shadow_trading.io import write_text


@dataclass(frozen=True)
class OutputArtifacts:
    figure_paths: dict[str, Path]
    table_paths: dict[str, Path]
    memo_paths: dict[str, Path]


def make_case_study_outputs(config: ProjectConfig) -> OutputArtifacts:
    case_paths = build_case_study_paths(config)
    required_files = [
        case_paths.case_event_file,
        case_paths.related_firms_file,
        case_paths.exact_contracts_file,
        case_paths.bucket_features_file,
        case_paths.matched_control_bucket_features_file,
        case_paths.abnormal_metrics_file,
        case_paths.control_matches_file,
    ]
    missing = [path for path in required_files if not path.exists()]
    if missing:
        missing_list = ", ".join(str(path) for path in missing)
        raise FileNotFoundError(
            f"Case-study outputs are missing: {missing_list}. Run the main-study step first."
        )

    figures_dir = config.paths.outputs_dir / "figures"
    tables_dir = config.paths.outputs_dir / "tables"
    memos_dir = config.paths.outputs_dir / "memos"
    figures_dir.mkdir(parents=True, exist_ok=True)
    tables_dir.mkdir(parents=True, exist_ok=True)
    memos_dir.mkdir(parents=True, exist_ok=True)

    case_event = pl.read_parquet(case_paths.case_event_file)
    related_firms = pl.read_parquet(case_paths.related_firms_file)
    exact_contracts = pl.read_parquet(case_paths.exact_contracts_file)
    bucket_features = pl.read_parquet(case_paths.bucket_features_file)
    matched_control_bucket_features = pl.read_parquet(
        case_paths.matched_control_bucket_features_file
    )
    abnormal_metrics = pl.read_parquet(case_paths.abnormal_metrics_file)
    control_matches = pl.read_parquet(case_paths.control_matches_file)
    all_bucket_features = pl.concat(
        [bucket_features, matched_control_bucket_features],
        how="vertical_relaxed",
    )

    figure_paths = {
        "timeline": figures_dir / "mdvn_timeline.svg",
        "exact_contracts": figures_dir / "mdvn_exact_contracts.svg",
        "incy_abnormal_activity": figures_dir / "mdvn_incy_abnormal_activity.svg",
        "linkage_rank": figures_dir / "mdvn_linkage_rank.svg",
        "watchlist": figures_dir / "mdvn_watchlist_framework.svg",
    }
    write_text(figure_paths["timeline"], _render_timeline_svg(config, case_event))
    write_text(figure_paths["exact_contracts"], _render_exact_contract_svg(exact_contracts))
    write_text(
        figure_paths["incy_abnormal_activity"],
        _render_incy_abnormal_svg(config, all_bucket_features, control_matches),
    )
    write_text(figure_paths["linkage_rank"], _render_linkage_rank_svg(config, related_firms))
    write_text(figure_paths["watchlist"], _render_watchlist_svg(config, related_firms))

    table_paths = {
        "chronology": tables_dir / "mdvn_case_chronology.md",
        "exact_contracts": tables_dir / "mdvn_exact_contract_inventory.md",
        "comparison": tables_dir / "mdvn_incy_vs_linked_vs_controls.md",
        "linkage_rank": tables_dir / "mdvn_linkage_rankings.md",
        "watchlist": tables_dir / "mdvn_watchlist_translation.md",
    }
    write_text(table_paths["chronology"], _table_with_title("Case chronology", case_event))
    write_text(
        table_paths["exact_contracts"],
        _table_with_title(
            "Exact complaint-named INCY contracts", _summarize_exact_contracts(exact_contracts)
        ),
    )
    write_text(
        table_paths["comparison"],
        _table_with_title(
            "INCY vs linked firms vs matched controls",
            abnormal_metrics.select(
                [
                    "comparison_role",
                    "underlying_symbol",
                    "primary_related_pair_flag",
                    "link_type",
                    "link_score",
                    "match_rank",
                    "pre_event_short_dated_otm_call_z_volume_mean",
                    "pre_event_short_dated_otm_call_z_premium_mean",
                    "terminal_case_short_dated_otm_call_z_volume_mean",
                    "return_0_1",
                ]
            ),
        ),
    )
    write_text(
        table_paths["linkage_rank"],
        _table_with_title(
            "Ex ante linkage ranking",
            related_firms.select(
                [
                    "linked_firm_id",
                    "linked_name",
                    "link_type",
                    "link_score",
                    "linked_rank_within_source",
                    "linked_percentile_within_source",
                    "primary_related_pair_flag",
                ]
            ),
        ),
    )
    write_text(
        table_paths["watchlist"],
        _build_watchlist_translation_table(config, related_firms, control_matches),
    )

    memo_paths = {
        "watchlist_compliance": memos_dir / "mdvn_watchlist_compliance_memo.md",
        "limitations": memos_dir / "mdvn_limitations.md",
    }
    write_text(
        memo_paths["watchlist_compliance"],
        _build_watchlist_compliance_memo(
            config=config,
            case_event=case_event,
            related_firms=related_firms,
            exact_contracts=exact_contracts,
            abnormal_metrics=abnormal_metrics,
            control_matches=control_matches,
        ),
    )
    write_text(
        memo_paths["limitations"],
        _build_limitations_memo(
            config=config,
            case_event=case_event,
            related_firms=related_firms,
            exact_contracts=exact_contracts,
            abnormal_metrics=abnormal_metrics,
            control_matches=control_matches,
        ),
    )

    return OutputArtifacts(
        figure_paths=figure_paths,
        table_paths=table_paths,
        memo_paths=memo_paths,
    )


def _render_timeline_svg(config: ProjectConfig, case_event: pl.DataFrame) -> str:
    row = case_event.row(0, named=True)
    private_date = row.get("case_private_context_date")
    public_date = row.get("event_trading_date")
    disclosure_dt = row.get("first_public_disclosure_dt")
    labels = [
        (110, f"Private context: {private_date}"),
        (330, f"Public disclosure: {disclosure_dt}"),
        (550, f"t = 0 trading date: {public_date}"),
    ]
    text_elements = "\n".join(
        f'<text x="{x}" y="120" font-size="14">{label}</text>' for x, label in labels
    )
    circle_elements = "\n".join(
        f'<circle cx="{x}" cy="80" r="9" fill="#0f766e" />' for x, _ in labels
    )
    return _svg_wrapper(
        "MDVN / Pfizer / INCY timeline",
        f"""
        <line x1="90" y1="80" x2="590" y2="80" stroke="#0f172a" stroke-width="3" />
        {circle_elements}
        {text_elements}
        <text x="40" y="180" font-size="15">
            {config.case_study.case_id}: event frozen from SEC-source timing evidence.
        </text>
        """,
        height=220,
    )


def _render_exact_contract_svg(exact_contracts: pl.DataFrame) -> str:
    if exact_contracts.height == 0:
        return _placeholder_svg(
            "Exact contract figure", "No litigated contract rows were extracted."
        )

    summary = (
        exact_contracts.group_by(["series_id", "relative_day"])
        .agg(pl.col("contract_volume").sum().alias("contract_volume"))
        .sort(["series_id", "relative_day"])
    )
    lines: list[str] = []
    colors = ["#0f766e", "#b45309", "#1d4ed8", "#7c3aed"]
    max_volume = max(
        float(value) for value in summary.get_column("contract_volume").to_list() or [1.0]
    )
    for index, series_id in enumerate(summary.get_column("series_id").unique().to_list()):
        series = summary.filter(pl.col("series_id") == series_id)
        points = []
        for row in series.iter_rows(named=True):
            x = 80 + ((int(row["relative_day"]) + 120) * 4)
            y = 300 - (float(row["contract_volume"]) / max(max_volume, 1.0)) * 180
            points.append(f"{x},{y}")
        color = colors[index % len(colors)]
        lines.append(
            f'<polyline fill="none" stroke="{color}" stroke-width="3" points="{" ".join(points)}" />'
        )
        lines.append(
            f'<text x="560" y="{50 + index * 18}" font-size="12" fill="{color}">{series_id}</text>'
        )
    return _svg_wrapper(
        "Exact INCY contract activity",
        """
        <line x1="70" y1="300" x2="560" y2="300" stroke="#0f172a" />
        <line x1="70" y1="80" x2="70" y2="300" stroke="#0f172a" />
        <text x="70" y="320" font-size="12">relative day</text>
        <text x="20" y="70" font-size="12">volume</text>
        """ + "\n".join(lines),
        height=360,
    )


def _render_incy_abnormal_svg(
    config: ProjectConfig,
    bucket_features: pl.DataFrame,
    control_matches: pl.DataFrame,
) -> str:
    focal = bucket_features.filter(
        (pl.col("option_type") == "C")
        & (pl.col("moneyness_bucket") == "call_otm")
        & pl.col("tenor_bucket").is_in(["0_7", "8_30"])
    )
    incy = focal.filter(pl.col("underlying_symbol") == config.case_study.primary_related_symbol)
    if incy.height == 0:
        return _placeholder_svg(
            "INCY abnormal-activity figure",
            "No short-dated OTM call rows were available for the primary related symbol.",
        )

    control_symbols = (
        control_matches.get_column("control_firm_id").to_list() if control_matches.height else []
    )
    control_mean = (
        focal.filter(pl.col("underlying_symbol").is_in(control_symbols))
        .group_by("relative_day")
        .agg(pl.col("z_volume").mean().alias("control_mean_z_volume"))
        .sort("relative_day")
    )
    incy_line = (
        incy.group_by("relative_day")
        .agg(pl.col("z_volume").mean().alias("z_volume"))
        .sort("relative_day")
    )
    max_abs = max(
        [
            1.0,
            *[
                abs(float(value))
                for value in incy_line.get_column("z_volume").drop_nulls().to_list()
            ],
            *[
                abs(float(value))
                for value in control_mean.get_column("control_mean_z_volume").drop_nulls().to_list()
            ],
        ]
    )
    incy_points = _line_points(incy_line, "z_volume", max_abs=max_abs)
    control_points = _line_points(control_mean, "control_mean_z_volume", max_abs=max_abs)
    return _svg_wrapper(
        "INCY abnormal short-dated OTM call activity",
        f"""
        <line x1="70" y1="190" x2="560" y2="190" stroke="#94a3b8" stroke-dasharray="4 4" />
        <line x1="70" y1="300" x2="560" y2="300" stroke="#0f172a" />
        <line x1="70" y1="80" x2="70" y2="300" stroke="#0f172a" />
        <polyline fill="none" stroke="#0f766e" stroke-width="3" points="{incy_points}" />
        <polyline fill="none" stroke="#b45309" stroke-width="3" points="{control_points}" />
        <text x="420" y="55" font-size="12" fill="#0f766e">INCY</text>
        <text x="420" y="72" font-size="12" fill="#b45309">Matched controls mean</text>
        <text x="75" y="320" font-size="12">relative day</text>
        """,
        height=360,
    )


def _render_linkage_rank_svg(config: ProjectConfig, related_firms: pl.DataFrame) -> str:
    ranked = related_firms.filter(
        pl.col("link_type").is_in(["horizontal_tnic", "vertical_vtnic", "primary_related_case"])
    ).head(10)
    if ranked.height == 0:
        return _placeholder_svg(
            "Linkage-rank figure", "No related firms were retained for the case."
        )

    max_score = max(
        [1.0, *[float(value) for value in ranked.get_column("link_score").fill_null(0).to_list()]]
    )
    bars: list[str] = []
    for index, row in enumerate(ranked.iter_rows(named=True)):
        y = 70 + index * 26
        width = 20 + (float(row.get("link_score") or 0.0) / max_score) * 260
        fill = "#0f766e" if row.get("primary_related_pair_flag") else "#94a3b8"
        label = row.get("linked_firm_id")
        bars.append(f'<rect x="180" y="{y}" width="{width}" height="16" fill="{fill}" rx="3" />')
        bars.append(f'<text x="40" y="{y + 12}" font-size="12">{label}</text>')
    return _svg_wrapper(
        "INCY within MDVN's ex ante linkage set",
        "\n".join(bars),
        height=360,
    )


def _render_watchlist_svg(config: ProjectConfig, related_firms: pl.DataFrame) -> str:
    horizontal_count = related_firms.filter(pl.col("link_type") == "horizontal_tnic").height
    vertical_count = related_firms.filter(pl.col("link_type") == "vertical_vtnic").height
    return _svg_wrapper(
        "Related-securities watchlist framework",
        f"""
        <rect x="40" y="60" width="180" height="90" rx="8" fill="#e2e8f0" />
        <rect x="260" y="60" width="180" height="90" rx="8" fill="#ccfbf1" />
        <rect x="480" y="60" width="180" height="90" rx="8" fill="#fef3c7" />
        <text x="60" y="92" font-size="16">Source issuer</text>
        <text x="60" y="116" font-size="14">{config.case_study.source_symbol}</text>
        <text x="280" y="92" font-size="16">Explicit comparable set</text>
        <text x="280" y="116" font-size="14">{config.case_study.primary_related_symbol} plus {horizontal_count} lagged TNIC peers</text>
        <text x="500" y="92" font-size="16">Vertical context</text>
        <text x="500" y="116" font-size="14">{vertical_count} lagged VTNIC relations</text>
        <text x="40" y="210" font-size="15">Policy output: cover related single-name shares and their listed options before public disclosure.</text>
        """,
        height=260,
    )


def _summarize_exact_contracts(exact_contracts: pl.DataFrame) -> pl.DataFrame:
    if exact_contracts.height == 0:
        return exact_contracts
    return (
        exact_contracts.group_by(["series_id", "expiration", "strike"])
        .agg(
            [
                pl.col("contract_volume").sum().alias("window_contract_volume"),
                pl.col("contract_premium").sum().alias("window_contract_premium"),
                pl.col("contract_lead_oi_change")
                .drop_nulls()
                .sum()
                .alias("window_contract_lead_oi_change"),
            ]
        )
        .sort("series_id")
    )


def _build_watchlist_translation_table(
    config: ProjectConfig,
    related_firms: pl.DataFrame,
    control_matches: pl.DataFrame,
) -> str:
    horizontal_count = related_firms.filter(pl.col("link_type") == "horizontal_tnic").height
    vertical_count = related_firms.filter(pl.col("link_type") == "vertical_vtnic").height
    rows = pl.DataFrame(
        [
            {
                "rule_component": "Explicit litigated related security",
                "case_application": config.case_study.primary_related_symbol,
                "watchlist_decision": "Include",
            },
            {
                "rule_component": "Lagged horizontal peers",
                "case_application": f"{horizontal_count} retained TNIC names",
                "watchlist_decision": "Include with ranked documentation",
            },
            {
                "rule_component": "Lagged vertical relations",
                "case_application": f"{vertical_count} retained VTNIC names",
                "watchlist_decision": "Include as unsigned risk context",
            },
            {
                "rule_component": "Matched non-linked controls",
                "case_application": f"{control_matches.height} diagnostic controls",
                "watchlist_decision": "Use for monitoring calibration only",
            },
            {
                "rule_component": "Listed options on retained names",
                "case_application": "Single-name options only",
                "watchlist_decision": "Include derivatives with the underlying watchlist",
            },
        ]
    )
    return _table_with_title("Watchlist translation matrix", rows)


def _build_watchlist_compliance_memo(
    *,
    config: ProjectConfig,
    case_event: pl.DataFrame,
    related_firms: pl.DataFrame,
    exact_contracts: pl.DataFrame,
    abnormal_metrics: pl.DataFrame,
    control_matches: pl.DataFrame,
) -> str:
    case_row = case_event.row(0, named=True)
    incy_row = (
        abnormal_metrics.filter(
            pl.col("underlying_symbol") == config.case_study.primary_related_symbol
        )
        .sort(["primary_related_pair_flag", "comparison_role"], descending=[True, False])
        .row(0, named=True)
    )
    exact_summary = _summarize_exact_contracts(exact_contracts)
    top_links = related_firms.head(5).select(
        [
            "linked_firm_id",
            "link_type",
            "link_score",
            "linked_rank_within_source",
            "primary_related_pair_flag",
        ]
    )
    lines = [
        "# MDVN Watchlist Compliance Memo",
        "",
        "## Case Frame",
        "",
        (
            f"The canonical case event is frozen to {case_row['event_id']} with first public "
            f"disclosure at {case_row['first_public_disclosure_dt']} and trading-date alignment "
            f"to {case_row['event_trading_date']}. The private-context date remains "
            f"{case_row['case_private_context_date']}."
        ),
        "",
        "## Related Security Focus",
        "",
        (
            f"The legally focal related security remains {config.case_study.primary_related_symbol}. "
            "The case-study summary treats abnormal pre-disclosure activity in that related "
            "single-name option as a shadow-trading risk signal, not proof of unlawful trading."
        ),
        "",
        (
            f"INCY pre-event short-dated OTM call z-volume mean: "
            f"{incy_row.get('pre_event_short_dated_otm_call_z_volume_mean')}"
        ),
        (
            f"INCY pre-event short-dated OTM call z-premium mean: "
            f"{incy_row.get('pre_event_short_dated_otm_call_z_premium_mean')}"
        ),
        (
            f"INCY terminal-case short-dated OTM call z-volume mean: "
            f"{incy_row.get('terminal_case_short_dated_otm_call_z_volume_mean')}"
        ),
        "",
        "## Exact Complaint-Named Contracts",
        "",
        _frame_to_markdown(exact_summary),
        "",
        "## Ex Ante Linkage Context",
        "",
        _frame_to_markdown(top_links),
        "",
        "## Watchlist Translation",
        "",
        (
            "A related-securities watchlist for this fact pattern should cover the source issuer, "
            f"the focal related security {config.case_study.primary_related_symbol}, other lagged "
            "horizontal peers retained from the ex ante linkage table, lagged vertical relations "
            "as unsigned context, and the listed single-name options on those retained names."
        ),
        (
            f"Matched non-linked controls retained for calibration: {control_matches.height}. "
            "These controls are diagnostic comparators rather than a policy watchlist."
        ),
        "",
    ]
    return "\n".join(lines)


def _build_limitations_memo(
    *,
    config: ProjectConfig,
    case_event: pl.DataFrame,
    related_firms: pl.DataFrame,
    exact_contracts: pl.DataFrame,
    abnormal_metrics: pl.DataFrame,
    control_matches: pl.DataFrame,
) -> str:
    case_row = case_event.row(0, named=True)
    lines = [
        "# MDVN Limitations",
        "",
        "## Scope",
        "",
        (
            f"This output is a single-case reconstruction for {config.case_study.case_id}. "
            "It is designed for reproducibility and legal relevance, not as a generalized "
            "trading strategy or accusation engine."
        ),
        "",
        "## Measurement Limits",
        "",
        (
            "Open interest is treated as start-of-day OCC open interest. Any opening-demand proxy "
            "therefore relies on next-day open-interest change and should be read as an approximation."
        ),
        (
            "The primary bucket summaries use daily option data and relative trading-day alignment. "
            "They do not recover intraday order flow, trader identity, or causal attribution."
        ),
        (
            f"The frozen event row uses first public disclosure timestamp "
            f"{case_row['first_public_disclosure_dt']} and maps it to trading date "
            f"{case_row['event_trading_date']}. If later legal work requires a different timestamp "
            "convention, the event table should be revised explicitly rather than silently re-used."
        ),
        "",
        "## Linkage and Control Limits",
        "",
        (
            "Linkages are lagged ex ante relations. They are useful for watchlist construction, "
            "but they are not proof that any retained firm was economically material to the deal."
        ),
        (
            f"The related-firm output retains {related_firms.height} names and the exact-contract "
            f"inventory contains {exact_contracts.height} case-window rows. These counts reflect the "
            "current processed data slice and should be rechecked whenever the underlying partitions change."
        ),
        (
            f"The matched-control comparison currently uses {control_matches.height} non-linked names. "
            "Controls are intended for calibration, not for legal inference about any individual security."
        ),
        "",
        "## Legal Interpretation",
        "",
        (
            "Abnormal pre-disclosure activity, suspicious footprints, and shadow-trading risk are "
            "compliance-oriented descriptions only. They should not be equated with liability or proof "
            "of insider trading."
        ),
        "",
    ]
    _ = abnormal_metrics
    return "\n".join(lines)


def _table_with_title(title: str, frame: pl.DataFrame) -> str:
    return f"# {title}\n\n{_frame_to_markdown(frame)}\n"


def _frame_to_markdown(frame: pl.DataFrame) -> str:
    if frame.height == 0:
        return "_No rows._"
    headers = list(frame.columns)
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in frame.iter_rows(named=True):
        values = [_escape_markdown_cell(row.get(column, "")) for column in headers]
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def _escape_markdown_cell(value: object) -> str:
    if value is None:
        return ""
    text = str(value)
    return text.replace("\\", "\\\\").replace("\r", " ").replace("\n", " ").replace("|", "\\|")


def _line_points(frame: pl.DataFrame, value_column: str, *, max_abs: float) -> str:
    points: list[str] = []
    for row in frame.iter_rows(named=True):
        if row.get(value_column) is None:
            continue
        x = 80 + ((int(row["relative_day"]) + 120) * 4)
        y = 190 - (float(row[value_column]) / max(max_abs, 1.0)) * 90
        points.append(f"{x},{y}")
    return " ".join(points)


def _placeholder_svg(title: str, message: str) -> str:
    return _svg_wrapper(
        title,
        f'<text x="40" y="120" font-size="16">{message}</text>',
        height=180,
    )


def _svg_wrapper(title: str, body: str, *, height: int) -> str:
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="720" height="{height}" '
        f'viewBox="0 0 720 {height}">'
        f'<rect width="100%" height="100%" fill="#ffffff" />'
        f'<text x="24" y="36" font-size="20" font-weight="700">{title}</text>'
        f"{body}</svg>"
    )
