from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from shadow_trading.case_study import build_case_study_paths
from shadow_trading.config import load_project_config

TRADING_DATES = [
    date(2016, 8, 12),
    date(2016, 8, 15),
    date(2016, 8, 16),
    date(2016, 8, 17),
    date(2016, 8, 18),
    date(2016, 8, 19),
    date(2016, 8, 22),
    date(2016, 8, 23),
    date(2016, 8, 24),
]


def build_case_study_config(tmp_path: Path):
    raw_dir = tmp_path / "raw"
    external_dir = tmp_path / "external"
    interim_dir = tmp_path / "interim"
    processed_dir = tmp_path / "processed"
    outputs_dir = tmp_path / "outputs"
    qc_dir = outputs_dir / "qc"
    for directory in [raw_dir, external_dir, interim_dir, processed_dir, outputs_dir, qc_dir]:
        directory.mkdir(parents=True, exist_ok=True)

    paths_yaml = tmp_path / "paths.yaml"
    paths_yaml.write_text(
        "\n".join(
            [
                "paths:",
                f"  raw_dir: {raw_dir.as_posix()}",
                f"  external_dir: {external_dir.as_posix()}",
                f"  interim_dir: {interim_dir.as_posix()}",
                f"  processed_dir: {processed_dir.as_posix()}",
                f"  outputs_dir: {outputs_dir.as_posix()}",
                f"  qc_dir: {qc_dir.as_posix()}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    research_yaml = tmp_path / "research.yaml"
    research_yaml.write_text(
        "\n".join(
            [
                "ingest_options:",
                "  output_dataset_dir: options_eod_summary",
                "build_underlyings:",
                "  output_file_name: underlying_daily.parquet",
                "build_events:",
                "  events_file_name: sec_mna_event_universe.parquet",
                "build_linkages:",
                "  output_file_name: linkages.parquet",
                "  controls_file_name: linkage_control_candidates.parquet",
                "  bridge_output_file_name: gvkey_underlying_bridge.parquet",
                "market:",
                '  timezone: "America/New_York"',
                '  regular_open: "09:30"',
                '  regular_close: "16:00"',
                "windows:",
                "  estimation: [-5, -3]",
                "  pre_event: [-2, -1]",
                "  announcement: [0, 1]",
                "case_study:",
                "  case_id: mdvn_panuwat_2016",
                "  source_symbol: MDVN",
                "  source_name: Medivation, Inc.",
                "  primary_related_symbol: INCY",
                "  primary_related_name: Incyte Corporation",
                "  acquirer_symbol: PFE",
                "  public_announcement_date: 2016-08-22",
                "  case_private_context_date: 2016-08-18",
                "  link_year: 2015",
                "  horizontal_top_k: 2",
                "  exact_contracts:",
                "    - underlying_symbol: INCY",
                "      expiration: 2016-09-16",
                "      strike: 80.0",
                "      option_type: C",
                "  windows:",
                "    estimation: [-5, -3]",
                "    pre_event: [-2, -1]",
                "    terminal_case: [-2, -1]",
                "    announcement: [0, 1]",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return load_project_config(tmp_path, paths_file=paths_yaml, research_file=research_yaml)


def seed_case_study_inputs(tmp_path: Path, *, include_frozen_event: bool = True):
    config = build_case_study_config(tmp_path)
    processed_dir = config.paths.processed_dir
    (processed_dir / config.ingest_options.output_dataset_dir).mkdir(parents=True, exist_ok=True)

    _write_underlyings(processed_dir / config.build_underlyings.output_file_name)
    _write_events(processed_dir / config.build_events.events_file_name)
    _write_linkages(processed_dir / config.build_linkages.output_file_name)
    _write_controls(processed_dir / config.build_linkages.controls_file_name)
    _write_bridge(processed_dir / config.build_linkages.bridge_output_file_name)
    _write_option_partitions(processed_dir / config.ingest_options.output_dataset_dir)

    if include_frozen_event:
        case_paths = build_case_study_paths(config)
        case_paths.case_dir.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(
            [
                {
                    "case_id": config.case_study.case_id,
                    "event_id": "sec_mna_mdvn_2016-08-22_pfizer",
                    "source_firm_id": "MDVN",
                    "source_symbol": "MDVN",
                    "source_name": "Medivation, Inc.",
                    "target_cik": "0001213115",
                    "target_gvkey": "1111",
                    "acquirer_symbol": "PFE",
                    "acquirer_cik": "0000078003",
                    "first_public_disclosure_dt": "2016-08-22T08:00:00+00:00",
                    "event_trading_date": date(2016, 8, 22),
                    "case_private_context_date": date(2016, 8, 18),
                    "review_status": "frozen",
                    "review_note": "Synthetic frozen event for test coverage.",
                    "evidence_source": "https://www.sec.gov/test",
                }
            ]
        ).write_parquet(case_paths.case_event_file, compression="zstd")

    return config


def seed_release_bundle_support_files(project_root: Path) -> None:
    (project_root / "docs").mkdir(parents=True, exist_ok=True)
    (project_root / "paper").mkdir(parents=True, exist_ok=True)

    (project_root / "README.md").write_text("# Test release bundle\n", encoding="utf-8")
    (project_root / "AGENTS.md").write_text("# Test agents\n", encoding="utf-8")
    (project_root / "Makefile").write_text("test:\n\tpython -m pytest -q\n", encoding="utf-8")
    (project_root / "references.bib").write_text(
        "@misc{sec2021panuwatcomplaint,\n  title = {Test complaint}\n}\n",
        encoding="utf-8",
    )
    (project_root / "docs" / "literature_review.md").write_text(
        "# Test literature review\n",
        encoding="utf-8",
    )
    (project_root / "docs" / "results_log.md").write_text("# Test results log\n", encoding="utf-8")
    (project_root / "docs" / "assumptions_log.md").write_text(
        "# Test assumptions log\n",
        encoding="utf-8",
    )
    (project_root / "docs" / "data_dictionary.md").write_text(
        "# Test data dictionary\n",
        encoding="utf-8",
    )
    (project_root / "paper" / "mdvn_panuwat_case_study.md").write_text(
        "# Test MDVN paper draft\n",
        encoding="utf-8",
    )


def _write_underlyings(path: Path) -> None:
    rows = []
    for symbol, base_price in {
        "MDVN": 65.0,
        "INCY": 80.0,
        "PEER": 52.0,
        "CTRL": 48.0,
    }.items():
        prior_price = None
        for index, quote_date in enumerate(TRADING_DATES):
            price = (
                base_price
                + index * 0.8
                + (
                    1.2
                    if symbol == "INCY" and quote_date in {date(2016, 8, 18), date(2016, 8, 19)}
                    else 0.0
                )
            )
            rows.append(
                {
                    "quote_date": quote_date,
                    "underlying_symbol": symbol,
                    "option_series_count": 2,
                    "underlying_bid_1545": price - 0.1,
                    "underlying_ask_1545": price + 0.1,
                    "implied_underlying_price_1545": price,
                    "active_underlying_price_1545": price,
                    "s_1545": price,
                    "underlying_bid_eod": price - 0.1,
                    "underlying_ask_eod": price + 0.1,
                    "s_eod": price,
                    "has_valid_underlying_1545_quote": True,
                    "has_valid_underlying_eod_quote": True,
                    "distinct_underlying_bid_1545_count": 1,
                    "distinct_underlying_ask_1545_count": 1,
                    "distinct_s_1545_count": 1,
                    "distinct_underlying_bid_eod_count": 1,
                    "distinct_underlying_ask_eod_count": 1,
                    "distinct_s_eod_count": 1,
                    "distinct_1545_snapshot_count": 1,
                    "distinct_eod_snapshot_count": 1,
                    "has_inconsistent_1545_snapshot": False,
                    "has_inconsistent_eod_snapshot": False,
                    "prior_s_eod": prior_price,
                    "raw_return": (price / prior_price - 1) if prior_price else None,
                }
            )
            prior_price = price
    pl.DataFrame(rows).write_parquet(path, compression="zstd")


def _write_events(path: Path) -> None:
    pl.DataFrame(
        [
            {
                "event_id": "sec_mna_mdvn_2016-08-22_pfizer",
                "source_firm_id": "MDVN",
                "source_cik": "0001213115",
                "source_name": "Medivation, Inc.",
                "source_ticker": "MDVN",
                "source_underlying_symbol": "MDVN",
                "target_firm_id": "MDVN",
                "target_cik": "0001213115",
                "target_name": "Medivation, Inc.",
                "target_ticker": "MDVN",
                "target_underlying_symbol": "MDVN",
                "acquirer_firm_id": "PFE",
                "acquirer_cik": "0000078003",
                "acquirer_name": "Pfizer Inc.",
                "acquirer_ticker": "PFE",
                "acquirer_underlying_symbol": "PFE",
                "first_public_disclosure_dt": "2016-08-22T08:00:00+00:00",
                "first_public_disclosure_filing_date": "2016-08-22",
                "event_trading_date": "2016-08-22",
                "pre_event_window_end": "2016-08-19",
                "announcement_form": "8-K",
                "announcement_accession_number": "0001213115-16-000001",
                "announcement_filing_url": "https://www.sec.gov/test",
                "deal_type": "merger",
                "counterparty_name": "Pfizer Inc.",
                "counterparty_slug": "pfizer-inc",
                "source_resolution": "filer",
                "target_resolution": "subject_company",
                "acquirer_resolution": "text_capture",
                "candidate_filing_count": 1,
                "candidate_forms": "8-K",
                "candidate_accessions": "0001213115-16-000001",
                "max_match_score": 8,
                "requires_manual_review": False,
                "has_conflicting_counterparties": False,
                "has_conflicting_acquirers": False,
                "cluster_start_dt": "2016-08-22T08:00:00+00:00",
                "cluster_end_dt": "2016-08-22T08:00:00+00:00",
            }
        ]
    ).write_parquet(path, compression="zstd")


def _write_linkages(path: Path) -> None:
    pl.DataFrame(
        [
            {
                "source_firm_id": "MDVN",
                "linked_firm_id": "INCY",
                "link_type": "horizontal_tnic",
                "link_year": 2015,
                "link_score": 0.92,
                "source_ticker": "MDVN",
                "linked_ticker": "INCY",
                "source_gvkey": "1111",
                "linked_gvkey": "2222",
                "source_name": "Medivation, Inc.",
                "linked_name": "Incyte Corporation",
                "link_rank": 1,
            },
            {
                "source_firm_id": "MDVN",
                "linked_firm_id": "PEER",
                "link_type": "horizontal_tnic",
                "link_year": 2015,
                "link_score": 0.74,
                "source_ticker": "MDVN",
                "linked_ticker": "PEER",
                "source_gvkey": "1111",
                "linked_gvkey": "3333",
                "source_name": "Medivation, Inc.",
                "linked_name": "Peer Holdings",
                "link_rank": 2,
            },
            {
                "source_firm_id": "MDVN",
                "linked_firm_id": "SUPP",
                "link_type": "vertical_vtnic",
                "link_year": 2015,
                "link_score": 0.33,
                "source_ticker": "MDVN",
                "linked_ticker": "SUPP",
                "source_gvkey": "1111",
                "linked_gvkey": "4444",
                "source_name": "Medivation, Inc.",
                "linked_name": "Supplier Inc.",
                "link_rank": 1,
            },
        ]
    ).write_parquet(path, compression="zstd")


def _write_controls(path: Path) -> None:
    pl.DataFrame(
        [
            {
                "source_firm_id": "MDVN",
                "event_year": 2016,
                "link_year": 2015,
                "link_type": "horizontal_tnic",
                "control_firm_id": "CTRL",
            }
        ]
    ).write_parquet(path, compression="zstd")


def _write_bridge(path: Path) -> None:
    pl.DataFrame(
        [
            {
                "gvkey": "1111",
                "iid": "01",
                "cik": "0001213115",
                "event_year": 2016,
                "underlying_symbol": "MDVN",
                "firm_id": "MDVN",
                "observed_start_date": date(2016, 8, 12),
                "observed_end_date": date(2016, 8, 24),
                "option_obs_count": 9,
                "seed_first_date": date(2010, 1, 1),
                "seed_last_date": date(2016, 12, 31),
                "issuer_ticker": "MDVN",
                "issuer_name": "Medivation, Inc.",
                "issuer_sources": "sec_event_source",
                "evidence_event_ids": "sec_mna_mdvn_2016-08-22_pfizer",
                "bridge_method": "sec_event_evidence_plus_gvkey_cik_seed",
                "bridge_confidence": "high",
            }
        ]
    ).write_parquet(path, compression="zstd")


def _write_option_partitions(dataset_dir: Path) -> None:
    for quote_date in TRADING_DATES:
        partition_dir = dataset_dir / f"quote_date={quote_date.isoformat()}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(_option_rows_for_date(quote_date)).write_parquet(
            partition_dir / "options_eod_summary.parquet",
            compression="zstd",
        )


def _option_rows_for_date(quote_date: date) -> list[dict[str, object]]:
    volumes = {
        date(2016, 8, 12): (8, 3, 4, 2),
        date(2016, 8, 15): (9, 3, 5, 2),
        date(2016, 8, 16): (10, 4, 5, 3),
        date(2016, 8, 17): (11, 4, 5, 3),
        date(2016, 8, 18): (45, 10, 12, 5),
        date(2016, 8, 19): (60, 12, 14, 6),
        date(2016, 8, 22): (18, 8, 10, 4),
        date(2016, 8, 23): (14, 6, 9, 4),
        date(2016, 8, 24): (12, 5, 8, 3),
    }
    incy_exact, incy_other, peer_volume, ctrl_volume = volumes[quote_date]
    mdvn_volume = 20 if quote_date in {date(2016, 8, 18), date(2016, 8, 19)} else 6
    underlyings = {"INCY": 80.0, "PEER": 52.0, "CTRL": 48.0, "MDVN": 65.0}
    exact_expiration = date(2016, 9, 16)
    short_expiration = date(2016, 9, 9)
    rows = [
        _option_row(
            quote_date,
            "INCY",
            80.0,
            incy_exact,
            open_interest=100 + TRADING_DATES.index(quote_date) * 3,
            vwap=2.4,
            delta=0.25,
            underlying_price=underlyings["INCY"],
            expiration=exact_expiration,
        ),
        _option_row(
            quote_date,
            "INCY",
            82.5,
            incy_other,
            open_interest=90 + TRADING_DATES.index(quote_date) * 2,
            vwap=1.6,
            delta=0.18,
            underlying_price=underlyings["INCY"],
            expiration=exact_expiration,
        ),
        _option_row(
            quote_date,
            "PEER",
            55.0,
            peer_volume,
            open_interest=60 + TRADING_DATES.index(quote_date),
            vwap=1.1,
            delta=0.22,
            underlying_price=underlyings["PEER"],
            expiration=short_expiration,
        ),
        _option_row(
            quote_date,
            "CTRL",
            50.0,
            ctrl_volume,
            open_interest=55 + TRADING_DATES.index(quote_date),
            vwap=0.9,
            delta=0.21,
            underlying_price=underlyings["CTRL"],
            expiration=short_expiration,
        ),
        _option_row(
            quote_date,
            "MDVN",
            70.0,
            mdvn_volume,
            open_interest=80 + TRADING_DATES.index(quote_date) * 2,
            vwap=2.1,
            delta=0.27,
            underlying_price=underlyings["MDVN"],
            expiration=short_expiration,
        ),
    ]
    return rows


def _option_row(
    quote_date: date,
    symbol: str,
    strike: float,
    trade_volume: int,
    *,
    open_interest: int,
    vwap: float,
    delta: float,
    underlying_price: float,
    expiration: date,
) -> dict[str, object]:
    mid = max(vwap, 0.5)
    return {
        "quote_date": quote_date,
        "underlying_symbol": symbol,
        "root": symbol,
        "expiration": expiration,
        "strike": strike,
        "option_type": "C",
        "trade_volume": trade_volume,
        "open_interest": open_interest,
        "vwap": vwap,
        "mid_1545": mid,
        "mid_eod": mid,
        "rel_spread_1545": 0.08,
        "implied_volatility_1545": 0.35,
        "delta_1545": delta,
        "s_1545": underlying_price,
        "s_eod": underlying_price,
        "dte_cal": (expiration - quote_date).days,
        "has_calcs": True,
        "series_id": f"{symbol}|{symbol}|{expiration.isoformat()}|{float(strike)}|C",
    }
