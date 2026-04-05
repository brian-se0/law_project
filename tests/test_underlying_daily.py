from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import polars as pl

from shadow_trading.config import load_project_config
from shadow_trading.pipelines import BuildUnderlyingsRunOptions, run_underlying_daily_build
from shadow_trading.underlyings import add_underlying_raw_returns, build_underlying_daily_frame


def test_build_underlying_daily_frame_deduplicates_and_flags_inconsistency() -> None:
    frame = pl.DataFrame(
        {
            "quote_date": [
                date(2021, 4, 9),
                date(2021, 4, 9),
                date(2021, 4, 12),
                date(2021, 4, 12),
            ],
            "underlying_symbol": ["ABC", "ABC", "ABC", "ABC"],
            "underlying_bid_1545": [10.0, 10.0, 11.0, 11.0],
            "underlying_ask_1545": [10.2, 10.2, 11.2, 11.2],
            "implied_underlying_price_1545": [10.1, 10.1, 11.1, 11.1],
            "active_underlying_price_1545": [10.1, 10.1, 11.1, 11.1],
            "underlying_bid_eod": [10.4, 10.4, 11.4, 11.5],
            "underlying_ask_eod": [10.6, 10.6, 11.6, 11.8],
            "s_1545": [10.1, 10.1, 11.1, 11.1],
            "s_eod": [10.5, 10.5, 11.5, 11.65],
            "has_valid_underlying_1545_quote": [True, True, True, True],
            "has_valid_underlying_eod_quote": [True, True, True, True],
        }
    )

    aggregated = add_underlying_raw_returns(build_underlying_daily_frame(frame))

    assert aggregated.height == 2
    assert aggregated.get_column("option_series_count").to_list() == [2, 2]
    assert aggregated.get_column("has_inconsistent_1545_snapshot").to_list() == [False, False]
    assert aggregated.get_column("has_inconsistent_eod_snapshot").to_list() == [False, True]
    assert aggregated.get_column("raw_return").to_list()[0] is None
    assert aggregated.get_column("raw_return").to_list()[1] == 11.5 / 10.5 - 1


def test_run_underlying_daily_build_end_to_end(tmp_path: Path) -> None:
    processed_dir = tmp_path / "processed"
    outputs_dir = tmp_path / "outputs"
    qc_dir = outputs_dir / "qc"
    input_dataset_dir = processed_dir / "options_eod_summary"
    (input_dataset_dir / "quote_date=2021-04-09").mkdir(parents=True)
    (input_dataset_dir / "quote_date=2021-04-12").mkdir(parents=True)

    day_one = pl.DataFrame(
        {
            "quote_date": [date(2021, 4, 9), date(2021, 4, 9), date(2021, 4, 9)],
            "underlying_symbol": ["AAA", "AAA", "BBB"],
            "underlying_bid_1545": [100.0, 100.0, 50.0],
            "underlying_ask_1545": [100.2, 100.2, 50.2],
            "implied_underlying_price_1545": [100.1, 100.1, 50.1],
            "active_underlying_price_1545": [100.1, 100.1, 50.1],
            "underlying_bid_eod": [100.4, 100.4, 50.3],
            "underlying_ask_eod": [100.6, 100.6, 50.5],
            "s_1545": [100.1, 100.1, 50.1],
            "s_eod": [100.5, 100.5, 50.4],
            "has_valid_underlying_1545_quote": [True, True, True],
            "has_valid_underlying_eod_quote": [True, True, True],
        }
    )
    day_two = pl.DataFrame(
        {
            "quote_date": [date(2021, 4, 12), date(2021, 4, 12), date(2021, 4, 12)],
            "underlying_symbol": ["AAA", "AAA", "BBB"],
            "underlying_bid_1545": [101.0, 101.0, 49.8],
            "underlying_ask_1545": [101.2, 101.2, 50.0],
            "implied_underlying_price_1545": [101.1, 101.1, 49.9],
            "active_underlying_price_1545": [101.1, 101.1, 49.9],
            "underlying_bid_eod": [101.4, 101.4, 49.7],
            "underlying_ask_eod": [101.6, 101.6, 49.9],
            "s_1545": [101.1, 101.1, 49.9],
            "s_eod": [101.5, 101.5, 49.8],
            "has_valid_underlying_1545_quote": [True, True, True],
            "has_valid_underlying_eod_quote": [True, True, True],
        }
    )

    day_one.write_parquet(
        input_dataset_dir / "quote_date=2021-04-09" / "options_eod_summary.parquet"
    )
    day_two.write_parquet(
        input_dataset_dir / "quote_date=2021-04-12" / "options_eod_summary.parquet"
    )

    paths_yaml = tmp_path / "paths.yaml"
    paths_yaml.write_text(
        "\n".join(
            [
                "paths:",
                f"  raw_dir: {(tmp_path / 'raw').as_posix()}",
                f"  external_dir: {(tmp_path / 'external').as_posix()}",
                f"  interim_dir: {(tmp_path / 'interim').as_posix()}",
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
                '  file_glob: "UnderlyingOptionsEODCalcs_*.zip"',
                "  output_dataset_dir: options_eod_summary",
                "  qc_report_stem: options_ingest_qc",
                "build_underlyings:",
                "  input_dataset_dir: options_eod_summary",
                "  output_file_name: underlying_daily.parquet",
                "  qc_report_stem: underlying_daily_qc",
                "market:",
                '  timezone: "America/New_York"',
                '  regular_open: "09:30"',
                '  regular_close: "16:00"',
                "windows:",
                "  estimation: [-120, -20]",
                "  pre_event: [-5, -1]",
                "  announcement: [0, 1]",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    config = load_project_config(tmp_path, paths_file=paths_yaml, research_file=research_yaml)
    artifacts = run_underlying_daily_build(
        config,
        BuildUnderlyingsRunOptions(overwrite=True),
    )

    assert artifacts.row_count == 4
    output_frame = pl.read_parquet(artifacts.output_file)
    assert output_frame.height == 4

    aaa_day_two = output_frame.filter(
        (pl.col("underlying_symbol") == "AAA") & (pl.col("quote_date") == date(2021, 4, 12))
    )
    assert aaa_day_two.get_column("raw_return").item() == 101.5 / 100.5 - 1

    qc_payload = json.loads(artifacts.qc_json_file.read_text(encoding="utf-8"))
    assert qc_payload["underlying_daily_row_count"] == 4
    assert qc_payload["source_partition_count"] == 2
