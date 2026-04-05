from __future__ import annotations

import json
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

import polars as pl

from shadow_trading.config import load_project_config
from shadow_trading.pipelines import IngestRunOptions, run_options_ingest

CSV_TEXT = """underlying_symbol,quote_date,root,expiration,strike,option_type,open,high,low,close,trade_volume,bid_size_1545,bid_1545,ask_size_1545,ask_1545,underlying_bid_1545,underlying_ask_1545,implied_underlying_price_1545,active_underlying_price_1545,implied_volatility_1545,delta_1545,gamma_1545,theta_1545,vega_1545,rho_1545,bid_size_eod,bid_eod,ask_size_eod,ask_eod,underlying_bid_eod,underlying_ask_eod,vwap,open_interest,delivery_code
A,2021-04-09,A,2021-04-16,65.000,C,0.0000,0.0000,0.0000,0.0000,0,77,66.0000,77,67.0000,131.4900,131.5200,131.5500,131.5050,0.0200,1.0000,0.0016,-0.5019,0.0192,1.0877,299,64.9000,222,69.5000,131.8800,131.9100,0.0000,0,
A,2021-04-09,A,2021-04-16,65.000,P,0.0000,0.0000,0.0000,0.0000,0,0,0.0000,142,0.1000,131.4900,131.5200,131.5500,131.5050,2.0133,-0.0039,0.0003,-0.0302,0.0021,-0.0108,0,0.0000,274,0.2000,131.8800,131.9100,0.0000,1,
"""


def test_run_options_ingest_reads_zip_archives_without_mutating_raw(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    processed_dir = tmp_path / "processed"
    outputs_dir = tmp_path / "outputs"
    qc_dir = outputs_dir / "qc"
    raw_dir.mkdir(parents=True)

    archive_path = raw_dir / "UnderlyingOptionsEODCalcs_2021-04-09.zip"
    with ZipFile(archive_path, "w", compression=ZIP_DEFLATED) as archive:
        archive.writestr("UnderlyingOptionsEODCalcs_2021-04-09.csv", CSV_TEXT)
    original_mtime = archive_path.stat().st_mtime_ns

    paths_yaml = tmp_path / "paths.yaml"
    paths_yaml.write_text(
        "\n".join(
            [
                "paths:",
                f"  raw_dir: {raw_dir.as_posix()}",
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
    artifacts = run_options_ingest(config, IngestRunOptions())

    assert artifacts.processed_file_count == 1
    assert archive_path.stat().st_mtime_ns == original_mtime

    output_file = (
        processed_dir
        / "options_eod_summary"
        / "quote_date=2021-04-09"
        / "options_eod_summary.parquet"
    )
    assert output_file.exists()

    frame = pl.read_parquet(output_file)
    assert frame.height == 2
    assert frame.get_column("series_id").to_list()[0] == "A|A|2021-04-16|65.0|C"
    assert frame.get_column("has_valid_1545_quote").to_list() == [True, False]

    qc_payload = json.loads(artifacts.qc_json_file.read_text(encoding="utf-8"))
    assert qc_payload["aggregate_row_count"] == 2
    assert qc_payload["file_reports"][0]["invalid_option_quote_1545_rows"] == 1
