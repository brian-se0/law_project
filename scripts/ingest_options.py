from __future__ import annotations

# ruff: noqa: E402

import argparse
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from shadow_trading.config import load_project_config
from shadow_trading.pipelines import IngestRunOptions, run_options_ingest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest vendor option EOD archives into Parquet.")
    parser.add_argument("--project-root", default=".", help="Project root directory.")
    parser.add_argument(
        "--paths-file", default=None, help="Optional override for the paths YAML file."
    )
    parser.add_argument(
        "--research-file",
        default=None,
        help="Optional override for the research parameters YAML file.",
    )
    parser.add_argument("--start-date", default=None, help="Optional YYYY-MM-DD lower date bound.")
    parser.add_argument("--end-date", default=None, help="Optional YYYY-MM-DD upper date bound.")
    parser.add_argument(
        "--limit-files", type=int, default=None, help="Optional number of archives to ingest."
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite processed Parquet partitions if they already exist.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_project_config(
        project_root=Path(args.project_root),
        paths_file=Path(args.paths_file) if args.paths_file else None,
        research_file=Path(args.research_file) if args.research_file else None,
    )
    artifacts = run_options_ingest(
        config,
        IngestRunOptions(
            start_date=_optional_date(args.start_date),
            end_date=_optional_date(args.end_date),
            limit_files=args.limit_files,
            overwrite=args.overwrite,
        ),
    )
    print(
        f"Wrote {artifacts.processed_row_count:,} normalized rows across "
        f"{artifacts.processed_file_count} quote date(s) to {artifacts.dataset_output_dir}"
    )
    if artifacts.skipped_existing_outputs:
        print(f"Skipped existing outputs: {artifacts.skipped_existing_outputs}")
    print(f"QC JSON: {artifacts.qc_json_file}")
    print(f"QC Markdown: {artifacts.qc_markdown_file}")


def _optional_date(value: str | None) -> date | None:
    return date.fromisoformat(value) if value else None


if __name__ == "__main__":
    main()
