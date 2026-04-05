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
from shadow_trading.pipelines import BuildUnderlyingsRunOptions, run_underlying_daily_build


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build the deduplicated underlying-daily table from processed options data."
    )
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
        "--overwrite",
        action="store_true",
        help="Overwrite the existing underlying-daily Parquet file if it already exists.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_project_config(
        project_root=Path(args.project_root),
        paths_file=Path(args.paths_file) if args.paths_file else None,
        research_file=Path(args.research_file) if args.research_file else None,
    )
    artifacts = run_underlying_daily_build(
        config,
        BuildUnderlyingsRunOptions(
            start_date=_optional_date(args.start_date),
            end_date=_optional_date(args.end_date),
            overwrite=args.overwrite,
        ),
    )
    print(
        f"Wrote {artifacts.row_count:,} underlying-daily rows from "
        f"{artifacts.source_partition_count} processed quote date(s) to {artifacts.output_file}"
    )
    print(f"QC JSON: {artifacts.qc_json_file}")
    print(f"QC Markdown: {artifacts.qc_markdown_file}")


def _optional_date(value: str | None) -> date | None:
    return date.fromisoformat(value) if value else None


if __name__ == "__main__":
    main()
