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
from shadow_trading.pipelines import BuildEventsRunOptions, run_sec_event_universe_build


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build the SEC M&A event universe for option-underlying issuers."
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
        "--limit-companies",
        type=int,
        default=None,
        help="Optional number of matched SEC companies to scan.",
    )
    parser.add_argument(
        "--symbols",
        nargs="*",
        default=None,
        help="Optional list of specific underlying symbols to scan.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing candidate/event outputs if they already exist.",
    )
    parser.add_argument(
        "--refresh-cache",
        action="store_true",
        help="Refresh cached SEC JSON/text files instead of reusing local copies.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_project_config(
        project_root=Path(args.project_root),
        paths_file=Path(args.paths_file) if args.paths_file else None,
        research_file=Path(args.research_file) if args.research_file else None,
    )
    artifacts = run_sec_event_universe_build(
        config,
        BuildEventsRunOptions(
            start_date=_optional_date(args.start_date),
            end_date=_optional_date(args.end_date),
            limit_companies=args.limit_companies,
            symbols=tuple(args.symbols) if args.symbols else None,
            overwrite=args.overwrite,
            refresh_cache=args.refresh_cache,
        ),
    )
    print(
        f"Wrote {artifacts.candidate_row_count:,} SEC candidate filings and "
        f"{artifacts.event_row_count:,} deduplicated events"
    )
    print(f"Candidates: {artifacts.candidates_file}")
    print(f"Events: {artifacts.events_file}")
    print(f"QC JSON: {artifacts.qc_json_file}")
    print(f"QC Markdown: {artifacts.qc_markdown_file}")


def _optional_date(value: str | None) -> date | None:
    return date.fromisoformat(value) if value else None


if __name__ == "__main__":
    main()
