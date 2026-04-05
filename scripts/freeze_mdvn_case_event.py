from __future__ import annotations

# ruff: noqa: E402

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from shadow_trading.config import load_project_config
from shadow_trading.pipelines import FreezeCaseRunOptions, run_case_event_freeze


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Freeze the canonical MDVN case event from the SEC event universe."
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
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite an existing frozen case event output.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_project_config(
        project_root=Path(args.project_root),
        paths_file=Path(args.paths_file) if args.paths_file else None,
        research_file=Path(args.research_file) if args.research_file else None,
    )
    artifacts = run_case_event_freeze(
        config,
        FreezeCaseRunOptions(overwrite=args.overwrite),
    )
    print(f"Wrote {artifacts.row_count:,} frozen case-event row")
    print(f"Case event: {artifacts.case_event_file}")
    print(f"QC JSON: {artifacts.qc_json_file}")
    print(f"QC Markdown: {artifacts.qc_markdown_file}")


if __name__ == "__main__":
    main()
