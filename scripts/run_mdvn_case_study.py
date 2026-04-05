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
from shadow_trading.pipelines import RunCaseStudyRunOptions, run_mdvn_case_study


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the MDVN -> INCY case-study pipeline and write summary outputs."
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
        help="Overwrite existing case-study outputs if they already exist.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_project_config(
        project_root=Path(args.project_root),
        paths_file=Path(args.paths_file) if args.paths_file else None,
        research_file=Path(args.research_file) if args.research_file else None,
    )
    artifacts = run_mdvn_case_study(
        config,
        RunCaseStudyRunOptions(overwrite=args.overwrite),
    )
    print(
        f"Wrote {artifacts.abnormal_metric_row_count:,} abnormal-summary rows and "
        f"{artifacts.control_match_row_count:,} matched-control rows"
    )
    print(f"Abnormal metrics: {artifacts.abnormal_metrics_file}")
    print(f"Control matches: {artifacts.control_matches_file}")
    print(f"QC JSON: {artifacts.qc_json_file}")
    print(f"QC Markdown: {artifacts.qc_markdown_file}")


if __name__ == "__main__":
    main()
