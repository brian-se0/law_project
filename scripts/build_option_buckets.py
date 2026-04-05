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
from shadow_trading.pipelines import BuildBucketsRunOptions, run_case_bucket_build


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build exact-contract and firm-day bucket features for the MDVN case study."
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
        help="Overwrite existing case-study bucket outputs if they already exist.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_project_config(
        project_root=Path(args.project_root),
        paths_file=Path(args.paths_file) if args.paths_file else None,
        research_file=Path(args.research_file) if args.research_file else None,
    )
    artifacts = run_case_bucket_build(
        config,
        BuildBucketsRunOptions(overwrite=args.overwrite),
    )
    print(
        f"Wrote {artifacts.related_firm_row_count:,} related-firm rows, "
        f"{artifacts.exact_contract_row_count:,} exact-contract rows, and "
        f"{artifacts.bucket_row_count:,} bucket rows"
    )
    print(f"Related firms: {artifacts.related_firms_file}")
    print(f"Exact contracts: {artifacts.exact_contracts_file}")
    print(f"Bucket features: {artifacts.bucket_features_file}")
    print(f"QC JSON: {artifacts.qc_json_file}")
    print(f"QC Markdown: {artifacts.qc_markdown_file}")


if __name__ == "__main__":
    main()
