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
from shadow_trading.pipelines import MakeOutputsRunOptions, run_case_output_build


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate figures and tables for the MDVN case-study outputs."
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
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_project_config(
        project_root=Path(args.project_root),
        paths_file=Path(args.paths_file) if args.paths_file else None,
        research_file=Path(args.research_file) if args.research_file else None,
    )
    artifacts = run_case_output_build(config, MakeOutputsRunOptions())
    print("Figures:")
    for label, path in artifacts.figure_paths.items():
        print(f"  {label}: {path}")
    print("Tables:")
    for label, path in artifacts.table_paths.items():
        print(f"  {label}: {path}")


if __name__ == "__main__":
    main()
