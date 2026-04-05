from __future__ import annotations

# ruff: noqa: E402

import argparse
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from shadow_trading.config import load_project_config
from shadow_trading.release import package_assessment_bundle


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create a commit-synced MDVN assessment bundle.")
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
    project_root = Path(args.project_root).resolve()
    config = load_project_config(
        project_root=project_root,
        paths_file=Path(args.paths_file) if args.paths_file else None,
        research_file=Path(args.research_file) if args.research_file else None,
    )
    commit_sha = subprocess.check_output(
        ["git", "rev-parse", "HEAD"],
        cwd=project_root,
        text=True,
    ).strip()
    bundle_path = package_assessment_bundle(
        config=config,
        project_root=project_root,
        commit_sha=commit_sha,
    )
    print(f"Packaged assessment bundle for commit {commit_sha}")
    print(f"Bundle: {bundle_path}")


if __name__ == "__main__":
    main()
