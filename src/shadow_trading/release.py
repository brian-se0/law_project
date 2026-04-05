from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from shutil import copy2, rmtree
from zipfile import ZIP_DEFLATED, ZipFile

from shadow_trading.case_study import build_case_study_paths
from shadow_trading.config import ProjectConfig
from shadow_trading.io import ensure_directories, write_text

PACKAGE_README_NAME = "PACKAGE_README.md"


def collect_assessment_files(config: ProjectConfig, project_root: Path) -> list[Path]:
    root = Path(project_root).resolve()
    case_paths = build_case_study_paths(config)
    required_files = [
        case_paths.case_event_file,
        case_paths.related_firms_file,
        case_paths.exact_contracts_file,
        case_paths.bucket_features_file,
        case_paths.matched_control_bucket_features_file,
        case_paths.abnormal_metrics_file,
        case_paths.control_matches_file,
        config.paths.outputs_dir / "tables" / "mdvn_exact_contract_window_summary.md",
        config.paths.outputs_dir / "memos" / "mdvn_watchlist_compliance_memo.md",
        config.paths.outputs_dir / "memos" / "mdvn_limitations.md",
        root / "docs" / "literature_review.md",
        root / "paper" / "mdvn_panuwat_case_study.md",
        root / "references.bib",
    ]
    missing_files = [path for path in required_files if not path.exists()]
    if missing_files:
        missing_labels = ", ".join(
            _display_path(path, config=config, project_root=root) for path in missing_files
        )
        raise FileNotFoundError(
            "Assessment bundle prerequisites are missing: "
            f"{missing_labels}. Run the case-study output steps before packaging."
        )

    files = _deduplicate_paths(
        [
            *required_files,
            *_existing_files(root / "docs", "*.md"),
            *_existing_files(root / "paper", "*.md"),
            *_existing_files(root / "configs", "*.yaml"),
            *_existing_files(config.paths.outputs_dir / "figures", "*"),
            *_existing_files(config.paths.outputs_dir / "tables", "*.md"),
            *_existing_files(config.paths.outputs_dir / "memos", "*.md"),
            *_existing_files(config.paths.qc_dir, "*"),
            *_existing_files(case_paths.case_dir, "*.parquet"),
            root / "README.md",
            root / "AGENTS.md",
            root / "Makefile",
        ]
    )
    return sorted(
        [path for path in files if path.exists()],
        key=lambda path: _bundle_relative_path(
            path.resolve(),
            config=config,
            project_root=root,
        ).as_posix(),
    )


def build_package_readme(*, commit_sha: str, created_at_iso: str, files: list[Path]) -> str:
    lines = [
        "# MDVN Assessment Package",
        "",
        f"Created: {created_at_iso}",
        f"Commit: {commit_sha}",
        "",
        "This bundle is synchronized to the commit above and packages the current MDVN case-study",
        "artifacts, paper materials, and supporting documentation.",
        "",
        "Included files:",
    ]
    for path in sorted(files, key=lambda candidate: candidate.as_posix()):
        lines.append(f"- {path.as_posix()}")
    lines.append("")
    return "\n".join(lines)


def package_assessment_bundle(
    config: ProjectConfig,
    project_root: Path,
    commit_sha: str,
) -> Path:
    root = Path(project_root).resolve()
    ensure_directories(config.paths)
    assessment_files = collect_assessment_files(config, root)
    created_at = datetime.now(UTC)
    bundle_stem = f"mdvn_assessment_package_{created_at.date().isoformat()}"
    dist_dir = root / "dist"
    dist_dir.mkdir(parents=True, exist_ok=True)
    bundle_dir = dist_dir / bundle_stem
    zip_path = dist_dir / f"{bundle_stem}.zip"

    _remove_existing_path(bundle_dir)
    if zip_path.exists():
        zip_path.unlink()

    bundle_relative_paths: list[Path] = []
    for source_path in assessment_files:
        relative_path = _bundle_relative_path(source_path, config=config, project_root=root)
        destination = bundle_dir / relative_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        copy2(source_path, destination)
        bundle_relative_paths.append(relative_path)

    readme_text = build_package_readme(
        commit_sha=commit_sha.strip(),
        created_at_iso=created_at.isoformat(),
        files=bundle_relative_paths,
    )
    write_text(bundle_dir / PACKAGE_README_NAME, readme_text)

    with ZipFile(zip_path, "w", compression=ZIP_DEFLATED) as archive:
        for path in sorted(bundle_dir.rglob("*")):
            if path.is_file():
                archive.write(path, arcname=path.relative_to(bundle_dir).as_posix())

    return zip_path


def _existing_files(directory: Path, pattern: str) -> list[Path]:
    if not directory.exists():
        return []
    return sorted(path.resolve() for path in directory.rglob(pattern) if path.is_file())


def _deduplicate_paths(paths: list[Path]) -> list[Path]:
    unique_paths: dict[Path, Path] = {}
    for path in paths:
        unique_paths[path.resolve()] = path.resolve()
    return list(unique_paths.values())


def _bundle_relative_path(path: Path, *, config: ProjectConfig, project_root: Path) -> Path:
    resolved_path = path.resolve()
    resolved_root = project_root.resolve()
    if resolved_path.is_relative_to(resolved_root):
        return resolved_path.relative_to(resolved_root)

    processed_root = config.paths.processed_dir.resolve()
    if resolved_path.is_relative_to(processed_root):
        return Path("data") / "processed" / resolved_path.relative_to(processed_root)

    outputs_root = config.paths.outputs_dir.resolve()
    if resolved_path.is_relative_to(outputs_root):
        return Path("outputs") / resolved_path.relative_to(outputs_root)

    raise ValueError(f"Cannot map {resolved_path} into the assessment bundle.")


def _display_path(path: Path, *, config: ProjectConfig, project_root: Path) -> str:
    try:
        return _bundle_relative_path(path, config=config, project_root=project_root).as_posix()
    except ValueError:
        return str(path.resolve())


def _remove_existing_path(path: Path) -> None:
    if not path.exists():
        return
    if path.is_dir():
        rmtree(path)
        return
    path.unlink()
