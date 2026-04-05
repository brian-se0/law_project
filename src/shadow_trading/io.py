from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from shadow_trading.config import PathsConfig


def ensure_directories(paths: PathsConfig) -> None:
    for directory in (
        paths.external_dir,
        paths.interim_dir,
        paths.processed_dir,
        paths.outputs_dir,
        paths.qc_dir,
    ):
        directory.mkdir(parents=True, exist_ok=True)


def discover_input_archives(root_dir: Path, pattern: str) -> list[Path]:
    if not root_dir.exists():
        return []
    return sorted(path.resolve() for path in root_dir.rglob(pattern) if path.is_file())


def write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n"
    path.write_text(serialized, encoding="utf-8")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
