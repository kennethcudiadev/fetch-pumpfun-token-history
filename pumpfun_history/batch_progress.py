"""Batch crawl progress for safe resume of main.py --file runs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from config import BATCH_PROGRESS_PATH, STATE_DIR
from utils import atomic_write_json


def save_batch_progress(payload: dict[str, Any]) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    atomic_write_json(BATCH_PROGRESS_PATH, payload)


def load_batch_progress(manifest_file: str) -> dict[str, Any] | None:
    if not BATCH_PROGRESS_PATH.exists():
        return None
    data = json.loads(BATCH_PROGRESS_PATH.read_text(encoding="utf-8"))
    if data.get("manifest_file") != str(Path(manifest_file).resolve()):
        return None
    if data.get("status") == "complete":
        return None
    return data


def clear_batch_progress() -> None:
    if BATCH_PROGRESS_PATH.exists():
        BATCH_PROGRESS_PATH.unlink()
