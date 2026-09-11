"""Load project-root config.json for CLI tools and pumpfun_history."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = PROJECT_ROOT / "config.json"


def load_project_config() -> dict[str, Any]:
    if not CONFIG_PATH.is_file():
        return {}
    with CONFIG_PATH.open(encoding="utf-8") as handle:
        return json.load(handle)


def get_target_wallet(cfg: dict[str, Any] | None = None) -> str:
    cfg = cfg if cfg is not None else load_project_config()
    return str(cfg.get("target_wallet") or "").strip()


def get_lookback_hours(cfg: dict[str, Any] | None = None) -> int:
    cfg = cfg if cfg is not None else load_project_config()
    hours = cfg.get("lookback_hours", 24)
    return max(1, int(hours))


def get_helius_keys(cfg: dict[str, Any] | None = None) -> list[str]:
    cfg = cfg if cfg is not None else load_project_config()
    raw = cfg.get("helius_keys") or []
    return [str(key).strip() for key in raw if str(key).strip()]


def get_log_level(cfg: dict[str, Any] | None = None) -> str:
    cfg = cfg if cfg is not None else load_project_config()
    return str(cfg.get("log_level") or "INFO").upper()


def get_viewer_port(cfg: dict[str, Any] | None = None) -> int:
    cfg = cfg if cfg is not None else load_project_config()
    return max(1, int(cfg.get("viewer_port") or 8080))
