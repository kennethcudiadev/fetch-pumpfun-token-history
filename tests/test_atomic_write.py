"""Tests for atomic JSON writes."""

from __future__ import annotations

import os
from pathlib import Path

from utils import atomic_write_json, atomic_write_text


def test_atomic_write_json(tmp_path: Path) -> None:
    path = tmp_path / "sample.json"
    atomic_write_json(path, {"hello": "world"})
    assert path.read_text(encoding="utf-8").startswith("{\n")
    assert '"hello": "world"' in path.read_text(encoding="utf-8")


def test_atomic_write_text_overwrites_existing(tmp_path: Path) -> None:
    path = tmp_path / "note.txt"
    atomic_write_text(path, "v1\n")
    atomic_write_text(path, "v2\n")
    assert path.read_text(encoding="utf-8") == "v2\n"


def test_atomic_write_retries_when_replace_blocked(tmp_path: Path, monkeypatch) -> None:
    path = tmp_path / "locked.json"
    path.write_text('{"old": true}\n', encoding="utf-8")
    calls = {"replace": 0}

    real_replace = os.replace

    def flaky_replace(src: str, dst: str) -> None:
        calls["replace"] += 1
        if calls["replace"] == 1:
            raise PermissionError("simulated lock")
        real_replace(src, dst)

    monkeypatch.setattr(os, "replace", flaky_replace)
    atomic_write_json(path, {"new": True})
    assert path.read_text(encoding="utf-8").startswith('{\n  "new": true')
