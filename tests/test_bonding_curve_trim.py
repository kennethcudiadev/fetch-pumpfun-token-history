"""Tests for bonding-curve-only event trimming."""

from __future__ import annotations

from utils import is_bonding_curve_complete, trim_events_to_bonding_curve


def _event(progress: float | None, timestamp: int = 1) -> dict:
    payload = {"timestamp": timestamp, "side": "BUY", "bonding_curve_progress": progress}
    return payload


def test_not_migrated_keeps_all_events() -> None:
    events = [_event(0.5, 1), _event(0.85, 2)]
    trimmed, meta = trim_events_to_bonding_curve(events)
    assert len(trimmed) == 2
    assert meta["migrated"] is False


def test_migrated_trims_after_completion_event() -> None:
    events = [_event(0.9, 1), _event(1.0, 2), _event(0.5, 3), _event(0.4, 4)]
    trimmed, meta = trim_events_to_bonding_curve(events)
    assert len(trimmed) == 2
    assert meta["migrated"] is True
    assert meta["bonding_curve_event_count"] == 2
    assert trimmed[-1]["timestamp"] == 2


def test_is_bonding_curve_complete() -> None:
    assert is_bonding_curve_complete(1.0) is True
    assert is_bonding_curve_complete(0.99) is False
    assert is_bonding_curve_complete(None) is False
