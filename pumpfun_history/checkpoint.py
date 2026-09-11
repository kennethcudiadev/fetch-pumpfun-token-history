"""Graceful checkpoint flushing on interrupt or sudden stop."""

from __future__ import annotations

import asyncio
import logging
import signal
import sys
from collections.abc import Awaitable, Callable

logger = logging.getLogger(__name__)

FlushCallback = Callable[[], Awaitable[None]]

_flush_callbacks: list[FlushCallback] = []
_interrupt_requested = False
_installed = False


def interrupt_requested() -> bool:
    return _interrupt_requested


def register_flush(callback: FlushCallback) -> None:
    if callback not in _flush_callbacks:
        _flush_callbacks.append(callback)


def clear_flush_callbacks() -> None:
    _flush_callbacks.clear()


async def flush_all() -> None:
    for callback in reversed(_flush_callbacks):
        try:
            await callback()
        except Exception as exc:
            logger.error("checkpoint flush failed: %s", exc)


def install_signal_handlers() -> None:
    global _installed
    if _installed:
        return
    _installed = True

    def _handler(signum: int, _frame: object) -> None:
        global _interrupt_requested
        if _interrupt_requested:
            sys.exit(130)
        _interrupt_requested = True
        logger.warning("stop signal received — saving checkpoint…")

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _handler)
        except (ValueError, OSError):
            pass


class CheckpointScope:
    async def __aenter__(self) -> CheckpointScope:
        install_signal_handlers()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        await flush_all()
        if exc_type is KeyboardInterrupt:
            return True
        return False
