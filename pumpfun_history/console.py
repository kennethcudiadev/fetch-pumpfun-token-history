"""Clean console logging and compact progress output."""

from __future__ import annotations

import logging
import sys

from config import LOG_LEVEL


class _CleanFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        message = record.getMessage()
        if record.levelno >= logging.ERROR:
            return f"ERROR  {message}"
        if record.levelno >= logging.WARNING:
            return f"WARN   {message}"
        return message


def setup_console_logging() -> None:
    """Configure minimal stdout logging for interactive and PM2 use."""
    level = getattr(logging, LOG_LEVEL.upper(), logging.INFO)
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(level)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    handler.setFormatter(_CleanFormatter())
    root.addHandler(handler)

    for name in ("aiohttp", "asyncio"):
        logging.getLogger(name).setLevel(logging.WARNING)


def log_progress(message: str) -> None:
    """Emit a single-line progress update."""
    logging.getLogger("progress").info(message)


def log_start(title: str, detail: str = "") -> None:
    line = f"=== {title} ==="
    if detail:
        line = f"{line}  {detail}"
    log_progress(line)


def log_done(message: str) -> None:
    log_progress(f"done  {message}")
