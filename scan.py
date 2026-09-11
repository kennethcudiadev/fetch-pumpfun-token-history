#!/usr/bin/env python3
"""
Step 1 — Scan a wallet for Pump.fun mints traded in a time window.

Usage:
  python scan.py                  # uses target_wallet + lookback_hours from config.json
  python scan.py <WALLET>         # wallet + default hours from config.json
  python scan.py <WALLET> <HOURS>

Output:
  data/<WALLET>.json
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "pumpfun_history"))

from project_config import CONFIG_PATH, get_lookback_hours, get_target_wallet  # noqa: E402
from trader_mints import main as scan_main  # noqa: E402


def _usage() -> None:
    print(
        "Usage:\n"
        "  python scan.py\n"
        "  python scan.py <WALLET>\n"
        "  python scan.py <WALLET> <HOURS>\n"
        f"\nDefaults from {CONFIG_PATH.name}: target_wallet, lookback_hours",
        file=sys.stderr,
    )


if __name__ == "__main__":
    args = sys.argv[1:]

    if len(args) >= 2:
        sys.exit(scan_main([args[0], args[1]]))
    if len(args) == 1:
        sys.exit(scan_main([args[0], str(get_lookback_hours())]))

    wallet = get_target_wallet()
    if not wallet:
        _usage()
        print("\nError: set target_wallet in config.json", file=sys.stderr)
        sys.exit(1)

    sys.exit(scan_main([wallet, str(get_lookback_hours())]))
