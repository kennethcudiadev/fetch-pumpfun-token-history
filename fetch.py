#!/usr/bin/env python3
"""
Step 2 — Fetch bonding-curve event history for every mint in a wallet manifest.

Usage:
  python fetch.py                  # uses target_wallet from config.json
  python fetch.py <WALLET>

Reads:
  data/<WALLET>.json

Writes:
  data/<WALLET>/<MINT>.json
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "pumpfun_history"))

from config import wallet_manifest_path  # noqa: E402
from main import main as fetch_main  # noqa: E402
from project_config import CONFIG_PATH, get_target_wallet  # noqa: E402


def _usage() -> None:
    print(
        "Usage:\n"
        "  python fetch.py\n"
        "  python fetch.py <WALLET>\n"
        f"\nDefault wallet from {CONFIG_PATH.name}: target_wallet",
        file=sys.stderr,
    )


if __name__ == "__main__":
    wallet = sys.argv[1].strip() if len(sys.argv) > 1 else get_target_wallet()
    if not wallet:
        _usage()
        print("\nError: set target_wallet in config.json", file=sys.stderr)
        sys.exit(1)

    manifest = wallet_manifest_path(wallet)
    if not manifest.is_file():
        print(f"Error: missing {manifest} — run scan.py first", file=sys.stderr)
        sys.exit(1)

    sys.exit(fetch_main(["--file", str(manifest)]))
