#!/usr/bin/env python3
"""
Pump.fun bonding-curve backtester (isolated from crawler).

Usage:
  python backtest.py                              # ALL mint JSON files under data/<wallet>/
  python backtest.py --wallet <WALLET>            # one wallet folder only
  python backtest.py --mint <path/to/mint.json>   # single mint file
  python backtest.py --limit 10                   # first N mints (with or without --wallet)
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent
ENGINE_DIR = ROOT / "backtest_engine"
sys.path.insert(0, str(ENGINE_DIR))

from exporter import export_results  # noqa: E402
from loader import discover_mint_datasets, iter_mint_files, load_mint_json  # noqa: E402
from progress import Progress  # noqa: E402
from logger import setup_logger  # noqa: E402
from simulator import load_config, load_rules, run_backtest  # noqa: E402

DATA_DIR = ROOT / "data"
OUTPUT_DIR = DATA_DIR / "backtest"
LOGS_DIR = ROOT / "logs"
DEFAULT_BT_CONFIG = ENGINE_DIR / "backtest_config.json"
DEFAULT_RULE_CONFIG = ENGINE_DIR / "rule_config.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pump.fun bonding-curve backtester",
        epilog="With no --wallet and no --mint, every data/<wallet>/<mint>.json file is processed.",
    )
    parser.add_argument("--wallet", help="Limit to one wallet folder under data/ (default: all wallets)")
    parser.add_argument("--mint", help="Single mint JSON file path")
    parser.add_argument("--data-dir", default=str(DATA_DIR), help="Data root (default: data/)")
    parser.add_argument("--output-dir", default=str(OUTPUT_DIR), help="Output dir (default: data/backtest/)")
    parser.add_argument("--backtest-config", default=str(DEFAULT_BT_CONFIG))
    parser.add_argument("--rule-config", default=str(DEFAULT_RULE_CONFIG))
    parser.add_argument("--limit", type=int, default=0, help="Max mints to process (0 = all)")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    log = setup_logger(LOGS_DIR)

    data_dir = Path(args.data_dir)
    output_dir = Path(args.output_dir)
    bt_config = load_config(Path(args.backtest_config))
    rule_config = load_rules(Path(args.rule_config))
    data_wallet = args.wallet
    if not data_wallet:
        gate_folder = data_dir / (bt_config.target_wallet or "")
        if bt_config.target_wallet and gate_folder.is_dir():
            data_wallet = bt_config.target_wallet
        else:
            folders = [
                p.name
                for p in sorted(data_dir.iterdir())
                if p.is_dir() and p.name not in {"backtest"} and not p.name.startswith(".")
            ]
            data_wallet = folders[0] if folders else None
            if data_wallet:
                log.info(
                    "Data folder for gate wallet %s missing; loading %s; gate stays %s",
                    (bt_config.target_wallet or "")[:12],
                    data_wallet[:12],
                    (bt_config.target_wallet or "")[:12],
                )
    log.info("Gate wallet %s  data folder %s", (bt_config.target_wallet or "")[:12], (data_wallet or "")[:12])

    log.info("Backtest starting")
    log.info("Execution config: %s", args.backtest_config)
    log.info("Rule config: %s", args.rule_config)

    if args.mint:
        mint_path = Path(args.mint)
        if not mint_path.is_file():
            log.error("Mint file not found: %s", mint_path)
            return 1
        datasets = [load_mint_json(mint_path)]
    else:
        scope = f"folder {data_wallet}" if data_wallet else "ALL wallets under data/"
        log.info("Scope: %s", scope)
        log.info("Discovering mint files in %s", data_dir)
        mint_paths = list(iter_mint_files(data_dir, wallet=data_wallet))
        wallet_dirs = len({p.parent.name for p in mint_paths})
        log.info("Found %d mint file(s) across %d wallet folder(s)", len(mint_paths), wallet_dirs)
        if args.limit > 0:
            mint_paths = mint_paths[: args.limit]
            log.info("Limit applied: processing first %d mint(s)", len(mint_paths))
        loading = Progress(len(mint_paths), "load")
        datasets = discover_mint_datasets(
            data_dir,
            wallet=data_wallet,
            paths=mint_paths,
            on_tick=loading.tick,
        )
        loading.close(f"loaded {len(datasets)}")

    if not datasets:
        log.error("No mint datasets found")
        return 1

    log.info("Loaded %d mint dataset(s)", len(datasets))

    trades_by_mint, summary = run_backtest(datasets, bt_config, rule_config)

    meta = {
        "run_at": datetime.now(UTC).isoformat(),
        "wallet": args.wallet,
        "scope": args.wallet or "all",
        "mint_count": len(datasets),
        "backtest_config": json.loads(Path(args.backtest_config).read_text(encoding="utf-8")),
        "rule_config": json.loads(Path(args.rule_config).read_text(encoding="utf-8")),
    }

    summarize_path, trades_path = export_results(output_dir, summary, trades_by_mint, meta=meta)
    log.info("Wrote %s", summarize_path)
    log.info("Wrote %s", trades_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
