#!/usr/bin/env python3
"""Build viewer-ready candles under data/<wallet>/<mint>.json from crawler events."""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"

sys.path.insert(0, str(PROJECT_ROOT / "pumpfun_history"))
from config import wallet_mint_json_path  # noqa: E402
from utils import trim_events_to_bonding_curve  # noqa: E402


def bucket_time(ts: int, interval_sec: int) -> int:
    return (ts // interval_sec) * interval_sec


def build_candles(trades: list[dict[str, Any]], interval_sec: int) -> list[dict[str, Any]]:
    buckets: dict[int, dict[str, Any]] = defaultdict(
        lambda: {"open": 0.0, "high": 0.0, "low": 0.0, "close": 0.0, "volume": 0.0}
    )

    for trade in sorted(trades, key=lambda t: int(t["time"])):
        price = float(trade["price"])
        if price <= 0:
            continue
        bt = bucket_time(int(trade["time"]), interval_sec)
        bar = buckets[bt]
        if bar["open"] == 0.0 and bar["close"] == 0.0 and bar["volume"] == 0.0:
            bar["open"] = bar["high"] = bar["low"] = bar["close"] = price
        else:
            bar["high"] = max(bar["high"], price)
            bar["low"] = min(bar["low"], price)
            bar["close"] = price
        bar["volume"] += float(trade.get("sol") or 0)

    candles: list[dict[str, Any]] = []
    for ts in sorted(buckets):
        bar = buckets[ts]
        candles.append(
            {
                "time": ts,
                "open": bar["open"],
                "high": bar["high"],
                "low": bar["low"],
                "close": bar["close"],
                "volume": bar["volume"],
            }
        )

    for i in range(1, len(candles)):
        candles[i]["open"] = candles[i - 1]["close"]
        candles[i]["high"] = max(candles[i]["high"], candles[i]["open"])
        candles[i]["low"] = min(candles[i]["low"], candles[i]["open"])

    return candles


def normalize_trades(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    trades: list[dict[str, Any]] = []
    for event in events:
        trades.append(
            {
                "time": int(event["timestamp"]),
                "side": event["side"],
                "wallet": event["wallet"],
                "sol": float(event.get("sol_amount") or 0),
                "price": float(event.get("price") or 0),
            }
        )
    trades.sort(key=lambda t: t["time"])
    return trades


def convert_mint(wallet: str, mint: str, interval_sec: int) -> Path:
    source = wallet_mint_json_path(wallet, mint)
    payload = json.loads(source.read_text(encoding="utf-8"))
    events, curve_meta = trim_events_to_bonding_curve(payload.get("events") or [])
    trades = normalize_trades(events)
    candles = build_candles(trades, interval_sec)

    out = {
        "mint": mint,
        "wallet": wallet,
        "migrated": curve_meta["migrated"],
        "candles": candles,
        "trades": trades,
        "exported_at": datetime.now(UTC).isoformat(),
        "source": str(source.relative_to(PROJECT_ROOT)),
    }

    out_path = wallet_mint_json_path(wallet, mint)
    out_path.write_text(json.dumps(out, indent=2) + "\n", encoding="utf-8")
    return out_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build 1s viewer candles in wallet mint JSON.")
    parser.add_argument("wallet", help="Target wallet address")
    parser.add_argument("mint", help="Mint address")
    parser.add_argument("--interval", type=int, default=1, help="Candle interval seconds")
    args = parser.parse_args(argv)

    source = wallet_mint_json_path(args.wallet, args.mint)
    if not source.is_file():
        print(f"Missing {source}", file=sys.stderr)
        return 1

    out = convert_mint(args.wallet, args.mint, args.interval)
    print(f"Wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
