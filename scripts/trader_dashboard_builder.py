#!/usr/bin/env python3
"""Build trader dashboard snapshots from wallet-organized mint JSON."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config.json"

sys.path.insert(0, str(PROJECT_ROOT / "pumpfun_history"))
from config import wallet_mint_dir  # noqa: E402
from utils import trim_events_to_bonding_curve  # noqa: E402


@dataclass
class Position:
    mint: str
    symbol: str = ""
    token_qty: float = 0.0
    cost_sol: float = 0.0
    first_buy_time: int = 0
    last_trade_time: int = 0
    last_price: float = 0.0

    @property
    def entry_price(self) -> float | None:
        if self.token_qty <= 1e-12:
            return None
        return self.cost_sol / self.token_qty

    @property
    def side(self) -> str:
        return "HOLD" if self.token_qty > 1e-9 else "FLAT"

    def apply_trade(
        self,
        *,
        side: str,
        token_amount: float,
        sol_amount: float,
        price: float,
        timestamp: int,
    ) -> None:
        self.last_trade_time = max(self.last_trade_time, timestamp)
        self.last_price = price
        if side == "BUY":
            if self.first_buy_time == 0:
                self.first_buy_time = timestamp
            self.token_qty += token_amount
            self.cost_sol += sol_amount
        elif side == "SELL" and self.token_qty > 1e-12:
            sell_ratio = min(1.0, token_amount / self.token_qty)
            self.cost_sol *= max(0.0, 1.0 - sell_ratio)
            self.token_qty = max(0.0, self.token_qty - token_amount)

    def to_market(self) -> dict[str, Any]:
        entry = self.entry_price
        current = self.last_price
        pnl_percent = None
        if entry and entry > 0 and current > 0:
            pnl_percent = round(((current - entry) / entry) * 100.0, 2)
        return {
            "mint": self.mint,
            "symbol": self.symbol or self.mint[:6],
            "first_buy_time": self.first_buy_time,
            "last_trade_time": self.last_trade_time,
            "entry_price": entry,
            "current_price": current,
            "pnl_percent": pnl_percent,
            "side": self.side,
        }


def load_json(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def events_from_mint_file(payload: dict[str, Any]) -> list[dict[str, Any]]:
    if "events" in payload:
        events, _ = trim_events_to_bonding_curve(payload["events"])
        return events
    return []


def iter_wallet_mint_files(wallet: str) -> list[Path]:
    directory = wallet_mint_dir(wallet)
    if not directory.is_dir():
        return []
    return sorted(directory.glob("*.json"))


def wallet_events(wallet: str, mint_files: list[Path]) -> dict[str, list[dict[str, Any]]]:
    by_mint: dict[str, list[dict[str, Any]]] = {}
    wallet_lower = wallet.lower()
    for path in mint_files:
        payload = load_json(path)
        if not payload:
            continue
        mint = payload.get("mint") or path.stem
        for event in events_from_mint_file(payload):
            if str(event.get("wallet", "")).lower() != wallet_lower:
                continue
            event = dict(event)
            event.setdefault("mint", mint)
            by_mint.setdefault(mint, []).append(event)
    for mint in by_mint:
        by_mint[mint].sort(key=lambda e: int(e.get("timestamp") or 0))
    return by_mint


def build_positions(wallet: str, mint_files: list[Path]) -> list[dict[str, Any]]:
    positions: dict[str, Position] = {}
    for mint, events in wallet_events(wallet, mint_files).items():
        pos = Position(mint=mint, symbol=mint[:6])
        for event in events:
            token_amount = float(event.get("token_amount") or 0)
            sol_amount = float(event.get("sol_amount") or 0)
            price = float(event.get("price") or 0)
            if token_amount <= 0 and sol_amount > 0 and price > 0:
                token_amount = sol_amount / price
            pos.apply_trade(
                side=str(event.get("side", "")).upper(),
                token_amount=token_amount,
                sol_amount=sol_amount,
                price=price,
                timestamp=int(event.get("timestamp") or 0),
            )
        if pos.last_trade_time > 0:
            positions[mint] = pos
    markets = [p.to_market() for p in positions.values()]
    markets.sort(key=lambda m: int(m.get("last_trade_time") or 0), reverse=True)
    return markets


def write_trader_json(wallet: str, markets: list[dict[str, Any]]) -> Path:
    out_path = DATA_DIR / wallet / "_dashboard.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"wallet": wallet, "updated": datetime.now(UTC).isoformat(), "markets": markets}
    out_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return out_path


def load_config(config_path: Path) -> list[dict[str, str]]:
    payload = load_json(config_path)
    if not payload:
        raise FileNotFoundError(f"Missing {config_path}")
    return [t for t in payload.get("tracked_traders") or [] if t.get("address")]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build trader dashboard from wallet mint JSON.")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH)
    parser.add_argument("--wallet", help="Build dashboard for one wallet directory")
    args = parser.parse_args(argv)

    wallets = [args.wallet] if args.wallet else [t["address"] for t in load_config(args.config)]
    if not wallets:
        print("No wallets to process", file=sys.stderr)
        return 1

    for wallet in wallets:
        mint_files = iter_wallet_mint_files(wallet)
        markets = build_positions(wallet, mint_files)
        out = write_trader_json(wallet, markets)
        print(f"{wallet[:8]}…: {len(markets)} markets -> {out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
