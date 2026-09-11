#!/usr/bin/env python3
"""
Replay-debug a single backtest trade against chart events.

Usage:
  python debug_trade.py --mint <mint_address> --trade-id <id>
  python debug_trade.py --mint <mint_address> --trade-id <id> --trades-json data/backtest/trades.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
ENGINE = ROOT / "backtest_engine"
sys.path.insert(0, str(ENGINE))

from execution import find_execution_event_index, simulate_execution  # noqa: E402
from loader import load_mint_json  # noqa: E402
from models import BacktestConfig, Signal  # noqa: E402
from reserves import pre_trade_reserves  # noqa: E402
from amm import AmmState, simulate_buy, simulate_sell  # noqa: E402


def find_mint_path(data_dir: Path, mint: str) -> Path | None:
    for path in data_dir.glob(f"*/{mint}.json"):
        return path
    for path in data_dir.glob(f"*/*{mint}*.json"):
        if mint in path.stem:
            return path
    return None


def load_trade(trades_json: Path, mint: str, trade_id: int) -> dict | None:
    root = json.loads(trades_json.read_text(encoding="utf-8"))
    for key, block in root.items():
        if mint not in key and not key.startswith(mint):
            continue
        for t in block.get("trades", []):
            if t["id"] == trade_id:
                return t
    for block in root.values():
        for t in block.get("trades", []):
            if t["id"] == trade_id:
                return t
    return None


def fmt_price(p: float | None) -> str:
    return f"{p:.10e}" if p else "n/a"


def main() -> int:
    parser = argparse.ArgumentParser(description="Debug a single backtest trade")
    parser.add_argument("--mint", required=True, help="Mint address (full or prefix)")
    parser.add_argument("--trade-id", type=int, required=True)
    parser.add_argument("--data-dir", default=str(ROOT / "data"))
    parser.add_argument("--trades-json", default=str(ROOT / "data" / "backtest" / "trades.json"))
    parser.add_argument("--config", default=str(ENGINE / "backtest_config.json"))
    args = parser.parse_args()

    cfg = BacktestConfig.from_dict(json.loads(Path(args.config).read_text(encoding="utf-8")))
    trade = load_trade(Path(args.trades_json), args.mint, args.trade_id)
    if not trade:
        print(f"Trade id {args.trade_id} not found for mint {args.mint}")
        return 1

    mint_path = find_mint_path(Path(args.data_dir), args.mint)
    if not mint_path:
        print(f"Mint JSON not found for {args.mint}")
        return 1

    ds = load_mint_json(mint_path)
    events = ds.events
    sig_index = {e.signature: i for i, e in enumerate(events)}

    print("=" * 60)
    print(f"TRADE #{trade['id']}  status={trade['status']}  mint={ds.mint[:20]}...")
    print(f"Buy reason:  {trade.get('strategy_reason', {}).get('buy', '')}")
    print(f"Sell reason: {trade.get('strategy_reason', {}).get('sell', '')}")
    print("=" * 60)

    buy = trade.get("buy", {})
    if buy:
        bt = buy.get("trigger_event", {})
        be = buy.get("execution", {})
        sig_i = sig_index.get(bt.get("signature", ""))
        exec_i = be.get("execution_event_index")
        if sig_i is not None and exec_i is None:
            exec_i = find_execution_event_index(events, sig_i, cfg.latency_ms)

        print("\n--- BUY ---")
        print(f"Signal event index:  {sig_i}")
        print(f"Signal timestamp:    {bt.get('timestamp')}")
        print(f"Signal price:        {fmt_price(bt.get('price'))}")
        print(f"Latency:             {be.get('latency_ms', cfg.latency_ms)} ms")
        print(f"Landing event index: {exec_i}")
        if exec_i is not None and exec_i < len(events):
            ev = events[exec_i]
            pre_vs, pre_vt = ev.virtual_sol_reserve, ev.virtual_token_reserve
            print(f"Landing timestamp:   {ev.timestamp}")
            print(f"Reserves at landing (post-event): vs={ev.virtual_sol_reserve:.6f} vt={ev.virtual_token_reserve:.2f}")
            sim = simulate_buy(AmmState(pre_vs, pre_vt), cfg.position_size_sol)
            print(f"Simulated fill:      {fmt_price(sim.fill_price)}")
            print(f"Simulated tokens:    {sim.token_amount:.4f}")
        print(f"Actual fill price:   {fmt_price(be.get('fill_price'))}")
        print(f"Tokens received:     {be.get('token_amount')}")
        print(f"Slippage:            {be.get('slippage_pct')}")
        print(f"Success:             {be.get('success')}")

    sell = trade.get("sell", {})
    if sell:
        st = sell.get("trigger_event", {})
        se = sell.get("execution", {})
        sig_i = sig_index.get(st.get("signature", ""))
        exec_i = se.get("execution_event_index")
        tokens = buy.get("execution", {}).get("token_amount", 0) if buy else 0
        if sig_i is not None and exec_i is None:
            exec_i = find_execution_event_index(events, sig_i, cfg.latency_ms)

        print("\n--- SELL ---")
        print(f"Signal event index:  {sig_i}")
        print(f"Signal timestamp:    {st.get('timestamp')}")
        print(f"Trigger price:       {fmt_price(st.get('price'))}")
        print(f"Latency:             {se.get('latency_ms', cfg.latency_ms)} ms")
        print(f"Landing event index: {exec_i}")
        if exec_i is not None and exec_i < len(events):
            ev = events[exec_i]
            print(f"Reserves at landing: vs={ev.virtual_sol_reserve:.6f} vt={ev.virtual_token_reserve:.2f}")
            sim = simulate_sell(AmmState(ev.virtual_sol_reserve, ev.virtual_token_reserve), tokens)
            net = sim.sol_amount * (1 - cfg.pump_fee)
            print(f"Simulated gross SOL: {sim.sol_amount:.6f}")
            print(f"Simulated net SOL:   {net:.6f}")
        print(f"Actual fill price:   {fmt_price(se.get('fill_price'))}")
        print(f"SOL received:        {se.get('sol_amount')}")
        print(f"Slippage:            {se.get('slippage_pct')}")

    pnl = trade.get("pnl", {})
    print("\n--- FINAL ---")
    print(f"Actual PNL:          {pnl.get('sol'):.6f} SOL  ({pnl.get('percent'):.2f}%)")

    if buy and sell and buy.get("execution") and sell.get("execution"):
        be, se = buy["execution"], sell["execution"]
        buy_cost = be["sol_amount"] + be.get("pump_fee_sol", 0) + be.get("tx_overhead_sol", 0)
        sell_net = se["sol_amount"] - se.get("pump_fee_sol", 0) - se.get("tx_overhead_sol", 0)
        expected = sell_net - buy_cost
        print(f"Expected PNL:        {expected:.6f} SOL")
        print(f"Match:               {abs(expected - (pnl.get('sol') or 0)) < 1e-9}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
