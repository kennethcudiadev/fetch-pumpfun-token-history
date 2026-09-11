"""Discover unique Pump.fun mints traded by a wallet within a time window."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from config import (
    DEFAULT_TRADER_LOOKBACK_HOURS,
    GTFORADDRESS_PAGE_LIMIT,
    HELIUS_KEYS,
    wallet_manifest_path,
)
from checkpoint import CheckpointScope, clear_flush_callbacks, interrupt_requested, register_flush
from console import log_done, log_progress, log_start, setup_console_logging
from helius_client import HeliusClient, HeliusRpcError
from pumpfun_decoder import (
    decode_trade_events_from_transaction,
    extract_transaction_signature,
    is_pumpfun_transaction,
    normalize_transaction_record,
)
from rate_limiter import RateLimiterPool
from utils import validate_wallet_address

logger = logging.getLogger(__name__)

SCAN_BACKEND = "helius_bulk"
IGNORED_MINTS = {
    "SOL",
    "So11111111111111111111111111111111111111112",
}


@dataclass
class MintStats:
    mint: str
    first_trade_time: int = 0
    last_trade_time: int = 0
    buy_count: int = 0
    sell_count: int = 0

    def record(self, timestamp: int, side: str) -> None:
        if self.first_trade_time == 0 or timestamp < self.first_trade_time:
            self.first_trade_time = timestamp
        if timestamp > self.last_trade_time:
            self.last_trade_time = timestamp
        if side == "BUY":
            self.buy_count += 1
        elif side == "SELL":
            self.sell_count += 1

    def to_dict(self) -> dict[str, Any]:
        return {
            "mint": self.mint,
            "first_trade_time": self.first_trade_time,
            "last_trade_time": self.last_trade_time,
            "buy_count": self.buy_count,
            "sell_count": self.sell_count,
        }


@dataclass
class TraderScanState:
    wallet: str
    hours: int
    from_timestamp: int
    to_timestamp: int
    output_path: Path
    mint_stats: dict[str, MintStats]
    signatures_collected: int = 0
    transactions_scanned: int = 0
    pumpfun_trades: int = 0
    credits_used: int = 0
    pagination_token: str | None = None
    processed_signatures: set[str] | None = None
    phase: str = "scanning_transactions"
    status: str = "in_progress"

    def __post_init__(self) -> None:
        if self.processed_signatures is None:
            self.processed_signatures = set()

    def to_payload(self) -> dict[str, Any]:
        mints = sorted(
            self.mint_stats.values(),
            key=lambda item: item.last_trade_time,
            reverse=True,
        )
        return {
            "wallet": self.wallet,
            "hours": self.hours,
            "from_timestamp": self.from_timestamp,
            "to_timestamp": self.to_timestamp,
            "status": self.status,
            "phase": self.phase,
            "scan_backend": SCAN_BACKEND,
            "signatures_collected": self.signatures_collected,
            "transactions_scanned": self.transactions_scanned,
            "pumpfun_trades": self.pumpfun_trades,
            "unique_mints": len(mints),
            "credits_used": self.credits_used,
            "pagination_token": self.pagination_token,
            "exported_at": datetime.now(UTC).isoformat(),
            "mints": [item.to_dict() for item in mints],
        }


def filter_signatures_for_window(
    batch: list[dict[str, Any]],
    from_timestamp: int,
) -> tuple[list[tuple[str, int, int | None]], bool]:
    entries: list[tuple[str, int, int | None]] = []
    stop = False
    for item in batch:
        if item.get("err") is not None:
            continue
        signature = item.get("signature")
        if not signature:
            continue
        block_time = item.get("blockTime")
        if block_time is not None and block_time < from_timestamp:
            stop = True
            break
        entries.append((signature, item.get("slot", 0), block_time))
    return entries, stop


def aggregate_wallet_trades(
    mint_stats: dict[str, MintStats],
    side: str,
    mint: str,
    timestamp: int,
) -> None:
    if mint not in mint_stats:
        mint_stats[mint] = MintStats(mint=mint)
    mint_stats[mint].record(timestamp, side)


def build_trader_result(
    wallet: str,
    hours: int,
    from_timestamp: int,
    to_timestamp: int,
    mint_stats: dict[str, MintStats],
    transactions_scanned: int,
    pumpfun_trades: int,
    credits_used: int = 0,
) -> dict[str, Any]:
    state = TraderScanState(
        wallet=wallet,
        hours=hours,
        from_timestamp=from_timestamp,
        to_timestamp=to_timestamp,
        output_path=wallet_manifest_path(wallet),
        mint_stats=mint_stats,
        transactions_scanned=transactions_scanned,
        pumpfun_trades=pumpfun_trades,
        credits_used=credits_used,
        phase="complete",
        status="complete",
    )
    return state.to_payload()


def save_trader_json(payload: dict[str, Any], path: Path) -> None:
    from utils import atomic_write_json

    atomic_write_json(path, payload)


def persist_scan_state(state: TraderScanState) -> None:
    save_trader_json(state.to_payload(), state.output_path)


def wallet_pump_trades(
    tx: dict[str, Any],
    wallet: str,
) -> list[tuple[str, str, int]]:
    """Mint/side pairs for this wallet from one Helius enhanced Pump.fun tx."""
    if tx.get("transactionError") or tx.get("error"):
        return []

    timestamp = int(tx.get("timestamp") or 0)
    trades: list[tuple[str, str, int]] = []
    seen: set[tuple[str, str]] = set()

    def add(mint: str | None, side: str) -> None:
        if not mint or mint in IGNORED_MINTS:
            return
        key = (mint, side)
        if key in seen:
            return
        seen.add(key)
        trades.append((mint, side, timestamp))

    swap = (tx.get("events") or {}).get("swap") or {}
    for item in swap.get("tokenOutputs") or []:
        if item.get("userAccount") == wallet:
            add(item.get("mint"), "BUY")
    for item in swap.get("tokenInputs") or []:
        if item.get("userAccount") == wallet:
            add(item.get("mint"), "SELL")
    if trades:
        return trades

    for transfer in tx.get("tokenTransfers") or []:
        mint = transfer.get("mint")
        if transfer.get("toUserAccount") == wallet:
            add(mint, "BUY")
        elif transfer.get("fromUserAccount") == wallet:
            add(mint, "SELL")
    return trades


def load_scan_state(
    output_path: Path,
    wallet: str,
    hours: int,
    from_timestamp: int,
    to_timestamp: int,
) -> TraderScanState | None:
    """Resume an interrupted enhanced scan. Old full-history checkpoints are ignored."""
    if not output_path.exists():
        return None

    with output_path.open("r", encoding="utf-8") as handle:
        head = handle.read(4096)
    if f'"scan_backend": "{SCAN_BACKEND}"' not in head:
        return None

    data = json.loads(output_path.read_text(encoding="utf-8"))
    if data.get("wallet") != wallet or data.get("hours") != hours:
        return None
    if data.get("status") == "complete":
        return None

    # Resume with the original scan window frozen at first run.
    saved_from = data.get("from_timestamp", from_timestamp)
    saved_to = data.get("to_timestamp", to_timestamp)

    mint_stats: dict[str, MintStats] = {}
    for entry in data.get("mints", []):
        mint_stats[entry["mint"]] = MintStats(
            mint=entry["mint"],
            first_trade_time=entry.get("first_trade_time", 0),
            last_trade_time=entry.get("last_trade_time", 0),
            buy_count=entry.get("buy_count", 0),
            sell_count=entry.get("sell_count", 0),
        )

    return TraderScanState(
        wallet=wallet,
        hours=hours,
        from_timestamp=saved_from,
        to_timestamp=saved_to,
        output_path=output_path,
        mint_stats=mint_stats,
        signatures_collected=data.get("signatures_collected", 0),
        transactions_scanned=data.get("transactions_scanned", 0),
        pumpfun_trades=data.get("pumpfun_trades", 0),
        credits_used=data.get("credits_used", 0),
        pagination_token=data.get("pagination_token"),
        status="in_progress",
    )


async def scan_trader_bulk(
    client: HeliusClient,
    wallet: str,
    from_timestamp: int,
    to_timestamp: int,
    scan_state: TraderScanState,
) -> None:
    """Walk wallet history and keep only Pump.fun trades made by this wallet."""
    page_num = 0

    async for records, next_token, page_credits in client.iter_transaction_pages(
        wallet,
        sort_order="desc",
        limit=GTFORADDRESS_PAGE_LIMIT,
        filters={
            "blockTime": {"gte": from_timestamp, "lte": to_timestamp},
            "status": "succeeded",
            "tokenAccounts": "balanceChanged",
        },
        start_token=scan_state.pagination_token,
    ):
        if interrupt_requested():
            scan_state.status = "interrupted"
            persist_scan_state(scan_state)
            return

        page_num += 1
        scan_state.credits_used += page_credits
        scan_state.signatures_collected += len(records)

        for record in records:
            signature = extract_transaction_signature(record) or ""
            scan_state.transactions_scanned += 1
            tx = normalize_transaction_record(record)
            if not tx or not is_pumpfun_transaction(tx):
                continue

            block_time = record.get("blockTime")
            for event in decode_trade_events_from_transaction(tx, signature):
                if event.wallet != wallet:
                    continue
                timestamp = event.timestamp
                if block_time is not None and timestamp <= 0:
                    timestamp = block_time
                if timestamp < from_timestamp or timestamp > to_timestamp:
                    continue
                aggregate_wallet_trades(
                    scan_state.mint_stats,
                    event.side,
                    event.mint,
                    timestamp,
                )
                scan_state.pumpfun_trades += 1

            if interrupt_requested():
                scan_state.status = "interrupted"
                persist_scan_state(scan_state)
                return

        scan_state.pagination_token = next_token
        scan_state.status = "complete" if next_token is None else "in_progress"
        persist_scan_state(scan_state)
        log_progress(
            f"{wallet[:8]}  page {page_num}  txs={len(records)}  "
            f"pump={scan_state.pumpfun_trades}  mints={len(scan_state.mint_stats)}  "
            f"credits={scan_state.credits_used}"
        )
        if next_token is None:
            return


async def scan_trader_mints(wallet_address: str, hours: int) -> Path:
    wallet_pubkey = validate_wallet_address(wallet_address)
    wallet = str(wallet_pubkey)
    output_path = wallet_manifest_path(wallet)

    to_timestamp = int(time.time())
    from_timestamp = to_timestamp - (hours * 3600)

    scan_state = load_scan_state(output_path, wallet, hours, from_timestamp, to_timestamp)
    if scan_state is None:
        scan_state = TraderScanState(
            wallet=wallet,
            hours=hours,
            from_timestamp=from_timestamp,
            to_timestamp=to_timestamp,
            output_path=output_path,
            mint_stats={},
        )
        persist_scan_state(scan_state)
    else:
        from_timestamp = scan_state.from_timestamp
        to_timestamp = scan_state.to_timestamp
        log_progress(
            f"{wallet[:8]}  resume  txs={scan_state.transactions_scanned}  "
            f"pump={scan_state.pumpfun_trades}  credits={scan_state.credits_used}"
        )

    async def _flush() -> None:
        if scan_state.status != "complete":
            scan_state.status = "interrupted"
        persist_scan_state(scan_state)

    clear_flush_callbacks()
    register_flush(_flush)

    log_start("trader scan", f"{wallet[:12]}…  {hours}h")

    rate_limiter = RateLimiterPool(HELIUS_KEYS)

    try:
        async with HeliusClient(rate_limiter) as client:
            await scan_trader_bulk(
                client,
                wallet,
                from_timestamp,
                to_timestamp,
                scan_state,
            )
        if scan_state.status != "interrupted":
            scan_state.status = "complete"
            scan_state.phase = "complete"
            scan_state.pagination_token = None
    finally:
        if scan_state.status != "complete":
            scan_state.status = "interrupted"
        persist_scan_state(scan_state)

    log_done(
        f"{wallet[:8]}  txs={scan_state.transactions_scanned}  "
        f"pump={scan_state.pumpfun_trades}  mints={len(scan_state.mint_stats)}  "
        f"credits={scan_state.credits_used}  -> {output_path.name}"
    )
    return output_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Discover Pump.fun mints traded by a wallet within a time window.",
    )
    parser.add_argument("wallet", help="Trader wallet address (base58)")
    parser.add_argument(
        "hours",
        nargs="?",
        type=int,
        default=DEFAULT_TRADER_LOOKBACK_HOURS,
        help=f"Hours to look back (default: {DEFAULT_TRADER_LOOKBACK_HOURS})",
    )
    args = parser.parse_args(argv)

    if args.hours <= 0:
        logger.error("hours must be positive")
        return 1

    setup_console_logging()

    async def _run() -> None:
        async with CheckpointScope():
            await scan_trader_mints(args.wallet, args.hours)

    try:
        asyncio.run(_run())
    except ValueError as exc:
        logger.error("%s", exc)
        return 1
    except HeliusRpcError as exc:
        logger.error("rpc failure: %s", exc)
        return 2
    except KeyboardInterrupt:
        log_progress("interrupted — checkpoint saved, re-run to resume")
        return 130

    return 0


if __name__ == "__main__":
    sys.exit(main())
