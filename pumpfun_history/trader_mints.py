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
    SCAN_PARALLEL,
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

    def merge(self, other: MintStats) -> None:
        if other.first_trade_time and (
            self.first_trade_time == 0 or other.first_trade_time < self.first_trade_time
        ):
            self.first_trade_time = other.first_trade_time
        if other.last_trade_time > self.last_trade_time:
            self.last_trade_time = other.last_trade_time
        self.buy_count += other.buy_count
        self.sell_count += other.sell_count

    def to_dict(self) -> dict[str, Any]:
        return {
            "mint": self.mint,
            "first_trade_time": self.first_trade_time,
            "last_trade_time": self.last_trade_time,
            "buy_count": self.buy_count,
            "sell_count": self.sell_count,
        }


@dataclass
class ScanShard:
    """One independent time slice of a wallet scan (safe to run in parallel)."""

    from_timestamp: int
    to_timestamp: int
    label: str = "shard"
    pagination_token: str | None = None
    status: str = "pending"
    signatures_collected: int = 0
    transactions_scanned: int = 0
    pumpfun_trades: int = 0
    credits_used: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "from_timestamp": self.from_timestamp,
            "to_timestamp": self.to_timestamp,
            "label": self.label,
            "pagination_token": self.pagination_token,
            "status": self.status,
            "signatures_collected": self.signatures_collected,
            "transactions_scanned": self.transactions_scanned,
            "pumpfun_trades": self.pumpfun_trades,
            "credits_used": self.credits_used,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ScanShard:
        return cls(
            from_timestamp=int(data["from_timestamp"]),
            to_timestamp=int(data["to_timestamp"]),
            label=str(data.get("label") or "shard"),
            pagination_token=data.get("pagination_token"),
            status=str(data.get("status") or "pending"),
            signatures_collected=int(data.get("signatures_collected", 0) or 0),
            transactions_scanned=int(data.get("transactions_scanned", 0) or 0),
            pumpfun_trades=int(data.get("pumpfun_trades", 0) or 0),
            credits_used=int(data.get("credits_used", 0) or 0),
        )


def split_time_range(from_ts: int, to_ts: int, parts: int) -> list[tuple[int, int]]:
    """Split [from_ts, to_ts] into up to `parts` contiguous inclusive windows."""
    if to_ts < from_ts:
        return []
    parts = max(1, int(parts))
    span = to_ts - from_ts + 1
    parts = min(parts, span)
    if parts == 1:
        return [(from_ts, to_ts)]

    windows: list[tuple[int, int]] = []
    start = from_ts
    for index in range(parts):
        remaining_parts = parts - index
        remaining_span = to_ts - start + 1
        size = max(1, remaining_span // remaining_parts)
        end = start + size - 1
        if index == parts - 1:
            end = to_ts
        windows.append((start, end))
        start = end + 1
        if start > to_ts:
            break
    return windows


def merge_mint_stats(dst: dict[str, MintStats], src: dict[str, MintStats]) -> None:
    for mint, stats in src.items():
        existing = dst.get(mint)
        if existing is None:
            dst[mint] = MintStats(
                mint=stats.mint,
                first_trade_time=stats.first_trade_time,
                last_trade_time=stats.last_trade_time,
                buy_count=stats.buy_count,
                sell_count=stats.sell_count,
            )
        else:
            existing.merge(stats)


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
    # Active Helius window (may be a gap when extending an older scan).
    query_from_timestamp: int | None = None
    query_to_timestamp: int | None = None
    shards: list[ScanShard] | None = None

    def __post_init__(self) -> None:
        if self.processed_signatures is None:
            self.processed_signatures = set()
        if self.query_from_timestamp is None:
            self.query_from_timestamp = self.from_timestamp
        if self.query_to_timestamp is None:
            self.query_to_timestamp = self.to_timestamp
        if self.shards is None:
            self.shards = []

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
            "query_from_timestamp": self.query_from_timestamp,
            "query_to_timestamp": self.query_to_timestamp,
            "status": self.status,
            "phase": self.phase,
            "scan_backend": SCAN_BACKEND,
            "signatures_collected": self.signatures_collected,
            "transactions_scanned": self.transactions_scanned,
            "pumpfun_trades": self.pumpfun_trades,
            "unique_mints": len(mints),
            "credits_used": self.credits_used,
            "pagination_token": self.pagination_token,
            "scan_shards": [shard.to_dict() for shard in (self.shards or [])],
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


def mint_stats_from_payload(data: dict[str, Any]) -> dict[str, MintStats]:
    mint_stats: dict[str, MintStats] = {}
    for entry in data.get("mints", []) or []:
        mint = entry.get("mint") if isinstance(entry, dict) else None
        if not mint:
            continue
        mint_stats[mint] = MintStats(
            mint=mint,
            first_trade_time=int(entry.get("first_trade_time", 0) or 0),
            last_trade_time=int(entry.get("last_trade_time", 0) or 0),
            buy_count=int(entry.get("buy_count", 0) or 0),
            sell_count=int(entry.get("sell_count", 0) or 0),
        )
    return mint_stats


def read_trader_manifest(output_path: Path) -> dict[str, Any] | None:
    if not output_path.exists():
        return None
    with output_path.open("r", encoding="utf-8") as handle:
        head = handle.read(4096)
    if f'"scan_backend": "{SCAN_BACKEND}"' not in head:
        return None
    return json.loads(output_path.read_text(encoding="utf-8"))


def uncovered_scan_windows(
    covered_from: int,
    covered_to: int,
    desired_from: int,
    desired_to: int,
) -> list[tuple[int, int, str]]:
    """Return Helius windows still needed for desired range (older gap first)."""
    windows: list[tuple[int, int, str]] = []
    if desired_from < covered_from:
        older_to = covered_from - 1
        if desired_from <= older_to:
            windows.append((desired_from, older_to, "extend_older"))
    if desired_to > covered_to:
        newer_from = covered_to + 1
        if newer_from <= desired_to:
            windows.append((newer_from, desired_to, "extend_newer"))
    return windows


def _shards_from_payload(data: dict[str, Any]) -> list[ScanShard]:
    raw = data.get("scan_shards") or []
    if not isinstance(raw, list):
        return []
    return [ScanShard.from_dict(item) for item in raw if isinstance(item, dict)]


def load_scan_state(
    output_path: Path,
    wallet: str,
    hours: int,
    from_timestamp: int,
    to_timestamp: int,
) -> TraderScanState | None:
    """Resume an interrupted enhanced scan. Complete manifests are handled by extend."""
    data = read_trader_manifest(output_path)
    if data is None:
        return None
    if data.get("wallet") != wallet:
        return None
    if data.get("status") == "complete":
        return None
    if data.get("hours") != hours:
        return None

    # Resume with the original scan window frozen at first run.
    saved_from = int(data.get("from_timestamp", from_timestamp))
    saved_to = int(data.get("to_timestamp", to_timestamp))
    query_from = int(data.get("query_from_timestamp", saved_from))
    query_to = int(data.get("query_to_timestamp", saved_to))
    shards = _shards_from_payload(data)
    if not shards and data.get("pagination_token"):
        shards = [
            ScanShard(
                from_timestamp=query_from,
                to_timestamp=query_to,
                label=str(data.get("phase") or "resume"),
                pagination_token=data.get("pagination_token"),
                status="in_progress",
                signatures_collected=int(data.get("signatures_collected", 0) or 0),
                transactions_scanned=int(data.get("transactions_scanned", 0) or 0),
                pumpfun_trades=int(data.get("pumpfun_trades", 0) or 0),
                credits_used=int(data.get("credits_used", 0) or 0),
            )
        ]

    return TraderScanState(
        wallet=wallet,
        hours=hours,
        from_timestamp=saved_from,
        to_timestamp=saved_to,
        output_path=output_path,
        mint_stats=mint_stats_from_payload(data),
        signatures_collected=data.get("signatures_collected", 0),
        transactions_scanned=data.get("transactions_scanned", 0),
        pumpfun_trades=data.get("pumpfun_trades", 0),
        credits_used=data.get("credits_used", 0),
        pagination_token=data.get("pagination_token"),
        phase=str(data.get("phase") or "scanning_transactions"),
        status="in_progress",
        query_from_timestamp=query_from,
        query_to_timestamp=query_to,
        shards=shards,
    )


def load_extend_scan_state(
    output_path: Path,
    wallet: str,
    hours: int,
    desired_from: int,
    desired_to: int,
) -> tuple[TraderScanState, list[tuple[int, int, str]]] | None:
    """Reuse a completed shorter scan: keep mints, only query uncovered gaps."""
    data = read_trader_manifest(output_path)
    if data is None or data.get("wallet") != wallet:
        return None
    if data.get("status") != "complete":
        return None

    covered_from = int(data.get("from_timestamp", desired_from))
    covered_to = int(data.get("to_timestamp", desired_to))
    windows = uncovered_scan_windows(covered_from, covered_to, desired_from, desired_to)
    state = TraderScanState(
        wallet=wallet,
        hours=hours,
        # Keep coverage as what's already fetched; expand after each gap.
        from_timestamp=covered_from,
        to_timestamp=covered_to,
        output_path=output_path,
        mint_stats=mint_stats_from_payload(data),
        signatures_collected=int(data.get("signatures_collected", 0) or 0),
        transactions_scanned=int(data.get("transactions_scanned", 0) or 0),
        pumpfun_trades=int(data.get("pumpfun_trades", 0) or 0),
        credits_used=int(data.get("credits_used", 0) or 0),
        pagination_token=None,
        phase="extend",
        status="in_progress",
        query_from_timestamp=desired_from,
        query_to_timestamp=desired_to,
        shards=[],
    )
    return state, windows


def build_shards_for_window(
    from_timestamp: int,
    to_timestamp: int,
    label: str,
    *,
    parts: int,
    existing: list[ScanShard] | None = None,
) -> list[ScanShard]:
    """Resume incomplete shards when present; otherwise split the window."""
    if existing:
        pending = [shard for shard in existing if shard.status != "complete"]
        if pending:
            return pending
        if existing and all(shard.status == "complete" for shard in existing):
            return []

    windows = split_time_range(from_timestamp, to_timestamp, parts)
    total = len(windows)
    return [
        ScanShard(
            from_timestamp=start,
            to_timestamp=end,
            label=f"{label}:{index + 1}/{total}" if total > 1 else label,
            status="pending",
        )
        for index, (start, end) in enumerate(windows)
    ]


async def scan_trader_shard(
    client: HeliusClient,
    wallet: str,
    shard: ScanShard,
    scan_state: TraderScanState,
    lock: asyncio.Lock,
    shard_id: int,
) -> None:
    """Scan one time shard; merge mints into shared state under lock."""
    if shard.status == "complete":
        return

    page_num = 0
    shard.status = "in_progress"
    from_timestamp = shard.from_timestamp
    to_timestamp = shard.to_timestamp

    async for records, next_token, page_credits in client.iter_transaction_pages(
        wallet,
        sort_order="desc",
        limit=GTFORADDRESS_PAGE_LIMIT,
        filters={
            "blockTime": {"gte": from_timestamp, "lte": to_timestamp},
            "status": "succeeded",
            "tokenAccounts": "balanceChanged",
        },
        start_token=shard.pagination_token,
    ):
        if interrupt_requested():
            shard.status = "interrupted"
            async with lock:
                scan_state.status = "interrupted"
                persist_scan_state(scan_state)
            return

        page_num += 1
        page_stats: dict[str, MintStats] = {}
        page_txs = 0
        page_pump = 0

        for record in records:
            signature = extract_transaction_signature(record) or ""
            page_txs += 1
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
                aggregate_wallet_trades(page_stats, event.side, event.mint, timestamp)
                page_pump += 1

            if interrupt_requested():
                shard.status = "interrupted"
                async with lock:
                    scan_state.status = "interrupted"
                    persist_scan_state(scan_state)
                return

        shard.pagination_token = next_token
        shard.signatures_collected += len(records)
        shard.transactions_scanned += page_txs
        shard.pumpfun_trades += page_pump
        shard.credits_used += page_credits
        shard.status = "complete" if next_token is None else "in_progress"

        async with lock:
            merge_mint_stats(scan_state.mint_stats, page_stats)
            scan_state.signatures_collected += len(records)
            scan_state.transactions_scanned += page_txs
            scan_state.pumpfun_trades += page_pump
            scan_state.credits_used += page_credits
            scan_state.query_from_timestamp = min(
                int(scan_state.query_from_timestamp or from_timestamp),
                from_timestamp,
            )
            scan_state.query_to_timestamp = max(
                int(scan_state.query_to_timestamp or to_timestamp),
                to_timestamp,
            )
            persist_scan_state(scan_state)
            log_progress(
                f"{wallet[:8]}  s{shard_id}  page {page_num}  txs={len(records)}  "
                f"pump={scan_state.pumpfun_trades}  mints={len(scan_state.mint_stats)}  "
                f"credits={scan_state.credits_used}"
            )

        if next_token is None:
            return


async def scan_trader_bulk(
    client: HeliusClient,
    wallet: str,
    from_timestamp: int,
    to_timestamp: int,
    scan_state: TraderScanState,
) -> None:
    """Backward-compatible single-window scan (one shard)."""
    lock = asyncio.Lock()
    shard = ScanShard(
        from_timestamp=from_timestamp,
        to_timestamp=to_timestamp,
        label=scan_state.phase or "scanning_transactions",
        pagination_token=scan_state.pagination_token,
        status="in_progress",
    )
    scan_state.shards = [shard]
    await scan_trader_shard(client, wallet, shard, scan_state, lock, 1)
    scan_state.pagination_token = shard.pagination_token
    if shard.status == "interrupted":
        scan_state.status = "interrupted"
    elif shard.status == "complete":
        scan_state.status = "complete"


async def scan_window_parallel(
    client: HeliusClient,
    wallet: str,
    from_timestamp: int,
    to_timestamp: int,
    label: str,
    scan_state: TraderScanState,
    *,
    parallel: int,
) -> None:
    """Split a time window into shards and scan them concurrently."""
    parts = max(1, min(parallel, max(1, len(HELIUS_KEYS))))
    shards = build_shards_for_window(
        from_timestamp,
        to_timestamp,
        label,
        parts=parts,
        existing=scan_state.shards,
    )
    if not shards:
        scan_state.status = "complete"
        scan_state.pagination_token = None
        scan_state.shards = []
        return

    scan_state.shards = shards
    scan_state.phase = label
    scan_state.status = "in_progress"
    scan_state.query_from_timestamp = from_timestamp
    scan_state.query_to_timestamp = to_timestamp
    persist_scan_state(scan_state)

    if len(shards) > 1:
        log_progress(
            f"{wallet[:8]}  parallel scan  shards={len(shards)}  "
            f"window=[{from_timestamp},{to_timestamp}]"
        )

    lock = asyncio.Lock()
    await asyncio.gather(
        *[
            scan_trader_shard(client, wallet, shard, scan_state, lock, index + 1)
            for index, shard in enumerate(shards)
        ]
    )

    if any(shard.status == "interrupted" for shard in shards):
        scan_state.status = "interrupted"
        scan_state.pagination_token = next(
            (shard.pagination_token for shard in shards if shard.status == "interrupted"),
            None,
        )
        return

    if all(shard.status == "complete" for shard in shards):
        scan_state.status = "complete"
        scan_state.pagination_token = None
        scan_state.shards = []
    else:
        scan_state.status = "in_progress"


async def scan_trader_mints(wallet_address: str, hours: int) -> Path:
    wallet_pubkey = validate_wallet_address(wallet_address)
    wallet = str(wallet_pubkey)
    output_path = wallet_manifest_path(wallet)
    parallel = max(1, SCAN_PARALLEL)

    desired_to = int(time.time())
    desired_from = desired_to - (hours * 3600)

    scan_state = load_scan_state(
        output_path, wallet, hours, desired_from, desired_to
    )
    pending_windows: list[tuple[int, int, str]] = []

    if scan_state is not None:
        if scan_state.shards:
            # Resume whatever shard set was already running.
            q_from = min(shard.from_timestamp for shard in scan_state.shards)
            q_to = max(shard.to_timestamp for shard in scan_state.shards)
            pending_windows = [(q_from, q_to, scan_state.phase or "resume")]
        else:
            q_from = int(scan_state.query_from_timestamp or scan_state.from_timestamp)
            q_to = int(scan_state.query_to_timestamp or scan_state.to_timestamp)
            pending_windows = [(q_from, q_to, scan_state.phase or "resume")]
        log_progress(
            f"{wallet[:8]}  resume  txs={scan_state.transactions_scanned}  "
            f"pump={scan_state.pumpfun_trades}  mints={len(scan_state.mint_stats)}  "
            f"credits={scan_state.credits_used}  parallel={parallel}"
        )
    else:
        extended = load_extend_scan_state(
            output_path, wallet, hours, desired_from, desired_to
        )
        if extended is not None:
            scan_state, pending_windows = extended
            if not pending_windows:
                scan_state.status = "complete"
                scan_state.phase = "complete"
                scan_state.hours = hours
                persist_scan_state(scan_state)
                log_progress(
                    f"{wallet[:8]}  already covers {hours}h  "
                    f"mints={len(scan_state.mint_stats)}  -> {output_path.name}"
                )
                return output_path
            kept = len(scan_state.mint_stats)
            for q_from, q_to, label in pending_windows:
                log_progress(
                    f"{wallet[:8]}  {label}  keep_mints={kept}  "
                    f"gap={q_to - q_from}s  query=[{q_from},{q_to}]"
                )
        else:
            scan_state = TraderScanState(
                wallet=wallet,
                hours=hours,
                from_timestamp=desired_from,
                to_timestamp=desired_to,
                output_path=output_path,
                mint_stats={},
                query_from_timestamp=desired_from,
                query_to_timestamp=desired_to,
                shards=[],
            )
            pending_windows = [(desired_from, desired_to, "scanning_transactions")]
            persist_scan_state(scan_state)

    async def _flush() -> None:
        if scan_state.status != "complete":
            scan_state.status = "interrupted"
        persist_scan_state(scan_state)

    clear_flush_callbacks()
    register_flush(_flush)

    log_start("trader scan", f"{wallet[:12]}…  {hours}h  parallel={parallel}")

    rate_limiter = RateLimiterPool(HELIUS_KEYS)

    def _expand_coverage(q_from: int, q_to: int) -> None:
        scan_state.from_timestamp = min(scan_state.from_timestamp, q_from)
        scan_state.to_timestamp = max(scan_state.to_timestamp, q_to)
        scan_state.hours = max(
            scan_state.hours,
            hours,
            max(1, (scan_state.to_timestamp - scan_state.from_timestamp + 3599) // 3600),
        )

    try:
        async with HeliusClient(rate_limiter) as client:
            while pending_windows:
                q_from, q_to, label = pending_windows.pop(0)
                await scan_window_parallel(
                    client,
                    wallet,
                    q_from,
                    q_to,
                    label,
                    scan_state,
                    parallel=parallel,
                )
                if scan_state.status == "interrupted":
                    break
                _expand_coverage(q_from, q_to)
                scan_state.shards = []
                if not pending_windows:
                    pending_windows = uncovered_scan_windows(
                        scan_state.from_timestamp,
                        scan_state.to_timestamp,
                        desired_from,
                        desired_to,
                    )
                    for n_from, n_to, n_label in pending_windows:
                        log_progress(
                            f"{wallet[:8]}  {n_label}  "
                            f"keep_mints={len(scan_state.mint_stats)}  "
                            f"gap={n_to - n_from}s  query=[{n_from},{n_to}]"
                        )

        if scan_state.status != "interrupted":
            scan_state.status = "complete"
            scan_state.phase = "complete"
            scan_state.pagination_token = None
            scan_state.shards = []
            scan_state.hours = hours
            scan_state.from_timestamp = min(scan_state.from_timestamp, desired_from)
            scan_state.to_timestamp = max(scan_state.to_timestamp, desired_to)
            scan_state.query_from_timestamp = scan_state.from_timestamp
            scan_state.query_to_timestamp = scan_state.to_timestamp
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
