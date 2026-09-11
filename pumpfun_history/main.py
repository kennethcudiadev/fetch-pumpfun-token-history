"""CLI entry point for fetching Pump.fun bonding curve trade history."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path
from typing import Any

from batch_progress import clear_batch_progress, save_batch_progress
from checkpoint import CheckpointScope, clear_flush_callbacks, interrupt_requested, register_flush
from config import (
    GTFORADDRESS_PAGE_LIMIT,
    HELIUS_KEYS,
    MAX_CONCURRENT_MINT_FETCHES,
    MAX_CONCURRENT_TX_FETCHES,
    SIGNATURES_PAGE_LIMIT,
    TX_BATCH_SIZE,
)
from console import log_done, log_progress, log_start, setup_console_logging
from helius_client import HeliusClient, HeliusRpcError
from models import CrawlState, TradeEvent
from pumpfun_decoder import (
    decode_trade_events_from_transaction,
    extract_transaction_signature,
    normalize_transaction_record,
)
from rate_limiter import RateLimiterPool
from storage import Storage
from utils import derive_bonding_curve_pda, is_bonding_curve_complete, validate_mint_address, validate_wallet_address

logger = logging.getLogger(__name__)

_progress_lock = asyncio.Lock()


def _short(value: str, length: int = 8) -> str:
    return value if len(value) <= length else value[:length]


def _trade_events_complete_curve(events: list[TradeEvent]) -> bool:
    return any(is_bonding_curve_complete(event.bonding_curve_progress) for event in events)


async def crawl_mint_bulk(
    client: HeliusClient,
    storage: Storage,
    mint: str,
    bonding_curve: str,
    state: CrawlState,
) -> CrawlState:
    """Credit-efficient crawl via Helius getTransactionsForAddress."""
    page_num = 0
    txs_seen = await storage.count_processed_txs(mint)
    state.total_signatures = txs_seen

    async for records, next_token, page_credits in client.iter_transaction_pages(
        bonding_curve,
        sort_order="asc",
        limit=GTFORADDRESS_PAGE_LIMIT,
        filters={"status": "succeeded", "tokenAccounts": "none"},
        start_token=state.pagination_token,
    ):
        page_num += 1
        state.credits_used += page_credits
        page_signatures = [
            signature
            for record in records
            if (signature := extract_transaction_signature(record))
        ]
        already_done = await storage.processed_signatures_among(mint, page_signatures)
        new_events: list[TradeEvent] = []
        processed: list[tuple[str, int]] = []
        curve_completed = False

        for record in records:
            signature = extract_transaction_signature(record)
            if not signature or signature in already_done:
                continue

            tx = normalize_transaction_record(record)
            slot = (tx or {}).get("slot") or record.get("slot") or 0
            if tx:
                events = decode_trade_events_from_transaction(
                    tx,
                    signature,
                    expected_mint=mint,
                )
                if events:
                    new_events.extend(events)
                    if _trade_events_complete_curve(events):
                        curve_completed = True

            processed.append((signature, slot))
            state.last_processed_signature = signature
            state.last_processed_slot = slot
            txs_seen += 1

            if curve_completed:
                break

        state.total_signatures = txs_seen
        if curve_completed or next_token is None:
            state.status = "complete"
            state.sig_collection_complete = True
            state.pagination_token = None
        else:
            state.status = "in_progress"
            state.sig_collection_complete = False
            state.pagination_token = next_token

        await storage.commit_crawl_page(state, new_events, processed)
        await storage.export_mint_json(
            mint,
            bonding_curve,
            signatures_total=state.total_signatures,
            status=state.status,
            pagination_token=state.pagination_token,
            credits_used=state.credits_used,
        )
        log_progress(
            f"{_short(mint)}  page {page_num}  txs={len(records)}  "
            f"done={txs_seen}  events={state.total_events}  credits={state.credits_used}"
        )
        if curve_completed:
            log_progress(f"{_short(mint)}  bonding curve complete — stopping crawl")
            return state
        if interrupt_requested() or next_token is None:
            break

    if state.sig_collection_complete:
        state.status = "complete"
        state.pagination_token = None
    elif state.status != "complete":
        state.status = "interrupted"
        await storage.commit_crawl_page(state, [], [])

    return state


# --- Legacy fallback ---


async def collect_signatures_legacy(
    client: HeliusClient,
    storage: Storage,
    mint: str,
    bonding_curve: str,
    state: CrawlState,
) -> CrawlState:
    if state.sig_collection_complete:
        return state

    before = state.sig_before_cursor
    while True:
        batch = await client.get_signatures_for_address(
            bonding_curve, before=before, limit=SIGNATURES_PAGE_LIMIT
        )
        if not batch:
            state.sig_collection_complete = True
            break

        entries = [
            (item["signature"], item.get("slot", 0))
            for item in batch
            if item.get("signature") and item.get("err") is None
        ]
        await storage.insert_signatures_batch(mint, entries)
        state.total_signatures = await storage.count_total_signatures(mint)
        state.status = "in_progress"
        await storage.save_checkpoint(state)

        if len(batch) < SIGNATURES_PAGE_LIMIT:
            state.sig_collection_complete = True
            break
        before = batch[-1]["signature"]
        state.sig_before_cursor = before

    return state


async def process_transactions_legacy(
    client: HeliusClient,
    storage: Storage,
    mint: str,
    bonding_curve: str,
    state: CrawlState,
) -> None:
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_TX_FETCHES)

    async def _one(signature: str, slot: int) -> bool:
        async with semaphore:
            tx = await client.get_transaction(signature)
            state.credits_used += 1
            events = decode_trade_events_from_transaction(tx, signature, expected_mint=mint)
            if events:
                await storage.insert_events(events)
            await storage.mark_signature_processed(signature, slot, mint)
            return _trade_events_complete_curve(events) if events else False

    while True:
        batch = await storage.fetch_unprocessed_signatures(mint, limit=TX_BATCH_SIZE)
        if not batch:
            break
        results = await asyncio.gather(*[_one(sig, slot) for sig, slot in batch])
        if any(results):
            state.status = "complete"
            state.sig_collection_complete = True
            await storage.save_checkpoint(state)
            log_progress(f"{_short(mint)}  bonding curve complete — stopping legacy crawl")
            break
        state.status = "in_progress"
        await storage.save_checkpoint(state)
        log_progress(f"{_short(mint)}  legacy batch  credits={state.credits_used}")


async def run_crawler(
    mint_address: str,
    wallet: str,
    *,
    client: HeliusClient | None = None,
    storage: Storage | None = None,
    load_events: bool = True,
) -> tuple[list[dict[str, Any]], Path]:
    mint_pubkey = validate_mint_address(mint_address)
    mint = str(mint_pubkey)
    bonding_curve, _ = derive_bonding_curve_pda(mint_pubkey)
    validate_wallet_address(wallet)

    log_start("mint history", f"{mint[:12]}…")

    owns_storage = storage is None
    if storage is None:
        storage = Storage()
    storage.set_export_wallet(wallet)
    if owns_storage:
        await storage.initialize()

    state = await storage.get_crawl_state(mint)
    if state is None:
        state = CrawlState(
            mint=mint,
            bonding_curve=bonding_curve,
            sig_before_cursor=None,
            sig_collection_complete=False,
            last_processed_signature=None,
            last_processed_slot=None,
            total_signatures=0,
            total_events=0,
        )
        await storage.save_checkpoint(state)
    elif state.status == "complete" and state.sig_collection_complete:
        log_progress(f"{_short(mint)}  already complete  events={state.total_events}")
        path = await storage.export_mint_json(
            mint,
            bonding_curve,
            signatures_total=state.total_signatures,
            status="complete",
            pagination_token=None,
            credits_used=state.credits_used,
        )
        events = await storage.fetch_events(mint) if load_events else []
        return events, path
    else:
        processed = await storage.count_processed_txs(mint)
        log_progress(
            f"{_short(mint)}  resume  status={state.status}  "
            f"txs={processed}  events={state.total_events}  credits={state.credits_used}"
        )

    async def _flush() -> None:
        if state.status != "complete":
            state.status = "interrupted"
            await storage.commit_crawl_page(state, [], [])

    if client is None:
        clear_flush_callbacks()
        register_flush(_flush)

    legacy_pending = await storage.count_pending_signatures(mint)

    async def _crawl(active_client: HeliusClient) -> None:
        nonlocal state
        if legacy_pending > 0 and state.pagination_token is None and not state.sig_collection_complete:
            log_progress(f"{_short(mint)}  legacy queue  pending={legacy_pending}")
            state = await collect_signatures_legacy(
                active_client, storage, mint, bonding_curve, state
            )
            await process_transactions_legacy(
                active_client, storage, mint, bonding_curve, state
            )
            state.status = "complete"
            state.sig_collection_complete = True
        else:
            state = await crawl_mint_bulk(
                active_client, storage, mint, bonding_curve, state
            )

    try:
        if client is None:
            async with HeliusClient(RateLimiterPool(HELIUS_KEYS)) as owned_client:
                await _crawl(owned_client)
        else:
            await _crawl(client)
    except asyncio.CancelledError:
        state.status = "interrupted"
        await storage.commit_crawl_page(state, [], [])
        raise

    if state.status == "complete":
        state.sig_collection_complete = True

    path = await storage.export_mint_json(
        mint,
        bonding_curve,
        signatures_total=state.total_signatures,
        status=state.status,
        pagination_token=state.pagination_token,
        credits_used=state.credits_used,
    )

    event_count = state.total_events
    log_done(
        f"{_short(mint)}  {event_count} events  status={state.status}  "
        f"credits={state.credits_used}  -> {path.name}"
    )
    events = await storage.fetch_events(mint) if load_events else []
    return events, path


async def export_mint_only(mint_address: str) -> Path:
    mint_pubkey = validate_mint_address(mint_address)
    mint = str(mint_pubkey)
    bonding_curve, _ = derive_bonding_curve_pda(mint_pubkey)

    storage = Storage()
    await storage.initialize()
    state = await storage.get_crawl_state(mint)
    return await storage.export_mint_json(
        mint,
        state.bonding_curve if state else bonding_curve,
        signatures_total=state.total_signatures if state else 0,
        signatures_pending=await storage.count_pending_signatures(mint),
        status="interrupted",
        pagination_token=state.pagination_token if state else None,
        credits_used=state.credits_used if state else 0,
    )


def load_wallet_manifest(path: Path) -> tuple[str, list[str]]:
    if not path.is_file():
        raise ValueError(f"Wallet manifest not found: {path}")

    data = json.loads(path.read_text(encoding="utf-8"))
    wallet = data.get("wallet") or path.stem
    validate_wallet_address(wallet)

    mints_raw = data.get("mints")
    if not isinstance(mints_raw, list):
        raise ValueError(f"Invalid wallet manifest (missing 'mints' list): {path}")

    mints: list[str] = []
    seen: set[str] = set()
    for entry in mints_raw:
        mint = entry if isinstance(entry, str) else entry.get("mint") if isinstance(entry, dict) else None
        if not mint or mint in seen:
            continue
        validate_mint_address(mint)
        seen.add(mint)
        mints.append(mint)

    if not mints:
        raise ValueError(f"No mint addresses found in wallet manifest: {path}")
    return wallet, mints


def load_mints_from_trader_json(path: Path) -> list[str]:
    """Backward-compatible helper — returns mint list only."""
    _, mints = load_wallet_manifest(path)
    return mints


async def run_crawler_batch(mints: list[str], wallet: str, manifest_file: str) -> list[tuple[str, Path]]:
    """Fetch several bonding curves at once. Resume skips mints already complete in SQLite."""
    results: list[tuple[str, Path]] = []
    total = len(mints)
    storage = Storage()
    storage.set_export_wallet(wallet)
    await storage.initialize()

    discarded = await storage.discard_incomplete_mints(mints)
    for mint in discarded:
        log_progress(f"batch  drop incomplete  {_short(mint)}  refetch from start")

    already_done = await storage.complete_mint_addresses()
    pending = [mint for mint in mints if mint not in already_done]
    completed_mints = [mint for mint in mints if mint in already_done]
    workers = max(1, min(MAX_CONCURRENT_MINT_FETCHES, len(HELIUS_KEYS), len(pending) or 1))

    if already_done:
        log_progress(
            f"batch  resume  done={len(completed_mints)}/{total}  "
            f"remaining={len(pending)}  workers={workers}"
        )
    else:
        log_progress(f"batch  workers={workers}  mints={total}")

    if not pending:
        return [(mint, storage.json_path_for_mint(mint)) for mint in mints]

    queue: asyncio.Queue[str] = asyncio.Queue()
    for mint in pending:
        queue.put_nowait(mint)

    async def _save_progress(status: str, last_error: str | None = None) -> None:
        next_index = len(completed_mints)
        payload = {
            "manifest_file": str(Path(manifest_file).resolve()),
            "wallet": wallet,
            "mints": mints,
            "next_index": next_index,
            "completed_mints": list(completed_mints),
            "status": status,
        }
        if last_error:
            payload["last_error"] = last_error
        async with _progress_lock:
            await asyncio.to_thread(save_batch_progress, payload)

    async def _worker(client: HeliusClient, worker_id: int) -> None:
        while not interrupt_requested():
            try:
                mint = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            try:
                log_progress(
                    f"batch  mint {len(completed_mints) + 1}/{total}  "
                    f"w{worker_id}  {_short(mint)}"
                )
                _, json_path = await run_crawler(
                    mint,
                    wallet,
                    client=client,
                    storage=storage,
                    load_events=False,
                )
                state = await storage.get_crawl_state(mint)
                if state and state.status == "complete":
                    async with _progress_lock:
                        completed_mints.append(mint)
                    results.append((mint, json_path))
                    await _save_progress("in_progress")
            except Exception as exc:
                logger.error("mint failed %s: %s", _short(mint), exc)
                await _save_progress("interrupted", str(exc))
            finally:
                queue.task_done()

    rate_limiter = RateLimiterPool(HELIUS_KEYS)
    async with HeliusClient(rate_limiter) as client:
        await asyncio.gather(*[_worker(client, index + 1) for index in range(workers)])

    if interrupt_requested():
        await _save_progress("interrupted")
    elif len(completed_mints) >= total:
        await _save_progress("complete")
    return results


async def _async_main(args: argparse.Namespace) -> int:
    if args.wallet_file:
        manifest_path = Path(args.wallet_file)
        wallet, mints = load_wallet_manifest(manifest_path)
        log_start("batch history", f"{len(mints)} mints for {wallet[:8]}…")
        results = await run_crawler_batch(mints, wallet, str(manifest_path))
        done = await Storage().complete_mint_addresses()
        finished = sum(1 for mint in mints if mint in done)
        if finished == len(mints):
            clear_batch_progress()
            log_done(f"{finished}/{len(mints)} mints exported -> data/{wallet}/")
        else:
            log_progress(f"batch paused  {finished}/{len(mints)} done — re-run to resume")
        return 0

    if not args.wallet:
        logger.error("single-mint mode requires --wallet (owner directory under data/)")
        return 1

    await run_crawler(args.mint, args.wallet)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Fetch historical Pump.fun bonding curve trade events for a mint.",
    )
    parser.add_argument("mint", nargs="?", help="Token mint address (base58)")
    parser.add_argument(
        "--file",
        dest="wallet_file",
        metavar="PATH",
        help="Wallet manifest JSON (data/<wallet>.json)",
    )
    parser.add_argument(
        "--wallet",
        metavar="ADDRESS",
        help="Target wallet — export dir data/<wallet>/ (required for single mint)",
    )
    parser.add_argument("--json", action="store_true", help="Print events to stdout")
    args = parser.parse_args(argv)

    setup_console_logging()

    if args.wallet_file and args.mint:
        logger.error("use either a mint address or --file, not both")
        return 1
    if not args.wallet_file and not args.mint:
        logger.error("provide a mint address or --file PATH")
        return 1
    if args.json and args.wallet_file:
        logger.error("--json only works for single-mint mode")
        return 1

    async def _run() -> int:
        async with CheckpointScope():
            return await _async_main(args)

    try:
        exit_code = asyncio.run(_run())
    except ValueError as exc:
        logger.error("%s", exc)
        return 1
    except HeliusRpcError as exc:
        logger.error("rpc failure: %s", exc)
        return 2
    except KeyboardInterrupt:
        log_progress("interrupted — checkpoint saved, re-run to resume")
        return 130

    if args.json and args.mint:
        storage = Storage()
        asyncio.run(storage.initialize())
        events = asyncio.run(storage.fetch_events(args.mint))
        print(json.dumps(events, indent=2))

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
