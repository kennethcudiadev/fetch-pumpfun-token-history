"""Decode Pump.fun TradeEvent data from Solana transactions."""

from __future__ import annotations

import base58
import logging
from typing import Any

from config import PUMP_PROGRAM_ID, TRADE_EVENT_DISCRIMINATOR
from models import TradeEvent, TradeSide
from utils import (
    compute_bonding_curve_progress,
    compute_price,
    lamports_to_sol,
    parse_trade_event_payload,
    raw_tokens_to_float,
)

logger = logging.getLogger(__name__)


def extract_transaction_signature(record: dict[str, Any]) -> str | None:
    """Extract signature from getTransaction or getTransactionsForAddress records."""
    signature = record.get("signature")
    if signature:
        return signature

    transaction = record.get("transaction") or {}
    signatures = transaction.get("signatures") or []
    return signatures[0] if signatures else None


def normalize_transaction_record(record: dict[str, Any]) -> dict[str, Any] | None:
    """
    Normalize Helius getTransactionsForAddress/full or getTransaction payloads
    into the shape expected by the TradeEvent decoder.
    """
    if not record:
        return None

    transaction = record.get("transaction")
    meta = record.get("meta")
    if transaction is None or meta is None:
        return None

    return {
        "slot": record.get("slot") or 0,
        "blockTime": record.get("blockTime"),
        "transaction": transaction,
        "meta": meta,
    }


def _decode_instruction_data(data: str | bytes) -> bytes | None:
    if isinstance(data, bytes):
        return data
    if not data:
        return None
    try:
        return base58.b58decode(data)
    except Exception:
        return None


def _find_trade_event_offset(data: bytes) -> int:
    """Return byte offset after TradeEvent discriminator, or -1."""
    idx = data.find(TRADE_EVENT_DISCRIMINATOR)
    if idx == -1:
        return -1
    return idx + len(TRADE_EVENT_DISCRIMINATOR)


def _extract_instruction_data_entries(tx: dict[str, Any]) -> list[bytes]:
    """Collect raw instruction data from top-level and inner instructions."""
    entries: list[bytes] = []
    transaction = tx.get("transaction") or {}
    message = transaction.get("message") or {}

    for ix in message.get("instructions") or []:
        raw = _decode_instruction_data(ix.get("data", ""))
        if raw:
            entries.append(raw)

    meta = tx.get("meta") or {}
    for inner_group in meta.get("innerInstructions") or []:
        for ix in inner_group.get("instructions") or []:
            raw = _decode_instruction_data(ix.get("data", ""))
            if raw:
                entries.append(raw)

    return entries


def decode_trade_events_from_transaction(
    tx: dict[str, Any],
    signature: str,
    expected_mint: str | None = None,
) -> list[TradeEvent]:
    """
    Extract all Pump.fun TradeEvent records from a transaction.

    TradeEvents are emitted via CPI and appear in inner instruction data.
    The 8-byte discriminator may appear anywhere in the payload.
    """
    if not tx:
        return []

    slot = tx.get("slot") or 0
    block_time = tx.get("blockTime")

    events: list[TradeEvent] = []
    seen_keys: set[tuple] = set()

    for data in _extract_instruction_data_entries(tx):
        offset = _find_trade_event_offset(data)
        if offset == -1:
            continue

        parsed = parse_trade_event_payload(data, offset)
        if not parsed:
            continue

        mint = parsed["mint"]
        if expected_mint and mint != expected_mint:
            continue

        side: TradeSide = "BUY" if parsed["is_buy"] else "SELL"
        dedupe_key = (
            signature,
            mint,
            side,
            parsed["user"],
            parsed["sol_amount"],
            parsed["token_amount"],
        )
        if dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)

        sol_lamports = parsed["sol_amount"]
        token_raw = parsed["token_amount"]
        timestamp = parsed["timestamp"]
        if block_time is not None and timestamp <= 0:
            timestamp = block_time

        event = TradeEvent(
            signature=signature,
            slot=slot,
            timestamp=timestamp,
            mint=mint,
            side=side,
            wallet=parsed["user"],
            sol_amount=lamports_to_sol(sol_lamports),
            token_amount=raw_tokens_to_float(token_raw),
            price=compute_price(sol_lamports, token_raw),
            virtual_sol_reserve=lamports_to_sol(parsed["virtual_sol_reserves"]),
            virtual_token_reserve=raw_tokens_to_float(parsed["virtual_token_reserves"]),
            bonding_curve_progress=compute_bonding_curve_progress(
                parsed["real_token_reserves"]
            ),
        )
        events.append(event)

    return events


def is_pumpfun_transaction(tx: dict[str, Any]) -> bool:
    """Quick check whether a transaction involves the Pump.fun program."""
    if not tx:
        return False

    transaction = tx.get("transaction") or {}
    message = transaction.get("message") or {}
    account_keys = message.get("accountKeys") or []

    for key_entry in account_keys:
        pubkey = key_entry if isinstance(key_entry, str) else key_entry.get("pubkey", "")
        if pubkey == PUMP_PROGRAM_ID:
            return True

    for data in _extract_instruction_data_entries(tx):
        if _find_trade_event_offset(data) != -1:
            return True

    return False
