"""Shared utilities for address validation and PDA derivation."""

from __future__ import annotations

import base58
import json
import logging
import os
import struct
import tempfile
import time
from pathlib import Path
from typing import Any

from solders.pubkey import Pubkey

from config import (
    BONDING_CURVE_SEED,
    INITIAL_REAL_TOKEN_RESERVES,
    LAMPORTS_PER_SOL,
    PUMP_PROGRAM_ID,
    TOKEN_DECIMALS,
)


def validate_mint_address(mint: str) -> Pubkey:
    """Validate and parse a base58 Solana mint address."""
    return validate_solana_address(mint, "mint")


def validate_wallet_address(wallet: str) -> Pubkey:
    """Validate and parse a base58 Solana wallet address."""
    return validate_solana_address(wallet, "wallet")


def validate_solana_address(address: str, label: str = "address") -> Pubkey:
    """Validate and parse a base58 Solana public key."""
    address = address.strip()
    if not address:
        raise ValueError(f"{label.capitalize()} address cannot be empty")

    try:
        raw = base58.b58decode(address)
    except Exception as exc:
        raise ValueError(f"Invalid base58 {label} address: {address}") from exc

    if len(raw) != 32:
        raise ValueError(f"{label.capitalize()} address must be 32 bytes, got {len(raw)}")

    try:
        return Pubkey.from_string(address)
    except Exception as exc:
        raise ValueError(f"Invalid {label} public key: {address}") from exc


def derive_bonding_curve_pda(mint: Pubkey | str) -> tuple[str, int]:
    """Derive the Pump.fun bonding curve PDA for a token mint."""
    if isinstance(mint, str):
        mint = Pubkey.from_string(mint)

    program_id = Pubkey.from_string(PUMP_PROGRAM_ID)
    pda, bump = Pubkey.find_program_address(
        [BONDING_CURVE_SEED, bytes(mint)],
        program_id,
    )
    return str(pda), bump


def lamports_to_sol(lamports: int) -> float:
    return lamports / LAMPORTS_PER_SOL


def raw_tokens_to_float(raw: int) -> float:
    return raw / (10**TOKEN_DECIMALS)


def compute_price(sol_lamports: int, token_raw: int) -> float:
    """Price in SOL per token."""
    if token_raw == 0:
        return 0.0
    sol = lamports_to_sol(sol_lamports)
    tokens = raw_tokens_to_float(token_raw)
    return sol / tokens


def compute_bonding_curve_progress(real_token_reserves: int) -> float:
    """
    Progress toward bonding curve completion (0.0 → 1.0).

    The curve completes when real_token_reserves reaches zero.
    """
    if INITIAL_REAL_TOKEN_RESERVES <= 0:
        return 0.0
    sold = INITIAL_REAL_TOKEN_RESERVES - real_token_reserves
    progress = sold / INITIAL_REAL_TOKEN_RESERVES
    return max(0.0, min(1.0, progress))


BONDING_CURVE_COMPLETE_PROGRESS = 1.0 - 1e-9


def is_bonding_curve_complete(progress: float | None) -> bool:
    """Return True when the bonding curve has graduated / migrated."""
    if progress is None:
        return False
    return float(progress) >= BONDING_CURVE_COMPLETE_PROGRESS


def trim_events_to_bonding_curve(events: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Keep only bonding-curve trades. For migrated tokens, drop everything after
    the first event that completes the curve (progress -> 1.0).
    """
    meta: dict[str, Any] = {
        "migrated": False,
        "bonding_curve_complete": False,
        "bonding_curve_event_count": len(events),
        "total_events_before_trim": len(events),
    }
    if not events:
        return events, meta

    for index, event in enumerate(events):
        progress = event.get("bonding_curve_progress")
        if progress is None:
            continue
        if is_bonding_curve_complete(progress):
            meta["migrated"] = True
            meta["bonding_curve_complete"] = True
            meta["bonding_curve_event_count"] = index + 1
            return events[: index + 1], meta

    return events, meta


def parse_trade_event_payload(data: bytes, offset: int) -> dict | None:
    """Parse TradeEvent fields from raw bytes at offset (after discriminator)."""
    min_len = offset + 32 + 8 + 8 + 1 + 32 + 8 + 8 + 8 + 8 + 8
    if len(data) < min_len:
        return None

    mint = Pubkey.from_bytes(data[offset : offset + 32])
    offset += 32

    sol_amount = struct.unpack_from("<Q", data, offset)[0]
    offset += 8
    token_amount = struct.unpack_from("<Q", data, offset)[0]
    offset += 8
    is_buy = data[offset] != 0
    offset += 1

    user = Pubkey.from_bytes(data[offset : offset + 32])
    offset += 32

    timestamp = struct.unpack_from("<q", data, offset)[0]
    offset += 8
    virtual_sol = struct.unpack_from("<Q", data, offset)[0]
    offset += 8
    virtual_token = struct.unpack_from("<Q", data, offset)[0]
    offset += 8
    real_sol = struct.unpack_from("<Q", data, offset)[0]
    offset += 8
    real_token = struct.unpack_from("<Q", data, offset)[0]

    return {
        "mint": str(mint),
        "sol_amount": sol_amount,
        "token_amount": token_amount,
        "is_buy": is_buy,
        "user": str(user),
        "timestamp": timestamp,
        "virtual_sol_reserves": virtual_sol,
        "virtual_token_reserves": virtual_token,
        "real_sol_reserves": real_sol,
        "real_token_reserves": real_token,
    }


logger = logging.getLogger(__name__)


def atomic_write_text(
    path: Path | str,
    content: str,
    *,
    retries: int = 5,
    retry_delay: float = 0.2,
) -> None:
    """Atomically write text, retrying when Windows locks the target file."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)

    last_err: OSError | None = None
    for attempt in range(retries):
        fd, tmp_path = tempfile.mkstemp(dir=target.parent, suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(content)
            try:
                os.replace(tmp_path, target)
                return
            except OSError as exc:
                last_err = exc
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
                if attempt < retries - 1:
                    time.sleep(retry_delay * (attempt + 1))
                    continue
                break
        except Exception:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            raise

    # Some editors hold a read lock that blocks replace but allow in-place overwrite.
    try:
        with target.open("w", encoding="utf-8") as handle:
            handle.write(content)
        if last_err is not None:
            logger.warning("atomic replace failed for %s; wrote directly (%s)", target, last_err)
        return
    except OSError:
        if last_err is not None:
            raise last_err
        raise


def atomic_write_json(path: Path | str, payload: Any, *, indent: int = 2) -> None:
    """Atomically write JSON to disk."""
    atomic_write_text(path, json.dumps(payload, indent=indent) + "\n")
