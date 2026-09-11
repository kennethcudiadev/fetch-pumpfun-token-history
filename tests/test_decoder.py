"""Tests for Pump.fun TradeEvent decoder."""

import struct

from solders.pubkey import Pubkey

from config import TRADE_EVENT_DISCRIMINATOR
from pumpfun_decoder import decode_trade_events_from_transaction
from utils import parse_trade_event_payload


MINT = Pubkey.from_string("EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm")
USER = Pubkey.from_string("11111111111111111111111111111112")


def _build_trade_event_bytes(
    *,
    sol_lamports: int = 1_000_000_000,
    token_raw: int = 4_000_000_000_000,
    is_buy: bool = True,
    virtual_sol: int = 30_000_000_000,
    virtual_token: int = 1_073_000_000_000_000,
    real_sol: int = 1_000_000_000,
    real_token: int = 793_000_000_000_000,
    timestamp: int = 1_700_000_000,
) -> bytes:
    payload = bytearray()
    payload += TRADE_EVENT_DISCRIMINATOR
    payload += bytes(MINT)
    payload += struct.pack("<Q", sol_lamports)
    payload += struct.pack("<Q", token_raw)
    payload += bytes([1 if is_buy else 0])
    payload += bytes(USER)
    payload += struct.pack("<q", timestamp)
    payload += struct.pack("<Q", virtual_sol)
    payload += struct.pack("<Q", virtual_token)
    payload += struct.pack("<Q", real_sol)
    payload += struct.pack("<Q", real_token)
    return bytes(payload)


def test_parse_trade_event_payload():
    data = _build_trade_event_bytes()
    parsed = parse_trade_event_payload(data, len(TRADE_EVENT_DISCRIMINATOR))

    assert parsed is not None
    assert parsed["mint"] == str(MINT)
    assert parsed["user"] == str(USER)
    assert parsed["sol_amount"] == 1_000_000_000
    assert parsed["token_amount"] == 4_000_000_000_000
    assert parsed["is_buy"] is True
    assert parsed["timestamp"] == 1_700_000_000


def test_decode_trade_events_from_transaction_inner_instruction():
    import base58

    event_data = _build_trade_event_bytes(is_buy=False, sol_lamports=500_000_000)
    encoded = base58.b58encode(event_data).decode()

    tx = {
        "slot": 250_000_000,
        "blockTime": 1_700_000_100,
        "transaction": {
            "message": {
                "instructions": [],
                "accountKeys": ["6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"],
            }
        },
        "meta": {
            "innerInstructions": [
                {
                    "index": 0,
                    "instructions": [
                        {"programId": "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P", "data": encoded}
                    ],
                }
            ]
        },
    }

    events = decode_trade_events_from_transaction(
        tx,
        signature="5testSig123456789012345678901234567890123456789012345678901234",
        expected_mint=str(MINT),
    )

    assert len(events) == 1
    event = events[0]
    assert event.side == "SELL"
    assert event.sol_amount == 0.5
    assert event.token_amount == 4_000_000.0
    assert event.price == 0.5 / 4_000_000.0
    assert event.slot == 250_000_000
    assert event.wallet == str(USER)


def test_decode_skips_wrong_mint():
    import base58

    other_mint = Pubkey.from_string("So11111111111111111111111111111111111111112")
    payload = bytearray()
    payload += TRADE_EVENT_DISCRIMINATOR
    payload += bytes(other_mint)
    payload += struct.pack("<Q", 1)
    payload += struct.pack("<Q", 1)
    payload += bytes([1])
    payload += bytes(USER)
    payload += struct.pack("<q", 1)
    payload += struct.pack("<Q", 1)
    payload += struct.pack("<Q", 1)
    payload += struct.pack("<Q", 1)
    payload += struct.pack("<Q", 1)

    tx = {
        "slot": 1,
        "blockTime": 1,
        "transaction": {"message": {"instructions": [{"data": base58.b58encode(payload).decode()}]}},
        "meta": {},
    }

    events = decode_trade_events_from_transaction(
        tx,
        signature="sig",
        expected_mint=str(MINT),
    )
    assert events == []
