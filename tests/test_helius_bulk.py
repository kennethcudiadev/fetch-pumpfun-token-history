"""Tests for Helius bulk fetch helpers and transaction normalization."""

from helius_client import HeliusClient
from pumpfun_decoder import (
    extract_transaction_signature,
    normalize_transaction_record,
)


def test_estimate_bulk_credits():
    assert HeliusClient.estimate_bulk_credits(0) == 0
    assert HeliusClient.estimate_bulk_credits(1) == 10
    assert HeliusClient.estimate_bulk_credits(100) == 10
    assert HeliusClient.estimate_bulk_credits(101) == 20
    assert HeliusClient.estimate_bulk_credits(1000) == 100


def test_normalize_get_transactions_for_address_record():
    record = {
        "slot": 123,
        "blockTime": 1_700_000_000,
        "transaction": {
            "signatures": ["sig123"],
            "message": {"instructions": [], "accountKeys": []},
        },
        "meta": {"innerInstructions": [], "err": None},
    }
    normalized = normalize_transaction_record(record)
    assert normalized is not None
    assert normalized["slot"] == 123
    assert normalized["blockTime"] == 1_700_000_000
    assert extract_transaction_signature(record) == "sig123"


def test_normalize_get_transaction_record():
    record = {
        "slot": 456,
        "blockTime": 1_800_000_000,
        "transaction": {
            "signatures": ["sig456"],
            "message": {"instructions": []},
        },
        "meta": {"innerInstructions": []},
    }
    normalized = normalize_transaction_record(record)
    assert normalized is not None
    assert extract_transaction_signature(record) == "sig456"
