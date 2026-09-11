"""Tests for trader mint discovery helpers."""

from main import load_mints_from_trader_json
from trader_mints import (
    MintStats,
    aggregate_wallet_trades,
    build_trader_result,
    filter_signatures_for_window,
    split_time_range,
    uncovered_scan_windows,
    wallet_pump_trades,
)


def test_filter_signatures_stops_at_window_boundary():
    from_ts = 1_000_000
    batch = [
        {"signature": "sig1", "slot": 10, "blockTime": 1_000_500, "err": None},
        {"signature": "sig2", "slot": 9, "blockTime": 999_000, "err": None},
    ]

    entries, stop = filter_signatures_for_window(batch, from_ts)

    assert len(entries) == 1
    assert entries[0][0] == "sig1"
    assert stop is True


def test_filter_signatures_skips_failed():
    entries, stop = filter_signatures_for_window(
        [{"signature": "sig1", "slot": 1, "blockTime": 2_000_000, "err": {"code": 1}}],
        1_000_000,
    )
    assert entries == []
    assert stop is False


def test_aggregate_wallet_trades():
    stats: dict[str, MintStats] = {}
    aggregate_wallet_trades(stats, "BUY", "MintA", 100)
    aggregate_wallet_trades(stats, "SELL", "MintA", 200)
    aggregate_wallet_trades(stats, "BUY", "MintB", 150)

    assert len(stats) == 2
    assert stats["MintA"].buy_count == 1
    assert stats["MintA"].sell_count == 1
    assert stats["MintA"].first_trade_time == 100
    assert stats["MintA"].last_trade_time == 200
    assert stats["MintB"].buy_count == 1


def test_uncovered_scan_windows_older_and_newer():
    windows = uncovered_scan_windows(
        covered_from=1_000_360,
        covered_to=1_000_720,
        desired_from=1_000_000,
        desired_to=1_000_800,
    )
    assert windows == [
        (1_000_000, 1_000_359, "extend_older"),
        (1_000_721, 1_000_800, "extend_newer"),
    ]


def test_uncovered_scan_windows_already_covered():
    assert uncovered_scan_windows(100, 200, 120, 180) == []


def test_split_time_range_parallel_shards():
    windows = split_time_range(1000, 1999, 4)
    assert windows == [(1000, 1249), (1250, 1499), (1500, 1749), (1750, 1999)]
    assert split_time_range(10, 12, 8) == [(10, 10), (11, 11), (12, 12)]
    assert split_time_range(50, 40, 3) == []


def test_build_trader_result():
    stats = {"MintA": MintStats(mint="MintA", first_trade_time=100, last_trade_time=200, buy_count=2)}
    result = build_trader_result(
        wallet="wallet1",
        hours=24,
        from_timestamp=100,
        to_timestamp=200,
        mint_stats=stats,
        transactions_scanned=10,
        pumpfun_trades=2,
    )

    assert result["wallet"] == "wallet1"
    assert result["hours"] == 24
    assert result["unique_mints"] == 1
    assert result["mints"][0]["mint"] == "MintA"


def test_load_mints_from_trader_json(tmp_path):
    wallet = "7ufmve7ZSFCzuNcKRunYrGtyb2Ka1MXzkWwf7jZhVsmL"
    payload = {
        "wallet": wallet,
        "hours": 24,
        "mints": [
            {"mint": "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm", "buy_count": 1},
            {"mint": "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm", "buy_count": 2},
        ],
    }
    path = tmp_path / f"{wallet}.json"
    path.write_text(__import__("json").dumps(payload), encoding="utf-8")

    mints = load_mints_from_trader_json(path)
    assert len(mints) == 1
    assert mints[0] == "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm"


def test_wallet_pump_trades_uses_swap_event_and_skips_sol():
    wallet = "Wallet111"
    mint = "MintPump111"
    tx = {
        "timestamp": 1_700_000_000,
        "events": {
            "swap": {
                "tokenOutputs": [
                    {"userAccount": wallet, "mint": mint},
                    {"userAccount": wallet, "mint": "So11111111111111111111111111111111111111112"},
                ],
                "tokenInputs": [],
            }
        },
        "tokenTransfers": [
            {"toUserAccount": wallet, "mint": "ShouldNotUseWhenSwapPresent"},
        ],
    }

    trades = wallet_pump_trades(tx, wallet)

    assert trades == [(mint, "BUY", 1_700_000_000)]


def test_wallet_pump_trades_falls_back_to_token_transfers():
    wallet = "Wallet111"
    tx = {
        "timestamp": 50,
        "tokenTransfers": [
            {"fromUserAccount": wallet, "mint": "MintSell"},
            {"toUserAccount": "other", "mint": "MintOther"},
        ],
    }

    assert wallet_pump_trades(tx, wallet) == [("MintSell", "SELL", 50)]
