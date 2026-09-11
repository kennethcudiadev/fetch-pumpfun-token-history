"""SQLite stays small: JSON is durable history, DB keeps resume state."""

from __future__ import annotations

import asyncio
from pathlib import Path

from models import CrawlState, TradeEvent
from storage import Storage


def _event(mint: str, signature: str, ts: int) -> TradeEvent:
    return TradeEvent(
        signature=signature,
        slot=ts,
        timestamp=ts,
        mint=mint,
        side="BUY",
        wallet="Wallet111111111111111111111111111111111",
        sol_amount=0.1,
        token_amount=1000.0,
        price=0.0001,
        virtual_sol_reserve=30.0,
        virtual_token_reserve=1_000_000.0,
        bonding_curve_progress=0.1,
    )


def test_export_merges_and_purges_events(tmp_path: Path, monkeypatch) -> None:
    async def _run() -> None:
        import config as app_config
        import storage as storage_mod

        monkeypatch.setattr(app_config, "DATA_DIR", tmp_path / "data")
        monkeypatch.setattr(storage_mod, "DATA_DIR", tmp_path / "data")
        monkeypatch.setattr(storage_mod, "STATE_DIR", tmp_path / "data" / ".state")
        monkeypatch.setattr(
            storage_mod,
            "wallet_mint_json_path",
            lambda wallet, mint: tmp_path / "data" / wallet / f"{mint}.json",
        )

        db_path = tmp_path / "slim.db"
        wallet = "7ufmve7ZSFCzuNcKRunYrGtyb2Ka1MXzkWwf7jZhVsmL"
        mint = "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm"
        storage = Storage(db_path=db_path)
        storage.set_export_wallet(wallet)
        await storage.initialize()

        state = CrawlState(
            mint=mint,
            bonding_curve="curve",
            sig_before_cursor=None,
            sig_collection_complete=False,
            last_processed_signature=None,
            last_processed_slot=None,
            total_signatures=0,
            total_events=0,
            status="in_progress",
        )

        await storage.commit_crawl_page(
            state,
            [_event(mint, "sig1", 100)],
            [("sig1", 100)],
        )
        assert state.total_events == 1
        await storage.export_mint_json(mint, "curve", status="in_progress")
        await storage.release_mint_sqlite_payload(mint, completed=False)
        assert await storage.count_events(mint) == 0
        assert await storage.count_processed_txs(mint) == 1

        await storage.commit_crawl_page(
            state,
            [_event(mint, "sig2", 200)],
            [("sig2", 200)],
        )
        assert state.total_events == 2
        state.status = "complete"
        state.sig_collection_complete = True
        await storage.commit_crawl_page(state, [], [])
        await storage.export_mint_json(mint, "curve", status="complete")
        await storage.release_mint_sqlite_payload(mint, completed=True)

        assert await storage.count_events(mint) == 0
        assert await storage.count_processed_txs(mint) == 0
        events = await storage.fetch_events(mint)
        assert [e["signature"] for e in events] == ["sig1", "sig2"]

        crawl = await storage.get_crawl_state(mint)
        assert crawl is not None
        assert crawl.status == "complete"
        assert crawl.total_events == 2

    asyncio.run(_run())
