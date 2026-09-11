"""SQLite persistence for trade events and crawl progress."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import aiosqlite

from config import DATABASE_PATH, DATA_DIR, STATE_DIR, wallet_mint_json_path
from models import CrawlState, TradeEvent
from utils import atomic_write_json, trim_events_to_bonding_curve

logger = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS pump_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    signature TEXT NOT NULL UNIQUE,
    slot INTEGER NOT NULL,
    timestamp INTEGER NOT NULL,
    mint TEXT NOT NULL,
    side TEXT NOT NULL,
    wallet TEXT NOT NULL,
    sol_amount REAL NOT NULL,
    token_amount REAL NOT NULL,
    price REAL NOT NULL,
    virtual_sol_reserve REAL NOT NULL,
    virtual_token_reserve REAL NOT NULL,
    bonding_curve_progress REAL NOT NULL DEFAULT 0.0
);

CREATE INDEX IF NOT EXISTS idx_pump_events_mint ON pump_events(mint);
CREATE INDEX IF NOT EXISTS idx_pump_events_timestamp ON pump_events(timestamp);
CREATE INDEX IF NOT EXISTS idx_pump_events_signature ON pump_events(signature);

CREATE TABLE IF NOT EXISTS crawl_state (
    mint TEXT PRIMARY KEY,
    bonding_curve TEXT NOT NULL,
    sig_before_cursor TEXT,
    sig_collection_complete INTEGER NOT NULL DEFAULT 0,
    last_processed_signature TEXT,
    last_processed_slot INTEGER,
    total_signatures INTEGER NOT NULL DEFAULT 0,
    total_events INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS pending_signatures (
    signature TEXT PRIMARY KEY,
    mint TEXT NOT NULL,
    slot INTEGER NOT NULL,
    processed INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_pending_mint_processed
    ON pending_signatures(mint, processed);

CREATE TABLE IF NOT EXISTS crawl_processed_txs (
    mint TEXT NOT NULL,
    signature TEXT NOT NULL,
    slot INTEGER NOT NULL,
    PRIMARY KEY (mint, signature)
);
"""


class Storage:
    """Async SQLite storage layer."""

    def __init__(self, db_path: Path | None = None) -> None:
        self.db_path = db_path or DATABASE_PATH
        self.export_wallet: str | None = None
        self._write_lock = asyncio.Lock()

    def set_export_wallet(self, wallet: str | None) -> None:
        """Mint JSON exports go under data/<wallet>/<mint>.json when set."""
        self.export_wallet = wallet

    async def initialize(self) -> None:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("PRAGMA journal_mode=WAL")
            await db.execute("PRAGMA synchronous=NORMAL")
            await db.executescript(SCHEMA)
            await self._migrate_schema(db)
            await db.commit()
        logger.debug("Database ready: %s", self.db_path)

    async def _migrate_schema(self, db: aiosqlite.Connection) -> None:
        async with db.execute("PRAGMA table_info(crawl_state)") as cursor:
            rows = await cursor.fetchall()
        columns = {row[1] for row in rows}
        if "pagination_token" not in columns:
            await db.execute(
                "ALTER TABLE crawl_state ADD COLUMN pagination_token TEXT"
            )
        if "credits_used" not in columns:
            await db.execute(
                "ALTER TABLE crawl_state ADD COLUMN credits_used INTEGER NOT NULL DEFAULT 0"
            )
        if "status" not in columns:
            await db.execute(
                "ALTER TABLE crawl_state ADD COLUMN status TEXT NOT NULL DEFAULT 'in_progress'"
            )

    async def get_crawl_state(self, mint: str) -> CrawlState | None:
        async with aiosqlite.connect(self.db_path, timeout=60) as db:
            await db.execute("PRAGMA busy_timeout = 60000")
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM crawl_state WHERE mint = ?",
                (mint,),
            ) as cursor:
                row = await cursor.fetchone()

        if row is None:
            return None

        return CrawlState(
            mint=row["mint"],
            bonding_curve=row["bonding_curve"],
            sig_before_cursor=row["sig_before_cursor"],
            sig_collection_complete=bool(row["sig_collection_complete"]),
            last_processed_signature=row["last_processed_signature"],
            last_processed_slot=row["last_processed_slot"],
            total_signatures=row["total_signatures"],
            total_events=row["total_events"],
            pagination_token=row["pagination_token"] if "pagination_token" in row.keys() else None,
            credits_used=row["credits_used"] if "credits_used" in row.keys() else 0,
            status=row["status"] if "status" in row.keys() else "in_progress",
        )

    async def upsert_crawl_state(self, state: CrawlState) -> None:
        async with self._write_lock:
            await self._upsert_crawl_state(state)

    async def _upsert_crawl_state(self, state: CrawlState) -> None:
        async with aiosqlite.connect(self.db_path, timeout=60) as db:
            await db.execute("PRAGMA busy_timeout = 60000")
            await db.execute(
                """
                INSERT INTO crawl_state (
                    mint, bonding_curve, sig_before_cursor,
                    sig_collection_complete, last_processed_signature,
                    last_processed_slot, total_signatures, total_events,
                    pagination_token, credits_used, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(mint) DO UPDATE SET
                    bonding_curve = excluded.bonding_curve,
                    sig_before_cursor = excluded.sig_before_cursor,
                    sig_collection_complete = excluded.sig_collection_complete,
                    last_processed_signature = excluded.last_processed_signature,
                    last_processed_slot = excluded.last_processed_slot,
                    total_signatures = excluded.total_signatures,
                    total_events = excluded.total_events,
                    pagination_token = excluded.pagination_token,
                    credits_used = excluded.credits_used,
                    status = excluded.status
                """,
                (
                    state.mint,
                    state.bonding_curve,
                    state.sig_before_cursor,
                    int(state.sig_collection_complete),
                    state.last_processed_signature,
                    state.last_processed_slot,
                    state.total_signatures,
                    state.total_events,
                    state.pagination_token,
                    state.credits_used,
                    state.status,
                ),
            )
            await db.commit()

    async def is_tx_processed(self, mint: str, signature: str) -> bool:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                """
                SELECT 1 FROM crawl_processed_txs
                WHERE mint = ? AND signature = ?
                LIMIT 1
                """,
                (mint, signature),
            ) as cursor:
                return await cursor.fetchone() is not None

    async def mark_tx_processed(self, mint: str, signature: str, slot: int) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT OR IGNORE INTO crawl_processed_txs (mint, signature, slot)
                VALUES (?, ?, ?)
                """,
                (mint, signature, slot),
            )
            await db.commit()

    async def count_processed_txs(self, mint: str) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT COUNT(*) FROM crawl_processed_txs WHERE mint = ?",
                (mint,),
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else 0

    async def save_checkpoint(
        self,
        state: CrawlState,
        *,
        export_json: bool = True,
    ) -> None:
        """Persist crawl state and optionally export JSON atomically."""
        state.total_events = await self.count_events(state.mint)
        await self.upsert_crawl_state(state)
        if export_json:
            await self.export_mint_json(
                state.mint,
                state.bonding_curve,
                signatures_total=state.total_signatures,
                signatures_pending=0,
                status=state.status,
                pagination_token=state.pagination_token,
                credits_used=state.credits_used,
            )

    async def signature_exists(self, signature: str) -> bool:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT 1 FROM pump_events WHERE signature = ? LIMIT 1",
                (signature,),
            ) as cursor:
                return await cursor.fetchone() is not None

    async def insert_signatures_batch(
        self,
        mint: str,
        signatures: list[tuple[str, int]],
    ) -> int:
        """Insert signatures, ignoring duplicates. Returns count of new rows."""
        if not signatures:
            return 0

        async with aiosqlite.connect(self.db_path) as db:
            await db.executemany(
                """
                INSERT OR IGNORE INTO pending_signatures (signature, mint, slot, processed)
                VALUES (?, ?, ?, 0)
                """,
                [(sig, mint, slot) for sig, slot in signatures],
            )
            await db.commit()
            return db.total_changes

    async def count_pending_signatures(self, mint: str) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT COUNT(*) FROM pending_signatures WHERE mint = ? AND processed = 0",
                (mint,),
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else 0

    async def count_total_signatures(self, mint: str) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT COUNT(*) FROM pending_signatures WHERE mint = ?",
                (mint,),
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else 0

    async def fetch_unprocessed_signatures(
        self,
        mint: str,
        limit: int = 100,
    ) -> list[tuple[str, int]]:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                """
                SELECT signature, slot FROM pending_signatures
                WHERE mint = ? AND processed = 0
                ORDER BY slot ASC
                LIMIT ?
                """,
                (mint, limit),
            ) as cursor:
                rows = await cursor.fetchall()
                return [(row[0], row[1]) for row in rows]

    async def mark_signature_processed(
        self,
        signature: str,
        slot: int,
        mint: str,
    ) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE pending_signatures SET processed = 1 WHERE signature = ?",
                (signature,),
            )
            await db.execute(
                """
                UPDATE crawl_state
                SET last_processed_signature = ?,
                    last_processed_slot = ?
                WHERE mint = ?
                """,
                (signature, slot, mint),
            )
            await db.commit()

    async def discard_incomplete_mints(self, mints: list[str]) -> list[str]:
        """Drop partial crawls so those mints are fetched again from the start."""
        if not mints:
            return []

        wanted = set(mints)
        async with aiosqlite.connect(self.db_path, timeout=30) as db:
            async with db.execute(
                """
                SELECT mint FROM crawl_state
                WHERE NOT (status = 'complete' AND sig_collection_complete = 1)
                """
            ) as cursor:
                rows = await cursor.fetchall()
        targets = [row[0] for row in rows if row[0] in wanted]
        if not targets:
            return []

        async with self._write_lock:
            async with aiosqlite.connect(self.db_path, timeout=30) as db:
                await db.execute("PRAGMA busy_timeout = 30000")
                for mint in targets:
                    await db.execute("DELETE FROM pump_events WHERE mint = ?", (mint,))
                    await db.execute(
                        "DELETE FROM crawl_processed_txs WHERE mint = ?",
                        (mint,),
                    )
                    await db.execute(
                        "DELETE FROM pending_signatures WHERE mint = ?",
                        (mint,),
                    )
                    await db.execute("DELETE FROM crawl_state WHERE mint = ?", (mint,))
                await db.commit()

        removed: list[str] = []
        for mint in targets:
            path = self.json_path_for_mint(mint)
            if path.is_file():
                path.unlink()
            removed.append(mint)
        return removed

    async def complete_mint_addresses(self) -> set[str]:
        """Mints whose bonding-curve crawl finished and can be skipped on resume."""
        async with aiosqlite.connect(self.db_path, timeout=30) as db:
            async with db.execute(
                """
                SELECT mint FROM crawl_state
                WHERE status = 'complete' AND sig_collection_complete = 1
                """
            ) as cursor:
                rows = await cursor.fetchall()
        return {row[0] for row in rows}

    async def processed_signatures_among(
        self,
        mint: str,
        signatures: list[str],
    ) -> set[str]:
        """Which of these signatures were already committed for this mint."""
        if not signatures:
            return set()

        found: set[str] = set()
        async with aiosqlite.connect(self.db_path, timeout=30) as db:
            await db.execute("PRAGMA busy_timeout = 30000")
            for offset in range(0, len(signatures), 400):
                chunk = signatures[offset : offset + 400]
                placeholders = ",".join("?" * len(chunk))
                async with db.execute(
                    f"""
                    SELECT signature FROM crawl_processed_txs
                    WHERE mint = ? AND signature IN ({placeholders})
                    """,
                    (mint, *chunk),
                ) as cursor:
                    rows = await cursor.fetchall()
                found.update(row[0] for row in rows)
        return found

    async def commit_crawl_page(
        self,
        state: CrawlState,
        events: list[TradeEvent],
        processed: list[tuple[str, int]],
    ) -> None:
        """Commit one fetched page, then advance the resume cursor."""
        event_rows = [
            (
                event.signature,
                event.slot,
                event.timestamp,
                event.mint,
                event.side,
                event.wallet,
                event.sol_amount,
                event.token_amount,
                event.price,
                event.virtual_sol_reserve,
                event.virtual_token_reserve,
                event.bonding_curve_progress,
            )
            for event in events
        ]

        async with self._write_lock:
            async with aiosqlite.connect(self.db_path, timeout=30) as db:
                await db.execute("PRAGMA busy_timeout = 30000")
                if event_rows:
                    await db.executemany(
                        """
                        INSERT OR IGNORE INTO pump_events (
                            signature, slot, timestamp, mint, side, wallet,
                            sol_amount, token_amount, price,
                            virtual_sol_reserve, virtual_token_reserve,
                            bonding_curve_progress
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        event_rows,
                    )
                if processed:
                    await db.executemany(
                        """
                        INSERT OR IGNORE INTO crawl_processed_txs (mint, signature, slot)
                        VALUES (?, ?, ?)
                        """,
                        [(state.mint, signature, slot) for signature, slot in processed],
                    )
                async with db.execute(
                    "SELECT COUNT(*) FROM pump_events WHERE mint = ?",
                    (state.mint,),
                ) as cursor:
                    row = await cursor.fetchone()
                    state.total_events = row[0] if row else 0
                await db.execute(
                    """
                    INSERT INTO crawl_state (
                        mint, bonding_curve, sig_before_cursor,
                        sig_collection_complete, last_processed_signature,
                        last_processed_slot, total_signatures, total_events,
                        pagination_token, credits_used, status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(mint) DO UPDATE SET
                        bonding_curve = excluded.bonding_curve,
                        sig_before_cursor = excluded.sig_before_cursor,
                        sig_collection_complete = excluded.sig_collection_complete,
                        last_processed_signature = excluded.last_processed_signature,
                        last_processed_slot = excluded.last_processed_slot,
                        total_signatures = excluded.total_signatures,
                        total_events = excluded.total_events,
                        pagination_token = excluded.pagination_token,
                        credits_used = excluded.credits_used,
                        status = excluded.status
                    """,
                    (
                        state.mint,
                        state.bonding_curve,
                        state.sig_before_cursor,
                        int(state.sig_collection_complete),
                        state.last_processed_signature,
                        state.last_processed_slot,
                        state.total_signatures,
                        state.total_events,
                        state.pagination_token,
                        state.credits_used,
                        state.status,
                    ),
                )
                await db.commit()

    async def insert_events(self, events: list[TradeEvent]) -> int:
        """Insert trade events, ignoring duplicate signatures. Returns new row count."""
        if not events:
            return 0

        rows = [
            (
                e.signature,
                e.slot,
                e.timestamp,
                e.mint,
                e.side,
                e.wallet,
                e.sol_amount,
                e.token_amount,
                e.price,
                e.virtual_sol_reserve,
                e.virtual_token_reserve,
                e.bonding_curve_progress,
            )
            for e in events
        ]

        async with aiosqlite.connect(self.db_path) as db:
            await db.executemany(
                """
                INSERT OR IGNORE INTO pump_events (
                    signature, slot, timestamp, mint, side, wallet,
                    sol_amount, token_amount, price,
                    virtual_sol_reserve, virtual_token_reserve,
                    bonding_curve_progress
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            await db.commit()
            return db.total_changes

    async def count_events(self, mint: str) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT COUNT(*) FROM pump_events WHERE mint = ?",
                (mint,),
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else 0

    async def update_total_events(self, mint: str) -> int:
        count = await self.count_events(mint)
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE crawl_state SET total_events = ? WHERE mint = ?",
                (count, mint),
            )
            await db.commit()
        return count

    async def fetch_events(self, mint: str) -> list[dict[str, Any]]:
        """Return all stored events for a mint ordered by timestamp."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                """
                SELECT signature, slot, timestamp, mint, side, wallet,
                       sol_amount, token_amount, price,
                       virtual_sol_reserve, virtual_token_reserve,
                       bonding_curve_progress
                FROM pump_events
                WHERE mint = ?
                ORDER BY timestamp ASC, slot ASC
                """,
                (mint,),
            ) as cursor:
                rows = await cursor.fetchall()

        return [
            {
                "signature": row["signature"],
                "slot": row["slot"],
                "timestamp": row["timestamp"],
                "mint": row["mint"],
                "side": row["side"],
                "wallet": row["wallet"],
                "sol_amount": row["sol_amount"],
                "token_amount": row["token_amount"],
                "price": row["price"],
                "virtual_sol_reserve": row["virtual_sol_reserve"],
                "virtual_token_reserve": row["virtual_token_reserve"],
                "bonding_curve_progress": row["bonding_curve_progress"],
            }
            for row in rows
        ]

    def json_path_for_mint(self, mint: str, wallet: str | None = None) -> Path:
        owner = wallet or self.export_wallet
        if not owner:
            raise ValueError("wallet is required for JSON export (set --wallet or use --file)")
        return wallet_mint_json_path(owner, mint)

    async def export_mint_json(
        self,
        mint: str,
        bonding_curve: str | None = None,
        *,
        wallet: str | None = None,
        signatures_total: int | None = None,
        signatures_pending: int | None = None,
        status: str = "in_progress",
        pagination_token: str | None = None,
        credits_used: int | None = None,
    ) -> Path:
        """Write bonding-curve events to data/<wallet>/<mint>.json."""
        events = await self.fetch_events(mint)
        events, curve_meta = trim_events_to_bonding_curve(events)
        path = self.json_path_for_mint(mint, wallet=wallet)
        path.parent.mkdir(parents=True, exist_ok=True)

        payload = {
            "mint": mint,
            "wallet": wallet or self.export_wallet,
            "bonding_curve": bonding_curve,
            "status": status,
            "migrated": curve_meta["migrated"],
            "bonding_curve_complete": curve_meta["bonding_curve_complete"],
            "event_count": len(events),
            "bonding_curve_event_count": curve_meta["bonding_curve_event_count"],
            "total_events_before_trim": curve_meta["total_events_before_trim"],
            "signatures_total": signatures_total,
            "signatures_pending": signatures_pending,
            "pagination_token": pagination_token,
            "credits_used": credits_used,
            "exported_at": datetime.now(UTC).isoformat(),
            "events": events,
        }

        def _write() -> None:
            atomic_write_json(path, payload)

        try:
            await asyncio.to_thread(_write)
        except OSError as exc:
            logger.warning(
                "JSON export skipped for %s (close the file in your editor if open): %s",
                path,
                exc,
            )
            return path

        logger.debug("Exported %d events to %s", len(events), path)
        return path
