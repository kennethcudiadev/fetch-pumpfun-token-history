"""Data models for Pump.fun trade events and crawl state."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


TradeSide = Literal["BUY", "SELL"]


@dataclass(frozen=True)
class TradeEvent:
    """Decoded Pump.fun bonding curve trade."""

    signature: str
    slot: int
    timestamp: int
    mint: str
    side: TradeSide
    wallet: str
    sol_amount: float
    token_amount: float
    price: float
    virtual_sol_reserve: float
    virtual_token_reserve: float
    bonding_curve_progress: float

    def to_dict(self) -> dict:
        return {
            "signature": self.signature,
            "slot": self.slot,
            "timestamp": self.timestamp,
            "mint": self.mint,
            "side": self.side,
            "wallet": self.wallet,
            "sol_amount": self.sol_amount,
            "token_amount": self.token_amount,
            "price": self.price,
            "virtual_sol_reserve": self.virtual_sol_reserve,
            "virtual_token_reserve": self.virtual_token_reserve,
            "bonding_curve_progress": self.bonding_curve_progress,
        }


@dataclass
class CrawlState:
    """Persisted progress for resumable crawls."""

    mint: str
    bonding_curve: str
    sig_before_cursor: str | None
    sig_collection_complete: bool
    last_processed_signature: str | None
    last_processed_slot: int | None
    total_signatures: int
    total_events: int
    pagination_token: str | None = None
    credits_used: int = 0
    status: str = "in_progress"
