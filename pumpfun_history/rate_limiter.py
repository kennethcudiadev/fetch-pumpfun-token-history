"""Per-key async rate limiting with round-robin key rotation."""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass, field

from config import KEY_COOLDOWN_SECONDS, REQUESTS_PER_SECOND_PER_KEY

logger = logging.getLogger(__name__)


@dataclass
class KeyState:
    """Tracks rate-limit and cooldown state for one Helius API key."""

    key: str
    timestamps: deque[float] = field(default_factory=deque)
    disabled_until: float = 0.0
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    @property
    def is_available(self) -> bool:
        return time.monotonic() >= self.disabled_until


class RateLimiterPool:
    """
    Round-robin pool of per-key rate limiters.

    Each key is limited to `requests_per_second` using a sliding 1-second window.
    Keys that receive HTTP 429 are temporarily disabled.
    """

    def __init__(
        self,
        keys: list[str],
        requests_per_second: float = REQUESTS_PER_SECOND_PER_KEY,
        cooldown_seconds: float = KEY_COOLDOWN_SECONDS,
    ) -> None:
        if not keys:
            raise ValueError("At least one Helius API key is required")

        self._keys = [KeyState(key=k) for k in keys]
        self._requests_per_second = requests_per_second
        self._cooldown_seconds = cooldown_seconds
        self._round_robin_index = 0
        self._pool_lock = asyncio.Lock()

    @property
    def key_count(self) -> int:
        return len(self._keys)

    def status_brief(self) -> str:
        """Compact rate-limiter summary for progress lines."""
        now = time.monotonic()
        cooling = sum(1 for state in self._keys if not state.is_available)
        active = self.key_count - cooling
        if cooling:
            return f"keys {active}/{self.key_count} ({cooling} cooling)"
        return f"keys {self.key_count} ok"

    def status_summary(self) -> str:
        now = time.monotonic()
        parts: list[str] = []
        for state in self._keys:
            masked = f"{state.key[:4]}...{state.key[-4:]}" if len(state.key) > 8 else "***"
            if not state.is_available:
                remaining = max(0.0, state.disabled_until - now)
                parts.append(f"{masked}:COOLDOWN({remaining:.0f}s)")
            else:
                parts.append(f"{masked}:OK({len(state.timestamps)}/{self._requests_per_second})")
        return " | ".join(parts)

    async def acquire(self) -> str:
        """Wait for an available key and return it."""
        while True:
            async with self._pool_lock:
                for _ in range(len(self._keys)):
                    state = self._keys[self._round_robin_index]
                    self._round_robin_index = (self._round_robin_index + 1) % len(self._keys)

                    if not state.is_available:
                        continue

                    if await self._try_acquire(state):
                        return state.key

            await asyncio.sleep(0.05)

    async def _try_acquire(self, state: KeyState) -> bool:
        async with state.lock:
            now = time.monotonic()
            window_start = now - 1.0

            while state.timestamps and state.timestamps[0] < window_start:
                state.timestamps.popleft()

            if len(state.timestamps) >= self._requests_per_second:
                return False

            state.timestamps.append(now)
            return True

    def mark_rate_limited(self, key: str) -> None:
        """Temporarily disable a key after HTTP 429."""
        for state in self._keys:
            if state.key == key:
                state.disabled_until = time.monotonic() + self._cooldown_seconds
                masked = f"{key[:4]}...{key[-4:]}" if len(key) > 8 else "***"
                logger.warning(
                    "Key %s rate-limited; disabled for %.0fs",
                    masked,
                    self._cooldown_seconds,
                )
                return
