"""Tests for async rate limiter pool."""

import asyncio
import time

import pytest

from rate_limiter import RateLimiterPool


@pytest.mark.asyncio
async def test_rate_limiter_respects_per_key_limit():
    pool = RateLimiterPool(["key-a"], requests_per_second=3)
    start = time.monotonic()

    for _ in range(3):
        await pool.acquire()

    elapsed = time.monotonic() - start
    assert elapsed < 0.5


@pytest.mark.asyncio
async def test_rate_limiter_blocks_when_limit_exceeded():
    pool = RateLimiterPool(["key-a"], requests_per_second=2)
    await pool.acquire()
    await pool.acquire()

    start = time.monotonic()
    await pool.acquire()
    elapsed = time.monotonic() - start

    assert elapsed >= 0.8


@pytest.mark.asyncio
async def test_round_robin_key_rotation():
    pool = RateLimiterPool(["key-a", "key-b", "key-c"], requests_per_second=100)
    seen: list[str] = []

    for _ in range(6):
        seen.append(await pool.acquire())

    assert seen == ["key-a", "key-b", "key-c", "key-a", "key-b", "key-c"]


@pytest.mark.asyncio
async def test_rate_limited_key_is_temporarily_disabled():
    pool = RateLimiterPool(["key-a", "key-b"], requests_per_second=100, cooldown_seconds=0.2)
    pool.mark_rate_limited("key-a")

    key = await pool.acquire()
    assert key == "key-b"

    await asyncio.sleep(0.25)
    keys = {await pool.acquire() for _ in range(2)}
    assert "key-a" in keys
