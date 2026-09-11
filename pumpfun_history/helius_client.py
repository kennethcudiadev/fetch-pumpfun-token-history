"""Async Helius RPC client with key rotation and retries."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from typing import Any
from urllib.parse import quote

import aiohttp

from config import (
    ENHANCED_TX_PAGE_LIMIT,
    ENHANCED_TX_REQUEST_CREDITS,
    HELIUS_API_BASE,
    HELIUS_RPC_BASE,
    INITIAL_BACKOFF_SECONDS,
    MAX_BACKOFF_SECONDS,
    MAX_RETRIES,
    PUMPFUN_TX_SOURCE,
)
from rate_limiter import RateLimiterPool

logger = logging.getLogger(__name__)


class HeliusRpcError(Exception):
    """Raised when an RPC call fails after retries."""


class HeliusClient:
    """Async JSON-RPC client for Helius with automatic key rotation."""

    def __init__(
        self,
        rate_limiter: RateLimiterPool,
        session: aiohttp.ClientSession | None = None,
    ) -> None:
        self._rate_limiter = rate_limiter
        self._session = session
        self._request_id = 0
        self._id_lock = asyncio.Lock()

    async def __aenter__(self) -> HeliusClient:
        if self._session is None:
            timeout = aiohttp.ClientTimeout(total=60, connect=15)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def _next_id(self) -> int:
        async with self._id_lock:
            self._request_id += 1
            return self._request_id

    async def rpc_call(self, method: str, params: list[Any]) -> Any:
        """Execute a JSON-RPC call with retries and exponential backoff."""
        if self._session is None:
            raise RuntimeError("HeliusClient must be used as async context manager")

        last_error: Exception | None = None

        for attempt in range(MAX_RETRIES):
            key = await self._rate_limiter.acquire()
            url = f"{HELIUS_RPC_BASE}/?api-key={key}"
            payload = {
                "jsonrpc": "2.0",
                "id": await self._next_id(),
                "method": method,
                "params": params,
            }

            try:
                async with self._session.post(url, json=payload) as response:
                    if response.status == 429:
                        self._rate_limiter.mark_rate_limited(key)
                        last_error = HeliusRpcError(f"HTTP 429 on {method}")
                        backoff = min(
                            INITIAL_BACKOFF_SECONDS * (2**attempt),
                            MAX_BACKOFF_SECONDS,
                        )
                        logger.debug(
                            "Rate limited on %s (attempt %d/%d), backoff %.1fs",
                            method,
                            attempt + 1,
                            MAX_RETRIES,
                            backoff,
                        )
                        await asyncio.sleep(backoff)
                        continue

                    if response.status >= 500:
                        last_error = HeliusRpcError(
                            f"HTTP {response.status} on {method}"
                        )
                        backoff = min(
                            INITIAL_BACKOFF_SECONDS * (2**attempt),
                            MAX_BACKOFF_SECONDS,
                        )
                        logger.debug(
                            "Server error on %s (attempt %d/%d), backoff %.1fs",
                            method,
                            attempt + 1,
                            MAX_RETRIES,
                            backoff,
                        )
                        await asyncio.sleep(backoff)
                        continue

                    body = await response.json()

                    if "error" in body:
                        err = body["error"]
                        code = err.get("code", 0)
                        message = err.get("message", str(err))

                        if code in (-32429, 429) or "rate" in message.lower():
                            self._rate_limiter.mark_rate_limited(key)
                            last_error = HeliusRpcError(f"RPC rate limit: {message}")
                            backoff = min(
                                INITIAL_BACKOFF_SECONDS * (2**attempt),
                                MAX_BACKOFF_SECONDS,
                            )
                            await asyncio.sleep(backoff)
                            continue

                        raise HeliusRpcError(f"RPC error {code}: {message}")

                    return body.get("result")

            except aiohttp.ClientError as exc:
                last_error = exc
                backoff = min(
                    INITIAL_BACKOFF_SECONDS * (2**attempt),
                    MAX_BACKOFF_SECONDS,
                )
                logger.debug(
                    "Network error on %s (attempt %d/%d): %s",
                    method,
                    attempt + 1,
                    MAX_RETRIES,
                    exc,
                )
                await asyncio.sleep(backoff)

        raise HeliusRpcError(
            f"Failed {method} after {MAX_RETRIES} attempts: {last_error}"
        )

    @staticmethod
    def estimate_bulk_credits(tx_count: int) -> int:
        """Helius credits for one getTransactionsForAddress full page."""
        if tx_count <= 0:
            return 0
        return max(10, ((tx_count + 99) // 100) * 10)

    async def get_transactions_page(
        self,
        address: str,
        *,
        sort_order: str = "asc",
        limit: int = 1000,
        pagination_token: str | None = None,
        filters: dict[str, Any] | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        """
        Fetch one page via Helius getTransactionsForAddress (full transactions).

        Returns (records, next_pagination_token).
        """
        config: dict[str, Any] = {
            "transactionDetails": "full",
            "sortOrder": sort_order,
            "limit": limit,
            "encoding": "json",
            "maxSupportedTransactionVersion": 0,
            "commitment": "confirmed",
            "filters": filters or {"status": "succeeded"},
        }
        if pagination_token:
            config["paginationToken"] = pagination_token

        result = await self.rpc_call(
            "getTransactionsForAddress",
            [address, config],
        )
        if not result:
            return [], None

        data = result.get("data") or []
        return data, result.get("paginationToken")

    async def iter_transaction_pages(
        self,
        address: str,
        *,
        sort_order: str = "asc",
        limit: int = 1000,
        filters: dict[str, Any] | None = None,
        start_token: str | None = None,
    ) -> AsyncIterator[tuple[list[dict[str, Any]], str | None, int]]:
        """Async generator yielding (records, next_token, page_credits)."""
        pagination_token = start_token
        while True:
            records, next_token = await self.get_transactions_page(
                address,
                sort_order=sort_order,
                limit=limit,
                pagination_token=pagination_token,
                filters=filters,
            )
            credits = self.estimate_bulk_credits(len(records))
            yield records, next_token, credits
            if not next_token or not records:
                break
            pagination_token = next_token

    async def get_pumpfun_history_page(
        self,
        address: str,
        *,
        before_signature: str | None = None,
        from_timestamp: int | None = None,
        to_timestamp: int | None = None,
        limit: int = ENHANCED_TX_PAGE_LIMIT,
    ) -> list[dict[str, Any]]:
        """
        One page of Pump.fun-sourced history for a wallet.

        Helius filters by `source=PUMP_FUN` so non-Pump trades are not downloaded.
        """
        params: dict[str, Any] = {
            "source": PUMPFUN_TX_SOURCE,
            "limit": limit,
            "sort-order": "desc",
            "token-accounts": "balanceChanged",
            "commitment": "confirmed",
        }
        if from_timestamp is not None:
            params["gte-time"] = from_timestamp
        if to_timestamp is not None:
            params["lte-time"] = to_timestamp
        if before_signature:
            params["before-signature"] = before_signature

        path = f"/v0/addresses/{quote(address, safe='')}/transactions"
        result = await self.api_get(path, params)
        if not result:
            return []
        if isinstance(result, dict):
            raise HeliusRpcError(f"Enhanced history error: {result}")
        return list(result)

    async def api_get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        """GET a Helius REST path with the same key rotation and retries as RPC."""
        if self._session is None:
            raise RuntimeError("HeliusClient must be used as async context manager")

        last_error: Exception | None = None
        query = dict(params or {})

        for attempt in range(MAX_RETRIES):
            key = await self._rate_limiter.acquire()
            query["api-key"] = key
            url = f"{HELIUS_API_BASE}{path}"

            try:
                async with self._session.get(url, params=query) as response:
                    if response.status == 429:
                        self._rate_limiter.mark_rate_limited(key)
                        last_error = HeliusRpcError(f"HTTP 429 on GET {path}")
                        backoff = min(
                            INITIAL_BACKOFF_SECONDS * (2**attempt),
                            MAX_BACKOFF_SECONDS,
                        )
                        await asyncio.sleep(backoff)
                        continue

                    if response.status >= 500:
                        last_error = HeliusRpcError(
                            f"HTTP {response.status} on GET {path}"
                        )
                        backoff = min(
                            INITIAL_BACKOFF_SECONDS * (2**attempt),
                            MAX_BACKOFF_SECONDS,
                        )
                        await asyncio.sleep(backoff)
                        continue

                    body = await response.json(content_type=None)
                    if response.status >= 400:
                        raise HeliusRpcError(
                            f"HTTP {response.status} on GET {path}: {body}"
                        )
                    return body

            except aiohttp.ClientError as exc:
                last_error = exc
                backoff = min(
                    INITIAL_BACKOFF_SECONDS * (2**attempt),
                    MAX_BACKOFF_SECONDS,
                )
                await asyncio.sleep(backoff)

        raise HeliusRpcError(
            f"Failed GET {path} after {MAX_RETRIES} attempts: {last_error}"
        )

    @staticmethod
    def estimate_enhanced_credits() -> int:
        return ENHANCED_TX_REQUEST_CREDITS

    async def get_signatures_for_address(
        self,
        address: str,
        before: str | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        """Fetch transaction signatures for an address."""
        config: dict[str, Any] = {"limit": limit}
        if before:
            config["before"] = before

        result = await self.rpc_call(
            "getSignaturesForAddress",
            [address, config],
        )
        return result or []

    async def get_transaction(self, signature: str) -> dict[str, Any] | None:
        """Fetch full transaction details."""
        result = await self.rpc_call(
            "getTransaction",
            [
                signature,
                {
                    "encoding": "json",
                    "maxSupportedTransactionVersion": 0,
                    "commitment": "confirmed",
                },
            ],
        )
        return result
