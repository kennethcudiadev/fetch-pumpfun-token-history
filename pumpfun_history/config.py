"""Application configuration."""

from __future__ import annotations

from pathlib import Path

# Helius API keys — loaded from project config.json helius_keys.
HELIUS_KEYS: list[str] = []

# Pump.fun program constants
PUMP_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
BONDING_CURVE_SEED = b"bonding-curve"

# TradeEvent Anchor discriminator: sha256("event:TradeEvent")[0:8]
TRADE_EVENT_DISCRIMINATOR = bytes.fromhex("bddb7fd34ee661ee")

# Bonding curve graduation progress baseline (from Global account defaults)
INITIAL_REAL_TOKEN_RESERVES = 793_100_000_000_000
TOKEN_DECIMALS = 6
LAMPORTS_PER_SOL = 1_000_000_000

# Helius RPC / Enhanced Transactions
HELIUS_RPC_BASE = "https://mainnet.helius-rpc.com"
HELIUS_API_BASE = "https://api.helius.xyz"

# Wallet scan asks Helius to return only Pump.fun-sourced history.
PUMPFUN_TX_SOURCE = "PUMP_FUN"
ENHANCED_TX_PAGE_LIMIT = 100
# Enhanced address history is metered per request, not per returned tx.
ENHANCED_TX_REQUEST_CREDITS = 100

# Rate limiting — stay under 8 req/s per key
REQUESTS_PER_SECOND_PER_KEY = 7
MAX_RETRIES = 5
INITIAL_BACKOFF_SECONDS = 0.5
MAX_BACKOFF_SECONDS = 30.0
KEY_COOLDOWN_SECONDS = 60.0

# Concurrency — one in-flight page per mint; extra workers fetch other mints.
MAX_CONCURRENT_TX_FETCHES = 21
MAX_CONCURRENT_MINT_FETCHES = 8
SIGNATURES_PAGE_LIMIT = 1000
TX_BATCH_SIZE = 50

# Helius getTransactionsForAddress (credit-efficient bulk fetch)
GTFORADDRESS_PAGE_LIMIT = 1000
GTFORADDRESS_ENCODING = "json"

# Paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
STATE_DIR = DATA_DIR / ".state"
DATABASE_PATH = STATE_DIR / "pump_events.db"
BATCH_PROGRESS_PATH = STATE_DIR / "batch_progress.json"

# Trader discovery
DEFAULT_TRADER_LOOKBACK_HOURS = 24
TRADER_TX_BATCH_SIZE = 50

# Logging defaults — overridden by config.json log_level when present.
LOG_LEVEL = "INFO"


def _apply_project_config() -> None:
    global HELIUS_KEYS, LOG_LEVEL, DEFAULT_TRADER_LOOKBACK_HOURS
    try:
        from project_config import (
            get_helius_keys,
            get_log_level,
            get_lookback_hours,
            load_project_config,
        )
    except ImportError:
        return

    cfg = load_project_config()
    keys = get_helius_keys(cfg)
    if keys:
        HELIUS_KEYS = keys
    LOG_LEVEL = get_log_level(cfg)
    DEFAULT_TRADER_LOOKBACK_HOURS = get_lookback_hours(cfg)


_apply_project_config()


def wallet_manifest_path(wallet: str) -> Path:
    """Step 1 output: mint list for a target wallet."""
    return DATA_DIR / f"{wallet}.json"


def wallet_mint_dir(wallet: str) -> Path:
    """Directory holding per-mint event JSON for a wallet batch."""
    return DATA_DIR / wallet


def wallet_mint_json_path(wallet: str, mint: str) -> Path:
    """Step 2 output: bonding-curve events for one mint under a wallet."""
    return wallet_mint_dir(wallet) / f"{mint}.json"
