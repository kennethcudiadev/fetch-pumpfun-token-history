# Pump.fun Historical Trade Event Fetcher

Production-quality Python crawler that fetches **raw buy/sell events** from Pump.fun bonding curves on Solana using Helius RPC. No OHLCV, no candles, no frontend APIs.

## Features

- Derives Pump.fun bonding curve PDA from any mint address
- Paginates all historical transactions via `getSignaturesForAddress`
- Fetches and decodes individual trades via `getTransaction`
- Parses Anchor `TradeEvent` CPI logs (buy/sell with reserves)
- Multi-key Helius rotation with per-key rate limiting (7 req/s)
- Async architecture (`asyncio` + `aiohttp`)
- SQLite storage with resume support
- Duplicate-safe inserts (`signature UNIQUE`)

## Install

```bash
cd fetch-pumpfun-token-history
python -m venv .venv

# Windows
.venv\Scripts\activate

# macOS / Linux
source .venv/bin/activate

pip install -r requirements.txt
```

## Configure Helius Keys

Edit `pumpfun_history/config.py`:

```python
HELIUS_KEYS = [
    "your-helius-key-1",
    "your-helius-key-2",
    "your-helius-key-3",
]
```

Or set an environment variable:

```bash
set HELIUS_KEYS=key1,key2,key3   # Windows
export HELIUS_KEYS=key1,key2,key3  # macOS / Linux
```

Each key is rate-limited to **7 requests/second**. With 21 keys you get ~147 req/s aggregate throughput.

### Realtime saves

All output files are flushed to disk **after every page** (atomic write via temp file + rename):

| Script | File | Saved after |
|--------|------|-------------|
| `scan.py` | `data/<wallet>.json` | each bulk tx page |
| `fetch.py` | `data/<wallet>/<mint>.json` | each bulk tx page |
| `main.py` | `data/pump_events.db` | each bulk tx page (SQLite) |

Safe to kill the process at any time — re-run the same command to resume.

**Sudden stop protection:**
- Checkpoints every **10 transactions** (mint crawl) or **every transaction** (trader scan)
- SQLite **WAL mode** for crash-safe writes
- `SIGINT` / `SIGTERM` handlers flush checkpoint before exit
- `status: interrupted` in JSON/DB (never falsely marked `complete`)
- PM2 **auto-restarts** on crash (not on clean exit / Ctrl+C)
- Batch `--file` runs resume via `data/batch_progress.json`

### Credit-efficient fetching

Both scripts use Helius **`getTransactionsForAddress`** (Developer plan+) instead of
`getSignaturesForAddress` + N×`getTransaction`:

| Workload | Old cost | New cost |
|----------|----------|----------|
| 1,500 wallet txs / 24h | ~1,501 credits | ~150 credits |
| 100k bonding-curve txs | ~100,001 credits | ~10,000 credits |

Pricing: **10 credits per 100 full transactions** returned (10 minimum per call), up to 1,000/call.

Console shows running `credits=` total. Resume deduplicates by signature so re-runs never double-charge for already-decoded trades.

Requires Helius **Developer plan or higher** for `getTransactionsForAddress`.

### Clean console

Progress is shown as short single lines (no rate-limiter spam):

```
=== trader scan ===  9abc…  24h
9abc1234  sig page 1  collected=500
9abc1234  txs 50/500  pump=12  mints=3  saved
done  9abc1234  txs=500  pump=87  mints=34  -> trader_9abc_24h.json
```

Set `"log_level": "DEBUG"` in `config.json` for verbose RPC/retry logs.

## Run

All commands run from the **project root**. Settings live in **`config.json`** (`target_wallet`, `lookback_hours`, `helius_keys`, `tracked_traders`).

### Workflow

```
python scan.py     ->  data/<wallet>.json
python fetch.py    ->  data/<wallet>/<mint>.json
python serve.py    ->  http://localhost:8080
```

### Step 1 — `scan.py` — find mints a wallet traded

```bash
python scan.py                         # uses config.json target_wallet + lookback_hours
python scan.py <WALLET>                # custom wallet, default hours from config
python scan.py <WALLET> 72             # custom wallet + 72 hour window
```

Output: `data/<wallet>.json`

```json
{
  "wallet": "xxxx",
  "hours": 24,
  "from_timestamp": 1234567890,
  "to_timestamp": 1234579999,
  "transactions_scanned": 1520,
  "pumpfun_trades": 87,
  "unique_mints": 34,
  "mints": [
    {
      "mint": "xxxxx",
      "first_trade_time": 1234567890,
      "last_trade_time": 1234569999,
      "buy_count": 12,
      "sell_count": 5
    }
  ]
}
```

### Step 2 — `fetch.py` — download event history for every mint

```bash
python fetch.py                        # uses config.json target_wallet
python fetch.py <WALLET>               # explicit wallet manifest
```

Reads `data/<wallet>.json`, writes `data/<wallet>/<mint>.json`.

Re-run to resume an interrupted batch.

### Step 3 — `serve.py` — open the chart viewer

```bash
python serve.py
```

Enter **wallet** + **mint** in the browser (or `?wallet=...&mint=...`).

### config.json

Events are automatically saved to:

```
data/<wallet>/<MINT_ADDRESS>.json
```

The file is updated after each processed batch and on completion/interrupt, so partial crawls are preserved. Format:

```json
{
  "mint": "...",
  "bonding_curve": "...",
  "event_count": 1234,
  "exported_at": "2026-08-08T09:18:00+00:00",
  "events": [ { "signature": "...", "side": "BUY", ... } ]
}
```

## PM2 (Ubuntu VPS & Windows 11)

Long-running jobs can be started with [PM2](https://pm2.keymetrics.io/).

### 1. Install

```bash
npm install -g pm2
pip install -r requirements.txt
```

### 2. Configure

Edit **`config.json`** at the project root:

```json
{
  "target_wallet": "YourWalletAddressHere",
  "lookback_hours": 24,
  "helius_keys": ["your-helius-api-key"],
  "tracked_traders": []
}
```

### 3. Start

**Ubuntu VPS:**

```bash
chmod +x scripts/pm2.sh
./scripts/pm2.sh scan
./scripts/pm2.sh fetch
./scripts/pm2.sh status
./scripts/pm2.sh logs
./scripts/pm2.sh stop
```

**Windows 11 (PowerShell):**

```powershell
.\scripts\pm2.ps1 scan
.\scripts\pm2.ps1 fetch
.\scripts\pm2.ps1 status
.\scripts\pm2.ps1 logs
.\scripts\pm2.ps1 stop
```

PM2 writes logs to `logs/scan.out.log` and `logs/fetch.out.log`.

Jobs auto-restart on crash (`autorestart: true`). Clean exit and Ctrl+C (code 130) do not restart.

**Ubuntu cron example (daily trader scan):**

```cron
0 0 * * * cd /path/to/fetch-pumpfun-token-history && ./scripts/pm2.sh trader
```

## Database Schema

Database path: `data/pump_events.db` (used for resume/progress; JSON is the primary output per mint)

Progress is persisted in SQLite. Re-run the same command to continue:

```bash
python main.py <MINT_ADDRESS>
```

The crawler resumes:
1. Signature pagination (from last `before` cursor)
2. Transaction fetching (unprocessed signatures only)

## Database Schema

Database path: `data/pump_events.db`

### `pump_events`

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PK | Auto-increment |
| signature | TEXT UNIQUE | Transaction signature |
| slot | INTEGER | Solana slot |
| timestamp | INTEGER | Unix timestamp |
| mint | TEXT | Token mint address |
| side | TEXT | `BUY` or `SELL` |
| wallet | TEXT | Trader wallet |
| sol_amount | REAL | SOL traded |
| token_amount | REAL | Tokens traded (6 decimals) |
| price | REAL | SOL per token |
| virtual_sol_reserve | REAL | Post-trade virtual SOL reserve |
| virtual_token_reserve | REAL | Post-trade virtual token reserve |
| bonding_curve_progress | REAL | 0.0 → 1.0 completion |

**Indexes:** `mint`, `timestamp`, `signature`

### `crawl_state`

Tracks per-mint progress for resume:

- `sig_before_cursor` — pagination cursor for signature collection
- `sig_collection_complete` — whether all signatures are collected
- `last_processed_signature` / `last_processed_slot` — last decoded transaction
- `total_signatures` / `total_events` — running totals

### `pending_signatures`

Queue of signatures awaiting transaction fetch/decode.

## How Pump.fun Bonding Curve Works

Pump.fun uses a **virtual constant-product AMM** (Uniswap V2 style):

```
k = virtual_sol_reserves × virtual_token_reserves
```

Each token gets a PDA account derived from:

```
seeds = ["bonding-curve", mint_pubkey]
program = 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P
```

**Buy (SOL → tokens):** User sends SOL; virtual SOL reserves increase, virtual token reserves decrease. A 1% fee goes to the fee recipient.

**Sell (tokens → SOL):** User sends tokens; virtual SOL reserves decrease, virtual token reserves increase.

The curve **graduates** when `real_token_reserves` reaches zero (`complete = true`), and liquidity migrates to PumpSwap.

Initial reserves (from Global config):
- `initial_virtual_token_reserves`: 1,073,000,000,000,000
- `initial_virtual_sol_reserves`: 30,000,000,000 lamports (30 SOL)
- `initial_real_token_reserves`: 793,100,000,000,000

**Bonding curve progress** in this tool:

```
progress = (INITIAL_REAL_TOKEN_RESERVES - real_token_reserves) / INITIAL_REAL_TOKEN_RESERVES
```

## How Event Decoding Works

Pump.fun emits Anchor **`TradeEvent`** logs via CPI (cross-program invocation). These appear in **inner instruction data**, not always at byte offset 0.

1. Fetch transaction with `getTransaction` (`encoding: json`)
2. Collect instruction data from top-level and inner instructions
3. Search for TradeEvent discriminator: `bddb7fd34ee661ee`
4. Parse fixed-layout payload:

```
mint                 (32 bytes)
sol_amount           (u64 LE, lamports)
token_amount         (u64 LE, raw units)
is_buy               (bool)
user                 (32 bytes)
timestamp            (i64 LE)
virtual_sol_reserves (u64 LE)
virtual_token_reserves (u64 LE)
real_sol_reserves    (u64 LE)
real_token_reserves  (u64 LE)
```

5. Convert lamports → SOL, raw tokens → human units (6 decimals)
6. Compute price = `sol_amount / token_amount`
7. Store in SQLite (ignore duplicate signatures)

### Output example

```json
{
  "signature": "5abc...",
  "slot": 250000000,
  "timestamp": 1700000000,
  "mint": "EKpQGSJ...",
  "side": "BUY",
  "wallet": "7xKX...",
  "sol_amount": 1.25,
  "token_amount": 5000000.0,
  "price": 2.5e-7,
  "virtual_sol_reserve": 31.25,
  "virtual_token_reserve": 1072000000000.0,
  "bonding_curve_progress": 0.15
}
```

## Architecture

```
CLI (main.py)
    │
    ├─ validate mint → derive bonding curve PDA
    │
    ├─ Phase 1: collect signatures
    │     HeliusClient.getSignaturesForAddress (paginated)
    │     → pending_signatures table
    │
    ├─ Phase 2: fetch & decode transactions
    │     HeliusClient.getTransaction (concurrent)
    │     pumpfun_decoder → TradeEvent
    │     → pump_events table
    │
    └─ RateLimiterPool (round-robin keys, 7 req/s each)
          HeliusClient (retry + exponential backoff + 429 cooldown)
```

## Tests

```bash
pytest tests/ -v
```

Tests cover:
- Bonding curve PDA derivation (cross-checked against `solders`)
- TradeEvent binary decoder
- Per-key rate limiter and round-robin rotation

## Performance

Designed for:
- Single token with 100k+ transactions
- Horizontal scaling via additional Helius keys
- Future multi-mint crawls (each mint has isolated crawl state)

Tune concurrency in `config.py`:

```python
MAX_CONCURRENT_TX_FETCHES = 21  # ~3 keys × 7 req/s
TX_BATCH_SIZE = 50
SIGNATURES_PAGE_LIMIT = 1000
```

## Project Structure

```
pumpfun_history/
    main.py              # CLI entry point (single mint or --file batch)
    trader_mints.py      # Discover Pump.fun mints traded by a wallet
    config.py            # Keys, constants, tuning
    helius_client.py     # Async Helius RPC client
    rate_limiter.py      # Per-key rate limiting + rotation
    pumpfun_decoder.py   # TradeEvent parser
    storage.py           # SQLite persistence
    models.py            # TradeEvent, CrawlState dataclasses
    utils.py             # PDA derivation, unit conversion
tests/
    test_pda.py
    test_decoder.py
    test_rate_limiter.py
requirements.txt
README.md
```

## License

MIT

python scan.py      # Step 1: find mints → data/<wallet>.json
python fetch.py     # Step 2: download events → data/<wallet>/<mint>.json
python serve.py     # Step 3: open viewer at http://localhost:8080