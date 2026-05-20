# PRD: MEXC Futures K-line Local Snapshot Service

## 1. Overview

A long-running Go daemon that maintains an always-fresh, on-disk JSON snapshot of OHLCV K-line (candlestick) data for a fixed list of futures contract symbols across all supported timeframes, sourced from the MEXC Futures WebSocket API. Each symbol+timeframe combination is stored in its own JSON file on the local filesystem. The service bootstraps historical data via the REST API on startup and then patches files in real-time via WebSocket push.

---

## 2. Goals

- Maintain a complete local mirror of K-line candles per symbol per timeframe.
- Never lose a candle: bootstrap history from REST before WebSocket takes over.
- Survive network disconnections and process restarts gracefully.
- Keep disk I/O efficient: only write to disk when a candle changes.
- Zero external database dependencies — plain JSON files only.

---

## 3. Non-Goals

- No HTTP server or API to expose the data.
- No authentication (all K-line endpoints are public).
- No order placement or private channel usage.
- No support for Spot API (Futures only).

---

## 4. Configuration

All configuration lives in a `.env` file (loaded at startup) and/or environment variables. No config file format other than `.env` is required.

```dotenv
# Space-separated list of symbols in MEXC symbol format (underscore separator)
# The app converts BTCUSDT → BTC_USDT internally
SYMBOLS=BTCUSDT,ETHUSDT,XRPUSDT,LTCUSDT,BCHUSDT,BNBUSDT,SOLUSDT,TRXUSDT,ZECUSDT,XMRUSDT,TONUSDT,AVAXUSDT,XAUTUSDT,DOTUSDT,PEPEUSDT,AAVEUSDT,KCSUSDT,DASHUSDT,CHZUSDT,SUNUSDT,DOGEUST,BTCUSDC,ETHUSDC,XRPUSDC,LTCUSDC,BCHUSDC,BNBUSDC,SOLUSDC,TRXUSDC,ZECUSDC,XMRUSDC,TONUSDC,AVAXUSDC,XAUTUSDC,DOTUSDC,PEPEUSDC,AAVEUSDC,KCSUSDC,DASHUSDC,CHZUSDC,SUNUSDC,DOGEUSDC,USDCUSDT,ETHBTC,XRPBTC,LTCBTC,BCHBTC,BNBBTC,SOLBTC,TRXBTC,ZECBTC,XMRBTC,TONBTC,AVAXBTC,XAUTBTC,DOTBTC,PEPEBTC,AAVEBTC,KCSBTC,DASHBTC,CHZBTC,SUNBTC,DOGEBTC

# Output directory for JSON files
KLINE_DIR=./kline

# REST bootstrap: how many candles to fetch per symbol+timeframe on startup
BOOTSTRAP_LIMIT=2000

# Ping interval for WebSocket keepalive (seconds)
WS_PING_INTERVAL=15

# Delay between REST requests during bootstrap to avoid rate-limiting (milliseconds)
REST_THROTTLE_MS=100

# Number of parallel workers for REST bootstrap phase
BOOTSTRAP_WORKERS=5

# Log level: debug | info | warn | error
LOG_LEVEL=info
```

---

## 5. Symbol & Timeframe Specification

### 5.1 Symbols

The 61 symbols provided as a comma-separated list in `SYMBOLS`. The application converts them from `BTCUSDT` format to MEXC's wire format `BTC_USDT` by inserting an underscore before the quote currency. Quote currencies to recognise: `USDT`, `USDC`, `BTC`.

Conversion rule (in order):
1. If symbol ends with `USDT` → insert `_` before `USDT`
2. Else if symbol ends with `USDC` → insert `_` before `USDC`
3. Else if symbol ends with `BTC`  → insert `_` before `BTC`
4. Fallback: use symbol as-is

### 5.2 Timeframes

All 10 MEXC-supported intervals must be subscribed:

| Interval ID | Label    | File directory  |
|-------------|----------|-----------------|
| `Min1`      | 1 minute | `kline/Min1/`   |
| `Min5`      | 5 min    | `kline/Min5/`   |
| `Min15`     | 15 min   | `kline/Min15/`  |
| `Min30`     | 30 min   | `kline/Min30/`  |
| `Min60`     | 1 hour   | `kline/Min60/`  |
| `Hour4`     | 4 hour   | `kline/Hour4/`  |
| `Hour8`     | 8 hour   | `kline/Hour8/`  |
| `Day1`      | Daily    | `kline/Day1/`   |
| `Week1`     | Weekly   | `kline/Week1/`  |
| `Month1`    | Monthly  | `kline/Month1/` |

---

## 6. Storage Layout

```
./kline/
  Min1/
    BTCUSDT.json
    ETHUSDT.json
    ...
  Min5/
    BTCUSDT.json
    ...
  Min15/
  Min30/
  Min60/
  Hour4/
  Hour8/
  Day1/
  Week1/
  Month1/
```

**File naming:** always use the user-facing symbol without underscore (e.g. `BTCUSDT.json`), regardless of MEXC wire format.

---

## 7. JSON File Format

Each file is a single JSON object with metadata and a candle array sorted ascending by timestamp. The last element in the array is always the **current (in-progress) candle**.

```json
{
  "symbol":    "BTC_USDT",
  "interval":  "Min60",
  "updatedAt": 1718000000,
  "candles": [
    {
      "t":  1717996800,
      "o":  "67500.0",
      "h":  "67910.5",
      "l":  "67200.0",
      "c":  "67850.0",
      "v":  "1234.56",
      "a":  "83200000.00",
      "q":  "1611754",
      "ro": "67501.0",
      "rc": "67849.5",
      "rh": "67911.0",
      "rl": "67200.5"
    }
  ]
}
```

**Field definitions:**

| Field       | Type   | Description                          |
|-------------|--------|--------------------------------------|
| `symbol`    | string | MEXC wire-format symbol              |
| `interval`  | string | MEXC interval string                 |
| `updatedAt` | int64  | Unix seconds of last file write      |
| `candles`   | array  | Array of candle objects (see below)  |

**Candle object fields** (all numeric values stored as strings to preserve decimal precision):

| Field | Type   | Source field | Description              |
|-------|--------|--------------|--------------------------|
| `t`   | int64  | `t`          | Window open time (Unix s)|
| `o`   | string | `o`          | Open price               |
| `h`   | string | `h`          | High price               |
| `l`   | string | `l`          | Low price                |
| `c`   | string | `c`          | Close price              |
| `v`   | string | `v`          | Volume (contracts)       |
| `a`   | string | `a`          | Traded amount (quote)    |
| `q`   | string | `q`          | Traded volume            |
| `ro`  | string | `ro`         | Real open                |
| `rc`  | string | `rc`         | Real close               |
| `rh`  | string | `rh`         | Real high                |
| `rl`  | string | `rl`         | Real low                 |

> **Note:** Fields `ro`, `rc`, `rh`, `rl` are only present in WebSocket push messages and may be absent in REST-bootstrapped candles. The application omits them (zero-value strings) when not available.

---

## 8. MEXC API Reference

### 8.1 WebSocket Endpoint

```
wss://contract.mexc.com/edge
```

### 8.2 Subscribe to K-line

```json
{
  "method": "sub.kline",
  "param": {
    "symbol": "BTC_USDT",
    "interval": "Min60"
  },
  "gzip": false
}
```

The server streams `push.kline` messages whenever the current candle updates.

### 8.3 Incoming Push Message

```json
{
  "channel": "push.kline",
  "data": {
    "a": 233.740269343644737245,
    "c": 6885,
    "h": 6910.5,
    "interval": "Min60",
    "l": 6885,
    "o": 6894.5,
    "q": 1611754,
    "ro": 6894.0,
    "rc": 6885.0,
    "rh": 6910.5,
    "rl": 6885.0,
    "symbol": "BTC_USDT",
    "t": 1587448800
  },
  "symbol": "BTC_USDT"
}
```

### 8.4 Unsubscribe

```json
{
  "method": "unsub.kline",
  "param": {
    "symbol": "BTC_USDT"
  }
}
```

### 8.5 Ping / Keepalive

Send every `WS_PING_INTERVAL` seconds:
```json
{ "method": "ping" }
```

Expected pong:
```json
{ "channel": "pong", "data": 1587453241453 }
```

If no pong is received or no ping is sent within 60 seconds, the server closes the connection.

### 8.6 REST Bootstrap Endpoint

```
GET https://contract.mexc.com/api/v1/contract/kline/{symbol}
```

Query parameters:

| Param      | Type   | Required | Description                                    |
|------------|--------|----------|------------------------------------------------|
| `interval` | string | yes      | e.g. `Min1`, `Min60`, `Day1`                  |
| `start`    | int64  | no       | Unix seconds. Omit to get latest candles.      |
| `end`      | int64  | no       | Unix seconds.                                  |

Maximum 2000 candles per request. Response format:

```json
{
  "success": true,
  "code": 0,
  "data": {
    "time":   [1609740600],
    "open":   [33016.5],
    "close":  [33040.5],
    "high":   [33094.0],
    "low":    [32995.0],
    "vol":    [67332.0],
    "amount": [222515.85925]
  }
}
```

The response arrays are parallel: `data.time[i]`, `data.open[i]`, etc. correspond to the same candle.

---

## 9. Application Architecture

### 9.1 Package Layout

```
mexc-kline-snapshot/
  cmd/
    server/
      main.go             ← entry point
  internal/
    config/
      config.go           ← env loading, symbol/interval lists
    symbol/
      symbol.go           ← MEXCUSDT → MEX_CUSDT conversion logic
    store/
      store.go            ← in-memory candle store (map[key]*CandleFile)
      file.go             ← atomic JSON read/write helpers
    rest/
      client.go           ← REST bootstrap HTTP client
      bootstrap.go        ← fetch & merge historical candles
    ws/
      conn.go             ← single WebSocket connection manager
      handler.go          ← message dispatcher & push.kline handler
      manager.go          ← multi-connection pool manager
    kline/
      candle.go           ← Candle struct, merge/upsert logic
  go.mod
  go.sum
  .env.example
  README.md
```

### 9.2 Component Responsibilities

#### `config`
- Loads `.env` using `github.com/joho/godotenv`.
- Parses `SYMBOLS` into a `[]string` slice.
- Exposes `Intervals []string` hardcoded to all 10 timeframes.
- Computes total subscription count: `len(symbols) × len(intervals)`.

#### `symbol`
- `ToMEXC(s string) string` — converts `BTCUSDT` → `BTC_USDT`.
- `ToFile(s string) string` — returns the user-facing filename base (no underscore).

#### `store`
- Thread-safe in-memory map keyed by `symbol+interval`.
- `CandleFile` struct holds the full file state.
- `Upsert(symbol, interval string, candle Candle)` — inserts or updates a candle by timestamp, keeps array sorted ascending by `t`.
- Marks the entry dirty when modified.

#### `file`
- `LoadFile(path string) (*CandleFile, error)` — reads existing JSON from disk.
- `SaveFile(path string, cf *CandleFile) error` — atomic write: marshal → write to `path.tmp` → `os.Rename` to `path`.
- A background goroutine flushes dirty entries to disk every **500 ms**.

#### `rest/bootstrap`
- Iterates all `symbol × interval` pairs using a worker pool of size `BOOTSTRAP_WORKERS`.
- For each pair:
  1. If the JSON file already exists, load it and determine the timestamp of the last closed candle.
  2. Fetch from REST starting after the last known candle (or from `now - BOOTSTRAP_LIMIT × interval_seconds` if file is absent).
  3. Merge fetched candles into the store.
  4. Throttle between requests by sleeping `REST_THROTTLE_MS` ms.
- Logs progress (symbol, interval, candles fetched).

#### `ws/conn`
- Wraps a single `wss://contract.mexc.com/edge` connection using `github.com/gorilla/websocket`.
- Reconnects automatically with exponential backoff (initial 1 s, max 60 s) on any error.
- On reconnect: re-subscribes all subscriptions assigned to this connection.
- Sends ping every `WS_PING_INTERVAL` seconds.
- Reads messages in a dedicated goroutine; dispatches to a shared `chan []byte`.

#### `ws/manager`
- MEXC allows multiple subscriptions per connection. To avoid overloading a single connection with 610 subscriptions (61 symbols × 10 timeframes), the manager shards subscriptions across **multiple connections**.
- Default shard size: **50 subscriptions per connection** → requires ~13 connections.
- Each connection is managed by its own `conn` instance.
- The manager starts all connections after bootstrap completes.

#### `ws/handler`
- Reads from the shared message channel.
- Parses JSON, checks `channel == "push.kline"`.
- Calls `store.Upsert(data.Symbol, data.Interval, candle)`.
- Ignores `pong` messages.
- Logs unexpected/malformed messages at `debug` level.

#### `kline/candle`
- `Candle` struct matching the JSON schema above.
- `Upsert(existing []Candle, incoming Candle) []Candle`:
  - Binary search by `t`.
  - If found: replace (update in-progress candle).
  - If not found: insert in sorted position.
  - Returns updated slice.

---

## 10. Startup Sequence

```
1. Load config from .env
2. Create all kline/{interval}/ directories if not exist
3. Load existing JSON files from disk into in-memory store
4. Start REST bootstrap worker pool
   └─ For each symbol × interval (in parallel, BOOTSTRAP_WORKERS at a time):
       a. Fetch up to BOOTSTRAP_LIMIT candles from REST
       b. Merge into store
       c. Write to disk immediately after each symbol+interval pair completes
5. Log: "Bootstrap complete. Total candles loaded: N"
6. Start disk flush goroutine (500ms ticker)
7. Start WebSocket manager
   └─ Create ceil(total_subs / 50) connections
   └─ Subscribe all symbol×interval pairs across connections
8. Start message handler goroutine
9. Block on OS signal (SIGINT / SIGTERM)
10. On shutdown:
    a. Close all WebSocket connections
    b. Flush all dirty store entries to disk
    c. Exit 0
```

---

## 11. Candle Upsert Logic (Detail)

The `push.kline` message carries the **current open candle** — it updates frequently within the candle's window. The candle is only "closed" (immutable) when the next candle's `t` arrives.

Rules for upsert:
1. Look up candle by `t` timestamp in the symbol+interval slice.
2. **Match found:** overwrite all OHLCV fields. This handles the in-progress candle being updated.
3. **No match (new candle):** append/insert in sorted position.
4. Mark entry dirty.
5. Do **not** delete old candles from the slice — accumulate history indefinitely.

---

## 12. File Flush Strategy

- A single background goroutine runs a `time.Ticker` at 500 ms.
- On each tick: iterate all store entries, collect dirty ones.
- For each dirty entry: call `file.SaveFile()`, then clear the dirty flag.
- Atomic write (temp file + rename) prevents partial JSON being read by consumers.
- On shutdown signal: perform one final forced flush of all dirty entries before exit.

---

## 13. WebSocket Reconnection

On any WebSocket error (read error, write error, abnormal close):
1. Log the error with symbol context.
2. Mark the connection as disconnected.
3. Sleep for backoff duration (starts at 1 s, doubles each attempt, caps at 60 s).
4. Dial a new WebSocket connection to `wss://contract.mexc.com/edge`.
5. Re-send all `sub.kline` subscriptions for this shard.
6. Reset backoff counter on successful message receipt.

During the reconnection window, no data is lost for closed candles (they were already written to disk). The in-progress candle may miss some ticks and will be corrected by the next push once reconnected.

---

## 14. Error Handling

| Scenario | Behaviour |
|---|---|
| REST returns non-200 | Log error, skip this symbol+interval pair, continue bootstrap |
| REST `"success": false` | Log error with `code` and message, skip pair |
| JSON file on disk is corrupt | Log warning, treat as empty (start fresh) |
| WebSocket message is malformed JSON | Log at debug, discard message |
| `push.kline` for unknown symbol/interval | Log at warn, discard |
| Disk write failure on flush | Log error with path, retain dirty flag, retry on next tick |
| Symbol not valid on MEXC (no data returned) | Log at warn, continue |

---

## 15. Dependencies

| Package | Purpose |
|---|---|
| `github.com/gorilla/websocket` | WebSocket client |
| `github.com/joho/godotenv` | `.env` file loading |
| `go.uber.org/zap` | Structured logging |

No ORM, no database driver, no framework. Standard library for HTTP (`net/http`), JSON (`encoding/json`), concurrency (`sync`, `context`), and filesystem (`os`, `path/filepath`).

---

## 16. Concurrency Model

```
main goroutine
  │
  ├─ bootstrap worker pool (BOOTSTRAP_WORKERS goroutines)
  │    └─ REST HTTP calls, store writes
  │
  ├─ flush goroutine (1 goroutine)
  │    └─ 500ms ticker → atomic file writes
  │
  ├─ WS connection goroutines (one per shard, ~13 goroutines)
  │    └─ read loop → sends to shared msgChan
  │
  ├─ WS handler goroutine (1 goroutine)
  │    └─ reads from msgChan → store.Upsert
  │
  └─ ping goroutines (one per WS connection)
       └─ 15s ticker → send ping frame
```

Store access is protected by a single `sync.RWMutex` — readers (flush) use `RLock`, writers (upsert) use `Lock`.

---

## 17. Logging

Use `go.uber.org/zap` in production mode (`zap.NewProduction()`). All log lines include:
- `symbol` field
- `interval` field (where applicable)
- `candles` count (during bootstrap)
- Error details on failures

Log levels:
- `INFO`: startup, bootstrap completion, connection events, shutdown
- `WARN`: skipped symbols, reconnection attempts, unexpected WS messages
- `ERROR`: disk write failures, fatal REST errors
- `DEBUG`: raw WS message receipt, candle upsert details

---

## 18. README (Summary)

The `README.md` must include:
1. Prerequisites: Go 1.22+
2. Setup: `cp .env.example .env && go run ./cmd/server`
3. Output: description of `./kline/` directory structure
4. How to read a file: example `cat ./kline/Min1/BTCUSDT.json | jq .candles[-1]`
5. How to stop: `Ctrl+C` (graceful shutdown)
6. Note: No API key required (public endpoints only)

---

## 19. Acceptance Criteria

- [ ] On first run with no existing files, all `symbol × interval` JSON files are created after bootstrap.
- [ ] On restart, existing files are loaded and only missing/newer candles are fetched from REST.
- [ ] Every `push.kline` message updates the correct file within ≤ 1 second (next flush tick).
- [ ] A killed and restarted process loses at most 500 ms of in-progress candle updates.
- [ ] WebSocket reconnects automatically after network interruption without manual intervention.
- [ ] No goroutine leaks: `pprof` shows stable goroutine count after startup.
- [ ] All 61 × 10 = 610 subscriptions are active simultaneously.
- [ ] JSON files are always valid JSON (never partial writes visible to readers).