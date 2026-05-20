# PRD: MEXC Order Book Local Snapshot Service

## 1. Overview

Build a production-grade Go application that maintains a **continuously accurate, real-time local snapshot** of the MEXC Spot order book for one or more configurable trading symbols. The snapshot is persisted to `./data/depth.json` and kept alive indefinitely using the official MEXC WebSocket diff-depth stream combined with REST snapshot initialization — following MEXC's own documented algorithm for local order book maintenance.

---

## 2. Goals

- Always have a correct, up-to-date local order book in `./data/depth.json`
- Use MEXC's diff-depth WebSocket stream (`spot@public.aggre.depth.v3.api.pb@100ms@<SYMBOL>`) as the primary update source
- Bootstrap and re-sync from REST (`GET /api/v3/depth`) whenever necessary
- Handle all edge cases: gaps in version sequence, stale snapshots, WebSocket disconnections, process crashes
- Zero data loss between updates: atomic file writes only
- Clean shutdown on OS signals

---

## 3. Non-Goals

- No trading, order placement, or authenticated endpoints
- No database (file-only persistence)
- No HTTP server or REST API exposure
- No support for Futures symbols (Spot V3 only)

---

## 4. Tech Stack

| Component | Choice |
|---|---|
| Language | Go 1.22+ |
| WebSocket client | `github.com/gorilla/websocket` |
| HTTP client | stdlib `net/http` |
| JSON | stdlib `encoding/json` |
| Configuration | `.env` file + `github.com/joho/godotenv` |
| Logging | stdlib `log/slog` (structured, leveled) |
| Build | Single binary, `go build ./cmd/orderbook` |

---

## 5. Configuration

All tunables live in a `.env` file at the project root (loaded at startup). If a variable is absent, use the listed default.

| Variable | Type | Default | Description |
|---|---|---|---|
| `SYMBOL` | string | `BTCUSDT` | Trading pair (uppercase). Comma-separated list supported for multi-symbol mode. |
| `DEPTH_LIMIT` | int | `5000` | Snapshot depth for REST bootstrap (`max 5000`) |
| `UPDATE_INTERVAL` | string | `100ms` | WebSocket stream interval: `100ms` or `10ms` |
| `OUTPUT_PATH` | string | `./data/depth.json` | Path for the JSON snapshot file |
| `LOG_LEVEL` | string | `info` | `debug`, `info`, `warn`, `error` |
| `SNAPSHOT_FLUSH_MS` | int | `500` | How often (ms) to flush in-memory book to disk |
| `PING_INTERVAL_SEC` | int | `20` | WebSocket keep-alive ping interval |
| `RECONNECT_DELAY_SEC` | int | `3` | Base delay before WebSocket reconnect |
| `MAX_RECONNECT_DELAY_SEC` | int | `60` | Cap for exponential back-off reconnect delay |
| `REST_BASE_URL` | string | `https://api.mexc.com` | MEXC REST base URL |
| `WS_BASE_URL` | string | `wss://wbs-api.mexc.com/ws` | MEXC WebSocket base URL |

---

## 6. Output File Schema

File location: `./data/depth.json`

```json
{
  "symbol": "BTCUSDT",
  "lastUpdateId": 10589632359,
  "updatedAt": "2026-05-20T14:32:01.123Z",
  "bids": [
    ["92877.58", "1.25000000"],
    ["92876.00", "0.50000000"]
  ],
  "asks": [
    ["92878.00", "2.00000000"],
    ["92880.50", "0.10000000"]
  ]
}
```

**Field definitions:**

| Field | Type | Description |
|---|---|---|
| `symbol` | string | Trading pair this book belongs to |
| `lastUpdateId` | int64 | `toVersion` of the last applied WebSocket update (or `lastUpdateId` from REST snapshot) |
| `updatedAt` | string | RFC 3339 UTC timestamp of last write |
| `bids` | `[][2]string` | Buy side, sorted descending by price. Each element: `[price, quantity]` |
| `asks` | `[][2]string` | Sell side, sorted ascending by price. Each element: `[price, quantity]` |

**For multi-symbol mode**, write one file per symbol: `./data/BTCUSDT_depth.json`, `./data/ETHUSDT_depth.json`, etc.

---

## 7. Project Structure

```
mexc-orderbook/
├── cmd/
│   └── orderbook/
│       └── main.go            # Entry point, wires everything together
├── internal/
│   ├── config/
│   │   └── config.go          # Loads .env, exposes Config struct
│   ├── exchange/
│   │   ├── rest.go            # REST snapshot fetcher (GET /api/v3/depth)
│   │   └── websocket.go       # WS connection, subscription, message dispatch
│   ├── orderbook/
│   │   ├── book.go            # In-memory order book (map[price]quantity)
│   │   ├── manager.go         # Orchestrates init/sync/apply/reinit algorithm
│   │   └── types.go           # Shared structs (DepthSnapshot, DiffDepthEvent, etc.)
│   └── persistence/
│       └── writer.go          # Atomic JSON file write logic
├── data/                      # Created at runtime; gitignored
│   └── .gitkeep
├── .env.example
├── .gitignore
├── go.mod
├── go.sum
└── README.md
```

---

## 8. Core Algorithm — Local Order Book Maintenance

Implement exactly as specified in the MEXC documentation. This is **critical correctness logic** — do not deviate.

```
Step 1. Open WS connection to wss://wbs-api.mexc.com/ws
        Subscribe: spot@public.aggre.depth.v3.api.pb@{interval}@{SYMBOL}

Step 2. Buffer all incoming diff-depth events in an in-memory queue.
        Record fromVersion of the FIRST received event.

Step 3. Fetch REST snapshot:
        GET https://api.mexc.com/api/v3/depth?symbol={SYMBOL}&limit=5000
        Record lastUpdateId from the response.

Step 4. If lastUpdateId < fromVersion of first buffered event:
        → Discard snapshot. Go to Step 3. Retry until condition is satisfied.

Step 5. From the buffered queue, discard all events where:
        event.toVersion <= lastUpdateId

Step 6. Examine the first remaining buffered event.
        If event.fromVersion > lastUpdateId + 1:
        → Gap detected. Discard all state. Go to Step 3 (reinitialize).

Step 7. Initialize the in-memory book from the REST snapshot.
        Set localVersion = lastUpdateId.

        Apply each remaining buffered event in order:
          - Validate: event.fromVersion == previousToVersion + 1
            (for the very first event: event.fromVersion <= lastUpdateId + 1
             AND event.toVersion > lastUpdateId)
          - If invalid → reinitialize from Step 3
          - Apply: for each price level in bids/asks:
              if quantity == "0" or "0.00000000" → delete price level
              else → upsert price level
          - Set localVersion = event.toVersion

        Continue applying all subsequent incoming WS events the same way.
```

---

## 9. Detailed Component Specifications

### 9.1 `internal/orderbook/book.go`

```go
type OrderBook struct {
    mu          sync.RWMutex
    Symbol      string
    Bids        map[string]string  // price → quantity
    Asks        map[string]string  // price → quantity
    LastVersion int64
    UpdatedAt   time.Time
}
```

Methods:
- `ApplySnapshot(snapshot RESTDepthResponse)` — replace the full book from REST response
- `ApplyDiff(event DiffDepthEvent) error` — apply a single incremental update; return error on version gap
- `ToSnapshot() DepthFileSnapshot` — produce sorted output struct for JSON serialization
  - Bids sorted descending by price (string-to-float comparison)
  - Asks sorted ascending by price

### 9.2 `internal/orderbook/manager.go`

The `Manager` struct drives the full algorithm lifecycle per symbol.

Fields:
- `book *OrderBook`
- `buffer []DiffDepthEvent` — pre-sync event queue
- `state enum{Initializing, Live}`
- `restClient *exchange.RESTClient`

The manager runs in a dedicated goroutine per symbol. It receives events via a channel from the WebSocket dispatcher. On receipt:
- If `state == Initializing`: append to buffer, trigger sync if not already in progress
- If `state == Live`: call `book.ApplyDiff(event)`; on error → trigger reinit

### 9.3 `internal/exchange/websocket.go`

- Maintain one WebSocket connection to `wss://wbs-api.mexc.com/ws`
- For multi-symbol: use a single connection for up to 30 symbols (MEXC limit); open additional connections if needed
- On connect: send subscription message for all configured symbols:
  ```json
  {
    "method": "SUBSCRIPTION",
    "params": ["spot@public.aggre.depth.v3.api.pb@100ms@BTCUSDT"]
  }
  ```
- Send periodic ping every `PING_INTERVAL_SEC` seconds (text `"ping"` or WebSocket ping frame) to keep connection alive (MEXC disconnects after 30s with no activity, 60s with no data)
- On any read error or close: trigger reconnect with exponential back-off starting at `RECONNECT_DELAY_SEC`, capped at `MAX_RECONNECT_DELAY_SEC`
- On reconnect: notify all managers to reinitialize (go back to Step 3)
- Parse incoming messages and dispatch to the correct symbol's manager channel

### 9.4 `internal/exchange/rest.go`

```
GET https://api.mexc.com/api/v3/depth?symbol={SYMBOL}&limit={DEPTH_LIMIT}
```

- No API key required (public endpoint)
- Handle HTTP 429: back off using `Retry-After` header if present; else 5s default
- Handle HTTP 5xx: retry up to 3 times with 2s delay; log warning
- Timeout: 10 seconds per request
- Return `RESTDepthResponse{LastUpdateId int64, Bids [][2]string, Asks [][2]string}`

### 9.5 `internal/persistence/writer.go`

Atomic write pattern:
1. Marshal `DepthFileSnapshot` to JSON (pretty-printed with 2-space indent)
2. Write to a temp file: `{OUTPUT_PATH}.tmp`
3. `os.Rename(tmpPath, outputPath)` — atomic on Linux/macOS/Windows (same filesystem)
4. Ensure `./data/` directory exists (create on startup with `os.MkdirAll`)

The `Manager` calls the writer on a ticker set to `SNAPSHOT_FLUSH_MS`. The writer is non-blocking: if a write is already in progress, skip the current flush cycle.

### 9.6 `internal/orderbook/types.go`

```go
// REST response from GET /api/v3/depth
type RESTDepthResponse struct {
    LastUpdateId int64      `json:"lastUpdateId"`
    Bids         [][2]string `json:"bids"`
    Asks         [][2]string `json:"asks"`
}

// Parsed WS diff-depth event
type DiffDepthEvent struct {
    Symbol      string
    FromVersion int64
    ToVersion   int64
    Bids        []PriceLevel
    Asks        []PriceLevel
    SendTime    int64
}

type PriceLevel struct {
    Price    string
    Quantity string
}

// Written to depth.json
type DepthFileSnapshot struct {
    Symbol       string      `json:"symbol"`
    LastUpdateId int64       `json:"lastUpdateId"`
    UpdatedAt    string      `json:"updatedAt"`
    Bids         [][2]string `json:"bids"`
    Asks         [][2]string `json:"asks"`
}
```

---

## 10. WebSocket Message Format

MEXC diff-depth stream sends JSON (not Protobuf unless `pb` endpoint is used). Parse the following structure:

```json
{
  "channel": "spot@public.aggre.depth.v3.api.pb@100ms@BTCUSDT",
  "publicincreasedepths": {
    "asksList": [
      { "price": "92878.00", "quantity": "2.00000000" }
    ],
    "bidsList": [
      { "price": "92877.58", "quantity": "0.00000000" }
    ],
    "eventtype": "spot@public.aggre.depth.v3.api.pb@100ms",
    "fromVersion": "10589632358",
    "toVersion": "10589632359"
  },
  "symbol": "BTCUSDT",
  "sendtime": 1736411507002
}
```

**Important:** `fromVersion` and `toVersion` are strings in the WS payload. Parse to `int64`.

A quantity of `"0.00000000"` means the price level must be **removed** from the local book.

---

## 11. Startup Sequence

```
1. Load config from .env
2. Create ./data/ directory if not exists
3. For each symbol:
   a. Create OrderBook instance
   b. Create Manager instance (state = Initializing)
   c. Start manager goroutine
4. Connect WebSocket, subscribe to all symbols
5. Start WS read loop → dispatch events to manager channels
6. Each Manager independently runs the init algorithm (Steps 1–7)
7. Start flush ticker per Manager → writes to depth.json
8. Block on OS signal (SIGINT, SIGTERM)
9. On signal: graceful shutdown
   a. Stop flush tickers
   b. Perform final flush to disk
   c. Close WebSocket
   d. Exit 0
```

---

## 12. Logging Requirements

Use `log/slog` with structured key-value fields. Log at these levels:

| Event | Level |
|---|---|
| Application start / config loaded | INFO |
| WebSocket connected | INFO |
| Subscription confirmed | INFO |
| REST snapshot fetched (symbol, lastUpdateId) | INFO |
| Book initialized (symbol, localVersion) | INFO |
| Diff event applied (symbol, toVersion) | DEBUG |
| File written (symbol, path, duration) | DEBUG |
| Version gap detected → reinitializing | WARN |
| REST retry (symbol, attempt, status) | WARN |
| WebSocket reconnect (symbol, attempt, delay) | WARN |
| REST 429 rate limit hit | WARN |
| Fatal error / panic | ERROR |

---

## 13. Error Handling

| Scenario | Behavior |
|---|---|
| WS read error | Log WARN, reconnect with back-off, trigger reinit for all symbols |
| WS message parse error | Log WARN, skip message, continue |
| REST request timeout | Log WARN, retry up to 3 times, then log ERROR and retry after 5s |
| REST 429 | Respect Retry-After header, then retry |
| Version gap (fromVersion > prevToVersion+1) | Log WARN, trigger reinit |
| REST snapshot stale (Step 4) | Log DEBUG, re-fetch snapshot |
| File write error | Log ERROR, do not crash; retry on next flush cycle |
| Unknown symbol in WS message | Log DEBUG, ignore |

---

## 14. File & Directory Layout at Runtime

```
./
├── .env
├── orderbook          ← compiled binary
└── data/
    ├── depth.json           ← single-symbol mode
    ├── BTCUSDT_depth.json   ← multi-symbol mode
    └── ETHUSDT_depth.json
```

---

## 15. `.env.example`

```dotenv
# Trading symbol(s) — comma-separated for multi-symbol
SYMBOL=BTCUSDT

# Depth levels to request from REST snapshot (max 5000)
DEPTH_LIMIT=5000

# WebSocket update interval: 100ms or 10ms
UPDATE_INTERVAL=100ms

# Output file path (single symbol mode)
OUTPUT_PATH=./data/depth.json

# Logging level: debug | info | warn | error
LOG_LEVEL=info

# How often to flush book to disk (milliseconds)
SNAPSHOT_FLUSH_MS=500

# WebSocket ping interval (seconds)
PING_INTERVAL_SEC=20

# Reconnect delays (seconds)
RECONNECT_DELAY_SEC=3
MAX_RECONNECT_DELAY_SEC=60

# MEXC endpoints (do not change unless using a proxy)
REST_BASE_URL=https://api.mexc.com
WS_BASE_URL=wss://wbs-api.mexc.com/ws
```

---

## 16. README Requirements

The `README.md` must include:

1. **Quick start** (clone → `cp .env.example .env` → `go run ./cmd/orderbook`)
2. **Build instructions** (`go build -o orderbook ./cmd/orderbook`)
3. **Configuration reference** (table of all `.env` variables)
4. **Output file format** (annotated JSON example)
5. **Multi-symbol usage example**
6. **Algorithm description** (the 7-step MEXC sync algorithm in plain English)
7. **Graceful shutdown** (Ctrl+C / SIGTERM)

---

## 17. Dependencies (`go.mod`)

```
module github.com/yourorg/mexc-orderbook

go 1.22

require (
    github.com/gorilla/websocket v1.5.3
    github.com/joho/godotenv v1.5.1
)
```

No other third-party dependencies. All other functionality uses the Go standard library.

---

## 18. Acceptance Criteria

- [ ] On first run, `./data/depth.json` is created within 5 seconds of startup
- [ ] The `lastUpdateId` in the file increments continuously as new WS events arrive
- [ ] Killing the process and restarting produces a fresh, valid snapshot (no stale state)
- [ ] Simulating a WS disconnect (e.g., network drop) triggers automatic reconnect and reinit without operator intervention
- [ ] `bids` array is sorted descending by price; `asks` array is sorted ascending by price
- [ ] A price level with quantity `0.00000000` is absent from the output file
- [ ] No partial/corrupt JSON is ever written (atomic writes enforced)
- [ ] `go vet ./...` and `go build ./...` pass with zero errors
- [ ] Application exits cleanly on SIGINT/SIGTERM with a final flush
