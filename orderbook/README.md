# MEXC Order Book Local Snapshot Service

Production-ready Go service that keeps an accurate local MEXC Spot order book snapshot on disk using WebSocket diff updates plus REST snapshot bootstrapping.

## Quick Start

```bash
git clone <your-repo-url>
cd orderbook
cp .env.example .env
go run ./cmd/orderbook
```

Within a few seconds, the service writes snapshot files under `./data/`.

## Build

```bash
go build -o orderbook ./cmd/orderbook
./orderbook
```

## Configuration

All settings are loaded from `.env` at startup.

| Variable | Type | Default | Description |
|---|---|---|---|
| `SYMBOL` | string | `BTCUSDT` | Trading pair (uppercase). Comma-separated list supported for multi-symbol mode. |
| `DEPTH_LIMIT` | int | `5000` | Snapshot depth for REST bootstrap (`max 5000`). |
| `UPDATE_INTERVAL` | string | `100ms` | WebSocket stream interval: `100ms` or `10ms`. |
| `OUTPUT_PATH` | string | `./data/depth.json` | Output path base. Single-symbol writes this file; multi-symbol writes one file per symbol in a derived directory (default `./data/depth/<SYMBOL>.json`). |
| `LOG_LEVEL` | string | `info` | `debug`, `info`, `warn`, or `error`. |
| `SNAPSHOT_FLUSH_MS` | int | `500` | Flush interval to write in-memory book to disk (ms). |
| `PING_INTERVAL_SEC` | int | `20` | WebSocket ping interval in seconds. |
| `RECONNECT_DELAY_SEC` | int | `3` | Initial reconnect delay in seconds. |
| `MAX_RECONNECT_DELAY_SEC` | int | `60` | Maximum reconnect delay cap in seconds. |
| `REST_BASE_URL` | string | `https://api.mexc.com` | MEXC REST base URL. |
| `WS_BASE_URL` | string | `wss://wbs-api.mexc.com/ws` | MEXC WebSocket base URL. |

## Output File Format

Single-symbol mode writes `OUTPUT_PATH` (default `./data/depth.json`).

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

Field notes:
- `symbol`: trading pair for this file.
- `lastUpdateId`: latest applied version (`toVersion` from WS or `lastUpdateId` from snapshot).
- `updatedAt`: RFC3339 UTC timestamp of latest flush.
- `bids`: sorted descending by numeric price.
- `asks`: sorted ascending by numeric price.

## Multi-Symbol Usage

Example `.env`:

```dotenv
SYMBOL=BTCUSDT,ETHUSDT,SOLUSDT
OUTPUT_PATH=./data/depth.json
```

In multi-symbol mode, output files are generated as:
- `./data/depth/BTCUSDT.json`
- `./data/depth/ETHUSDT.json`
- `./data/depth/SOLUSDT.json`

## Synchronization Algorithm (7 Steps)

1. Open WebSocket and subscribe to `spot@public.aggre.depth.v3.api.pb@{interval}@{SYMBOL}`.
2. Buffer incoming diff events in memory.
3. Fetch REST snapshot from `/api/v3/depth`.
4. If snapshot `lastUpdateId` is older than the first buffered event `fromVersion`, fetch snapshot again.
5. Drop buffered events with `toVersion <= lastUpdateId`.
6. If the first remaining event has `fromVersion > lastUpdateId + 1`, a gap exists; reinitialize.
7. Apply snapshot, then apply all remaining buffered events in order; continue live diff application and reinitialize on version gap.

## Graceful Shutdown

The service handles `SIGINT` and `SIGTERM`.

On shutdown it:
1. Stops manager flush tickers.
2. Performs one final flush per symbol.
3. Closes WebSocket connections.
4. Exits cleanly.


