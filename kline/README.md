# MEXC Futures K-line Local Snapshot Service

A long-running Go daemon that keeps local JSON snapshots of MEXC Futures K-line data for every configured symbol and supported timeframe.

## Prerequisites

- Go 1.22+

## Setup

```bash
cp .env.example .env
go run ./cmd/server
```

## Output Layout

Snapshots are written under `./kline/` by interval, then symbol file name:

```text
./kline/
  Min1/
    BTCUSDT.json
  Min5/
    BTCUSDT.json
  Min15/
  Min30/
  Min60/
  Hour4/
  Hour8/
  Day1/
  Week1/
  Month1/
```

## Read Last Candle Example

```bash
cat ./kline/Min1/BTCUSDT.json | jq .candles[-1]
```

## Stop Service

Press `Ctrl+C` to trigger graceful shutdown. The service closes websocket connections and performs a final dirty-file flush before exiting.

## Notes

- No API key is required; all used MEXC endpoints are public.
