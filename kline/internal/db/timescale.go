package db

import (
	"context"
	"fmt"
	"time"

	"mexc-kline-snapshot/internal/kline"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const upsertSQL = `
INSERT INTO klines (symbol, interval, open_time, open, high, low, close, volume, amount)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (symbol, interval, open_time)
DO UPDATE SET
  open = EXCLUDED.open,
  high = EXCLUDED.high,
  low = EXCLUDED.low,
  close = EXCLUDED.close,
  volume = EXCLUDED.volume,
  amount = EXCLUDED.amount;
`

const loadLatestSQL = `
SELECT symbol, interval, open_time, open, high, low, close, volume, amount
FROM klines
WHERE symbol = $1 AND interval = $2
ORDER BY open_time DESC
LIMIT $3;
`

// DB provides TimescaleDB operations for kline history.
type DB struct {
	pool *pgxpool.Pool
}

// New creates a DB connection pool and applies schema migrations.
func New(ctx context.Context, dsn string) (*DB, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse timescale dsn: %w", err)
	}
	cfg.MaxConns = 10
	cfg.MinConns = 2

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect timescale: %w", err)
	}

	d := &DB{pool: pool}
	if err := d.Migrate(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("migrate timescale: %w", err)
	}
	return d, nil
}

// Migrate creates klines schema objects if they do not already exist.
func (d *DB) Migrate(ctx context.Context) error {
	if d == nil || d.pool == nil {
		return fmt.Errorf("timescale db is not initialized")
	}

	if _, err := d.pool.Exec(ctx, `
CREATE TABLE IF NOT EXISTS klines (
  symbol     TEXT        NOT NULL,
  interval   TEXT        NOT NULL,
  open_time  TIMESTAMPTZ NOT NULL,
  open       NUMERIC     NOT NULL,
  high       NUMERIC     NOT NULL,
  low        NUMERIC     NOT NULL,
  close      NUMERIC     NOT NULL,
  volume     NUMERIC     NOT NULL,
  amount     NUMERIC     NOT NULL,
  PRIMARY KEY (symbol, interval, open_time)
);`); err != nil {
		return fmt.Errorf("create klines table: %w", err)
	}

	if _, err := d.pool.Exec(ctx, `
SELECT create_hypertable('klines', 'open_time',
  if_not_exists => TRUE,
  chunk_time_interval => INTERVAL '7 days');`); err != nil {
		return fmt.Errorf("create hypertable: %w", err)
	}

	if _, err := d.pool.Exec(ctx, `
CREATE INDEX IF NOT EXISTS idx_klines_sym_itvl_time
ON klines (symbol, interval, open_time DESC);`); err != nil {
		return fmt.Errorf("create klines index: %w", err)
	}

	return nil
}

// UpsertBatch inserts or updates candles for one symbol and interval.
func (d *DB) UpsertBatch(ctx context.Context, symbol, interval string, candles []kline.Candle) error {
	if d == nil || d.pool == nil {
		return fmt.Errorf("timescale db is not initialized")
	}
	if len(candles) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	for _, candle := range candles {
		rec, err := toRecord(symbol, interval, candle)
		if err != nil {
			return err
		}
		batch.Queue(upsertSQL, rec.symbol, rec.interval, rec.openTime, rec.open, rec.high, rec.low, rec.close, rec.volume, rec.amount)
	}

	results := d.pool.SendBatch(ctx, batch)
	defer results.Close()

	for range candles {
		if _, err := results.Exec(); err != nil {
			return fmt.Errorf("upsert candle batch: %w", err)
		}
	}
	return nil
}

// LoadLatest loads recent candles and returns them in ascending open_time order.
func (d *DB) LoadLatest(ctx context.Context, symbol, interval string, limit int) ([]kline.Candle, error) {
	if d == nil || d.pool == nil {
		return nil, fmt.Errorf("timescale db is not initialized")
	}
	if limit <= 0 {
		return make([]kline.Candle, 0), nil
	}

	rows, err := d.pool.Query(ctx, loadLatestSQL, symbol, interval, limit)
	if err != nil {
		return nil, fmt.Errorf("query latest klines: %w", err)
	}
	defer rows.Close()

	candles := make([]kline.Candle, 0, limit)
	for rows.Next() {
		var outSymbol string
		var outInterval string
		var openTime time.Time
		var open, high, low, close, volume, amount float64

		if err := rows.Scan(&outSymbol, &outInterval, &openTime, &open, &high, &low, &close, &volume, &amount); err != nil {
			return nil, fmt.Errorf("scan kline row: %w", err)
		}

		candles = append(candles, kline.Candle{
			T: openTime.Unix(),
			O: formatNumeric(open),
			H: formatNumeric(high),
			L: formatNumeric(low),
			C: formatNumeric(close),
			V: formatNumeric(volume),
			A: formatNumeric(amount),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate kline rows: %w", err)
	}

	reverseCandles(candles)
	return candles, nil
}

// Close shuts down the underlying connection pool.
func (d *DB) Close() {
	if d == nil || d.pool == nil {
		return
	}
	d.pool.Close()
}
