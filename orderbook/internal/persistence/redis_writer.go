package persistence

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/yourorg/mexc-orderbook/internal/orderbook"
	"github.com/yourorg/mexc-orderbook/internal/redisclient"
)

const redisWriteTimeout = 3 * time.Second

// RedisWriter persists snapshots to Redis with non-blocking writes.
type RedisWriter struct {
	client   *redisclient.Client
	symbol   string
	inFlight atomic.Bool
}

// NewRedisWriter creates a Redis snapshot writer for one symbol.
func NewRedisWriter(client *redisclient.Client, symbol string) *RedisWriter {
	return &RedisWriter{client: client, symbol: symbol}
}

// WriteSnapshot starts a non-blocking Redis write and skips while a write is in flight.
func (w *RedisWriter) WriteSnapshot(ctx context.Context, snapshot orderbook.DepthFileSnapshot) (bool, error) {
	if w.client == nil {
		return false, fmt.Errorf("redis writer client is nil")
	}

	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}

	if !w.inFlight.CompareAndSwap(false, true) {
		return false, nil
	}

	go func() {
		defer w.inFlight.Store(false)
		writeCtx, cancel := context.WithTimeout(ctx, redisWriteTimeout)
		defer cancel()
		if err := w.client.SetOrderBook(writeCtx, snapshot); err != nil {
			slog.Warn("failed to write redis snapshot", slog.String("symbol", snapshot.Symbol), slog.String("error", err.Error()))
		}
	}()

	return true, nil
}

// WriteSnapshotBlocking waits for in-flight work and then writes directly to Redis.
func (w *RedisWriter) WriteSnapshotBlocking(ctx context.Context, snapshot orderbook.DepthFileSnapshot) error {
	if w.client == nil {
		return fmt.Errorf("redis writer client is nil")
	}

	for {
		if w.inFlight.CompareAndSwap(false, true) {
			break
		}
		if err := waitFor(ctx, 10*time.Millisecond); err != nil {
			return fmt.Errorf("wait for in-flight redis write: %w", err)
		}
	}
	defer w.inFlight.Store(false)

	writeCtx, cancel := context.WithTimeout(ctx, redisWriteTimeout)
	defer cancel()

	if err := w.client.SetOrderBook(writeCtx, snapshot); err != nil {
		return fmt.Errorf("write redis snapshot: %w", err)
	}
	return nil
}

// OutputPath returns a synthetic destination string for log output.
func (w *RedisWriter) OutputPath() string {
	return fmt.Sprintf("redis:orderbook:%s", w.symbol)
}
