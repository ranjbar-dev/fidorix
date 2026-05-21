package redisclient

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/yourorg/mexc-orderbook/internal/orderbook"
)

// Client provides Redis operations for order book snapshots.
type Client struct {
	rdb *redis.Client
}

// New constructs a Redis client.
func New(addr, password string, db int) *Client {
	return &Client{
		rdb: redis.NewClient(&redis.Options{
			Addr:     addr,
			Password: password,
			DB:       db,
		}),
	}
}

// SetOrderBook stores the full book snapshot as JSON.
func (c *Client) SetOrderBook(ctx context.Context, snapshot orderbook.DepthFileSnapshot) error {
	if c == nil || c.rdb == nil {
		return fmt.Errorf("redis client is not initialized")
	}

	payload, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("marshal orderbook snapshot: %w", err)
	}

	key := fmt.Sprintf("orderbook:%s", snapshot.Symbol)
	if err := c.rdb.Set(ctx, key, payload, 0).Err(); err != nil {
		return fmt.Errorf("set redis orderbook %s: %w", snapshot.Symbol, err)
	}
	return nil
}

// Close closes the Redis client.
func (c *Client) Close() error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.rdb.Close()
}
