package redisclient

import (
	"context"
	"encoding/json"
	"fmt"

	"mexc-kline-snapshot/internal/kline"

	"github.com/redis/go-redis/v9"
)

// Client provides Redis operations for latest kline state.
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

// SetLatestBar stores the latest candle JSON for a symbol and interval.
func (c *Client) SetLatestBar(ctx context.Context, symbol, interval string, candle kline.Candle) error {
	if c == nil || c.rdb == nil {
		return fmt.Errorf("redis client is not initialized")
	}

	payload, err := json.Marshal(candle)
	if err != nil {
		return fmt.Errorf("marshal latest candle: %w", err)
	}
	key := fmt.Sprintf("kline:latest:%s:%s", symbol, interval)
	if err := c.rdb.Set(ctx, key, payload, 0).Err(); err != nil {
		return fmt.Errorf("set latest candle: %w", err)
	}
	return nil
}

// PublishBar publishes a candle update on the symbol+interval channel.
func (c *Client) PublishBar(ctx context.Context, symbol, interval string, candle kline.Candle) error {
	if c == nil || c.rdb == nil {
		return fmt.Errorf("redis client is not initialized")
	}

	payload, err := json.Marshal(candle)
	if err != nil {
		return fmt.Errorf("marshal publish candle: %w", err)
	}
	channel := fmt.Sprintf("kline:update:%s:%s", symbol, interval)
	if err := c.rdb.Publish(ctx, channel, payload).Err(); err != nil {
		return fmt.Errorf("publish candle: %w", err)
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
