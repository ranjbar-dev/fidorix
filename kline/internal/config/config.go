package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

// Intervals defines all supported MEXC Futures kline intervals.
var Intervals = []string{
	"Min1",
	"Min5",
	"Min15",
	"Min30",
	"Min60",
	"Hour4",
	"Hour8",
	"Day1",
	"Week1",
	"Month1",
}

var intervalSeconds = map[string]int{
	"Min1":   60,
	"Min5":   300,
	"Min15":  900,
	"Min30":  1800,
	"Min60":  3600,
	"Hour4":  14400,
	"Hour8":  28800,
	"Day1":   86400,
	"Week1":  604800,
	"Month1": 2592000,
}

// Config stores runtime settings loaded from environment variables.
type Config struct {
	Symbols          []string
	KlineDir         string
	BootstrapLimit   int
	WSPingInterval   time.Duration
	RESTThrottleMS   time.Duration
	BootstrapWorkers int
	LogLevel         string
	ShardSize        int
	Intervals        []string
}

// Load reads configuration from .env and environment variables.
func Load() (*Config, error) {
	if err := godotenv.Load(); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("load .env: %w", err)
	}

	symbols := parseSymbols(os.Getenv("SYMBOLS"))
	if len(symbols) == 0 {
		return nil, fmt.Errorf("SYMBOLS is required and cannot be empty")
	}

	bootstrapLimit, err := intFromEnv("BOOTSTRAP_LIMIT", 2000)
	if err != nil {
		return nil, err
	}

	pingSeconds, err := intFromEnv("WS_PING_INTERVAL", 15)
	if err != nil {
		return nil, err
	}

	throttleMS, err := intFromEnv("REST_THROTTLE_MS", 100)
	if err != nil {
		return nil, err
	}

	workers, err := intFromEnv("BOOTSTRAP_WORKERS", 5)
	if err != nil {
		return nil, err
	}

	shardSize, err := intFromEnv("WS_SHARD_SIZE", 50)
	if err != nil {
		return nil, err
	}

	if workers <= 0 {
		workers = 1
	}
	if shardSize <= 0 {
		shardSize = 50
	}

	klineDir := strings.TrimSpace(os.Getenv("KLINE_DIR"))
	if klineDir == "" {
		klineDir = "./kline"
	}

	logLevel := strings.TrimSpace(os.Getenv("LOG_LEVEL"))
	if logLevel == "" {
		logLevel = "info"
	}

	cfg := &Config{
		Symbols:          symbols,
		KlineDir:         klineDir,
		BootstrapLimit:   bootstrapLimit,
		WSPingInterval:   time.Duration(pingSeconds) * time.Second,
		RESTThrottleMS:   time.Duration(throttleMS) * time.Millisecond,
		BootstrapWorkers: workers,
		LogLevel:         strings.ToLower(logLevel),
		ShardSize:        shardSize,
		Intervals:        append([]string(nil), Intervals...),
	}

	return cfg, nil
}

// SubscriptionCount returns total symbol-interval subscriptions.
func (c *Config) SubscriptionCount() int {
	return len(c.Symbols) * len(c.Intervals)
}

// IntervalSeconds returns the interval size in seconds.
func IntervalSeconds(interval string) (int, error) {
	seconds, ok := intervalSeconds[interval]
	if !ok {
		return 0, fmt.Errorf("unsupported interval: %s", interval)
	}
	return seconds, nil
}

// parseSymbols splits and cleans comma-separated symbols.
func parseSymbols(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		s := strings.TrimSpace(part)
		if s == "" {
			continue
		}
		out = append(out, s)
	}
	return out
}

// intFromEnv reads an integer env var or returns default.
func intFromEnv(key string, fallback int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", key, err)
	}
	return value, nil
}
