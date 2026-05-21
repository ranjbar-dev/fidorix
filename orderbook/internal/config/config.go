// Package config loads and validates runtime configuration from environment variables.
package config

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

const (
	defaultSymbol               = "BTCUSDT"
	defaultDepthLimit           = 5000
	defaultUpdateInterval       = "100ms"
	defaultOutputPath           = "./data/depth.json"
	defaultLogLevel             = "info"
	defaultSnapshotFlushMS      = 500
	defaultPingIntervalSec      = 20
	defaultReconnectDelaySec    = 3
	defaultMaxReconnectDelaySec = 60
	defaultRESTBaseURL          = "https://api.mexc.com"
	defaultWSBaseURL            = "wss://wbs-api.mexc.com/ws"
	defaultRedisAddr            = "redis:6379"
	defaultRedisDB              = 0
)

// Config contains all runtime settings for the application.
type Config struct {
	Symbols              []string
	DepthLimit           int
	UpdateInterval       string
	OutputPath           string
	LogLevel             slog.Level
	LogLevelText         string
	SnapshotFlushMS      int
	PingIntervalSec      int
	ReconnectDelaySec    int
	MaxReconnectDelaySec int
	RESTBaseURL          string
	WSBaseURL            string
	RedisAddr            string
	RedisPassword        string
	RedisDB              int
}

// Load reads the .env file when present, applies defaults, and validates values.
func Load() (Config, error) {
	if err := godotenv.Load(); err != nil && !os.IsNotExist(err) {
		return Config{}, fmt.Errorf("load .env file: %w", err)
	}

	rawSymbols := getEnv("SYMBOLS", getEnv("SYMBOL", defaultSymbol))
	symbols := parseSymbols(rawSymbols)
	if len(symbols) == 0 {
		return Config{}, fmt.Errorf("SYMBOLS must contain at least one value")
	}

	depthLimit, err := parseInt("DEPTH_LIMIT", getEnv("DEPTH_LIMIT", strconv.Itoa(defaultDepthLimit)))
	if err != nil {
		return Config{}, err
	}
	if depthLimit < 1 || depthLimit > 5000 {
		return Config{}, fmt.Errorf("DEPTH_LIMIT must be between 1 and 5000")
	}

	updateInterval := strings.TrimSpace(getEnv("UPDATE_INTERVAL", defaultUpdateInterval))
	if updateInterval != "100ms" && updateInterval != "10ms" {
		return Config{}, fmt.Errorf("UPDATE_INTERVAL must be 100ms or 10ms")
	}

	logLevelText := strings.ToLower(strings.TrimSpace(getEnv("LOG_LEVEL", defaultLogLevel)))
	logLevel, err := parseLogLevel(logLevelText)
	if err != nil {
		return Config{}, err
	}

	snapshotFlushMS, err := parseInt("SNAPSHOT_FLUSH_MS", getEnv("SNAPSHOT_FLUSH_MS", strconv.Itoa(defaultSnapshotFlushMS)))
	if err != nil {
		return Config{}, err
	}
	if snapshotFlushMS < 1 {
		return Config{}, fmt.Errorf("SNAPSHOT_FLUSH_MS must be greater than 0")
	}

	pingIntervalSec, err := parseInt("PING_INTERVAL_SEC", getEnv("PING_INTERVAL_SEC", strconv.Itoa(defaultPingIntervalSec)))
	if err != nil {
		return Config{}, err
	}
	if pingIntervalSec < 1 {
		return Config{}, fmt.Errorf("PING_INTERVAL_SEC must be greater than 0")
	}

	reconnectDelaySec, err := parseInt("RECONNECT_DELAY_SEC", getEnv("RECONNECT_DELAY_SEC", strconv.Itoa(defaultReconnectDelaySec)))
	if err != nil {
		return Config{}, err
	}
	if reconnectDelaySec < 1 {
		return Config{}, fmt.Errorf("RECONNECT_DELAY_SEC must be greater than 0")
	}

	maxReconnectDelaySec, err := parseInt("MAX_RECONNECT_DELAY_SEC", getEnv("MAX_RECONNECT_DELAY_SEC", strconv.Itoa(defaultMaxReconnectDelaySec)))
	if err != nil {
		return Config{}, err
	}
	if maxReconnectDelaySec < reconnectDelaySec {
		return Config{}, fmt.Errorf("MAX_RECONNECT_DELAY_SEC must be greater than or equal to RECONNECT_DELAY_SEC")
	}

	outputPath := strings.TrimSpace(getEnv("OUTPUT_PATH", defaultOutputPath))
	if outputPath == "" {
		return Config{}, fmt.Errorf("OUTPUT_PATH cannot be empty")
	}

	restBaseURL := strings.TrimSpace(getEnv("REST_BASE_URL", defaultRESTBaseURL))
	if restBaseURL == "" {
		return Config{}, fmt.Errorf("REST_BASE_URL cannot be empty")
	}

	wsBaseURL := strings.TrimSpace(getEnv("WS_BASE_URL", defaultWSBaseURL))
	if wsBaseURL == "" {
		return Config{}, fmt.Errorf("WS_BASE_URL cannot be empty")
	}

	redisAddr := getEnvWithDefaultAllowEmpty("REDIS_ADDR", defaultRedisAddr)
	redisPassword := strings.TrimSpace(os.Getenv("REDIS_PASSWORD"))
	redisDB, err := parseInt("REDIS_DB", getEnv("REDIS_DB", strconv.Itoa(defaultRedisDB)))
	if err != nil {
		return Config{}, err
	}

	return Config{
		Symbols:              symbols,
		DepthLimit:           depthLimit,
		UpdateInterval:       updateInterval,
		OutputPath:           outputPath,
		LogLevel:             logLevel,
		LogLevelText:         logLevelText,
		SnapshotFlushMS:      snapshotFlushMS,
		PingIntervalSec:      pingIntervalSec,
		ReconnectDelaySec:    reconnectDelaySec,
		MaxReconnectDelaySec: maxReconnectDelaySec,
		RESTBaseURL:          restBaseURL,
		WSBaseURL:            wsBaseURL,
		RedisAddr:            redisAddr,
		RedisPassword:        redisPassword,
		RedisDB:              redisDB,
	}, nil
}

// SnapshotFlushInterval returns the configured flush interval as a duration.
func (c Config) SnapshotFlushInterval() time.Duration {
	return time.Duration(c.SnapshotFlushMS) * time.Millisecond
}

// IsMultiSymbol reports whether multiple symbols are configured.
func (c Config) IsMultiSymbol() bool {
	return len(c.Symbols) > 1
}

// OutputPathForSymbol returns the output path for the provided symbol.
func (c Config) OutputPathForSymbol(symbol string) string {
	if !c.IsMultiSymbol() {
		return c.OutputPath
	}

	baseDir := multiSymbolOutputDir(c.OutputPath)
	name := strings.ToUpper(strings.TrimSpace(symbol))
	if name == "" {
		name = defaultSymbol
	}

	return filepath.Join(baseDir, name+".json")
}

func multiSymbolOutputDir(outputPath string) string {
	cleaned := filepath.Clean(strings.TrimSpace(outputPath))
	if cleaned == "" || cleaned == "." {
		return filepath.Join(".", "data", "depth")
	}

	if filepath.Ext(cleaned) == "" {
		return cleaned
	}

	parent := filepath.Dir(cleaned)
	base := strings.TrimSuffix(filepath.Base(cleaned), filepath.Ext(cleaned))
	if base == "" || base == "." {
		base = "depth"
	}
	return filepath.Join(parent, base)
}

func getEnv(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func getEnvWithDefaultAllowEmpty(key, fallback string) string {
	raw, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	return strings.TrimSpace(raw)
}

func parseInt(name, raw string) (int, error) {
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", name, err)
	}
	return value, nil
}

func parseLogLevel(level string) (slog.Level, error) {
	switch level {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("LOG_LEVEL must be one of: debug, info, warn, error")
	}
}

func parseSymbols(raw string) []string {
	parts := strings.Split(raw, ",")
	unique := make(map[string]struct{}, len(parts))
	symbols := make([]string, 0, len(parts))
	for _, part := range parts {
		symbol := strings.ToUpper(strings.TrimSpace(part))
		if symbol == "" {
			continue
		}
		if _, exists := unique[symbol]; exists {
			continue
		}
		unique[symbol] = struct{}{}
		symbols = append(symbols, symbol)
	}
	return symbols
}
