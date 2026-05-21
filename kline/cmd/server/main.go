package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"mexc-kline-snapshot/internal/config"
	"mexc-kline-snapshot/internal/db"
	"mexc-kline-snapshot/internal/redisclient"
	"mexc-kline-snapshot/internal/rest"
	"mexc-kline-snapshot/internal/store"
	"mexc-kline-snapshot/internal/symbol"
	"mexc-kline-snapshot/internal/ws"

	"go.uber.org/zap"
)

func main() {
	bootstrapLog, _ := zap.NewProduction()
	defer func() {
		_ = bootstrapLog.Sync()
	}()

	cfg, err := config.Load()
	if err != nil {
		bootstrapLog.Fatal("failed to load config", zap.Error(err))
	}

	log, err := newLogger(cfg.LogLevel)
	if err != nil {
		bootstrapLog.Fatal("failed to create logger", zap.Error(err))
	}
	defer func() {
		_ = log.Sync()
	}()
	zap.ReplaceGlobals(log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	timescaleDB := initTimescale(ctx, cfg, log)
	if timescaleDB != nil {
		defer timescaleDB.Close()
	}

	redisClient := initRedis(cfg, log)
	if redisClient != nil {
		defer func() {
			if err := redisClient.Close(); err != nil {
				log.Warn("failed to close redis client", zap.Error(err))
			}
		}()
	}

	s := store.NewFull(timescaleDB, redisClient)

	if err := ensureIntervalDirs(cfg.KlineDir, cfg.Intervals); err != nil {
		log.Fatal("failed to create output directories", zap.Error(err))
	}

	if timescaleDB != nil {
		preloadLatestFromDB(cfg, s, timescaleDB, log)
	} else {
		preloadExistingFiles(cfg, s, log)
	}

	if err := rest.Bootstrap(ctx, cfg, s, cfg.KlineDir, timescaleDB, log); err != nil {
		if errors.Is(err, context.Canceled) {
			log.Info("bootstrap canceled")
			flushOnShutdown(s, cfg.KlineDir, log)
			return
		}
		log.Error("bootstrap failed", zap.Error(err))
	}

	totalCandles := countCandles(s)
	log.Info("Bootstrap complete. Total candles loaded", zap.Int("candles", totalCandles))

	filteredSymbols, droppedSymbols := symbolsWithBootstrapData(cfg, s)
	if len(droppedSymbols) > 0 {
		sample := droppedSymbols
		if len(sample) > 10 {
			sample = sample[:10]
		}
		log.Warn("dropping symbols with no bootstrap candles", zap.Int("dropped", len(droppedSymbols)), zap.Strings("sample", sample))
	}
	cfg.Symbols = filteredSymbols

	flushDone := make(chan struct{})
	go func() {
		defer close(flushDone)
		store.FlushLoop(ctx, s, cfg.KlineDir, 500*time.Millisecond, log)
	}()

	msgChan := make(chan []byte, 1000)
	manager := ws.New(cfg, msgChan, log)
	handler := ws.NewHandler(s, log)

	log.Info("starting websocket manager", zap.Int("subscriptions", manager.SubscriptionCount()), zap.Int("shards", manager.ConnectionCount()))

	go manager.Run(ctx)
	go handler.Run(ctx, msgChan)

	<-ctx.Done()

	manager.Close()
	<-flushDone
	flushOnShutdown(s, cfg.KlineDir, log)
	log.Info("shutdown complete")
}

// newLogger constructs a production zap logger for a configured level.
func newLogger(level string) (*zap.Logger, error) {
	cfg := zap.NewProductionConfig()
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		cfg.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	case "info", "":
		cfg.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	case "warn":
		cfg.Level = zap.NewAtomicLevelAt(zap.WarnLevel)
	case "error":
		cfg.Level = zap.NewAtomicLevelAt(zap.ErrorLevel)
	default:
		return nil, fmt.Errorf("invalid LOG_LEVEL: %s", level)
	}
	return cfg.Build()
}

// ensureIntervalDirs creates all interval output directories.
func ensureIntervalDirs(baseDir string, intervals []string) error {
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return fmt.Errorf("create base dir %s: %w", baseDir, err)
	}
	for _, interval := range intervals {
		dir := filepath.Join(baseDir, interval)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("create interval dir %s: %w", dir, err)
		}
	}
	return nil
}

// preloadExistingFiles loads existing disk snapshots into the in-memory store.
func preloadExistingFiles(cfg *config.Config, s *store.Store, log *zap.Logger) {
	for _, fileSymbol := range cfg.Symbols {
		wireSymbol := symbol.ToMEXC(fileSymbol)
		for _, interval := range cfg.Intervals {
			path := filepath.Join(cfg.KlineDir, interval, symbol.ToFile(fileSymbol)+".json")
			cf, err := store.LoadFile(path)
			if err != nil {
				log.Warn("failed to read existing file; treating as empty", zap.String("path", path), zap.Error(err))
			}

			if cf.Symbol == "" {
				cf.Symbol = wireSymbol
			}
			if cf.Interval == "" {
				cf.Interval = interval
			}

			s.Load(wireSymbol, interval, cf)
		}
	}
}

func preloadLatestFromDB(cfg *config.Config, s *store.Store, d *db.DB, log *zap.Logger) {
	limit := cfg.BootstrapLimit
	if limit <= 0 {
		limit = 2000
	}

	for _, fileSymbol := range cfg.Symbols {
		wireSymbol := symbol.ToMEXC(fileSymbol)
		for _, interval := range cfg.Intervals {
			loadCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			candles, err := d.LoadLatest(loadCtx, wireSymbol, interval, limit)
			cancel()
			if err != nil {
				log.Warn("failed to preload candles from timescaledb; using empty cache", zap.String("symbol", wireSymbol), zap.String("interval", interval), zap.Error(err))
				candles = nil
			}

			s.Load(wireSymbol, interval, store.CandleFile{
				Symbol:   wireSymbol,
				Interval: interval,
				Candles:  candles,
			})
		}
	}
}

func initTimescale(ctx context.Context, cfg *config.Config, log *zap.Logger) *db.DB {
	dsn := strings.TrimSpace(cfg.TimescaleDSN)
	if dsn == "" {
		return nil
	}

	openCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	d, err := db.New(openCtx, dsn)
	if err != nil {
		log.Warn("timescaledb init failed; continuing without timescaledb", zap.Error(err))
		return nil
	}

	log.Info("timescaledb enabled")
	return d
}

func initRedis(cfg *config.Config, log *zap.Logger) *redisclient.Client {
	addr := strings.TrimSpace(cfg.RedisAddr)
	if addr == "" {
		return nil
	}

	r := redisclient.New(addr, cfg.RedisPassword, cfg.RedisDB)
	log.Info("redis latest/publish enabled", zap.String("addr", addr), zap.Int("db", cfg.RedisDB))
	return r
}

// countCandles computes the total candles currently loaded in the store.
func countCandles(s *store.Store) int {
	total := 0
	for _, entry := range s.All() {
		total += len(entry.File.Candles)
	}
	return total
}

// flushOnShutdown flushes all dirty entries before process exit.
func flushOnShutdown(s *store.Store, baseDir string, log *zap.Logger) {
	flushed := store.FlushDirtyEntries(s, baseDir, log)
	log.Info("final flush complete", zap.Int("files", flushed))
}

// symbolsWithBootstrapData keeps symbols that produced at least one candle in store.
func symbolsWithBootstrapData(cfg *config.Config, s *store.Store) ([]string, []string) {
	entries := s.All()
	hasData := make(map[string]bool, len(entries))
	for _, entry := range entries {
		if len(entry.File.Candles) == 0 {
			continue
		}
		hasData[entry.File.Symbol] = true
	}

	kept := make([]string, 0, len(cfg.Symbols))
	dropped := make([]string, 0)
	for _, fileSymbol := range cfg.Symbols {
		wireSymbol := symbol.ToMEXC(fileSymbol)
		if hasData[wireSymbol] {
			kept = append(kept, fileSymbol)
			continue
		}
		dropped = append(dropped, fileSymbol)
	}

	if len(kept) == 0 {
		return append([]string(nil), cfg.Symbols...), dropped
	}

	sort.Strings(dropped)
	return kept, dropped
}
