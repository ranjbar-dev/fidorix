package rest

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"mexc-kline-snapshot/internal/config"
	"mexc-kline-snapshot/internal/kline"
	"mexc-kline-snapshot/internal/store"
	"mexc-kline-snapshot/internal/symbol"

	"go.uber.org/zap"
)

// Bootstrap fetches historical candles and merges them into local files and store.
func Bootstrap(ctx context.Context, cfg *config.Config, s *store.Store, baseDir string, log *zap.Logger) error {
	if cfg == nil {
		return fmt.Errorf("config is nil")
	}
	if s == nil {
		return fmt.Errorf("store is nil")
	}
	if log == nil {
		log = zap.NewNop()
	}

	workers := cfg.BootstrapWorkers
	if workers <= 0 {
		workers = 1
	}

	client := NewClient()
	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	var loadedCandles atomic.Int64
	var invalidSymbols sync.Map

launch:
	for _, fileSymbol := range cfg.Symbols {
		for _, interval := range cfg.Intervals {
			select {
			case <-ctx.Done():
				break launch
			case sem <- struct{}{}:
			}

			fileSymbol := fileSymbol
			interval := interval

			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() {
					<-sem
				}()

				wireSymbol := symbol.ToMEXC(fileSymbol)
				if _, skip := invalidSymbols.Load(wireSymbol); skip {
					return
				}

				fetched, err := bootstrapPair(ctx, cfg, client, s, baseDir, fileSymbol, wireSymbol, interval, log)
				if err != nil {
					if isContractNotExist(err) {
						if _, seen := invalidSymbols.LoadOrStore(wireSymbol, struct{}{}); !seen {
							log.Warn("symbol not found on MEXC; skipping remaining intervals", zap.String("symbol", wireSymbol), zap.Error(err))
						}
						_ = sleepWithContext(ctx, cfg.RESTThrottleMS)
						return
					}

					log.Warn("bootstrap pair failed; skipping", zap.String("symbol", wireSymbol), zap.String("interval", interval), zap.Error(err))
					_ = sleepWithContext(ctx, cfg.RESTThrottleMS)
					return
				}

				loadedCandles.Add(int64(fetched))
				log.Info("bootstrap pair complete", zap.String("symbol", wireSymbol), zap.String("interval", interval), zap.Int("candles", fetched))

				if err := sleepWithContext(ctx, cfg.RESTThrottleMS); err != nil {
					return
				}
			}()
		}
	}

	wg.Wait()

	if ctx.Err() != nil {
		return ctx.Err()
	}

	log.Info("bootstrap workers complete", zap.Int64("candles", loadedCandles.Load()))
	return nil
}

// bootstrapPair handles one symbol-interval bootstrap lifecycle.
func bootstrapPair(ctx context.Context, cfg *config.Config, client *Client, s *store.Store, baseDir, fileSymbol, wireSymbol, interval string, log *zap.Logger) (int, error) {
	path := filepath.Join(baseDir, interval, symbol.ToFile(fileSymbol)+".json")

	existing, err := store.LoadFile(path)
	if err != nil {
		if log != nil {
			log.Warn("failed to load existing bootstrap file; treating as empty", zap.String("path", path), zap.String("symbol", wireSymbol), zap.String("interval", interval), zap.Error(err))
		}
		existing = store.CandleFile{}
	}
	if existing.Symbol == "" {
		existing.Symbol = wireSymbol
	}
	if existing.Interval == "" {
		existing.Interval = interval
	}
	if existing.Candles == nil {
		existing.Candles = make([]kline.Candle, 0)
	}

	s.Load(wireSymbol, interval, existing)

	start, err := computeStart(existing, interval, cfg.BootstrapLimit)
	if err != nil {
		return 0, err
	}

	candles, err := client.FetchKline(ctx, wireSymbol, interval, start, 0, cfg.BootstrapLimit)
	if err != nil {
		return 0, err
	}

	merged := existing
	for _, c := range candles {
		merged.Candles = kline.Upsert(merged.Candles, c)
	}
	merged.Symbol = wireSymbol
	merged.Interval = interval
	merged.UpdatedAt = time.Now().Unix()

	if err := store.SaveFile(path, merged); err != nil {
		return 0, err
	}

	s.Load(wireSymbol, interval, merged)
	return len(candles), nil
}

// computeStart computes REST start time from local snapshot state.
func computeStart(cf store.CandleFile, interval string, limit int) (int64, error) {
	if limit <= 0 {
		limit = 2000
	}

	if len(cf.Candles) > 0 {
		idx := len(cf.Candles) - 1
		if len(cf.Candles) > 1 {
			idx = len(cf.Candles) - 2
		}
		return cf.Candles[idx].T + 1, nil
	}

	seconds, err := config.IntervalSeconds(interval)
	if err != nil {
		return 0, err
	}

	return time.Now().Unix() - int64(limit*seconds), nil
}

// sleepWithContext throttles requests while still respecting cancellation.
func sleepWithContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// isContractNotExist reports whether an error means the symbol is unsupported on MEXC.
func isContractNotExist(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "code=1001") || strings.Contains(msg, "contract does not exist")
}
