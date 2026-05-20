package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"mexc-kline-snapshot/internal/kline"
	"mexc-kline-snapshot/internal/symbol"

	"go.uber.org/zap"
)

// CandleFile is the serialized on-disk snapshot format per symbol and interval.
type CandleFile struct {
	Symbol    string         `json:"symbol"`
	Interval  string         `json:"interval"`
	UpdatedAt int64          `json:"updatedAt"`
	Candles   []kline.Candle `json:"candles"`
}

// LoadFile reads a candle snapshot from disk.
func LoadFile(path string) (CandleFile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return CandleFile{}, nil
		}
		return CandleFile{}, fmt.Errorf("read file %s: %w", path, err)
	}

	var cf CandleFile
	if err := json.Unmarshal(data, &cf); err != nil {
		zap.L().Warn("corrupt candle file; treating as empty", zap.String("path", path), zap.Error(err))
		return CandleFile{}, fmt.Errorf("decode file %s: %w", path, err)
	}

	if cf.Candles == nil {
		cf.Candles = make([]kline.Candle, 0)
	}
	return cf, nil
}

// SaveFile atomically writes a candle snapshot using a temp file and rename.
func SaveFile(path string, cf CandleFile) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create dir for %s: %w", path, err)
	}

	if cf.Candles == nil {
		cf.Candles = make([]kline.Candle, 0)
	}
	cf.UpdatedAt = time.Now().Unix()

	payload, err := json.Marshal(cf)
	if err != nil {
		return fmt.Errorf("marshal candle file %s: %w", path, err)
	}

	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, payload, 0o644); err != nil {
		return fmt.Errorf("write temp file %s: %w", tmpPath, err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("rename temp file %s to %s: %w", tmpPath, path, err)
	}

	return nil
}

// FlushLoop periodically flushes dirty entries and performs one final flush on shutdown.
func FlushLoop(ctx context.Context, store *Store, baseDir string, interval time.Duration, log *zap.Logger) {
	if log == nil {
		log = zap.NewNop()
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			FlushDirtyEntries(store, baseDir, log)
			return
		case <-ticker.C:
			FlushDirtyEntries(store, baseDir, log)
		}
	}
}

// FlushDirtyEntries writes all dirty entries to disk and clears their dirty flags.
func FlushDirtyEntries(store *Store, baseDir string, log *zap.Logger) int {
	if log == nil {
		log = zap.NewNop()
	}

	dirty := store.GetDirty()
	flushed := 0
	for _, entry := range dirty {
		path, err := filePathForEntry(baseDir, entry)
		if err != nil {
			log.Error("failed to resolve output path", zap.String("key", entry.Key), zap.Error(err))
			continue
		}

		if err := SaveFile(path, entry.File); err != nil {
			log.Error("failed to flush candle file", zap.String("path", path), zap.Error(err))
			continue
		}

		store.ClearDirty(entry.Key)
		flushed++
	}

	return flushed
}

// filePathForEntry computes the target JSON path for a store entry.
func filePathForEntry(baseDir string, entry *Entry) (string, error) {
	if entry == nil {
		return "", fmt.Errorf("entry is nil")
	}

	wireSymbol := strings.TrimSpace(entry.File.Symbol)
	interval := strings.TrimSpace(entry.File.Interval)

	if wireSymbol == "" || interval == "" {
		parts := strings.SplitN(entry.Key, ":", 2)
		if len(parts) != 2 {
			return "", fmt.Errorf("invalid store key: %s", entry.Key)
		}
		if wireSymbol == "" {
			wireSymbol = parts[0]
		}
		if interval == "" {
			interval = parts[1]
		}
	}

	fileSymbol := symbol.ToFile(strings.ReplaceAll(wireSymbol, "_", ""))
	if fileSymbol == "" {
		return "", fmt.Errorf("empty file symbol for key: %s", entry.Key)
	}

	return filepath.Join(baseDir, interval, fileSymbol+".json"), nil
}
