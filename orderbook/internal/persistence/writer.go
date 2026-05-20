// Package persistence writes order book snapshots to disk using atomic file replacement.
package persistence

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/yourorg/mexc-orderbook/internal/orderbook"
)

// Writer persists snapshots to a single output file.
type Writer struct {
	outputPath string
	logger     *slog.Logger
	inProgress atomic.Bool
}

// NewWriter creates a writer and ensures the output directory exists.
func NewWriter(outputPath string, logger *slog.Logger) (*Writer, error) {
	if outputPath == "" {
		return nil, fmt.Errorf("output path is required")
	}
	if logger == nil {
		logger = slog.Default()
	}

	dir := filepath.Dir(outputPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create output directory %q: %w", dir, err)
	}

	return &Writer{
		outputPath: outputPath,
		logger:     logger,
	}, nil
}

// OutputPath returns the writer destination path.
func (w *Writer) OutputPath() string {
	return w.outputPath
}

// WriteSnapshot starts a non-blocking write. If another write is active, it skips this cycle.
func (w *Writer) WriteSnapshot(ctx context.Context, snapshot orderbook.DepthFileSnapshot) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}

	if !w.inProgress.CompareAndSwap(false, true) {
		return false, nil
	}

	go func() {
		defer w.inProgress.Store(false)
		if err := w.writeSnapshot(snapshot); err != nil {
			w.logger.Error(
				"snapshot write failed",
				slog.String("symbol", snapshot.Symbol),
				slog.String("path", w.outputPath),
				slog.String("error", err.Error()),
			)
		}
	}()

	return true, nil
}

// WriteSnapshotBlocking writes synchronously, waiting for any in-flight write to finish.
func (w *Writer) WriteSnapshotBlocking(ctx context.Context, snapshot orderbook.DepthFileSnapshot) error {
	for {
		if w.inProgress.CompareAndSwap(false, true) {
			break
		}
		if err := waitFor(ctx, 10*time.Millisecond); err != nil {
			return fmt.Errorf("wait for in-progress write: %w", err)
		}
	}
	defer w.inProgress.Store(false)

	if err := w.writeSnapshot(snapshot); err != nil {
		return fmt.Errorf("blocking snapshot write failed: %w", err)
	}
	return nil
}

func (w *Writer) writeSnapshot(snapshot orderbook.DepthFileSnapshot) error {
	start := time.Now()
	content, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal snapshot: %w", err)
	}
	content = append(content, '\n')

	tmpPath := w.outputPath + ".tmp"
	if err := os.WriteFile(tmpPath, content, 0o644); err != nil {
		return fmt.Errorf("write temp snapshot file: %w", err)
	}

	if err := os.Rename(tmpPath, w.outputPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("rename temp snapshot file atomically: %w", err)
	}

	w.logger.Debug(
		"file written",
		slog.String("symbol", snapshot.Symbol),
		slog.String("path", w.outputPath),
		slog.Int64("last_update_id", snapshot.LastUpdateId),
		slog.Int64("duration_ms", time.Since(start).Milliseconds()),
	)
	return nil
}

func waitFor(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
