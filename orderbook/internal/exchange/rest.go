// Package exchange implements REST and WebSocket integration with MEXC.
package exchange

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/yourorg/mexc-orderbook/internal/orderbook"
)

const (
	restRequestTimeout    = 10 * time.Second
	restMaxRetries        = 3
	restRetryDelay        = 2 * time.Second
	restRateLimitFallback = 5 * time.Second
)

// RESTClient fetches order book snapshots from MEXC REST endpoints.
type RESTClient struct {
	baseURL    string
	depthLimit int
	httpClient *http.Client
	logger     *slog.Logger
}

// NewRESTClient creates a REST snapshot client.
func NewRESTClient(baseURL string, depthLimit int, logger *slog.Logger) *RESTClient {
	return &RESTClient{
		baseURL:    strings.TrimRight(baseURL, "/"),
		depthLimit: depthLimit,
		httpClient: &http.Client{Timeout: restRequestTimeout},
		logger:     logger,
	}
}

// FetchDepthSnapshot loads a full depth snapshot for a symbol.
func (c *RESTClient) FetchDepthSnapshot(ctx context.Context, symbol string) (orderbook.RESTDepthResponse, error) {
	endpoint, err := url.Parse(c.baseURL + "/api/v3/depth")
	if err != nil {
		return orderbook.RESTDepthResponse{}, fmt.Errorf("parse REST endpoint: %w", err)
	}

	query := endpoint.Query()
	query.Set("symbol", symbol)
	query.Set("limit", strconv.Itoa(c.depthLimit))
	endpoint.RawQuery = query.Encode()

	var lastErr error
	for attempt := 1; attempt <= restMaxRetries; attempt++ {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
		if err != nil {
			return orderbook.RESTDepthResponse{}, fmt.Errorf("build REST request: %w", err)
		}

		start := time.Now()
		response, err := c.httpClient.Do(request)
		if err != nil {
			lastErr = fmt.Errorf("execute REST request: %w", err)
			if attempt < restMaxRetries {
				c.logger.Warn(
					"REST retry after request error",
					slog.String("symbol", symbol),
					slog.Int("attempt", attempt),
					slog.String("error", err.Error()),
				)
				if sleepErr := sleepWithContext(ctx, restRetryDelay); sleepErr != nil {
					return orderbook.RESTDepthResponse{}, sleepErr
				}
				continue
			}
			break
		}

		switch {
		case response.StatusCode == http.StatusTooManyRequests:
			retryDelay := parseRetryAfter(response.Header.Get("Retry-After"), restRateLimitFallback)
			_ = response.Body.Close()
			c.logger.Warn(
				"REST 429 rate limit hit",
				slog.String("symbol", symbol),
				slog.Int("attempt", attempt),
				slog.Duration("retry_after", retryDelay),
			)
			lastErr = fmt.Errorf("rate limited with status 429")
			if sleepErr := sleepWithContext(ctx, retryDelay); sleepErr != nil {
				return orderbook.RESTDepthResponse{}, sleepErr
			}
			continue

		case response.StatusCode >= http.StatusInternalServerError:
			_ = response.Body.Close()
			lastErr = fmt.Errorf("REST server returned status %d", response.StatusCode)
			c.logger.Warn(
				"REST retry due to server error",
				slog.String("symbol", symbol),
				slog.Int("attempt", attempt),
				slog.Int("status", response.StatusCode),
			)
			if attempt < restMaxRetries {
				if sleepErr := sleepWithContext(ctx, restRetryDelay); sleepErr != nil {
					return orderbook.RESTDepthResponse{}, sleepErr
				}
				continue
			}
			break

		case response.StatusCode != http.StatusOK:
			bodyBytes, _ := io.ReadAll(io.LimitReader(response.Body, 1024))
			_ = response.Body.Close()
			return orderbook.RESTDepthResponse{}, fmt.Errorf("unexpected REST status %d: %s", response.StatusCode, string(bodyBytes))

		default:
			var snapshot orderbook.RESTDepthResponse
			decoder := json.NewDecoder(response.Body)
			if decodeErr := decoder.Decode(&snapshot); decodeErr != nil {
				_ = response.Body.Close()
				lastErr = fmt.Errorf("decode REST snapshot: %w", decodeErr)
				if attempt < restMaxRetries {
					c.logger.Warn(
						"REST retry after decode error",
						slog.String("symbol", symbol),
						slog.Int("attempt", attempt),
						slog.String("error", decodeErr.Error()),
					)
					if sleepErr := sleepWithContext(ctx, restRetryDelay); sleepErr != nil {
						return orderbook.RESTDepthResponse{}, sleepErr
					}
					continue
				}
				break
			}
			_ = response.Body.Close()

			c.logger.Info(
				"REST snapshot fetched",
				slog.String("symbol", symbol),
				slog.Int64("last_update_id", snapshot.LastUpdateId),
				slog.Int64("duration_ms", time.Since(start).Milliseconds()),
			)
			return snapshot, nil
		}

		if lastErr != nil && attempt == restMaxRetries {
			break
		}
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("unknown REST failure")
	}
	return orderbook.RESTDepthResponse{}, fmt.Errorf("fetch depth snapshot failed after retries: %w", lastErr)
}

func parseRetryAfter(value string, fallback time.Duration) time.Duration {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fallback
	}

	seconds, err := strconv.Atoi(trimmed)
	if err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}

	when, err := http.ParseTime(trimmed)
	if err != nil {
		return fallback
	}
	until := time.Until(when)
	if until <= 0 {
		return fallback
	}
	return until
}

func sleepWithContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return fmt.Errorf("context canceled while waiting to retry: %w", ctx.Err())
	case <-timer.C:
		return nil
	}
}
