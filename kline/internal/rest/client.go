package rest

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"mexc-kline-snapshot/internal/kline"
)

const restBaseURL = "https://contract.mexc.com/api/v1/contract/kline"

// Client wraps HTTP access to the MEXC Futures REST API.
type Client struct {
	httpClient *http.Client
}

// NewClient creates a REST client with a 10-second timeout.
func NewClient() *Client {
	return &Client{
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}
}

// FetchKline retrieves kline candles for one symbol and interval.
func (c *Client) FetchKline(ctx context.Context, symbol, interval string, start, end int64, limit int) ([]kline.Candle, error) {
	endpoint := fmt.Sprintf("%s/%s", restBaseURL, url.PathEscape(symbol))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	query := req.URL.Query()
	query.Set("interval", interval)
	if start > 0 {
		query.Set("start", strconv.FormatInt(start, 10))
	}
	if end > 0 {
		query.Set("end", strconv.FormatInt(end, 10))
	}
	req.URL.RawQuery = query.Encode()

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request kline: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	var payload restResponse
	decoder := json.NewDecoder(resp.Body)
	decoder.UseNumber()
	if err := decoder.Decode(&payload); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	if !payload.Success {
		message := payload.Message
		if message == "" {
			message = payload.Msg
		}
		return nil, fmt.Errorf("mexc rest error code=%d message=%s", payload.Code, message)
	}

	candles, err := payload.Data.toCandles()
	if err != nil {
		return nil, err
	}

	if limit > 0 && len(candles) > limit {
		candles = candles[len(candles)-limit:]
	}

	return candles, nil
}

type restResponse struct {
	Success bool          `json:"success"`
	Code    int           `json:"code"`
	Message string        `json:"message"`
	Msg     string        `json:"msg"`
	Data    restKlineData `json:"data"`
}

type restKlineData struct {
	Time   []int64       `json:"time"`
	Open   []json.Number `json:"open"`
	Close  []json.Number `json:"close"`
	High   []json.Number `json:"high"`
	Low    []json.Number `json:"low"`
	Vol    []json.Number `json:"vol"`
	Amount []json.Number `json:"amount"`
}

// toCandles converts parallel REST arrays into candle objects.
func (d restKlineData) toCandles() ([]kline.Candle, error) {
	n := len(d.Time)
	if n == 0 {
		return make([]kline.Candle, 0), nil
	}
	if len(d.Open) != n || len(d.Close) != n || len(d.High) != n || len(d.Low) != n || len(d.Vol) != n || len(d.Amount) != n {
		return nil, fmt.Errorf("mismatched REST kline array lengths")
	}

	candles := make([]kline.Candle, 0, n)
	for i := 0; i < n; i++ {
		candles = append(candles, kline.Candle{
			T:  d.Time[i],
			O:  d.Open[i].String(),
			H:  d.High[i].String(),
			L:  d.Low[i].String(),
			C:  d.Close[i].String(),
			V:  d.Vol[i].String(),
			A:  d.Amount[i].String(),
			Q:  "",
			RO: "",
			RC: "",
			RH: "",
			RL: "",
		})
	}

	return candles, nil
}
