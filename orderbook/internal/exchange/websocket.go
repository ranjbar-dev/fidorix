// Package exchange implements REST and WebSocket integration with MEXC.
package exchange

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	wsdto "github.com/kattana-io/mexc-golang-sdk/websocket/dto"
	"github.com/yourorg/mexc-orderbook/internal/orderbook"
	"google.golang.org/protobuf/proto"
)

const maxSymbolsPerConnection = 30

// WSConfig configures the MEXC WebSocket client.
type WSConfig struct {
	BaseURL              string
	Symbols              []string
	UpdateInterval       string
	PingIntervalSec      int
	ReconnectDelaySec    int
	MaxReconnectDelaySec int
}

// WSEventHandler handles parsed diff-depth events.
type WSEventHandler func(event orderbook.DiffDepthEvent)

// WSReconnectHandler notifies callers that managers must reinitialize.
type WSReconnectHandler func()

// WSClient maintains one or more WebSocket connections and dispatches events.
type WSClient struct {
	cfg         WSConfig
	logger      *slog.Logger
	onEvent     WSEventHandler
	onReconnect WSReconnectHandler

	mu      sync.Mutex
	conns   map[int]*websocket.Conn
	cancel  context.CancelFunc
	started bool
	wg      sync.WaitGroup
}

// NewWSClient creates a WebSocket client for one or more symbols.
func NewWSClient(cfg WSConfig, logger *slog.Logger, onEvent WSEventHandler, onReconnect WSReconnectHandler) (*WSClient, error) {
	if strings.TrimSpace(cfg.BaseURL) == "" {
		return nil, fmt.Errorf("WS base URL is required")
	}
	if len(cfg.Symbols) == 0 {
		return nil, fmt.Errorf("at least one symbol is required")
	}
	if cfg.UpdateInterval != "100ms" && cfg.UpdateInterval != "10ms" {
		return nil, fmt.Errorf("update interval must be 100ms or 10ms")
	}
	if cfg.PingIntervalSec < 1 {
		return nil, fmt.Errorf("ping interval must be greater than zero")
	}
	if cfg.ReconnectDelaySec < 1 {
		return nil, fmt.Errorf("reconnect delay must be greater than zero")
	}
	if cfg.MaxReconnectDelaySec < cfg.ReconnectDelaySec {
		return nil, fmt.Errorf("max reconnect delay must be >= reconnect delay")
	}

	if logger == nil {
		logger = slog.Default()
	}

	return &WSClient{
		cfg:         cfg,
		logger:      logger,
		onEvent:     onEvent,
		onReconnect: onReconnect,
		conns:       make(map[int]*websocket.Conn),
	}, nil
}

// Start starts all required WebSocket connection loops.
func (c *WSClient) Start(ctx context.Context) {
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return
	}
	c.started = true
	runCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	c.mu.Unlock()

	groups := chunkSymbols(c.cfg.Symbols, maxSymbolsPerConnection)
	for index, symbols := range groups {
		connectionID := index + 1
		chunk := append([]string(nil), symbols...)
		c.wg.Add(1)
		go c.runConnection(runCtx, connectionID, chunk)
	}
}

// Close stops all connection loops and closes active sockets.
func (c *WSClient) Close() {
	c.mu.Lock()
	cancel := c.cancel
	c.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	c.mu.Lock()
	for id, conn := range c.conns {
		_ = conn.Close()
		delete(c.conns, id)
	}
	c.mu.Unlock()
}

// Wait blocks until all WebSocket loops exit.
func (c *WSClient) Wait() {
	c.wg.Wait()
}

func (c *WSClient) runConnection(ctx context.Context, connectionID int, symbols []string) {
	defer c.wg.Done()

	connectedOnce := false
	for {
		connection, err := c.connectWithBackoff(ctx, connectionID, symbols)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.logger.Error(
				"websocket connection loop exiting",
				slog.Int("connection_id", connectionID),
				slog.String("error", err.Error()),
			)
			return
		}

		if connectedOnce && c.onReconnect != nil {
			c.onReconnect()
		}
		connectedOnce = true

		readErr := c.readLoop(ctx, connectionID, symbols, connection)
		c.unregisterConnection(connectionID, connection)
		_ = connection.Close()

		if ctx.Err() != nil {
			return
		}

		c.logger.Warn(
			"websocket read loop ended",
			slog.Int("connection_id", connectionID),
			slog.String("error", readErr.Error()),
		)
	}
}

func (c *WSClient) connectWithBackoff(ctx context.Context, connectionID int, symbols []string) (*websocket.Conn, error) {
	delay := c.cfg.ReconnectDelaySec
	attempt := 1

	for {
		connection, err := c.connectOnce(ctx, connectionID, symbols)
		if err == nil {
			return connection, nil
		}

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		c.logger.Warn(
			"websocket reconnect",
			slog.Int("connection_id", connectionID),
			slog.Int("attempt", attempt),
			slog.Int("delay_sec", delay),
			slog.String("error", err.Error()),
		)

		if err := waitFor(ctx, time.Duration(delay)*time.Second); err != nil {
			return nil, err
		}
		attempt++
		delay = min(delay*2, c.cfg.MaxReconnectDelaySec)
	}
}

func (c *WSClient) connectOnce(ctx context.Context, connectionID int, symbols []string) (*websocket.Conn, error) {
	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	connection, _, err := dialer.DialContext(ctx, c.cfg.BaseURL, nil)
	if err != nil {
		return nil, fmt.Errorf("dial websocket: %w", err)
	}

	subscription := map[string]any{
		"method": "SUBSCRIPTION",
		"params": buildSubscriptionParams(c.cfg.UpdateInterval, symbols),
	}
	if err := connection.WriteJSON(subscription); err != nil {
		_ = connection.Close()
		return nil, fmt.Errorf("send subscription: %w", err)
	}

	c.registerConnection(connectionID, connection)

	c.logger.Info(
		"websocket connected",
		slog.Int("connection_id", connectionID),
		slog.String("url", c.cfg.BaseURL),
		slog.Int("symbol_count", len(symbols)),
	)
	c.logger.Info(
		"subscription confirmed",
		slog.Int("connection_id", connectionID),
		slog.Int("symbol_count", len(symbols)),
	)

	return connection, nil
}

func (c *WSClient) readLoop(ctx context.Context, connectionID int, symbols []string, connection *websocket.Conn) error {
	pingDone := make(chan struct{})
	var pingWG sync.WaitGroup

	pingWG.Add(1)
	go func() {
		defer pingWG.Done()
		c.pingLoop(ctx, connectionID, connection, pingDone)
	}()

	defer func() {
		close(pingDone)
		pingWG.Wait()
	}()

	for {
		messageType, payload, err := connection.ReadMessage()
		if err != nil {
			return fmt.Errorf("read websocket message: %w", err)
		}

		event, ok, parseErr := parseWSDepthMessage(messageType, payload)
		if parseErr != nil {
			c.logger.Warn(
				"failed to parse websocket message",
				slog.Int("connection_id", connectionID),
				slog.String("error", parseErr.Error()),
			)
			continue
		}
		if !ok {
			continue
		}

		if c.onEvent != nil {
			c.onEvent(event)
		}
	}
}

func (c *WSClient) pingLoop(ctx context.Context, connectionID int, connection *websocket.Conn, done <-chan struct{}) {
	ticker := time.NewTicker(time.Duration(c.cfg.PingIntervalSec) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case <-ticker.C:
			if err := connection.WriteMessage(websocket.PingMessage, nil); err != nil {
				c.logger.Warn(
					"websocket ping failed",
					slog.Int("connection_id", connectionID),
					slog.String("error", err.Error()),
				)
				_ = connection.Close()
				return
			}
		}
	}
}

func (c *WSClient) registerConnection(connectionID int, connection *websocket.Conn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.conns[connectionID] = connection
}

func (c *WSClient) unregisterConnection(connectionID int, connection *websocket.Conn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if current, exists := c.conns[connectionID]; exists && current == connection {
		delete(c.conns, connectionID)
	}
}

func buildSubscriptionParams(interval string, symbols []string) []string {
	params := make([]string, 0, len(symbols))
	for _, symbol := range symbols {
		params = append(params, "spot@public.aggre.depth.v3.api.pb@"+interval+"@"+symbol)
	}
	return params
}

type wsDepthMessage struct {
	Channel  string `json:"channel"`
	Symbol   string `json:"symbol"`
	SendTime int64  `json:"sendtime"`
	Data     struct {
		AsksList    []wsPriceLevel `json:"asksList"`
		BidsList    []wsPriceLevel `json:"bidsList"`
		EventType   string         `json:"eventtype"`
		FromVersion string         `json:"fromVersion"`
		ToVersion   string         `json:"toVersion"`
	} `json:"publicincreasedepths"`
}

type wsPriceLevel struct {
	Price    string `json:"price"`
	Quantity string `json:"quantity"`
}

func parseWSDepthMessage(messageType int, payload []byte) (orderbook.DiffDepthEvent, bool, error) {
	switch messageType {
	case websocket.BinaryMessage:
		return parseWSDepthProtoMessage(payload)
	case websocket.TextMessage:
		return parseWSDepthJSONMessage(payload)
	default:
		return orderbook.DiffDepthEvent{}, false, nil
	}
}

func parseWSDepthProtoMessage(payload []byte) (orderbook.DiffDepthEvent, bool, error) {
	var message wsdto.PushDataV3ApiWrapper
	if err := proto.Unmarshal(payload, &message); err != nil {
		return orderbook.DiffDepthEvent{}, false, fmt.Errorf("unmarshal websocket protobuf payload: %w", err)
	}

	depth := message.GetPublicAggreDepths()
	if depth == nil {
		return orderbook.DiffDepthEvent{}, false, nil
	}

	symbol := strings.TrimSpace(message.GetSymbol())
	fromVersionText := strings.TrimSpace(depth.GetFromVersion())
	toVersionText := strings.TrimSpace(depth.GetToVersion())
	if symbol == "" || fromVersionText == "" || toVersionText == "" {
		return orderbook.DiffDepthEvent{}, false, nil
	}

	fromVersion, err := strconv.ParseInt(fromVersionText, 10, 64)
	if err != nil {
		return orderbook.DiffDepthEvent{}, false, fmt.Errorf("parse fromVersion %q: %w", fromVersionText, err)
	}
	toVersion, err := strconv.ParseInt(toVersionText, 10, 64)
	if err != nil {
		return orderbook.DiffDepthEvent{}, false, fmt.Errorf("parse toVersion %q: %w", toVersionText, err)
	}

	event := orderbook.DiffDepthEvent{
		Symbol:      strings.ToUpper(symbol),
		FromVersion: fromVersion,
		ToVersion:   toVersion,
		SendTime:    message.GetSendTime(),
		Bids:        make([]orderbook.PriceLevel, 0, len(depth.GetBids())),
		Asks:        make([]orderbook.PriceLevel, 0, len(depth.GetAsks())),
	}
	for _, level := range depth.GetBids() {
		if level == nil {
			continue
		}
		event.Bids = append(event.Bids, orderbook.PriceLevel{Price: level.GetPrice(), Quantity: level.GetQuantity()})
	}
	for _, level := range depth.GetAsks() {
		if level == nil {
			continue
		}
		event.Asks = append(event.Asks, orderbook.PriceLevel{Price: level.GetPrice(), Quantity: level.GetQuantity()})
	}

	return event, true, nil
}

func parseWSDepthJSONMessage(payload []byte) (orderbook.DiffDepthEvent, bool, error) {
	var message wsDepthMessage
	if err := json.Unmarshal(payload, &message); err != nil {
		return orderbook.DiffDepthEvent{}, false, fmt.Errorf("unmarshal websocket payload: %w", err)
	}

	if strings.TrimSpace(message.Symbol) == "" || strings.TrimSpace(message.Data.FromVersion) == "" || strings.TrimSpace(message.Data.ToVersion) == "" {
		return orderbook.DiffDepthEvent{}, false, nil
	}

	fromVersion, err := strconv.ParseInt(message.Data.FromVersion, 10, 64)
	if err != nil {
		return orderbook.DiffDepthEvent{}, false, fmt.Errorf("parse fromVersion %q: %w", message.Data.FromVersion, err)
	}
	toVersion, err := strconv.ParseInt(message.Data.ToVersion, 10, 64)
	if err != nil {
		return orderbook.DiffDepthEvent{}, false, fmt.Errorf("parse toVersion %q: %w", message.Data.ToVersion, err)
	}

	event := orderbook.DiffDepthEvent{
		Symbol:      strings.ToUpper(strings.TrimSpace(message.Symbol)),
		FromVersion: fromVersion,
		ToVersion:   toVersion,
		SendTime:    message.SendTime,
		Bids:        make([]orderbook.PriceLevel, 0, len(message.Data.BidsList)),
		Asks:        make([]orderbook.PriceLevel, 0, len(message.Data.AsksList)),
	}
	for _, level := range message.Data.BidsList {
		event.Bids = append(event.Bids, orderbook.PriceLevel{Price: level.Price, Quantity: level.Quantity})
	}
	for _, level := range message.Data.AsksList {
		event.Asks = append(event.Asks, orderbook.PriceLevel{Price: level.Price, Quantity: level.Quantity})
	}

	return event, true, nil
}

func chunkSymbols(symbols []string, chunkSize int) [][]string {
	chunks := make([][]string, 0, (len(symbols)+chunkSize-1)/chunkSize)
	for start := 0; start < len(symbols); start += chunkSize {
		end := start + chunkSize
		if end > len(symbols) {
			end = len(symbols)
		}
		chunks = append(chunks, symbols[start:end])
	}
	return chunks
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

func min(left, right int) int {
	if left < right {
		return left
	}
	return right
}
