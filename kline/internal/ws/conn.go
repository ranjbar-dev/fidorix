package ws

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

const (
	subscribeMethod = "sub.kline"
	pingMethod      = "ping"
)

// Subscription defines one symbol-interval stream assignment.
type Subscription struct {
	Symbol   string
	Interval string
}

// Conn manages one MEXC websocket connection and its assigned subscriptions.
type Conn struct {
	id            int
	endpoint      string
	subscriptions []Subscription
	pingInterval  time.Duration
	msgChan       chan<- []byte
	log           *zap.Logger

	connMu  sync.RWMutex
	conn    *websocket.Conn
	writeMu sync.Mutex
}

// NewConn constructs a websocket connection worker.
func NewConn(id int, endpoint string, subscriptions []Subscription, msgChan chan<- []byte, pingInterval time.Duration, log *zap.Logger) *Conn {
	if log == nil {
		log = zap.NewNop()
	}
	return &Conn{
		id:            id,
		endpoint:      endpoint,
		subscriptions: append([]Subscription(nil), subscriptions...),
		pingInterval:  pingInterval,
		msgChan:       msgChan,
		log:           log,
	}
}

// Run starts the dial-subscribe-read loop with reconnect and backoff.
func (c *Conn) Run(ctx context.Context) {
	backoff := time.Second

	for {
		if ctx.Err() != nil {
			c.Close()
			return
		}

		if err := c.connect(ctx); err != nil {
			c.log.Warn("websocket dial failed", zap.Int("connID", c.id), zap.Error(err))
			if !waitBackoff(ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff)
			continue
		}

		connCtx, cancel := context.WithCancel(ctx)
		pingDone := make(chan struct{})
		go func() {
			defer close(pingDone)
			c.pingLoop(connCtx)
		}()

		if err := c.subscribeAll(); err != nil {
			cancel()
			<-pingDone
			c.Close()
			c.log.Warn("subscription setup failed", zap.Int("connID", c.id), zap.Error(err))
			if !waitBackoff(ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff)
			continue
		}

		hadMessage, err := c.readLoop(connCtx)
		cancel()
		<-pingDone
		c.Close()

		if err == nil {
			return
		}
		if hadMessage {
			backoff = time.Second
		}

		c.log.Warn("websocket read loop ended", zap.Int("connID", c.id), zap.Error(err))
		if !waitBackoff(ctx, backoff) {
			return
		}
		backoff = nextBackoff(backoff)
	}
}

// Close closes the active websocket connection.
func (c *Conn) Close() {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	c.connMu.Lock()
	defer c.connMu.Unlock()

	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
}

// sendJSON writes a JSON frame with a guarded write lock.
func (c *Conn) sendJSON(v any) error {
	conn := c.getConn()
	if conn == nil {
		return fmt.Errorf("connection is not established")
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if err := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return fmt.Errorf("set write deadline: %w", err)
	}
	if err := conn.WriteJSON(v); err != nil {
		return fmt.Errorf("write json frame: %w", err)
	}
	return nil
}

// connect establishes a websocket session.
func (c *Conn) connect(ctx context.Context) error {
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, c.endpoint, nil)
	if err != nil {
		return fmt.Errorf("dial websocket: %w", err)
	}
	c.connMu.Lock()
	c.conn = conn
	c.connMu.Unlock()
	return nil
}

// subscribeAll sends all assigned sub.kline messages.
func (c *Conn) subscribeAll() error {
	for _, sub := range c.subscriptions {
		req := subscribeRequest{
			Method: subscribeMethod,
			Param: subscribeParam{
				Symbol:   sub.Symbol,
				Interval: sub.Interval,
			},
			Gzip: false,
		}
		if err := c.sendJSON(req); err != nil {
			return fmt.Errorf("subscribe %s %s: %w", sub.Symbol, sub.Interval, err)
		}
	}
	return nil
}

// pingLoop sends periodic ping messages until cancellation.
func (c *Conn) pingLoop(ctx context.Context) {
	interval := c.pingInterval
	if interval <= 0 {
		interval = 15 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.sendJSON(pingRequest{Method: pingMethod}); err != nil {
				c.log.Warn("websocket ping failed", zap.Int("connID", c.id), zap.Error(err))
				c.Close()
				return
			}
		}
	}
}

// readLoop forwards websocket messages to the shared handler channel.
func (c *Conn) readLoop(ctx context.Context) (bool, error) {
	hadMessage := false
	for {
		select {
		case <-ctx.Done():
			return hadMessage, nil
		default:
		}

		conn := c.getConn()
		if conn == nil {
			return hadMessage, fmt.Errorf("connection is not established")
		}

		_, payload, err := conn.ReadMessage()
		if err != nil {
			return hadMessage, fmt.Errorf("read message: %w", err)
		}
		hadMessage = true

		c.log.Debug("websocket message received", zap.Int("connID", c.id), zap.Int("bytes", len(payload)))

		select {
		case <-ctx.Done():
			return hadMessage, nil
		case c.msgChan <- payload:
		}
	}
}

// getConn returns the current websocket connection pointer.
func (c *Conn) getConn() *websocket.Conn {
	c.connMu.RLock()
	defer c.connMu.RUnlock()
	return c.conn
}

// waitBackoff waits for the retry delay unless context is cancelled.
func waitBackoff(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// nextBackoff doubles delay up to 60 seconds.
func nextBackoff(current time.Duration) time.Duration {
	next := current * 2
	if next > 60*time.Second {
		return 60 * time.Second
	}
	return next
}

type subscribeRequest struct {
	Method string         `json:"method"`
	Param  subscribeParam `json:"param"`
	Gzip   bool           `json:"gzip"`
}

type subscribeParam struct {
	Symbol   string `json:"symbol"`
	Interval string `json:"interval"`
}

type pingRequest struct {
	Method string `json:"method"`
}
