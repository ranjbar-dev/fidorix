package ws

import (
	"context"
	"sync"

	"mexc-kline-snapshot/internal/config"
	"mexc-kline-snapshot/internal/symbol"

	"go.uber.org/zap"
)

const wsEndpoint = "wss://contract.mexc.com/edge"

// Manager owns websocket shard connections for all symbol-interval subscriptions.
type Manager struct {
	conns   []*Conn
	msgChan chan []byte
	log     *zap.Logger
	wg      sync.WaitGroup
}

// New builds websocket shards using the configured shard size.
func New(cfg *config.Config, msgChan chan []byte, log *zap.Logger) *Manager {
	if log == nil {
		log = zap.NewNop()
	}
	if msgChan == nil {
		msgChan = make(chan []byte, 1000)
	}

	shardSize := cfg.ShardSize
	if shardSize <= 0 {
		shardSize = 50
	}

	subs := make([]Subscription, 0, cfg.SubscriptionCount())
	for _, fileSymbol := range cfg.Symbols {
		wireSymbol := symbol.ToMEXC(fileSymbol)
		for _, interval := range cfg.Intervals {
			subs = append(subs, Subscription{Symbol: wireSymbol, Interval: interval})
		}
	}

	conns := make([]*Conn, 0)
	for i := 0; i < len(subs); i += shardSize {
		end := i + shardSize
		if end > len(subs) {
			end = len(subs)
		}

		chunk := append([]Subscription(nil), subs[i:end]...)
		connID := len(conns) + 1
		conns = append(conns, NewConn(
			connID,
			wsEndpoint,
			chunk,
			msgChan,
			cfg.WSPingInterval,
			log.With(zap.Int("connID", connID)),
		))
	}

	return &Manager{
		conns:   conns,
		msgChan: msgChan,
		log:     log,
	}
}

// Run starts all shard connections and blocks until context cancellation.
func (m *Manager) Run(ctx context.Context) {
	for _, conn := range m.conns {
		conn := conn
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			conn.Run(ctx)
		}()
	}

	<-ctx.Done()
	m.wg.Wait()
}

// Close closes all active websocket connections.
func (m *Manager) Close() {
	for _, conn := range m.conns {
		conn.Close()
	}
}

// ConnectionCount returns the number of shard connections.
func (m *Manager) ConnectionCount() int {
	return len(m.conns)
}

// SubscriptionCount returns the total subscriptions across all shards.
func (m *Manager) SubscriptionCount() int {
	total := 0
	for _, conn := range m.conns {
		total += len(conn.subscriptions)
	}
	return total
}
