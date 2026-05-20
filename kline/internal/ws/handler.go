package ws

import (
	"bytes"
	"context"
	"encoding/json"

	"mexc-kline-snapshot/internal/kline"
	"mexc-kline-snapshot/internal/store"

	"go.uber.org/zap"
)

const (
	pushKlineChannel = "push.kline"
	pongChannel      = "pong"
)

// Handler processes websocket messages and applies candle updates to the store.
type Handler struct {
	store *store.Store
	log   *zap.Logger
}

// NewHandler creates a websocket message handler.
func NewHandler(s *store.Store, log *zap.Logger) *Handler {
	if log == nil {
		log = zap.NewNop()
	}
	return &Handler{store: s, log: log}
}

// Run consumes websocket messages until the context is cancelled.
func (h *Handler) Run(ctx context.Context, msgChan <-chan []byte) {
	for {
		select {
		case <-ctx.Done():
			return
		case payload, ok := <-msgChan:
			if !ok {
				return
			}
			h.handleMessage(payload)
		}
	}
}

// handleMessage dispatches parsed websocket frames by channel type.
func (h *Handler) handleMessage(payload []byte) {
	var envelope wsEnvelope
	if err := decodeJSON(payload, &envelope); err != nil {
		h.log.Debug("discarding malformed websocket message", zap.Error(err))
		return
	}

	switch envelope.Channel {
	case pushKlineChannel:
		h.handlePushKline(envelope.Data)
	case pongChannel:
		return
	default:
		h.log.Debug("unexpected websocket channel", zap.String("channel", envelope.Channel))
	}
}

// handlePushKline parses push.kline data and upserts it to store.
func (h *Handler) handlePushKline(data json.RawMessage) {
	var push wsPushKline
	if err := decodeJSON(data, &push); err != nil {
		h.log.Debug("discarding malformed push.kline payload", zap.Error(err))
		return
	}

	if push.Symbol == "" || push.Interval == "" {
		h.log.Debug("discarding push.kline with missing symbol/interval")
		return
	}

	if !h.store.Has(push.Symbol, push.Interval) {
		h.log.Warn("push.kline for unknown symbol/interval", zap.String("symbol", push.Symbol), zap.String("interval", push.Interval))
		return
	}

	candle := kline.Candle{
		T:  push.T,
		O:  numberString(push.O),
		H:  numberString(push.H),
		L:  numberString(push.L),
		C:  numberString(push.C),
		V:  numberString(push.V),
		A:  numberString(push.A),
		Q:  numberString(push.Q),
		RO: numberString(push.RO),
		RC: numberString(push.RC),
		RH: numberString(push.RH),
		RL: numberString(push.RL),
	}

	h.store.Upsert(push.Symbol, push.Interval, candle)
	h.log.Debug("upserted push.kline", zap.String("symbol", push.Symbol), zap.String("interval", push.Interval), zap.Int64("t", push.T))
}

// decodeJSON unmarshals JSON while preserving numbers as json.Number.
func decodeJSON(payload []byte, out any) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	return decoder.Decode(out)
}

// numberString converts a json.Number to its string form.
func numberString(n json.Number) string {
	if n == "" {
		return ""
	}
	return n.String()
}

type wsEnvelope struct {
	Channel string          `json:"channel"`
	Data    json.RawMessage `json:"data"`
}

type wsPushKline struct {
	Symbol   string      `json:"symbol"`
	Interval string      `json:"interval"`
	T        int64       `json:"t"`
	O        json.Number `json:"o"`
	H        json.Number `json:"h"`
	L        json.Number `json:"l"`
	C        json.Number `json:"c"`
	V        json.Number `json:"v"`
	A        json.Number `json:"a"`
	Q        json.Number `json:"q"`
	RO       json.Number `json:"ro"`
	RC       json.Number `json:"rc"`
	RH       json.Number `json:"rh"`
	RL       json.Number `json:"rl"`
}
