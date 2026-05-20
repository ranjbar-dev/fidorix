package kline

import "sort"

// Candle is a single OHLCV kline record.
type Candle struct {
	T  int64  `json:"t"`
	O  string `json:"o"`
	H  string `json:"h"`
	L  string `json:"l"`
	C  string `json:"c"`
	V  string `json:"v"`
	A  string `json:"a"`
	Q  string `json:"q"`
	RO string `json:"ro,omitempty"`
	RC string `json:"rc,omitempty"`
	RH string `json:"rh,omitempty"`
	RL string `json:"rl,omitempty"`
}

// Upsert inserts or replaces a candle by timestamp while preserving ascending order.
func Upsert(existing []Candle, incoming Candle) []Candle {
	idx := sort.Search(len(existing), func(i int) bool {
		return existing[i].T >= incoming.T
	})

	if idx < len(existing) && existing[idx].T == incoming.T {
		out := make([]Candle, len(existing))
		copy(out, existing)
		out[idx] = incoming
		return out
	}

	out := make([]Candle, len(existing)+1)
	copy(out, existing[:idx])
	out[idx] = incoming
	copy(out[idx+1:], existing[idx:])
	return out
}
