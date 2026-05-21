package store

import (
	"context"
	"sync"
	"time"

	"mexc-kline-snapshot/internal/db"
	"mexc-kline-snapshot/internal/kline"
	"mexc-kline-snapshot/internal/redisclient"

	"go.uber.org/zap"
)

// Entry represents one symbol-interval candle snapshot tracked in memory.
type Entry struct {
	Key   string
	File  CandleFile
	dirty bool
}

// Store provides thread-safe access to symbol-interval candle snapshots.
type Store struct {
	mu      sync.RWMutex
	entries map[string]*Entry
	db      *db.DB
	redis   *redisclient.Client
}

// New creates an empty in-memory store.
func New() *Store {
	return NewFull(nil, nil)
}

// NewWithDB creates a store backed by TimescaleDB writes.
func NewWithDB(d *db.DB) *Store {
	return NewFull(d, nil)
}

// NewWithRedis creates a store backed by Redis latest+publish writes.
func NewWithRedis(r *redisclient.Client) *Store {
	return NewFull(nil, r)
}

// NewFull creates a store backed by optional TimescaleDB and Redis clients.
func NewFull(d *db.DB, r *redisclient.Client) *Store {
	return &Store{
		entries: make(map[string]*Entry),
		db:      d,
		redis:   r,
	}
}

// Upsert inserts or updates a candle for the given symbol and interval.
func (s *Store) Upsert(symbol, interval string, c kline.Candle) {
	var dbClient *db.DB
	var redisClient *redisclient.Client

	s.mu.Lock()
	dbClient = s.db
	redisClient = s.redis

	key := keyFor(symbol, interval)
	entry, ok := s.entries[key]
	if !ok {
		entry = &Entry{
			Key: key,
			File: CandleFile{
				Symbol:   symbol,
				Interval: interval,
				Candles:  make([]kline.Candle, 0),
			},
		}
		s.entries[key] = entry
	}

	entry.File.Symbol = symbol
	entry.File.Interval = interval
	entry.File.Candles = kline.Upsert(entry.File.Candles, c)
	entry.dirty = true
	s.mu.Unlock()

	if dbClient != nil || redisClient != nil {
		go persistExternal(dbClient, redisClient, symbol, interval, c)
	}
}

// GetDirty returns snapshot copies of all dirty entries.
func (s *Store) GetDirty() []*Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	dirty := make([]*Entry, 0)
	for _, entry := range s.entries {
		if !entry.dirty {
			continue
		}
		dirty = append(dirty, cloneEntry(entry))
	}
	return dirty
}

// ClearDirty clears the dirty flag for an entry by key.
func (s *Store) ClearDirty(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.entries[key]
	if !ok {
		return
	}
	entry.dirty = false
}

// Load seeds or replaces an entry in store during startup/bootstrap.
func (s *Store) Load(symbol, interval string, cf CandleFile) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if cf.Symbol == "" {
		cf.Symbol = symbol
	}
	if cf.Interval == "" {
		cf.Interval = interval
	}
	if cf.Candles == nil {
		cf.Candles = make([]kline.Candle, 0)
	}

	key := keyFor(symbol, interval)
	s.entries[key] = &Entry{
		Key:   key,
		File:  cf,
		dirty: false,
	}
}

// All returns a snapshot copy of all entries.
func (s *Store) All() map[string]*Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make(map[string]*Entry, len(s.entries))
	for key, entry := range s.entries {
		out[key] = cloneEntry(entry)
	}
	return out
}

// Has reports whether the symbol-interval pair exists in the store.
func (s *Store) Has(symbol, interval string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, ok := s.entries[keyFor(symbol, interval)]
	return ok
}

// keyFor builds the canonical map key for store entries.
func keyFor(symbol, interval string) string {
	return symbol + ":" + interval
}

// cloneEntry creates a deep copy of an entry snapshot.
func cloneEntry(in *Entry) *Entry {
	if in == nil {
		return nil
	}
	out := &Entry{
		Key:   in.Key,
		File:  in.File,
		dirty: in.dirty,
	}
	out.File.Candles = append([]kline.Candle(nil), in.File.Candles...)
	return out
}

func persistCandleToDB(client *db.DB, symbol, interval string, candle kline.Candle) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.UpsertBatch(ctx, symbol, interval, []kline.Candle{candle}); err != nil {
		zap.L().Warn("failed to upsert candle to timescaledb", zap.String("symbol", symbol), zap.String("interval", interval), zap.Error(err))
	}
}

func persistExternal(dbClient *db.DB, redisClient *redisclient.Client, symbol, interval string, candle kline.Candle) {
	if dbClient != nil {
		persistCandleToDB(dbClient, symbol, interval, candle)
	}
	if redisClient != nil {
		publishCandleToRedis(redisClient, symbol, interval, candle)
	}
}

func publishCandleToRedis(client *redisclient.Client, symbol, interval string, candle kline.Candle) {
	if err := setLatestBarWithTimeout(client, symbol, interval, candle); err != nil {
		zap.L().Warn("failed to set latest candle in redis", zap.String("symbol", symbol), zap.String("interval", interval), zap.Error(err))
	}
	if err := publishBarWithTimeout(client, symbol, interval, candle); err != nil {
		zap.L().Warn("failed to publish candle to redis", zap.String("symbol", symbol), zap.String("interval", interval), zap.Error(err))
	}
}

func setLatestBarWithTimeout(client *redisclient.Client, symbol, interval string, candle kline.Candle) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return client.SetLatestBar(ctx, symbol, interval, candle)
}

func publishBarWithTimeout(client *redisclient.Client, symbol, interval string, candle kline.Candle) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return client.PublishBar(ctx, symbol, interval, candle)
}
