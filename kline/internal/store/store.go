package store

import (
	"sync"

	"mexc-kline-snapshot/internal/kline"
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
}

// New creates an empty in-memory store.
func New() *Store {
	return &Store{
		entries: make(map[string]*Entry),
	}
}

// Upsert inserts or updates a candle for the given symbol and interval.
func (s *Store) Upsert(symbol, interval string, c kline.Candle) {
	s.mu.Lock()
	defer s.mu.Unlock()

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
