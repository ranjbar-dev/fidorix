package db

import (
	"fmt"
	"strconv"
	"time"

	"mexc-kline-snapshot/internal/kline"
)

type candleRecord struct {
	symbol   string
	interval string
	openTime time.Time
	open     float64
	high     float64
	low      float64
	close    float64
	volume   float64
	amount   float64
}

func toRecord(symbol, interval string, candle kline.Candle) (candleRecord, error) {
	open, err := parseNumeric("open", candle.O)
	if err != nil {
		return candleRecord{}, err
	}
	high, err := parseNumeric("high", candle.H)
	if err != nil {
		return candleRecord{}, err
	}
	low, err := parseNumeric("low", candle.L)
	if err != nil {
		return candleRecord{}, err
	}
	closePrice, err := parseNumeric("close", candle.C)
	if err != nil {
		return candleRecord{}, err
	}
	volume, err := parseNumeric("volume", candle.V)
	if err != nil {
		return candleRecord{}, err
	}
	amount, err := parseNumeric("amount", candle.A)
	if err != nil {
		return candleRecord{}, err
	}

	return candleRecord{
		symbol:   symbol,
		interval: interval,
		openTime: time.Unix(candle.T, 0).UTC(),
		open:     open,
		high:     high,
		low:      low,
		close:    closePrice,
		volume:   volume,
		amount:   amount,
	}, nil
}

func parseNumeric(name, raw string) (float64, error) {
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, fmt.Errorf("parse candle %s value %q: %w", name, raw, err)
	}
	return v, nil
}

func formatNumeric(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}

func reverseCandles(candles []kline.Candle) {
	for i, j := 0, len(candles)-1; i < j; i, j = i+1, j-1 {
		candles[i], candles[j] = candles[j], candles[i]
	}
}
