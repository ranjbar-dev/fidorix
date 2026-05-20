package kline

import "testing"

func TestUpsertInsert(t *testing.T) {
	existing := []Candle{
		{T: 10, C: "1"},
		{T: 30, C: "3"},
	}
	incoming := Candle{T: 20, C: "2"}

	got := Upsert(existing, incoming)

	if len(got) != 3 {
		t.Fatalf("len(got) = %d, want 3", len(got))
	}
	if got[0].T != 10 || got[1].T != 20 || got[2].T != 30 {
		t.Fatalf("unexpected order after insert: %+v", got)
	}
}

func TestUpsertUpdateInPlace(t *testing.T) {
	existing := []Candle{
		{T: 10, C: "old"},
		{T: 20, C: "keep"},
	}
	incoming := Candle{T: 10, C: "new"}

	got := Upsert(existing, incoming)

	if len(got) != 2 {
		t.Fatalf("len(got) = %d, want 2", len(got))
	}
	if got[0].C != "new" {
		t.Fatalf("got[0].C = %q, want %q", got[0].C, "new")
	}
}

func TestUpsertAppendNew(t *testing.T) {
	existing := []Candle{
		{T: 10, C: "1"},
		{T: 20, C: "2"},
	}
	incoming := Candle{T: 30, C: "3"}

	got := Upsert(existing, incoming)

	if len(got) != 3 {
		t.Fatalf("len(got) = %d, want 3", len(got))
	}
	if got[2].T != 30 {
		t.Fatalf("got[2].T = %d, want 30", got[2].T)
	}
}
