package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
)

func TestSyntheticPayloadIsDeterministic(t *testing.T) {
	const size = 1024
	first := SyntheticPayload(7, size)
	second := SyntheticPayload(7, size)
	if !bytes.Equal(first, second) {
		t.Error("the same sequence produced different payloads")
	}
	if bytes.Equal(first, SyntheticPayload(8, size)) {
		t.Error("different sequences produced the same payload")
	}
	if got := binary.BigEndian.Uint32(first[:4]); got != 7 {
		t.Errorf("payload header sequence = %d, want 7", got)
	}
	if len(SyntheticPayload(7, 3)) != 3 {
		t.Error("a payload smaller than the header field was not honoured")
	}
}

func TestSyntheticStreamBoundedRange(t *testing.T) {
	s := NewSyntheticStream(SyntheticConfig{Size: 128})

	var got []uint32
	for raw, err := range s.RawLedgers(t.Context(), ledgerbackend.BoundedRange(5, 9)) {
		if err != nil {
			t.Fatalf("RawLedgers: %v", err)
		}
		got = append(got, binary.BigEndian.Uint32(raw[:4]))
	}
	want := []uint32{5, 6, 7, 8, 9}
	if len(got) != len(want) {
		t.Fatalf("got %d ledgers (%v), want %d", len(got), got, len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ledger sequences = %v, want %v", got, want)
		}
	}
}

func TestSyntheticStreamReusesItsBuffer(t *testing.T) {
	// The SDK contract says a yielded slice is borrowed. The synthetic source
	// keeps that promise honest, which is what forces the server to copy.
	s := NewSyntheticStream(SyntheticConfig{Size: 64})
	var firstSlice []byte
	var firstCopy []byte
	i := 0
	for raw, err := range s.RawLedgers(t.Context(), ledgerbackend.BoundedRange(1, 2)) {
		if err != nil {
			t.Fatalf("RawLedgers: %v", err)
		}
		if i == 0 {
			firstSlice = raw
			firstCopy = bytes.Clone(raw)
		}
		i++
	}
	if bytes.Equal(firstSlice, firstCopy) {
		t.Error("the second ledger did not overwrite the first borrow")
	}
}

func TestSyntheticStreamStopsOnBreak(t *testing.T) {
	s := NewSyntheticStream(SyntheticConfig{Size: 32})
	pulled := 0
	for range s.RawLedgers(t.Context(), ledgerbackend.UnboundedRange(1)) {
		pulled++
		if pulled == 3 {
			break
		}
	}
	if pulled != 3 {
		t.Errorf("pulled %d ledgers, want 3", pulled)
	}
}

func TestSyntheticStreamHonoursContextCancel(t *testing.T) {
	s := NewSyntheticStream(SyntheticConfig{Size: 32})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var lastErr error
	count := 0
	for _, err := range s.RawLedgers(ctx, ledgerbackend.UnboundedRange(1)) {
		if err != nil {
			lastErr = err
			break
		}
		count++
		cancel()
	}
	if count != 1 {
		t.Errorf("delivered %d ledgers before cancel took effect, want 1", count)
	}
	if !errors.Is(lastErr, context.Canceled) {
		t.Errorf("error after cancel = %v, want context.Canceled", lastErr)
	}
}

func TestCountedRange(t *testing.T) {
	unbounded := CountedRange(10, 0)
	if unbounded.Bounded() || unbounded.From() != 10 {
		t.Errorf("CountedRange(10, 0) = %s, want [10,latest)", unbounded)
	}
	bounded := CountedRange(10, 3)
	if !bounded.Bounded() || bounded.From() != 10 || bounded.To() != 12 {
		t.Errorf("CountedRange(10, 3) = %s, want [10,12]", bounded)
	}
}
