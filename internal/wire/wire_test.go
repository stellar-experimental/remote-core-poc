package wire

import (
	"bytes"
	"errors"
	"math"
	"testing"
)

func TestAppendLedgerRoundTrip(t *testing.T) {
	payload := []byte("not-really-xdr")
	msg := AppendLedger(nil, 42, 1234567890123456789, payload)

	if len(msg) != HeaderSize+len(payload) {
		t.Fatalf("message length = %d, want %d", len(msg), HeaderSize+len(payload))
	}

	h, got, err := Decode(msg)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if h.Version != Version || h.Type != TypeLedger {
		t.Errorf("header = %+v, want version 0x%02x type 0x%02x", h, Version, TypeLedger)
	}
	if h.Sequence != 42 {
		t.Errorf("Sequence = %d, want 42", h.Sequence)
	}
	if h.EmitUnixNano != 1234567890123456789 {
		t.Errorf("EmitUnixNano = %d, want 1234567890123456789", h.EmitUnixNano)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("payload = %q, want %q", got, payload)
	}
	// The payload must alias the message, so a retained message retains it.
	if &got[0] != &msg[HeaderSize] {
		t.Error("payload does not alias the message")
	}
}

func TestAppendLedgerMultiMegabytePayload(t *testing.T) {
	const size = 5 << 20
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i)
	}

	msg := AppendLedger(make([]byte, 0, HeaderSize+size), math.MaxUint32, -1, payload)
	h, got, err := Decode(msg)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if h.Sequence != math.MaxUint32 {
		t.Errorf("Sequence = %d, want %d", h.Sequence, uint32(math.MaxUint32))
	}
	if h.EmitUnixNano != -1 {
		t.Errorf("EmitUnixNano = %d, want -1", h.EmitUnixNano)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("payload of %d bytes did not round-trip", size)
	}
}

func TestAppendLedgerEmptyPayload(t *testing.T) {
	h, payload, err := Decode(AppendLedger(nil, 1, 0, nil))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if h.Sequence != 1 || h.EmitUnixNano != 0 {
		t.Errorf("header = %+v, want sequence 1 and emit 0", h)
	}
	if len(payload) != 0 {
		t.Errorf("payload = %d bytes, want 0", len(payload))
	}
}

func TestDecodeErrors(t *testing.T) {
	good := AppendLedger(nil, 7, 1, []byte("x"))

	badVersion := bytes.Clone(good)
	badVersion[0] = 0x02

	badType := bytes.Clone(good)
	badType[1] = 0x09

	tests := []struct {
		name string
		msg  []byte
		want error
	}{
		{"empty", nil, ErrShortMessage},
		{"truncated header", good[:HeaderSize-1], ErrShortMessage},
		{"version", badVersion, ErrVersion},
		{"type", badType, ErrType},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, _, err := Decode(tt.msg); !errors.Is(err, tt.want) {
				t.Errorf("Decode error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestAppendHeaderReusesBuffer(t *testing.T) {
	buf := make([]byte, 0, 64)
	buf = AppendHeader(buf, Header{Version: Version, Type: TypeLedger, Sequence: 3})
	if len(buf) != HeaderSize {
		t.Fatalf("len = %d, want %d", len(buf), HeaderSize)
	}
	if cap(buf) != 64 {
		t.Errorf("cap = %d, want the original 64 (no reallocation)", cap(buf))
	}
}
