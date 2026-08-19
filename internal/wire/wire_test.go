package wire

import (
	"bytes"
	"errors"
	"math"
	"testing"
)

func TestBeginRoundTrip(t *testing.T) {
	msg := AppendBegin(nil, 42, 1234567890123456789)
	if len(msg) != BeginSize {
		t.Fatalf("BEGIN length = %d, want %d", len(msg), BeginSize)
	}
	m, err := Decode(msg)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if m.Type != TypeBegin || m.Seq != 42 || m.EmitStartUnixNano != 1234567890123456789 {
		t.Errorf("decoded %+v, want BEGIN for ledger 42 with the emit-start stamp", m)
	}
}

func TestChunkRoundTrip(t *testing.T) {
	payload := []byte("one piece of a ledger")
	msg := AppendChunk(nil, 7, 3, payload)
	if len(msg) != ChunkHeaderSize+len(payload) {
		t.Fatalf("CHUNK length = %d, want %d", len(msg), ChunkHeaderSize+len(payload))
	}
	m, err := Decode(msg)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if m.Type != TypeChunk || m.Seq != 7 || m.ChunkIndex != 3 {
		t.Errorf("decoded %+v, want chunk 3 of ledger 7", m)
	}
	if !bytes.Equal(m.Payload, payload) {
		t.Errorf("payload = %q, want %q", m.Payload, payload)
	}
	// The payload must alias the message, so a retained message retains it.
	if &m.Payload[0] != &msg[ChunkHeaderSize] {
		t.Error("payload does not alias the message")
	}
}

func TestPutChunkHeaderMatchesAppendChunk(t *testing.T) {
	// The in-place framing used by the hot path must produce the same bytes as
	// the appending form, or the two would silently speak different dialects.
	payload := []byte("payload read straight into the message")
	appended := AppendChunk(nil, 9, 4, payload)

	inPlace := make([]byte, ChunkHeaderSize+len(payload))
	copy(inPlace[ChunkHeaderSize:], payload)
	PutChunkHeader(inPlace, 9, 4)

	if !bytes.Equal(appended, inPlace) {
		t.Errorf("PutChunkHeader framing = %x, AppendChunk framing = %x", inPlace, appended)
	}
}

func TestChunkEmptyPayload(t *testing.T) {
	m, err := Decode(AppendChunk(nil, 1, 0, nil))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(m.Payload) != 0 {
		t.Errorf("payload = %d bytes, want 0", len(m.Payload))
	}
}

func TestEndRoundTrip(t *testing.T) {
	msg := AppendEnd(nil, math.MaxUint32, 58, 15196324, -1, 0xdeadbeefcafef00d)
	if len(msg) != EndSize {
		t.Fatalf("END length = %d, want %d", len(msg), EndSize)
	}
	m, err := Decode(msg)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if m.Type != TypeEnd || m.Seq != math.MaxUint32 {
		t.Errorf("decoded %+v, want END for the ceiling sequence", m)
	}
	if m.ChunkCount != 58 || m.TotalLen != 15196324 {
		t.Errorf("counts = (%d chunks, %d bytes), want (58, 15196324)", m.ChunkCount, m.TotalLen)
	}
	if m.EmitEndUnixNano != -1 || m.Checksum != 0xdeadbeefcafef00d {
		t.Errorf("stamp/checksum = (%d, %#x), want (-1, 0xdeadbeefcafef00d)", m.EmitEndUnixNano, m.Checksum)
	}
}

func TestDecodeErrors(t *testing.T) {
	begin := AppendBegin(nil, 1, 2)
	end := AppendEnd(nil, 1, 2, 3, 4, 5)

	// 0x01 was the retired one-message-per-ledger protocol's version byte; a
	// current decoder must refuse it like any other unknown version.
	oldVersion := AppendChunk(nil, 1, 0, []byte("x"))
	oldVersion[0] = 0x01

	badType := AppendBegin(nil, 1, 2)
	badType[1] = 0x7f

	tests := []struct {
		name string
		msg  []byte
		want error
	}{
		{"empty", nil, ErrShortMessage},
		{"shorter than any header", begin[:5], ErrShortMessage},
		{"truncated BEGIN", begin[:BeginSize-1], ErrShortMessage},
		{"oversized BEGIN", append(bytes.Clone(begin), 0), ErrShortMessage},
		{"truncated CHUNK header", AppendChunk(nil, 1, 0, nil)[:ChunkHeaderSize-1], ErrShortMessage},
		{"truncated END", end[:EndSize-1], ErrShortMessage},
		{"oversized END", append(bytes.Clone(end), 0), ErrShortMessage},
		{"retired version byte", oldVersion, ErrVersion},
		{"unknown type", badType, ErrType},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Decode(tt.msg); !errors.Is(err, tt.want) {
				t.Errorf("Decode error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestChunkSizeBounds(t *testing.T) {
	if DefaultChunkSize != 256<<10 {
		t.Errorf("DefaultChunkSize = %d, want 256 KiB", DefaultChunkSize)
	}
	if err := ValidateChunkSize(DefaultChunkSize); err != nil {
		t.Errorf("the default chunk size fails its own validation: %v", err)
	}
	for _, bad := range []int{0, MinChunkSize - 1, MaxChunkSize + 1} {
		if err := ValidateChunkSize(bad); err == nil {
			t.Errorf("ValidateChunkSize(%d) passed, want an error", bad)
		}
	}
	// A reader sized for one chunk message must still admit an END, or every
	// flow would die at its last message.
	if int64(MinChunkSize)+ChunkHeaderSize < EndSize {
		t.Error("the smallest chunk read limit cannot admit an END message")
	}
}

func TestPayloadCap(t *testing.T) {
	if DefaultMaxPayloadSize != 256<<20 {
		t.Errorf("DefaultMaxPayloadSize = %d, want 256 MiB", DefaultMaxPayloadSize)
	}
}
