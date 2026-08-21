package remoteledger

import (
	"bytes"
	"testing"

	"github.com/klauspost/compress/zstd"

	"github.com/stellar-experimental/remote-core-poc/internal/wire"
)

// TestAppendChunkPayloadAssemblesPastOneChunk pins the bound that a real
// ledger crosses and the unit fixtures never did: chunks decompress by
// APPENDING into the assembly buffer, and zstd's memory limit counts the
// whole output slice, so a limit set per chunk rejects a legitimate flow the
// moment the assembled ledger outgrows it. A stress ledger is ~14.5 MiB of
// 256 KiB chunks — this walks well past any chunk-sized ceiling.
func TestAppendChunkPayloadAssemblesPastOneChunk(t *testing.T) {
	dec, err := newDecoder(wire.MaxChunkSize)
	if err != nil {
		t.Fatal(err)
	}
	defer dec.Close()
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		t.Fatal(err)
	}
	defer enc.Close()

	const chunkSize = 256 << 10
	const chunks = 80 // 20 MiB assembled, past MaxChunkSize (8 MiB)
	piece := bytes.Repeat([]byte("ledger-close-meta-"), chunkSize/18)
	frame := enc.EncodeAll(piece, nil)

	var asm, scratch []byte
	for i := range chunks {
		asm, err = appendChunkPayload(dec, asm, frame, wire.CodecZstd, &scratch)
		if err != nil {
			t.Fatalf("chunk %d of %d failed after %d bytes assembled: %v", i, chunks, len(asm), err)
		}
	}
	if want := len(piece) * chunks; len(asm) != want {
		t.Fatalf("assembled %d bytes, want %d", len(asm), want)
	}
	if !bytes.Equal(asm[:len(piece)], piece) || !bytes.Equal(asm[len(asm)-len(piece):], piece) {
		t.Fatal("assembled bytes differ from the pieces that went in")
	}
}
