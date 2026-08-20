package server

import (
	"bytes"
	"context"
	"errors"
	"math/rand"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"

	"github.com/stellar-experimental/remote-core-poc/internal/store"

	"github.com/klauspost/compress/zstd"

	"github.com/stellar-experimental/remote-core-poc/internal/wire"
)

func decodeOne(t *testing.T, msg []byte) wire.Message {
	t.Helper()
	m, err := wire.Decode(msg)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if m.Type != wire.TypeChunk {
		t.Fatalf("type = 0x%02x, want CHUNK", m.Type)
	}
	return m
}

func expand(t *testing.T, m wire.Message) []byte {
	t.Helper()
	if m.Codec == wire.CodecRaw {
		return m.Payload
	}
	dec, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer dec.Close()
	out, err := dec.DecodeAll(m.Payload, nil)
	if err != nil {
		t.Fatalf("decompress: %v", err)
	}
	return out
}

// TestEncodeChunkRoundTrips pins the codec contract in both directions:
// compressible payloads ship smaller under CodecZstd, incompressible ones ship
// verbatim under CodecRaw rather than paying to inflate, and either way the
// bytes a receiver recovers are exactly the bytes that went in.
func TestEncodeChunkRoundTrips(t *testing.T) {
	enc, err := newEncoder()
	if err != nil {
		t.Fatal(err)
	}
	defer enc.Close()

	compressible := bytes.Repeat([]byte("ledger-close-meta-"), 4096)
	incompressible := make([]byte, 64<<10)
	rng := rand.New(rand.NewSource(7))
	rng.Read(incompressible)

	for _, tc := range []struct {
		name      string
		raw       []byte
		wantCodec byte
	}{
		{"compressible", compressible, wire.CodecZstd},
		{"incompressible", incompressible, wire.CodecRaw},
		{"empty", nil, wire.CodecRaw},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := decodeOne(t, encodeChunk(enc, nil, 42, 3, tc.raw))
			if m.Seq != 42 || m.ChunkIndex != 3 {
				t.Fatalf("framing lost: seq %d idx %d", m.Seq, m.ChunkIndex)
			}
			if m.Codec != tc.wantCodec {
				t.Fatalf("codec = 0x%02x, want 0x%02x", m.Codec, tc.wantCodec)
			}
			if got := expand(t, m); !bytes.Equal(got, tc.raw) {
				t.Fatalf("payload round-trip differs (%d bytes vs %d)", len(got), len(tc.raw))
			}
			if tc.wantCodec == wire.CodecZstd && len(m.Payload) >= len(tc.raw) {
				t.Fatalf("compressed payload %d not smaller than raw %d", len(m.Payload), len(tc.raw))
			}
		})
	}

	// A nil encoder is how the server ships an uncompressed flow.
	m := decodeOne(t, encodeChunk(nil, nil, 1, 0, compressible))
	if m.Codec != wire.CodecRaw {
		t.Fatalf("codec with no encoder = 0x%02x, want raw", m.Codec)
	}
}

// TestChunkPipelinePublishesInOrder pins the ordering contract the client
// depends on: chunks compress concurrently but must reach the broadcaster in
// submission order, or the receiver rejects the flow on the chunk index.
func TestChunkPipelinePublishesInOrder(t *testing.T) {
	const chunks = 64
	var got [][]byte
	p, err := newChunkPipeline(4, func(msg []byte) {
		got = append(got, append([]byte(nil), msg...))
	})
	if err != nil {
		t.Fatal(err)
	}
	// Payloads of wildly different compressibility make the workers finish out
	// of order if anything but submission order governs publication.
	raws := make([][]byte, chunks)
	rng := rand.New(rand.NewSource(11))
	for i := range raws {
		if i%2 == 0 {
			raws[i] = bytes.Repeat([]byte{byte(i)}, 32<<10)
		} else {
			raws[i] = make([]byte, 32<<10)
			rng.Read(raws[i])
		}
		p.submit(9, uint32(i), raws[i])
	}
	p.flush()
	p.close()
	p.close() // idempotent: a second close must not panic

	if len(got) != chunks {
		t.Fatalf("published %d chunks, want %d", len(got), chunks)
	}
	for i, msg := range got {
		m := decodeOne(t, msg)
		if m.ChunkIndex != uint32(i) {
			t.Fatalf("published chunk %d carries index %d — order broken", i, m.ChunkIndex)
		}
		if payload := expand(t, m); !bytes.Equal(payload, raws[i]) {
			t.Fatalf("chunk %d payload differs after round-trip", i)
		}
	}
}

// TestRunWithCompressionSurvivesEarlyReturn is the test whose absence let a
// process-crashing bug through: nothing exercised Run with compression on, so
// a pipeline leaked on every early return, its publisher outlived the
// broadcaster's finish, and the next publish double-closed the notify channel
// — panicking the daemon on an ordinary SIGINT.
func TestRunWithCompressionSurvivesEarlyReturn(t *testing.T) {
	ring, err := store.Open(filepath.Join(t.TempDir(), "ledgers"), 100)
	if err != nil {
		t.Fatal(err)
	}
	// A source that fails partway: the failure path is the one that skipped
	// the pipeline's cleanup.
	srv, err := New(Config{
		Source:   failingSource{err: errors.New("source died mid-flow")},
		Range:    ledgerbackend.UnboundedRange(1),
		Store:    ring,
		Compress: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	before := runtime.NumGoroutine()
	if err := srv.Run(context.Background()); err == nil {
		t.Fatal("want the source failure surfaced")
	}
	// Publishing after Run returned is what panicked; give any leaked
	// publisher a chance to do it.
	time.Sleep(50 * time.Millisecond)
	if leaked := runtime.NumGoroutine() - before; leaked > 0 {
		t.Fatalf("%d goroutines outlived Run — the pipeline was not closed", leaked)
	}
}
