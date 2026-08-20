package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"

	"github.com/klauspost/compress/zstd"

	"github.com/stellar-experimental/remote-core-poc/internal/store"
	"github.com/stellar-experimental/remote-core-poc/internal/wire"
	"github.com/stellar-experimental/remote-core-poc/remoteledger"
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
	enc, err := newEncoder(4)
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
	penc, err := newEncoder(6)
	if err != nil {
		t.Fatal(err)
	}
	defer penc.Close()
	var fallbacks atomic.Uint64
	p := newChunkPipeline(penc, 4, 256<<10, &fallbacks, func(msg []byte) {
		got = append(got, append([]byte(nil), msg...))
	})
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
		// submit takes the source's framing buffer: a header gap in front of
		// the payload, which is what lets the raw fallback frame in place.
		msg := make([]byte, wire.ChunkHeaderSize+len(raws[i]))
		copy(msg[wire.ChunkHeaderSize:], raws[i])
		p.submit(9, uint32(i), msg)
	}
	// flush must publish everything by itself: asserting after close would
	// pass even with flush gutted to a no-op, since close drains too.
	p.flush()
	if len(got) != chunks {
		t.Fatalf("flush published %d chunks, want %d", len(got), chunks)
	}
	p.close()
	p.close() // idempotent: a second close must not panic

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
	// A source that fails partway: the failure path is the one that skipped
	// the pipeline's cleanup.
	srv, err := New(Config{
		Source:   failingSource{err: errors.New("source died mid-flow")},
		Range:    ledgerbackend.UnboundedRange(1),
		Store:    mustStore(t),
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

// TestCompressedFlowRoundTripsThroughClient closes the gap two review rounds
// named: nothing ran a real client against a real COMPRESSING server, so the
// seam between "the pipeline publishes in order" and "the client expands
// frames" was never exercised, nor was writeRingFlow's use of the shared
// encoder — the entire reason newEncoder reserves concurrency for replays.
// The synthetic source cannot serve here: its payloads are PCG output and
// never shrink, so a compressed harness over it would be a negative control.
func TestCompressedFlowRoundTripsThroughClient(t *testing.T) {
	// Compressible, distinguishable, and larger than one chunk so a ledger
	// spans several frames.
	payloads := make([][]byte, 6)
	for i := range payloads {
		payloads[i] = bytes.Repeat(fmt.Appendf(nil, "ledger-%d-meta-", i), 3000)
	}
	dump := writeDump(t, 1, payloads...)

	h := startHarness(t, harnessOpts{
		start: 1, count: uint32(len(payloads)),
		dumpDir: dump, compress: true, chunkSize: wire.MinChunkSize,
	})

	// Live delivery.
	var got [][]byte
	for raw, err := range remoteledger.New(h.url).RawLedgers(
		t.Context(), ledgerbackend.BoundedRange(1, uint32(len(payloads)))) {
		if err != nil {
			t.Fatalf("live stream: %v", err)
		}
		got = append(got, bytes.Clone(raw))
	}
	if len(got) != len(payloads) {
		t.Fatalf("received %d ledgers, want %d", len(got), len(payloads))
	}
	for i := range payloads {
		if !bytes.Equal(got[i], payloads[i]) {
			t.Fatalf("ledger %d differs after compress/expand round trip", i+1)
		}
	}

	// Replay the same range from the retention ring, which compresses through
	// the shared encoder on the serving goroutine rather than the pipeline.
	var replayed [][]byte
	for raw, err := range remoteledger.New(h.url).RawLedgers(
		t.Context(), ledgerbackend.BoundedRange(1, uint32(len(payloads)))) {
		if err != nil {
			t.Fatalf("ring replay: %v", err)
		}
		replayed = append(replayed, bytes.Clone(raw))
	}
	for i := range payloads {
		if !bytes.Equal(replayed[i], payloads[i]) {
			t.Fatalf("ring-replayed ledger %d differs", i+1)
		}
	}
}

// TestRingReplaysDoNotStarveLivePath pins the invariant the shared-encoder
// simplification broke: "nothing a subscriber does can slow the source loop".
// zstd.Encoder is a fixed pool of encoder states and EncodeAll BLOCKS when
// they are all borrowed, so a ring replay drawing from the live pool made
// live chunks fall back to raw — 13% of chunks at 8 catch-up subscribers,
// 37% at 16. Separate encoders is what keeps that at zero.
func TestRingReplaysDoNotStarveLivePath(t *testing.T) {
	srv, err := New(Config{
		Source:   scriptedSource{seqs: []uint32{1}},
		Range:    ledgerbackend.UnboundedRange(1),
		Store:    mustStore(t),
		Compress: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if srv.enc == srv.ringEnc {
		t.Fatal("live and ring replays share one encoder pool — a catch-up subscriber can throttle the source")
	}
	if srv.ringEnc == nil {
		t.Fatal("compressing server has no ring encoder")
	}
}

func mustStore(t *testing.T) *store.Store {
	t.Helper()
	ring, err := store.Open(filepath.Join(t.TempDir(), "ledgers"), 100)
	if err != nil {
		t.Fatal(err)
	}
	return ring
}
