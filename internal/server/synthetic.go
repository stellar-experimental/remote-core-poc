package server

import (
	"context"
	"encoding/binary"
	"iter"
	"math/rand/v2"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
)

// SyntheticStream is a LedgerStream that fabricates ledgers, so the server, the
// client and the benchmark all run with no stellar-core binary and no network.
//
// Its payloads are NOT valid LedgerCloseMeta XDR. Nothing in this prototype
// decodes a ledger body — the seam carries opaque bytes end to end — so a
// deterministic blob is enough, and being deterministic is what lets a consumer
// verify integrity by regenerating the bytes for a sequence.
//
// Like FileStream, it never sleeps: ledger close time is PacedSource's
// cadence, so there is exactly one owner of the schedule.
type SyntheticStream struct {
	cfg SyntheticConfig
}

// SyntheticConfig tunes the fabricated stream.
type SyntheticConfig struct {
	// Size is the payload bytes per ledger. Zero means DefaultSyntheticSize.
	Size int

	// Compressible fabricates meta-SHAPED payloads — repetitive, like the
	// XDR they stand in for — instead of the default incompressible noise.
	// Real ledger meta compresses ~7.6x per chunk, so a benchmark of the
	// compressing path over the default payload measures only discarded
	// work; this is how the synthetic source reaches the cadence and ledger
	// counts a tail measurement needs while still exercising compression.
	Compressible bool
}

// DefaultSyntheticSize is the payload size of a fabricated ledger, in the range
// of a busy pubnet ledger.
const DefaultSyntheticSize = 200 << 10

// NewSyntheticStream returns a synthetic source tuned by cfg.
func NewSyntheticStream(cfg SyntheticConfig) *SyntheticStream {
	if cfg.Size <= 0 {
		cfg.Size = DefaultSyntheticSize
	}
	return &SyntheticStream{cfg: cfg}
}

var _ ledgerbackend.LedgerStream = (*SyntheticStream)(nil)

// RawLedgers yields fabricated ledgers over ledgerRange, as fast as the
// consumer pulls. Like every LedgerStream, the yielded slice is borrowed: this
// one reuses a single buffer, so a consumer that fails to copy sees its data
// change underneath it.
func (s *SyntheticStream) RawLedgers(
	ctx context.Context, ledgerRange ledgerbackend.Range, _ ...ledgerbackend.StreamOption,
) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		buf := make([]byte, s.cfg.Size)
		for seq := ledgerRange.From(); ; seq++ {
			if err := ctx.Err(); err != nil {
				yield(nil, err)
				return
			}
			FillSyntheticPayload(buf, seq, s.cfg.Compressible)
			if !yield(buf, nil) {
				return
			}
			if ledgerRange.Bounded() && seq == ledgerRange.To() {
				return
			}
		}
	}
}

// SyntheticPayload returns the fabricated payload of ledger seq at the given
// size. Regenerating it is how a consumer checks that what arrived over the
// network is what the source produced.
func SyntheticPayload(seq uint32, size int) []byte {
	buf := make([]byte, size)
	FillSyntheticPayload(buf, seq, false)
	return buf
}

// FillSyntheticPayload writes ledger seq's payload into buf. The first four
// bytes carry the sequence; the rest depends on the shape asked for. Both
// shapes are reproducible from the sequence alone — independent of platform
// and of how many ledgers came before — which is what lets a consumer verify
// what arrived by regenerating it.
//
// The default is PCG output, INCOMPRESSIBLE by construction: a negative
// control that proves the compressing path degrades to CodecRaw without
// expanding anything. compressible instead repeats a per-sequence block, the
// way real XDR meta repeats field layouts, account IDs and asset codes —
// which is what a benchmark of the compressing path needs, since real meta
// compresses ~7.6x per chunk and this default compresses not at all.
func FillSyntheticPayload(buf []byte, seq uint32, compressible bool) {
	if len(buf) >= 4 {
		binary.BigEndian.PutUint32(buf[:4], seq)
	}
	r := rand.New(rand.NewPCG(uint64(seq), syntheticSeed))
	if compressible {
		// Meta-shaped: fixed-size records that share a template but carry
		// their own entropy, the way real XDR repeats field layouts around
		// per-transaction hashes and IDs. The entropy fraction — not the
		// buffer length — sets the ratio, which is what keeps it near real
		// meta's ~7.6x at ANY chunk size. A single repeated block instead
		// compresses better the more of it you take (455x at a 256 KiB
		// chunk), which silently turns a wire measurement into a no-op.
		var template [recordSize]byte
		for i := 0; i < len(template); i += 8 {
			binary.LittleEndian.PutUint64(template[i:], r.Uint64())
		}
		for off := 4; off < len(buf); off += recordSize {
			n := copy(buf[off:], template[:])
			// The tail of each record is unique, so every record costs
			// literal bytes no matter how many records the window holds.
			for i := n - recordEntropy; i >= 0 && i+8 <= n; i += 8 {
				binary.LittleEndian.PutUint64(buf[off+i:], r.Uint64())
			}
		}
		return
	}
	for i := 4; i < len(buf); i += 8 {
		var word [8]byte
		binary.LittleEndian.PutUint64(word[:], r.Uint64())
		copy(buf[i:], word[:])
	}
}

const (
	syntheticSeed = 0x5CE11A8

	// recordSize and recordEntropy shape the compressible payload: a record
	// of recordSize bytes whose last recordEntropy bytes are fresh noise. The
	// ratio is bounded by recordSize/recordEntropy and measures ~7x, matching
	// real ledger meta closely enough for a wire benchmark.
	recordSize    = 256
	recordEntropy = 32
)

// CountedRange builds a start-plus-count range: count ledgers from start, or
// unbounded when count is zero. It is how the synthetic and file sources — the
// ones configured by a count flag rather than a final sequence — express their
// range.
func CountedRange(start uint32, count uint32) ledgerbackend.Range {
	if count == 0 {
		return ledgerbackend.UnboundedRange(start)
	}
	return ledgerbackend.BoundedRange(start, start+count-1)
}
