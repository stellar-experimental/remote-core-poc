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
			FillSyntheticPayload(buf, seq)
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
	FillSyntheticPayload(buf, seq)
	return buf
}

// FillSyntheticPayload writes ledger seq's payload into buf. The first four
// bytes carry the sequence; the rest is a stream seeded by it.
//
// The payload is PCG output, so it is INCOMPRESSIBLE by construction. That
// makes this source a negative control for chunk compression — every chunk
// falls back to CodecRaw — and useless for measuring compression's benefit.
// Measure that against real meta: the pipe source running stellar-core, or
// the file source replaying a captured dump.
func FillSyntheticPayload(buf []byte, seq uint32) {
	if len(buf) >= 4 {
		binary.BigEndian.PutUint32(buf[:4], seq)
	}
	// A per-sequence PCG keeps the payload reproducible from the sequence alone,
	// independent of platform and of how many ledgers came before.
	r := rand.New(rand.NewPCG(uint64(seq), syntheticSeed))
	for i := 4; i < len(buf); i += 8 {
		var word [8]byte
		binary.LittleEndian.PutUint64(word[:], r.Uint64())
		copy(buf[i:], word[:])
	}
}

const syntheticSeed = 0x5CE11A8

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
