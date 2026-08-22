package server

import (
	"io"

	"github.com/cespare/xxhash/v2"
)

// Work the source loop used to do inline, moved off it.
//
// The loop that drains core's pipe sets the emission window, and everything it
// does between reads is time core spends blocked on a full pipe. Measured
// against a bare reader on the same pipe, the relay stretched that window from
// 4.73 ms to 7.85 ms per stress ledger — so what runs here is not bookkeeping,
// it is the schedule.

// fillChunk reads until p is full, the source ends, or it fails. One Read
// returns at most what the pipe holds, so framing whatever a single Read
// returned made the CHUNK size a function of the PIPE size: against the 64 KiB
// default a 14.48 MiB ledger shipped as ~234 chunks of 64 KiB rather than ~58
// of 256 KiB, quadrupling every per-chunk cost on both ends. Topping the chunk
// up decouples the two, so the pipe can stay small enough to stay cache-warm
// while chunks stay the size the wire wants.
func fillChunk(r io.Reader, p []byte) (int, error) {
	n := 0
	for n < len(p) {
		m, err := r.Read(p[n:])
		n += m
		if err != nil {
			return n, err
		}
		if m == 0 {
			// A reader returning (0, nil) forever would spin here.
			return n, io.ErrNoProgress
		}
	}
	return n, nil
}

// ledgerHasher runs the ledger checksum on its own goroutine. Hashing 14.48
// MiB costs ~1.4 ms, and inline that lands between two pipe reads; here it
// overlaps them. One goroutine serves the whole run, because a per-ledger one
// would churn a goroutine every 600 ms for work measured in microseconds.
//
// Chunks are hashed in submission order, which is the order the bytes must be
// checksummed in, and sum fences against the caller reusing those bytes: it
// does not return until every chunk handed over has been consumed.
type ledgerHasher struct {
	in   chan []byte
	sums chan uint64
}

func newLedgerHasher() *ledgerHasher {
	h := &ledgerHasher{in: make(chan []byte, 256), sums: make(chan uint64)}
	go func() {
		digest := xxhash.New()
		for data := range h.in {
			if data == nil { // end of a ledger: report and start the next
				h.sums <- digest.Sum64()
				digest.Reset()
				continue
			}
			_, _ = digest.Write(data) // xxhash's Write cannot fail
		}
	}()
	return h
}

// write queues one chunk's bytes. They must stay unmodified until sum returns.
func (h *ledgerHasher) write(data []byte) { h.in <- data }

// sum finishes the current ledger and returns its checksum, having consumed
// every chunk written so far.
func (h *ledgerHasher) sum() uint64 {
	h.in <- nil
	return <-h.sums
}

func (h *ledgerHasher) close() { close(h.in) }
