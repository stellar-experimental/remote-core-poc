package server

import (
	"context"
	"io"
	"iter"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
)

// Emission is one ledger arriving as an incremental byte flow. This is the
// source seam: the server reads Body a chunk at a time and forwards each chunk
// the moment it exists, so transfer overlaps with the source's own emission
// instead of waiting for the complete ledger the way a LedgerStream forces.
type Emission struct {
	// Seq is the ledger this flow carries.
	Seq uint32

	// Size is the total byte count when the source knows it ahead of time, or
	// zero when only draining Body reveals it (a real core pipe would not know).
	Size int64

	// Body yields the ledger's raw bytes as the source produces them; a Read
	// blocks while the source is still emitting. It must be drained to io.EOF
	// before the next emission is pulled: the bytes it reads from are borrowed
	// from the underlying source and the next pull overwrites them.
	Body io.Reader
}

// EmittingStream is a source of ledgers as incremental emissions, in sequence
// order with no gap. It is consumed exactly once, by Server.Run.
type EmittingStream interface {
	Emissions(ctx context.Context, rng ledgerbackend.Range) iter.Seq2[Emission, error]
}

// PacedSource adapts a complete-ledger LedgerStream into an EmittingStream.
//
// window paces each ledger's bytes over that duration, simulating a source
// that emits over a window the way captive core writes its meta pipe — the
// mechanism chunked streaming exists to overlap with. Zero releases each
// ledger's bytes all at once (right for captive core itself: the SDK seam
// only surfaces complete metas, so their production time is already spent).
//
// cadence schedules emission starts that far apart on an absolute grid, so the
// interval does not drift with per-ledger work. Zero starts each emission as
// soon as the source yields, letting the source pace itself. The first
// emission always starts immediately.
func PacedSource(source ledgerbackend.LedgerStream, window, cadence time.Duration) EmittingStream {
	return &pacedStream{source: source, window: window, cadence: cadence}
}

type pacedStream struct {
	source  ledgerbackend.LedgerStream
	window  time.Duration
	cadence time.Duration
}

func (p *pacedStream) Emissions(ctx context.Context, rng ledgerbackend.Range) iter.Seq2[Emission, error] {
	return func(yield func(Emission, error) bool) {
		// Like Server.Run over a LedgerStream, sequences are counted from the
		// range: a LedgerStream yields exactly the requested range in order.
		seq := rng.From()
		var nextStart time.Time
		body := &pacedBody{}
		for raw, err := range p.source.RawLedgers(ctx, rng) {
			if err != nil {
				yield(Emission{Seq: seq}, err)
				return
			}
			if p.cadence > 0 {
				if nextStart.IsZero() {
					nextStart = time.Now()
				}
				if err := sleepUntil(ctx, nextStart); err != nil {
					yield(Emission{Seq: seq}, err)
					return
				}
				nextStart = nextStart.Add(p.cadence)
				// A source or subscriber hiccup that overran the grid must not
				// be repaid as a burst of back-to-back ledgers: re-anchor.
				if now := time.Now(); nextStart.Before(now) {
					nextStart = now
				}
			}
			body.reset(ctx, raw, p.window)
			if !yield(Emission{Seq: seq, Size: int64(len(raw)), Body: body}, nil) {
				return
			}
			seq++
		}
	}
}

// sleepUntil sleeps to the deadline, ending early with the context's error if
// it is cancelled first. A deadline already past returns immediately.
func sleepUntil(ctx context.Context, deadline time.Time) error {
	wait := time.Until(deadline)
	if wait <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// pacedBody releases a payload's bytes linearly over an emission window: byte
// k becomes due at window*k/len(payload) after the first Read. A Read blocks
// until everything it asks for is due, then returns it in full — so a server
// reading chunk-size pieces sees one chunk per chunk-time, which is exactly
// the profile the window is standing in for. A zero window releases everything
// immediately. Cancelling the context ends a pacing wait early with the
// context's error — the flags accept windows of minutes, and a SIGINT must
// not wait one out.
//
// The struct is reused across ledgers via reset, so a drained body allocates
// nothing per ledger.
type pacedBody struct {
	ctx     context.Context
	payload []byte
	window  time.Duration
	off     int
	started time.Time
}

func (b *pacedBody) reset(ctx context.Context, payload []byte, window time.Duration) {
	b.ctx, b.payload, b.window, b.off, b.started = ctx, payload, window, 0, time.Time{}
}

func (b *pacedBody) Read(p []byte) (int, error) {
	if b.off >= len(b.payload) {
		return 0, io.EOF
	}
	if b.started.IsZero() {
		b.started = time.Now()
	}
	n := min(len(p), len(b.payload)-b.off)
	if b.window > 0 && n > 0 {
		// The last byte of this piece is due at window * (off+n)/total, which
		// puts the payload's final byte due at exactly the window's close.
		// Float arithmetic on purpose: the integer product overflows int64
		// for large window×payload combinations (~40s × 256MiB), which would
		// silently unpace the tail; float64's 53-bit mantissa is exact far
		// beyond any plausible window.
		frac := float64(b.off+n) / float64(len(b.payload))
		due := b.started.Add(time.Duration(float64(b.window) * frac))
		if err := sleepUntil(b.ctx, due); err != nil {
			return 0, err
		}
	}
	copy(p, b.payload[b.off:b.off+n])
	b.off += n
	return n, nil
}
