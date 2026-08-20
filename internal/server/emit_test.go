package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"iter"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
)

// drainEmissions consumes a source, returning each emission's payload read in
// pieces of readSize bytes, plus the wall-clock span of each body drain.
func drainEmissions(
	t *testing.T, src EmittingStream, rng ledgerbackend.Range, readSize int,
) (payloads [][]byte, spans []time.Duration) {
	t.Helper()
	buf := make([]byte, readSize)
	for em, err := range src.Emissions(t.Context(), rng) {
		if err != nil {
			t.Fatalf("emission %d: %v", em.Seq, err)
		}
		var payload []byte
		started := time.Now()
		for {
			n, rerr := em.Body.Read(buf)
			payload = append(payload, buf[:n]...)
			if rerr == io.EOF {
				break
			}
			if rerr != nil {
				t.Fatalf("read emission %d: %v", em.Seq, rerr)
			}
		}
		payloads = append(payloads, payload)
		spans = append(spans, time.Since(started))
		if em.Size != int64(len(payload)) {
			t.Errorf("emission %d declared %d bytes, drained %d", em.Seq, em.Size, len(payload))
		}
	}
	return payloads, spans
}

func TestPacedSourcePreservesBytes(t *testing.T) {
	src := PacedSource(NewSyntheticStream(SyntheticConfig{Size: 4096}), 0, 0)
	payloads, _ := drainEmissions(t, src, ledgerbackend.BoundedRange(3, 5), 100)
	if len(payloads) != 3 {
		t.Fatalf("received %d emissions, want 3", len(payloads))
	}
	for i, payload := range payloads {
		seq := uint32(3 + i)
		if !bytes.Equal(payload, SyntheticPayload(seq, 4096)) {
			t.Errorf("emission %d does not reassemble to the source ledger", seq)
		}
	}
}

func TestPacedBodySpreadsBytesOverTheWindow(t *testing.T) {
	// The point of the window: the body must NOT be readable all at once. The
	// assertion is one-sided — at least most of the window elapses — because
	// oversleep is unbounded on a loaded machine, but undersleep would mean
	// store-and-forward snuck back in.
	const window = 50 * time.Millisecond
	src := PacedSource(NewSyntheticStream(SyntheticConfig{Size: 64 << 10}), window, 0)
	_, spans := drainEmissions(t, src, ledgerbackend.BoundedRange(1, 2), 8<<10)
	for i, span := range spans {
		if span < window*8/10 {
			t.Errorf("emission %d drained in %s, want most of the %s window", i+1, span, window)
		}
	}
}

func TestPacedBodyZeroWindowIsImmediate(t *testing.T) {
	src := PacedSource(NewSyntheticStream(SyntheticConfig{Size: 64 << 10}), 0, 0)
	_, spans := drainEmissions(t, src, ledgerbackend.BoundedRange(1, 1), 8<<10)
	if spans[0] > 100*time.Millisecond {
		t.Errorf("an unpaced emission took %s to drain", spans[0])
	}
}

func TestPacedSourceCadenceSpacesEmissions(t *testing.T) {
	const cadence = 30 * time.Millisecond
	src := PacedSource(NewSyntheticStream(SyntheticConfig{Size: 512}), 0, cadence)
	started := time.Now()
	_, _ = drainEmissions(t, src, ledgerbackend.BoundedRange(1, 3), 512)
	// Three emissions on a 30ms grid: the first immediate, the last at 60ms.
	if elapsed := time.Since(started); elapsed < 2*cadence {
		t.Errorf("three emissions completed in %s, want at least %s of grid", elapsed, 2*cadence)
	}
}

// failingStream errors on its first pull, standing in for a source that dies.
type failingStream struct{ err error }

func (f failingStream) RawLedgers(
	context.Context, ledgerbackend.Range, ...ledgerbackend.StreamOption,
) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) { yield(nil, f.err) }
}

func TestPacedSourceForwardsSourceErrors(t *testing.T) {
	boom := errors.New("boom")
	src := PacedSource(failingStream{err: boom}, 0, 0)
	sawErr := false
	for _, err := range src.Emissions(t.Context(), ledgerbackend.BoundedRange(1, 5)) {
		if !errors.Is(err, boom) {
			t.Fatalf("error = %v, want the source's own failure", err)
		}
		sawErr = true
		break
	}
	if !sawErr {
		t.Fatal("the source's error never surfaced through the adapter")
	}
}

func TestPacedBodyCancelledMidWindow(t *testing.T) {
	// The flags accept emission windows of minutes; a SIGINT must interrupt a
	// pacing wait, not sit it out. Cancellation surfaces as the body's read
	// error, which is how Run notices it mid-emission.
	src := PacedSource(NewSyntheticStream(SyntheticConfig{Size: 64 << 10}), time.Hour, 0)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	start := time.Now()
	var got error
	for em, err := range src.Emissions(ctx, ledgerbackend.BoundedRange(1, 1)) {
		if err != nil {
			t.Fatalf("emission: %v", err)
		}
		go func() {
			time.Sleep(50 * time.Millisecond)
			cancel()
		}()
		buf := make([]byte, 8<<10)
		for {
			if _, rerr := em.Body.Read(buf); rerr != nil {
				got = rerr
				break
			}
		}
	}
	if !errors.Is(got, context.Canceled) {
		t.Fatalf("read error = %v, want context.Canceled", got)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("cancellation took %s to interrupt an hour-long window", elapsed)
	}
}

func TestPacedSourceCancelledMidCadenceWait(t *testing.T) {
	src := PacedSource(NewSyntheticStream(SyntheticConfig{Size: 512}), 0, time.Hour)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	count := 0
	var got error
	for em, err := range src.Emissions(ctx, ledgerbackend.BoundedRange(1, 5)) {
		if err != nil {
			got = err
			break
		}
		if _, rerr := io.ReadAll(em.Body); rerr != nil {
			t.Fatalf("drain emission %d: %v", em.Seq, rerr)
		}
		count++
		cancel() // the next emission waits an hour on the grid; cancel must end it
	}
	if count != 1 {
		t.Fatalf("received %d emissions, want the 1 before the hour-long wait", count)
	}
	if !errors.Is(got, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", got)
	}
}
