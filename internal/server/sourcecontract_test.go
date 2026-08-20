package server

import (
	"context"
	"errors"
	"iter"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"

	"github.com/stellar-experimental/remote-core-poc/internal/store"
)

// scriptedSource emits the given sequences, each with a tiny body, then ends.
type scriptedSource struct{ seqs []uint32 }

func (s scriptedSource) Emissions(context.Context, ledgerbackend.Range) iter.Seq2[Emission, error] {
	return func(yield func(Emission, error) bool) {
		for _, seq := range s.seqs {
			body := strings.NewReader("ledger-body")
			if !yield(Emission{Seq: seq, Size: int64(body.Len()), Body: body}, nil) {
				return
			}
		}
	}
}

func runWithSource(t *testing.T, src EmittingStream) error {
	t.Helper()
	ring, err := store.Open(filepath.Join(t.TempDir(), "ledgers"), 100)
	if err != nil {
		t.Fatal(err)
	}
	srv, err := New(Config{Source: src, Range: ledgerbackend.UnboundedRange(1), Store: ring})
	if err != nil {
		t.Fatal(err)
	}
	return srv.Run(context.Background())
}

// TestRunRefusesBrokenSequenceChain pins the ring's protection: a source that
// skips or rewinds must fail the run loudly. The retention ring's own answer
// to a break in the chain is to unlink everything it holds, so a silent
// acceptance here would discard retained history a subscriber may be mid-replay
// of.
func TestRunRefusesBrokenSequenceChain(t *testing.T) {
	for _, tc := range []struct {
		name string
		seqs []uint32
	}{
		{"rewind", []uint32{1, 2, 3, 2}},
		{"gap", []uint32{1, 2, 5}},
		{"repeat", []uint32{1, 2, 2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := runWithSource(t, scriptedSource{seqs: tc.seqs})
			if err == nil {
				t.Fatal("want a loud failure on a broken sequence chain")
			}
			if !strings.Contains(err.Error(), "dense and ascending") {
				t.Fatalf("error does not name the broken invariant: %v", err)
			}
		})
	}
	if err := runWithSource(t, scriptedSource{seqs: []uint32{7, 8, 9}}); err != nil {
		t.Fatalf("a dense ascending chain must run clean, got %v", err)
	}
}

// failingSource yields one good ledger, then fails.
type failingSource struct{ err error }

func (f failingSource) Emissions(context.Context, ledgerbackend.Range) iter.Seq2[Emission, error] {
	return func(yield func(Emission, error) bool) {
		body := strings.NewReader("first")
		if !yield(Emission{Seq: 1, Size: int64(body.Len()), Body: body}, nil) {
			return
		}
		yield(Emission{Seq: 2}, f.err)
	}
}

// TestFinishCarriesSourceFailure pins that a failed source is distinguishable
// from a finished one: an unbounded consumer that cannot tell them apart ends
// its iteration with a nil error and silently stops ingesting.
func TestFinishCarriesSourceFailure(t *testing.T) {
	boom := errors.New("disk on fire")
	err := runWithSource(t, failingSource{err: boom})
	if !errors.Is(err, boom) {
		t.Fatalf("Run error = %v, want it to wrap the source failure", err)
	}

	b := newBroadcaster()
	b.finish(boom)
	if got := b.failure(); !errors.Is(got, boom) {
		t.Fatalf("broadcaster.failure() = %v, want the source failure", got)
	}
	clean := newBroadcaster()
	clean.finish(nil)
	if got := clean.failure(); got != nil {
		t.Fatalf("a clean finish must report no failure, got %v", got)
	}
}
