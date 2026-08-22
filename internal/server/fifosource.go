package server

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// FifoSource reads framed LedgerCloseMeta from an existing FIFO rather than
// spawning the producer itself. It exists for one measurement: comparing a
// local consumer against a remote one PER LEDGER.
//
// Separate runs cannot answer that. Ledger N of one run is not the same event
// as ledger N of another, so their arrival times cannot be subtracted — only
// their distributions compared, which says nothing about the spread of the
// difference. Pointing a patched core's mirror fd at this FIFO puts both
// consumers on the same ledgers of the same run, on one clock, so the delta
// is a real per-ledger quantity.
//
// Opening blocks until the writer arrives, which is the handshake: start this
// reader before core.
func FifoSource(path string, pipeBytes int) EmittingStream {
	return &fifoStream{path: path, pipeBytes: pipeBytes}
}

type fifoStream struct {
	path      string
	pipeBytes int
}

func (f *fifoStream) Emissions(ctx context.Context, _ ledgerbackend.Range) iter.Seq2[Emission, error] {
	return func(yield func(Emission, error) bool) {
		r, err := os.OpenFile(f.path, os.O_RDONLY, 0)
		if err != nil {
			yield(Emission{}, fmt.Errorf("fifo source: open %q: %w", f.path, err))
			return
		}
		defer r.Close()
		// A FIFO is a pipe, so the same capacity argument applies: the local
		// consumer should be measured at its own best setting, not at the
		// default it happens to inherit.
		want := f.pipeBytes
		if want == 0 {
			want = DefaultPipeBytes
		}
		if _, err := setPipeSize(r, want); err != nil {
			_ = err // best effort
		}
		// Same unblock-on-cancel contract as the pipe source: a FIFO read
		// parks indefinitely when the writer goes quiet, and the deadline is
		// what lets a cancelled context end it.
		stop := context.AfterFunc(ctx, func() { _ = r.SetReadDeadline(time.Now()) })
		defer stop()

		br := bufio.NewReaderSize(r, 1<<20)
		prefix := make([]byte, seqPrefixLen)
		for {
			length, err := xdr.ReadFrameLength(br)
			if err != nil {
				if errors.Is(err, io.EOF) || ctx.Err() != nil {
					return // the writer finished, or we were asked to stop
				}
				yield(Emission{}, fmt.Errorf("fifo source: read frame marker: %w", err))
				return
			}
			size := int64(length)
			n := min(size, seqPrefixLen)
			if _, err := io.ReadFull(br, prefix[:n]); err != nil {
				if ctx.Err() != nil {
					return
				}
				yield(Emission{}, fmt.Errorf("fifo source: read frame prefix: %w", err))
				return
			}
			seq, err := xdr.LedgerCloseMetaView(prefix[:n]).LedgerSequence()
			if err != nil {
				yield(Emission{}, fmt.Errorf("fifo source: read ledger seq: %w", err))
				return
			}
			body := io.MultiReader(bytes.NewReader(prefix[:n]),
				&frameTail{ctx: ctx, r: br, remaining: size - n})
			if !yield(Emission{Seq: seq, Size: size, Body: body}, nil) {
				return
			}
		}
	}
}
