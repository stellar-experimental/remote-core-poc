package server

import (
	"context"
	"fmt"
	"io"
	"iter"
	"os"
	"path/filepath"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"

	"github.com/stellar-experimental/remote-core-poc/internal/store"
)

// FileStream replays a dump directory of raw ledgers: one ledger-<seq>.xdr
// file per ledger, the exact layout corestreamd's own retention ring writes
// (the naming and contiguity rules live in the store package, so the two
// cannot drift). Capturing a dump is therefore just pointing --file-dir at a
// previous run's <data-dir>/ledgers.
//
// The dump's own sequence numbers only order the records; emitted ledgers are
// renumbered from the requested range's start, and an unbounded range cycles
// the dump forever. That is what lets a fixed dump — say a thousand sac-6000
// stress ledgers — stand in for an endless source in a long measurement run,
// and it is harmless because nothing in this prototype decodes a ledger body.
//
// FileStream itself never sleeps: pace it with PacedSource, which owns the
// emission window and the ledger cadence.
type FileStream struct {
	files []string // one path per record, in dump order
}

var _ ledgerbackend.LedgerStream = (*FileStream)(nil)

// NewFileStream scans dir for ledger-<seq>.xdr files. Their sequences must
// form a contiguous range — a gap means an incomplete dump, and replaying
// around one would silently misrepresent the recorded workload.
func NewFileStream(dir string) (*FileStream, error) {
	seqs, err := store.ScanLedgerDir(dir)
	if err != nil {
		return nil, fmt.Errorf("file source: %w", err)
	}
	if len(seqs) == 0 {
		return nil, fmt.Errorf("file source: %q holds no ledger-<seq>.xdr files", dir)
	}
	files := make([]string, len(seqs))
	for i, seq := range seqs {
		files[i] = filepath.Join(dir, store.LedgerFileName(seq))
	}
	return &FileStream{files: files}, nil
}

// Ledgers is how many records the dump holds.
func (f *FileStream) Ledgers() int { return len(f.files) }

// RawLedgers yields the dump's records renumbered over ledgerRange, cycling
// the dump when the range outruns it. The yielded slice is BORROWED — it is
// one buffer reused across records, so a long replay does not churn a
// ledger-sized allocation (and its GC pressure) into every cadence tick.
func (f *FileStream) RawLedgers(
	ctx context.Context, ledgerRange ledgerbackend.Range, _ ...ledgerbackend.StreamOption,
) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		var buf []byte
		record := 0
		for seq := ledgerRange.From(); ; seq++ {
			if err := ctx.Err(); err != nil {
				yield(nil, err)
				return
			}
			raw, err := readInto(buf, f.files[record])
			if err != nil {
				yield(nil, fmt.Errorf("file source: ledger %d: %w", seq, err))
				return
			}
			buf = raw
			if !yield(raw, nil) {
				return
			}
			if ledgerRange.Bounded() && seq == ledgerRange.To() {
				return
			}
			if record = record + 1; record == len(f.files) {
				record = 0
			}
		}
	}
}

// readInto reads path's whole contents into buf, growing it only when a
// record is larger than any read before.
func readInto(buf []byte, path string) ([]byte, error) {
	fh, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer fh.Close()
	info, err := fh.Stat()
	if err != nil {
		return nil, err
	}
	size := int(info.Size())
	if cap(buf) < size {
		buf = make([]byte, size)
	} else {
		buf = buf[:size]
	}
	if _, err := io.ReadFull(fh, buf); err != nil {
		return nil, err
	}
	return buf, nil
}
