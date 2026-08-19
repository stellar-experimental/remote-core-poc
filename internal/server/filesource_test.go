package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
)

// writeDump lays out one ledger-<seq>.xdr per payload, numbered from first.
func writeDump(t *testing.T, first uint32, payloads ...[]byte) string {
	t.Helper()
	dir := t.TempDir()
	for i, p := range payloads {
		name := filepath.Join(dir, fmt.Sprintf("ledger-%d.xdr", first+uint32(i)))
		if err := os.WriteFile(name, p, 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	return dir
}

func collectRaw(t *testing.T, src ledgerbackend.LedgerStream, rng ledgerbackend.Range, limit int) [][]byte {
	t.Helper()
	var out [][]byte
	for raw, err := range src.RawLedgers(t.Context(), rng) {
		if err != nil {
			t.Fatalf("stream error after %d ledgers: %v", len(out), err)
		}
		out = append(out, bytes.Clone(raw))
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out
}

func TestFileStreamReplaysTheDumpInOrder(t *testing.T) {
	dir := writeDump(t, 900, []byte("first"), []byte("second"), []byte("third"))
	src, err := NewFileStream(dir)
	if err != nil {
		t.Fatalf("NewFileStream: %v", err)
	}
	if src.Ledgers() != 3 {
		t.Fatalf("Ledgers() = %d, want 3", src.Ledgers())
	}

	got := collectRaw(t, src, ledgerbackend.BoundedRange(1, 3), 0)
	want := [][]byte{[]byte("first"), []byte("second"), []byte("third")}
	if len(got) != len(want) {
		t.Fatalf("received %d ledgers, want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Errorf("ledger %d = %q, want %q: the dump's own numbering must only order records", i+1, got[i], want[i])
		}
	}
}

func TestFileStreamCyclesWhenTheRangeOutrunsTheDump(t *testing.T) {
	dir := writeDump(t, 1, []byte("a"), []byte("b"))
	src, err := NewFileStream(dir)
	if err != nil {
		t.Fatalf("NewFileStream: %v", err)
	}

	got := collectRaw(t, src, ledgerbackend.BoundedRange(10, 14), 0)
	want := []string{"a", "b", "a", "b", "a"}
	if len(got) != len(want) {
		t.Fatalf("received %d ledgers, want %d", len(got), len(want))
	}
	for i := range want {
		if string(got[i]) != want[i] {
			t.Errorf("ledger %d = %q, want %q", 10+i, got[i], want[i])
		}
	}
}

func TestFileStreamUnboundedIsEndless(t *testing.T) {
	dir := writeDump(t, 5, []byte("x"))
	src, err := NewFileStream(dir)
	if err != nil {
		t.Fatalf("NewFileStream: %v", err)
	}
	// Far more pulls than records: only the consumer ends an unbounded replay.
	got := collectRaw(t, src, ledgerbackend.UnboundedRange(1), 10)
	if len(got) != 10 {
		t.Fatalf("received %d ledgers, want 10", len(got))
	}
}

func TestFileStreamHonoursCancellation(t *testing.T) {
	dir := writeDump(t, 1, []byte("x"))
	src, err := NewFileStream(dir)
	if err != nil {
		t.Fatalf("NewFileStream: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	count := 0
	var got error
	for _, err := range src.RawLedgers(ctx, ledgerbackend.UnboundedRange(1)) {
		if err != nil {
			got = err
			break
		}
		count++
		cancel()
	}
	if count != 1 || !errors.Is(got, context.Canceled) {
		t.Fatalf("got %d ledgers and error %v, want 1 then context.Canceled", count, got)
	}
}

func TestNewFileStreamRejectsBadDumps(t *testing.T) {
	t.Run("empty directory", func(t *testing.T) {
		if _, err := NewFileStream(t.TempDir()); err == nil {
			t.Error("an empty dump directory was accepted")
		}
	})
	t.Run("missing directory", func(t *testing.T) {
		if _, err := NewFileStream(filepath.Join(t.TempDir(), "nope")); err == nil {
			t.Error("a missing directory was accepted")
		}
	})
	t.Run("gap in the dump", func(t *testing.T) {
		dir := writeDump(t, 1, []byte("a"), []byte("b"))
		if err := os.Rename(filepath.Join(dir, "ledger-2.xdr"), filepath.Join(dir, "ledger-4.xdr")); err != nil {
			t.Fatal(err)
		}
		_, err := NewFileStream(dir)
		if err == nil {
			t.Fatal("a dump with a gap was accepted")
		}
	})
	t.Run("unrelated files are ignored", func(t *testing.T) {
		dir := writeDump(t, 1, []byte("a"))
		if err := os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("hi"), 0o644); err != nil {
			t.Fatal(err)
		}
		src, err := NewFileStream(dir)
		if err != nil {
			t.Fatalf("NewFileStream: %v", err)
		}
		if src.Ledgers() != 1 {
			t.Errorf("Ledgers() = %d, want the 1 real record", src.Ledgers())
		}
	})
}
