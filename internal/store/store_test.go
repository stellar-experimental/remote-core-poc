package store

import (
	"bytes"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func ledgerBytes(seq uint32) []byte {
	return bytes.Repeat([]byte{byte(seq)}, 32)
}

// putN stores ledgers from..to inclusive. It breaks after to instead of testing
// seq <= to so that a range ending at math.MaxUint32 terminates.
func putN(t *testing.T, s *Store, from, to uint32) {
	t.Helper()
	for seq := from; ; seq++ {
		if _, err := s.Put(seq, ledgerBytes(seq)); err != nil {
			t.Fatalf("Put(%d): %v", seq, err)
		}
		if seq == to {
			return
		}
	}
}

func wantBounds(t *testing.T, s *Store, oldest, latest uint32) {
	t.Helper()
	gotOldest, gotLatest, filled := s.Bounds()
	if !filled || gotOldest != oldest || gotLatest != latest {
		t.Fatalf("Bounds() = (%d, %d, %v), want (%d, %d, true)", gotOldest, gotLatest, filled, oldest, latest)
	}
}

func TestOpenRejectsNonPositiveRetention(t *testing.T) {
	if _, err := Open(t.TempDir(), 0); err == nil {
		t.Fatal("Open with retention 0 succeeded, want error")
	}
}

func TestPutGet(t *testing.T) {
	s, err := Open(t.TempDir(), 100)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, _, filled := s.Bounds(); filled {
		t.Error("a fresh store reports itself filled")
	}

	putN(t, s, 10, 14)
	wantBounds(t, s, 10, 14)

	for seq := uint32(10); seq <= 14; seq++ {
		got, err := s.Get(seq)
		if err != nil {
			t.Fatalf("Get(%d): %v", seq, err)
		}
		if !bytes.Equal(got, ledgerBytes(seq)) {
			t.Errorf("Get(%d) returned the wrong bytes", seq)
		}
	}

	for _, seq := range []uint32{9, 15, 1000} {
		if _, err := s.Get(seq); !errors.Is(err, ErrNotRetained) {
			t.Errorf("Get(%d) error = %v, want ErrNotRetained", seq, err)
		}
	}
}

func TestPruneAtRetention(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, 3)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	putN(t, s, 1, 10)
	wantBounds(t, s, 8, 10)

	if _, err := s.Get(7); !errors.Is(err, ErrNotRetained) {
		t.Errorf("Get(7) error = %v, want ErrNotRetained", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 3 {
		names := make([]string, len(entries))
		for i, e := range entries {
			names[i] = e.Name()
		}
		t.Errorf("directory holds %d files (%s), want 3", len(entries), strings.Join(names, ", "))
	}
}

func TestOpenRescansExistingDirectory(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, 10)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	putN(t, s, 100, 104)

	reopened, err := Open(dir, 10)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	wantBounds(t, reopened, 100, 104)

	got, err := reopened.Get(102)
	if err != nil {
		t.Fatalf("Get(102) after reopen: %v", err)
	}
	if !bytes.Equal(got, ledgerBytes(102)) {
		t.Error("Get(102) after reopen returned the wrong bytes")
	}
}

func TestOpenShrinksToSmallerRetention(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, 10)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	putN(t, s, 1, 8)

	reopened, err := Open(dir, 2)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	wantBounds(t, reopened, 7, 8)
}

func TestOpenRejectsNonContiguousDirectory(t *testing.T) {
	dir := t.TempDir()
	for _, seq := range []uint32{5, 6, 9} {
		name := filepath.Join(dir, "ledger-"+string(rune('0'+seq))+".xdr")
		if err := os.WriteFile(name, ledgerBytes(seq), 0o644); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
	}
	_, err := Open(dir, 10)
	if err == nil {
		t.Fatal("Open on a gapped directory succeeded, want error")
	}
	if !strings.Contains(err.Error(), "non-contiguous") {
		t.Errorf("error = %v, want it to mention non-contiguous retention", err)
	}
}

func TestOpenIgnoresForeignFiles(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"README.md", "ledger-.xdr", "ledger-abc.xdr", "ledger-7.xdr.tmp"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
	}
	s, err := Open(dir, 5)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, _, filled := s.Bounds(); filled {
		t.Error("foreign files were counted as ledgers")
	}
}

func TestPutResetsOnGap(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, 10)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	putN(t, s, 1, 4)

	reset, err := s.Put(9, ledgerBytes(9))
	if err != nil {
		t.Fatalf("Put(9): %v", err)
	}
	if !reset {
		t.Error("Put across a gap did not report a reset")
	}
	wantBounds(t, s, 9, 9)
	if _, err := s.Get(3); !errors.Is(err, ErrNotRetained) {
		t.Errorf("Get(3) after reset error = %v, want ErrNotRetained", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("directory holds %d files after reset, want 1", len(entries))
	}
}

func TestPutResetsOnRewind(t *testing.T) {
	s, err := Open(t.TempDir(), 10)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	putN(t, s, 50, 55)

	// A restart that begins the source at the original start ledger rewinds.
	reset, err := s.Put(50, ledgerBytes(50))
	if err != nil {
		t.Fatalf("Put(50): %v", err)
	}
	if !reset {
		t.Error("Put that rewinds did not report a reset")
	}
	wantBounds(t, s, 50, 50)
}

func TestPutRejectsSequenceZero(t *testing.T) {
	// Ledger sequences start at 1, so a 0 means a counter wrapped past
	// math.MaxUint32. Accepting it would let a wrapped ring look contiguous.
	s, err := Open(t.TempDir(), 10)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := s.Put(0, ledgerBytes(0)); !errors.Is(err, ErrInvalidSequence) {
		t.Fatalf("Put(0) error = %v, want ErrInvalidSequence", err)
	}
	if _, _, filled := s.Bounds(); filled {
		t.Error("the rejected ledger was retained anyway")
	}

	// Also at the ceiling, where latest+1 is what wraps to 0.
	putN(t, s, math.MaxUint32-1, math.MaxUint32)
	if _, err := s.Put(0, ledgerBytes(0)); !errors.Is(err, ErrInvalidSequence) {
		t.Errorf("Put(0) after the ceiling error = %v, want ErrInvalidSequence", err)
	}
	wantBounds(t, s, math.MaxUint32-1, math.MaxUint32)
}

func TestPutResetsAtTheSequenceCeiling(t *testing.T) {
	// Nothing can continue math.MaxUint32, so the next ledger restarts the ring.
	// The clearing loop must not run forever counting past the ceiling.
	s, err := Open(t.TempDir(), 10)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	putN(t, s, math.MaxUint32-2, math.MaxUint32)

	reset, err := s.Put(7, ledgerBytes(7))
	if err != nil {
		t.Fatalf("Put(7) after the ceiling: %v", err)
	}
	if !reset {
		t.Error("a ledger that cannot continue the ceiling did not reset the ring")
	}
	wantBounds(t, s, 7, 7)
}

func TestGetAfterPruneRace(t *testing.T) {
	s, err := Open(t.TempDir(), 2)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	putN(t, s, 1, 2)

	// Simulate a replay that resolved bounds and then lost the file to a prune.
	if err := os.Remove(s.path(1)); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if _, err := s.Get(1); !errors.Is(err, ErrNotRetained) {
		t.Errorf("Get(1) error = %v, want ErrNotRetained", err)
	}
}
