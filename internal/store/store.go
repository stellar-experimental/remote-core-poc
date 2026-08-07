// Package store is corestreamd's disk-backed retention ring: the last N
// ledgers, one file per ledger, so a subscriber can replay recent history
// before joining the live stream.
//
// The ring is always a contiguous range [oldest, latest]. Reads go through the
// file system on purpose — the page cache serves the hot replay path, and the
// server never holds a second copy of a ledger in memory.
package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
)

// ErrNotRetained is returned by Get for a ledger the ring does not hold, either
// because it was never stored or because it was pruned (possibly between the
// caller's bounds check and its read).
var ErrNotRetained = errors.New("store: ledger not retained")

// ErrInvalidSequence is returned by Put for a sequence no ledger can have.
var ErrInvalidSequence = errors.New("store: invalid ledger sequence")

const (
	filePrefix = "ledger-"
	fileSuffix = ".xdr"
)

// Store is a retention ring rooted at a directory. It is safe for concurrent
// use: one writer (the source loop) and many readers (subscriber replays).
type Store struct {
	dir       string
	retention int

	mu     sync.RWMutex
	oldest uint32
	latest uint32
	filled bool
}

// Open prepares dir as a retention ring holding at most retention ledgers,
// creating the directory if needed. An existing directory is rescanned: its
// ledger files must form a contiguous range, otherwise Open fails and asks the
// operator to clean the directory, because replay correctness depends on
// contiguity.
func Open(dir string, retention int) (*Store, error) {
	if retention <= 0 {
		return nil, fmt.Errorf("store: retention must be positive, got %d", retention)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("store: create %q: %w", dir, err)
	}
	s := &Store{dir: dir, retention: retention}
	if err := s.rescan(); err != nil {
		return nil, err
	}
	// A directory left by a run with a larger retention shrinks on open.
	if err := s.prune(); err != nil {
		return nil, err
	}
	return s, nil
}

// rescan rebuilds the in-memory index from the directory contents.
func (s *Store) rescan() error {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return fmt.Errorf("store: scan %q: %w", s.dir, err)
	}
	var seqs []uint32
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		seq, ok := parseName(e.Name())
		if !ok {
			continue
		}
		seqs = append(seqs, seq)
	}
	if len(seqs) == 0 {
		return nil
	}
	slices.Sort(seqs)
	for i := 1; i < len(seqs); i++ {
		if seqs[i] != seqs[i-1]+1 {
			return fmt.Errorf(
				"store: %q holds a non-contiguous retention range (gap between ledger %d and %d); "+
					"delete the directory contents and restart",
				s.dir, seqs[i-1], seqs[i])
		}
	}
	s.oldest, s.latest, s.filled = seqs[0], seqs[len(seqs)-1], true
	return nil
}

// Put stores raw as ledger seq and prunes anything beyond the retention count.
//
// Ledgers normally arrive in order, one after the last. A sequence that does
// not continue the ring (a gap, or a rewind after a restart against an existing
// directory) makes the retained range unusable for replay, so Put empties the
// ring and starts a new one at seq. Reset reports whether that happened.
//
// Sequence 0 is rejected: stellar ledger sequences start at 1, so a 0 here means
// a counter ran past math.MaxUint32 and wrapped. The protocol has no room for a
// wrap, and accepting it would let a wrapped ring look contiguous.
func (s *Store) Put(seq uint32, raw []byte) (reset bool, err error) {
	if seq == 0 {
		return false, fmt.Errorf("%w: ledger sequences start at 1, so 0 means a wrapped counter", ErrInvalidSequence)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.filled && seq != s.latest+1 {
		if err := s.clearLocked(); err != nil {
			return false, err
		}
		reset = true
	}
	if err := s.write(seq, raw); err != nil {
		return reset, err
	}
	if s.filled {
		s.latest = seq
	} else {
		s.oldest, s.latest, s.filled = seq, seq, true
	}
	if err := s.pruneLocked(); err != nil {
		return reset, err
	}
	return reset, nil
}

// write puts the bytes on disk via a temporary file, so a reader never observes
// a half-written ledger under its final name.
func (s *Store) write(seq uint32, raw []byte) error {
	final := s.path(seq)
	tmp := final + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return fmt.Errorf("store: write ledger %d: %w", seq, err)
	}
	if err := os.Rename(tmp, final); err != nil {
		return fmt.Errorf("store: publish ledger %d: %w", seq, err)
	}
	return nil
}

// Get returns a copy of ledger seq's raw XDR bytes.
func (s *Store) Get(seq uint32) ([]byte, error) {
	s.mu.RLock()
	oldest, latest, filled := s.oldest, s.latest, s.filled
	s.mu.RUnlock()

	if !filled || seq < oldest || seq > latest {
		return nil, fmt.Errorf("%w: ledger %d outside [%d,%d]", ErrNotRetained, seq, oldest, latest)
	}
	raw, err := os.ReadFile(s.path(seq))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// Pruned between the bounds check and the read.
			return nil, fmt.Errorf("%w: ledger %d was pruned", ErrNotRetained, seq)
		}
		return nil, fmt.Errorf("store: read ledger %d: %w", seq, err)
	}
	return raw, nil
}

// Bounds reports the retained range. filled is false while the ring is empty.
func (s *Store) Bounds() (oldest, latest uint32, filled bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.oldest, s.latest, s.filled
}

// Retention is the maximum number of ledgers the ring holds.
func (s *Store) Retention() int { return s.retention }

func (s *Store) prune() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pruneLocked()
}

func (s *Store) pruneLocked() error {
	if !s.filled {
		return nil
	}
	for int(s.latest-s.oldest)+1 > s.retention {
		if err := os.Remove(s.path(s.oldest)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("store: prune ledger %d: %w", s.oldest, err)
		}
		s.oldest++
	}
	return nil
}

func (s *Store) clearLocked() error {
	// Breaking after the last sequence instead of testing seq <= latest keeps the
	// loop finite when latest is math.MaxUint32, where seq++ would wrap to 0 and
	// the comparison would never end it.
	for seq := s.oldest; s.filled; seq++ {
		if err := os.Remove(s.path(seq)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("store: clear ledger %d: %w", seq, err)
		}
		if seq == s.latest {
			break
		}
	}
	s.oldest, s.latest, s.filled = 0, 0, false
	return nil
}

func (s *Store) path(seq uint32) string {
	return filepath.Join(s.dir, filePrefix+strconv.FormatUint(uint64(seq), 10)+fileSuffix)
}

func parseName(name string) (uint32, bool) {
	if !strings.HasPrefix(name, filePrefix) || !strings.HasSuffix(name, fileSuffix) {
		return 0, false
	}
	digits := name[len(filePrefix) : len(name)-len(fileSuffix)]
	seq, err := strconv.ParseUint(digits, 10, 32)
	if err != nil {
		return 0, false
	}
	return uint32(seq), true
}
