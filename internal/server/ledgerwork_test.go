package server

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/cespare/xxhash/v2"
)

// dribble returns at most n bytes per Read, the way a pipe does when the
// writer is mid-flush. It is the shape that made chunk size follow pipe size.
type dribble struct {
	src *bytes.Reader
	n   int
}

func (d *dribble) Read(p []byte) (int, error) {
	if len(p) > d.n {
		p = p[:d.n]
	}
	return d.src.Read(p)
}

// TestFillChunkTopsUpShortReads pins the decoupling: however small the reads
// coming back are, a chunk is filled before it is framed, so the CHUNK size is
// the configured one and not whatever the pipe happened to hold.
func TestFillChunkTopsUpShortReads(t *testing.T) {
	payload := bytes.Repeat([]byte("meta"), 64<<10) // 256 KiB
	for _, readSize := range []int{1, 17, 4096, 64 << 10} {
		src := &dribble{src: bytes.NewReader(payload), n: readSize}
		var got []byte
		full := 0
		for {
			buf := make([]byte, 32<<10)
			n, err := fillChunk(src, buf)
			got = append(got, buf[:n]...)
			if n == len(buf) {
				full++
			}
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatalf("reads of %d: %v", readSize, err)
			}
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("reads of %d: payload round-trip differs", readSize)
		}
		if want := len(payload) / (32 << 10); full != want {
			t.Fatalf("reads of %d: %d full chunks, want %d — chunk size still tracks read size",
				readSize, full, want)
		}
	}
}

// TestFillChunkStopsOnAStalledReader guards the loop: a reader that returns
// (0, nil) must end the read rather than spin the source goroutine forever.
func TestFillChunkStopsOnAStalledReader(t *testing.T) {
	stalled := io.Reader(readerFunc(func([]byte) (int, error) { return 0, nil }))
	n, err := fillChunk(stalled, make([]byte, 1024))
	if n != 0 || !errors.Is(err, io.ErrNoProgress) {
		t.Fatalf("fillChunk = (%d, %v), want (0, ErrNoProgress)", n, err)
	}
}

type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) { return f(p) }

// TestLedgerHasherMatchesInlineHashing pins the order chunks are folded in and
// the fence sum provides. Chunks are distinct buffers, as the arena's regions
// are — the hasher borrows them and never copies, so a caller that reused one
// before sum returned would be violating the contract, not finding a bug.
func TestLedgerHasherMatchesInlineHashing(t *testing.T) {
	h := newLedgerHasher()
	defer h.close()

	for ledger := range 3 {
		var whole []byte
		for chunk := range 40 {
			// A fresh buffer per chunk, exactly like a carve out of the arena.
			data := make([]byte, 4096)
			for i := range data {
				data[i] = byte(ledger*7 + chunk*3 + i)
			}
			whole = append(whole, data...)
			h.write(data)
		}
		// sum must consume the whole queue before answering: the channel is
		// buffered, so a hasher that reported early would hash a prefix.
		if got, want := h.sum(), xxhashOf(whole); got != want {
			t.Fatalf("ledger %d: hash %d, want %d", ledger, got, want)
		}
	}
}

// TestLedgerHasherIsOrderSensitive is the mutation guard for the test above:
// folding the same chunks in a different order must produce a different sum,
// so the check is really pinning order and not just total bytes.
func TestLedgerHasherIsOrderSensitive(t *testing.T) {
	a, b := make([]byte, 512), make([]byte, 512)
	for i := range a {
		a[i], b[i] = byte(i), byte(255-i)
	}
	h := newLedgerHasher()
	defer h.close()

	h.write(a)
	h.write(b)
	forward := h.sum()
	h.write(b)
	h.write(a)
	if reverse := h.sum(); reverse == forward {
		t.Fatal("hash is order-insensitive: the ordering assertion proves nothing")
	}
}

func xxhashOf(b []byte) uint64 { return xxhash.Sum64(b) }
