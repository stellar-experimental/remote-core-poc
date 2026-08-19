package server

import (
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stellar-experimental/remote-core-poc/internal/store"
)

// harnessOpts configures a test server backed by the synthetic source.
type harnessOpts struct {
	start      uint32
	count      uint32 // 0 = endless
	size       int
	interval   time.Duration
	emitWindow time.Duration // 0 = each ledger's bytes all at once
	chunkSize  int           // 0 = the protocol default
	retention  int
}

// harness is a running server: a real HTTP listener on 127.0.0.1 with the source
// loop going, which is what the client tests talk to.
type harness struct {
	srv   *Server
	store *store.Store
	url   string
	dir   string
	opts  harnessOpts

	cancelSource context.CancelFunc
	runErr       chan error
	waitOnce     sync.Once
	runResult    error
}

func startHarness(t *testing.T, opts harnessOpts) *harness {
	t.Helper()
	if opts.start == 0 {
		opts.start = 1
	}
	if opts.size == 0 {
		opts.size = 256
	}
	if opts.retention == 0 {
		opts.retention = 1000
	}

	dir := filepath.Join(t.TempDir(), "ledgers")
	ring, err := store.Open(dir, opts.retention)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	srv, err := New(Config{
		Source:    PacedSource(NewSyntheticStream(SyntheticConfig{Size: opts.size}), opts.emitWindow, opts.interval),
		ChunkSize: opts.chunkSize,
		Range:     CountedRange(opts.start, opts.count),
		Store:     ring,
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}

	ts := httptest.NewServer(srv.Handler())
	srcCtx, cancel := context.WithCancel(context.Background())
	h := &harness{
		srv:          srv,
		store:        ring,
		url:          ts.URL,
		dir:          dir,
		opts:         opts,
		cancelSource: cancel,
		runErr:       make(chan error, 1),
	}
	go func() { h.runErr <- srv.Run(srcCtx) }()

	t.Cleanup(func() {
		cancel()
		// Closing the test server waits for every handler to return, which is
		// the leak check: a subscriber handler that ignored its context hangs
		// here instead of passing quietly.
		ts.Close()
		if err := h.wait(t); err != nil {
			t.Errorf("source loop failed: %v", err)
		}
	})
	return h
}

// wait stops waiting for the source loop and reports its result. It is safe to
// call more than once, so a test can assert on it before cleanup does.
func (h *harness) wait(t *testing.T) error {
	t.Helper()
	h.waitOnce.Do(func() {
		select {
		case h.runResult = <-h.runErr:
		case <-time.After(10 * time.Second):
			t.Error("source loop did not stop within 10s")
		}
	})
	return h.runResult
}

// waitForLedger blocks until the retained range reaches seq.
func (h *harness) waitForLedger(t *testing.T, seq uint32) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, latest, filled := h.store.Bounds(); filled && latest >= seq {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("ledger %d was not retained within 10s", seq)
}

// payload is the bytes the synthetic source produced for seq in this harness.
func (h *harness) payload(seq uint32) []byte {
	return SyntheticPayload(seq, h.opts.size)
}
