package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stellar/remote-core-poc/internal/server"
	"github.com/stellar/remote-core-poc/internal/store"
	"github.com/stellar/remote-core-poc/internal/wire"
)

func TestParseFlagsDefaults(t *testing.T) {
	o, err := parseFlags(io.Discard, nil)
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if o.listen != ":8462" {
		t.Errorf("default listen address = %q, want :8462", o.listen)
	}
	if o.source != "synthetic" {
		t.Errorf("default source = %q, want synthetic", o.source)
	}
	if o.retention != 10_000 {
		t.Errorf("default retention = %d, want 10000", o.retention)
	}
	if o.syntheticInterval != time.Second {
		t.Errorf("default synthetic interval = %s, want 1s", o.syntheticInterval)
	}
}

func TestParseFlagsCaptiveRequirements(t *testing.T) {
	// Captive mode needs a start ledger, a config and archives; the synthetic
	// default needs nothing.
	tests := map[string][]string{
		"no start ledger": {"--source", "captive", "--core-config", "c", "--history-archive-urls", "u"},
		"no core config":  {"--source", "captive", "--start-ledger", "5", "--history-archive-urls", "u"},
		"no archives":     {"--source", "captive", "--start-ledger", "5", "--core-config", "c"},
		"bad source":      {"--source", "telepathy"},
		"stray argument":  {"extra"},
		"unknown flag":    {"--turbo"},
	}
	for name, args := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := parseFlags(io.Discard, args); err == nil {
				t.Errorf("parseFlags(%v) succeeded, want an error", args)
			}
		})
	}

	o, err := parseFlags(io.Discard, []string{
		"--source", "captive", "--start-ledger", "55000000",
		"--core-config", "captive-core.cfg", "--history-archive-urls", "https://a,https://b",
	})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if got := splitURLs(o.historyArchiveURL); len(got) != 2 || got[0] != "https://a" || got[1] != "https://b" {
		t.Errorf("archive URLs = %v, want two", got)
	}
}

func TestParseFlagsRejectsOversizedSequences(t *testing.T) {
	for _, args := range [][]string{
		{"--start-ledger", "4294967296"},
		{"--synthetic-count", "4294967296"},
	} {
		if _, err := parseFlags(io.Discard, args); err == nil {
			t.Errorf("parseFlags(%v) succeeded, want an error", args)
		}
	}
}

func TestParseFlagsRejectsRangesReachingTheSequenceCeiling(t *testing.T) {
	// The last representable sequence is not streamable, so a range that would
	// retain it is refused before the ring can hold it.
	rejected := map[string][]string{
		"start at the ceiling":         {"--start-ledger", "4294967295"},
		"count reaches the ceiling":    {"--start-ledger", "4294967293", "--synthetic-count", "3"},
		"count runs past the ceiling":  {"--start-ledger", "4294967290", "--synthetic-count", "10"},
		"captive start at the ceiling": {"--source", "captive", "--start-ledger", "4294967295", "--core-config", "c", "--history-archive-urls", "u"},
	}
	for name, args := range rejected {
		t.Run(name, func(t *testing.T) {
			_, err := parseFlags(io.Discard, args)
			if err == nil {
				t.Fatalf("parseFlags(%v) succeeded, want an error", args)
			}
			if !strings.Contains(err.Error(), "4294967295") {
				t.Errorf("error = %v, want it to name the ceiling", err)
			}
		})
	}

	// Stopping one short of the ceiling is fine.
	if _, err := parseFlags(io.Discard, []string{"--start-ledger", "4294967290", "--synthetic-count", "5"}); err != nil {
		t.Errorf("a range ending at 4294967294 was rejected: %v", err)
	}
}

func TestParseFlagsRejectsOversizedSyntheticLedgers(t *testing.T) {
	// The payload cap is protocol-defined, so an oversized synthetic ledger is a
	// flag error rather than something the operator can raise.
	over := strconv.FormatInt(wire.DefaultMaxPayloadSize+1, 10)
	_, err := parseFlags(io.Discard, []string{"--synthetic-size", over})
	if err == nil {
		t.Fatalf("--synthetic-size %s was accepted, want an error", over)
	}
	if !strings.Contains(err.Error(), strconv.FormatInt(wire.DefaultMaxPayloadSize, 10)) {
		t.Errorf("error = %v, want it to name the %d-byte cap", err, wire.DefaultMaxPayloadSize)
	}

	// Exactly at the cap is allowed: it is the largest ledger the protocol carries.
	atCap := strconv.FormatInt(wire.DefaultMaxPayloadSize, 10)
	if _, err := parseFlags(io.Discard, []string{"--synthetic-size", atCap}); err != nil {
		t.Errorf("--synthetic-size %s was rejected: %v", atCap, err)
	}
}

func TestSplitURLs(t *testing.T) {
	got := splitURLs(" a , b ,, c ")
	if len(got) != 3 || got[0] != "a" || got[2] != "c" {
		t.Errorf("splitURLs = %v, want [a b c]", got)
	}
	if len(splitURLs("")) != 0 {
		t.Error("an empty list produced URLs")
	}
}

// testServices builds a synthetic server and an HTTP server over a loopback
// listener, the pair runServices supervises.
func testServices(t *testing.T, count uint32) (*server.Server, *http.Server, net.Listener) {
	t.Helper()
	ring, err := store.Open(filepath.Join(t.TempDir(), "ledgers"), 100)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	srv, err := server.New(server.Config{
		Source: server.NewSyntheticStream(server.SyntheticConfig{Size: 64, Interval: time.Millisecond}),
		Range:  server.SyntheticRange(1, count),
		Store:  ring,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	return srv, &http.Server{Handler: srv.Handler()}, ln
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestRunServicesStopsWhenTheSourceEnds(t *testing.T) {
	srv, httpSrv, ln := testServices(t, 3)
	addr := ln.Addr().String()

	if err := runServices(t.Context(), srv, httpSrv, ln, quietLogger()); err != nil {
		t.Fatalf("runServices: %v", err)
	}
	// The listener must be gone with the source: a server still accepting
	// connections would hand subscribers an empty stream.
	if _, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
		t.Error("the HTTP listener is still accepting after the source ended")
	}
}

func TestRunServicesStopsWhenTheHTTPServerFails(t *testing.T) {
	// An endless source: if a Serve failure did not bring the process down, this
	// call would never return and the test would time out.
	srv, httpSrv, ln := testServices(t, 0)

	// Closing the listener under Serve is how it fails in production too — the
	// socket going away.
	if err := ln.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}

	err := runServices(t.Context(), srv, httpSrv, ln, quietLogger())
	if err == nil {
		t.Fatal("runServices returned no error after the HTTP server failed")
	}
	if !strings.Contains(err.Error(), "http server") {
		t.Errorf("error = %v, want it to name the http server", err)
	}
}

func TestRunServicesStopsOnContextCancel(t *testing.T) {
	// A SIGINT arrives as a cancelled context: both components stop and the exit
	// is clean, not an error.
	srv, httpSrv, ln := testServices(t, 0)
	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	if err := runServices(ctx, srv, httpSrv, ln, quietLogger()); err != nil {
		t.Fatalf("runServices after cancel: %v", err)
	}
}

func TestNewLogger(t *testing.T) {
	// An unreadable level falls back to info rather than failing to start.
	if newLogger("nonsense") == nil {
		t.Error("newLogger returned nothing for an unknown level")
	}
	if newLogger("debug") == nil {
		t.Error("newLogger returned nothing for debug")
	}
}
