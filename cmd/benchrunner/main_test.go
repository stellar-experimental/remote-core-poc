package main

import (
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

func TestParseFlagsDefaults(t *testing.T) {
	o, err := parseFlags(io.Discard, nil)
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if o.mode != "loopback" {
		t.Errorf("default mode = %q, want loopback", o.mode)
	}
	if o.count == 0 {
		t.Error("default loopback ledger count is zero")
	}
}

func TestParseFlagsRejects(t *testing.T) {
	tests := map[string][]string{
		"unknown mode":           {"--mode", "carrier-pigeon"},
		"end before start":       {"--mode", "remote", "--start", "9", "--end", "4"},
		"zero count in loopback": {"--mode", "loopback", "--count", "0"},
		"local without start":    {"--mode", "local", "--core-config", "x", "--history-archive-urls", "y"},
		"local without config":   {"--mode", "local", "--start", "5", "--history-archive-urls", "y"},
		"local without archives": {"--mode", "local", "--start", "5", "--core-config", "x"},
		"sequence out of range":  {"--mode", "remote", "--start", "4294967296"},
		"stray argument":         {"--mode", "remote", "extra"},
		"unknown flag":           {"--turbo"},
	}
	for name, args := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := parseFlags(io.Discard, args); err == nil {
				t.Errorf("parseFlags(%v) succeeded, want an error", args)
			}
		})
	}
}

func TestRunLoopback(t *testing.T) {
	// The whole path in one process: synthetic source, server, WebSocket, client.
	// --verify makes it an integrity check too.
	o, err := parseFlags(io.Discard, []string{
		"--mode", "loopback", "--count", "8", "--synthetic-size", "4096",
		"--synthetic-interval", "1ms", "--verify",
	})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}

	c, err := runLoopback(t.Context(), o)
	if err != nil {
		t.Fatalf("runLoopback: %v", err)
	}
	if len(c.samples) != 8 {
		t.Fatalf("received %d ledgers, want 8", len(c.samples))
	}
	for i, s := range c.samples {
		if s.seq != uint32(i+1) {
			t.Fatalf("ledger sequences are not 1..8: %+v", c.samples)
		}
		if s.bytes != 4096 {
			t.Errorf("ledger %d arrived with %d bytes, want 4096", s.seq, s.bytes)
		}
	}
	// The consumer subscribes before the source starts, so every ledger is
	// delivered live and none drops out of the latency measurement.
	if got, want := len(c.deliveries()), len(c.samples); got != want {
		t.Errorf("%d of %d ledgers carried an emit stamp; loopback must deliver them all live", got, want)
	}

	summary := c.summary()
	for _, want := range []string{"ledgers      8 (1..8)", "throughput", "delivery"} {
		if !strings.Contains(summary, want) {
			t.Errorf("summary is missing %q:\n%s", want, summary)
		}
	}
}

func TestWriteCSVFile(t *testing.T) {
	c := newCollector("file")
	c.add(1, 10, time.Now(), time.Millisecond, true)
	path := t.TempDir() + "/out.csv"
	if err := writeCSVFile(path, c); err != nil {
		t.Fatalf("writeCSVFile: %v", err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()
	body, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !strings.HasPrefix(string(body), "sequence,bytes") {
		t.Errorf("csv file starts with %q", string(body[:min(20, len(body))]))
	}
}

func TestSplitURLs(t *testing.T) {
	got := splitURLs(" a , b ,, c ")
	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("splitURLs = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("splitURLs = %v, want %v", got, want)
		}
	}
	if len(splitURLs("")) != 0 {
		t.Error("an empty list produced URLs")
	}
}
