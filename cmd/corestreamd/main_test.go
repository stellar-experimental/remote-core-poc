package main

import (
	"io"
	"testing"
	"time"
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

func TestSplitURLs(t *testing.T) {
	got := splitURLs(" a , b ,, c ")
	if len(got) != 3 || got[0] != "a" || got[2] != "c" {
		t.Errorf("splitURLs = %v, want [a b c]", got)
	}
	if len(splitURLs("")) != 0 {
		t.Error("an empty list produced URLs")
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
