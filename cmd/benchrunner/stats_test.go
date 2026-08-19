package main

import (
	"encoding/csv"
	"strings"
	"testing"
	"time"
)

func TestPercentile(t *testing.T) {
	sorted := []time.Duration{
		1 * time.Millisecond, 2 * time.Millisecond, 3 * time.Millisecond, 4 * time.Millisecond,
		5 * time.Millisecond, 6 * time.Millisecond, 7 * time.Millisecond, 8 * time.Millisecond,
		9 * time.Millisecond, 10 * time.Millisecond,
	}
	// Nearest rank over ten values: p50 is the 5th, p90 the 9th, p99 the 10th.
	tests := []struct {
		p    int
		want time.Duration
	}{
		{0, 1 * time.Millisecond},
		{10, 1 * time.Millisecond},
		{50, 5 * time.Millisecond},
		{90, 9 * time.Millisecond},
		{95, 10 * time.Millisecond},
		{99, 10 * time.Millisecond},
		{100, 10 * time.Millisecond},
	}
	for _, tt := range tests {
		if got := percentile(sorted, tt.p); got != tt.want {
			t.Errorf("percentile(p%d) = %s, want %s", tt.p, got, tt.want)
		}
	}
	if got := percentile(nil, 50); got != 0 {
		t.Errorf("percentile of nothing = %s, want 0", got)
	}
}

func TestCollectorSummary(t *testing.T) {
	c := newCollector("test run")
	base := time.Now()
	for i := range 4 {
		// One ledger every 10 ms, each delivered 1 ms after it was emitted.
		c.add(base.Add(time.Duration(i)*10*time.Millisecond),
			sample{seq: uint32(100 + i), bytes: 1024, delivery: time.Millisecond, hasDelivery: true})
	}

	out := c.summary()
	for _, want := range []string{"test run", "ledgers      4 (100..103)", "delivery", "1.00ms"} {
		if !strings.Contains(out, want) {
			t.Errorf("summary is missing %q:\n%s", want, out)
		}
	}
	if c.elapsed() != 30*time.Millisecond {
		t.Errorf("elapsed = %s, want 30ms", c.elapsed())
	}
	if gaps := c.interArrivals(); len(gaps) != 3 || gaps[0] != 10*time.Millisecond {
		t.Errorf("inter-arrivals = %v, want three 10ms gaps", gaps)
	}
}

func TestCollectorSummaryWithoutStamps(t *testing.T) {
	c := newCollector("local")
	base := time.Now()
	c.add(base, sample{seq: 1, bytes: 10})
	c.add(base.Add(time.Millisecond), sample{seq: 2, bytes: 10})

	out := c.summary()
	if !strings.Contains(out, "no emit stamps") {
		t.Errorf("summary does not explain the missing latencies:\n%s", out)
	}
}

func TestCollectorSummaryEmpty(t *testing.T) {
	if out := newCollector("empty").summary(); !strings.Contains(out, "no ledgers received") {
		t.Errorf("summary of an empty run = %q", out)
	}
}

func TestCollectorPartialStamps(t *testing.T) {
	c := newCollector("mixed")
	base := time.Now()
	c.add(base, sample{seq: 1, bytes: 10, chunks: 1}) // replayed, no stamp
	c.add(base.Add(time.Millisecond), sample{
		seq: 2, bytes: 10, chunks: 1,
		delivery: 2 * time.Millisecond, hasDelivery: true,
		pipeline: 17 * time.Millisecond, hasPipeline: true,
		emit: 15 * time.Millisecond, hasEmit: true,
	})

	out := c.summary()
	for _, want := range []string{
		"1 ledger(s) excluded",
		// A chunked run reports the whole metric set, and states the
		// fallback count even at zero — the gate is zero, so the report must
		// show it was measured.
		"t_emit", "15.00ms", "pipeline", "17.00ms",
		"fallbacks    0 partial deliveries discarded",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("summary is missing %q:\n%s", want, out)
		}
	}
}

func TestCollectorCountsFallbacks(t *testing.T) {
	c := newCollector("fallbacks")
	base := time.Now()
	c.add(base, sample{seq: 1, bytes: 10, chunks: 1, discarded: 2})
	c.add(base.Add(time.Millisecond), sample{seq: 2, bytes: 10, chunks: 1, discarded: 1})

	if c.fallbacks() != 3 {
		t.Errorf("fallbacks = %d, want 3", c.fallbacks())
	}
	if !strings.Contains(c.summary(), "3 partial deliveries discarded") {
		t.Errorf("summary does not total the fallbacks:\n%s", c.summary())
	}
}

func TestWriteCSV(t *testing.T) {
	c := newCollector("csv")
	base := time.Now()
	c.add(base, sample{seq: 7, bytes: 100})
	c.add(base.Add(5*time.Millisecond), sample{
		seq: 8, bytes: 200, chunks: 2,
		delivery: 3 * time.Millisecond, hasDelivery: true,
		pipeline: 18 * time.Millisecond, hasPipeline: true,
		emit: 15 * time.Millisecond, hasEmit: true,
		discarded: 1,
	})

	var buf strings.Builder
	if err := c.writeCSV(&buf); err != nil {
		t.Fatalf("writeCSV: %v", err)
	}
	rows, err := csv.NewReader(strings.NewReader(buf.String())).ReadAll()
	if err != nil {
		t.Fatalf("parse csv: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("wrote %d rows, want a header and two ledgers", len(rows))
	}
	if rows[0][0] != "sequence" || rows[0][5] != "emit_ns" || rows[0][6] != "delivery_ns" {
		t.Errorf("header = %v", rows[0])
	}
	if rows[1][0] != "7" || rows[1][4] != "0" || rows[1][5] != "" || rows[1][6] != "" {
		t.Errorf("first ledger row = %v, want sequence 7, no gap and no measurements", rows[1])
	}
	if rows[2][4] != "5000000" || rows[2][5] != "15000000" || rows[2][6] != "3000000" ||
		rows[2][7] != "18000000" || rows[2][8] != "1" {
		t.Errorf("second ledger row = %v, want the 5ms gap and every measurement column filled", rows[2])
	}
}

func TestFmtDur(t *testing.T) {
	tests := map[time.Duration]string{
		500 * time.Nanosecond:  "500ns",
		1500 * time.Nanosecond: "1.5µs",
		2 * time.Millisecond:   "2.00ms",
	}
	for d, want := range tests {
		if got := fmtDur(d); got != want {
			t.Errorf("fmtDur(%s) = %q, want %q", d, got, want)
		}
	}
}
