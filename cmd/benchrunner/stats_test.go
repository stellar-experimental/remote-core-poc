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
		c.add(uint32(100+i), 1024, base.Add(time.Duration(i)*10*time.Millisecond), time.Millisecond, true)
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
	c.add(1, 10, base, 0, false)
	c.add(2, 10, base.Add(time.Millisecond), 0, false)

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
	c.add(1, 10, base, 0, false) // replayed, no stamp
	c.add(2, 10, base.Add(time.Millisecond), 2*time.Millisecond, true)

	out := c.summary()
	if !strings.Contains(out, "1 ledger(s) excluded") {
		t.Errorf("summary does not report the excluded ledger:\n%s", out)
	}
}

func TestWriteCSV(t *testing.T) {
	c := newCollector("csv")
	base := time.Now()
	c.add(7, 100, base, 0, false)
	c.add(8, 200, base.Add(5*time.Millisecond), 3*time.Millisecond, true)

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
	if rows[0][0] != "sequence" || rows[0][4] != "delivery_ns" {
		t.Errorf("header = %v", rows[0])
	}
	if rows[1][0] != "7" || rows[1][3] != "0" || rows[1][4] != "" {
		t.Errorf("first ledger row = %v, want sequence 7, no gap and no delivery", rows[1])
	}
	if rows[2][3] != "5000000" || rows[2][4] != "3000000" {
		t.Errorf("second ledger row = %v, want a 5ms gap and a 3ms delivery", rows[2])
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
