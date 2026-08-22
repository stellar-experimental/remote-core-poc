package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"slices"
	"strconv"
	"time"
)

// sample is one received ledger.
type sample struct {
	seq uint32
	// bytes is the ledger payload size; chunks how many messages carried it
	// (zero when consumed locally with no stream at all).
	bytes  int
	chunks int
	// sinceStart is the offset from the first ledger of the run, which is what
	// inter-arrival gaps are computed from. It is filled in by add.
	sinceStart time.Duration
	// arrivedUnixNano is when this ledger became usable, on the wall clock.
	// Relative offsets cannot be compared across processes; this can, which
	// is what lets two consumers of ONE core run be subtracted per ledger.
	arrivedUnixNano int64
	// delivery is assembled-and-verified minus the server's emit-end stamp:
	// the headline metric, how long after the source finished the ledger was
	// usable. Absent for a locally consumed ledger and for one replayed from
	// the server's retention.
	delivery    time.Duration
	hasDelivery bool
	// pipeline is assembled minus emit-start: the whole path from the source's
	// first byte. Absent whenever delivery is.
	pipeline    time.Duration
	hasPipeline bool
	// emit is the server-side emission window T_emit (emitEnd - emitStart),
	// measured on the server's clock alone.
	emit    time.Duration
	hasEmit bool
	// discarded counts partial assemblies of this ledger thrown away before it
	// arrived complete: each one is a server-side ring fallback. The
	// acceptance gate is zero of these inside the measurement window.
	discarded int
}

// collector accumulates samples for one run.
type collector struct {
	label   string
	start   time.Time
	samples []sample
	bytes   int64
}

func newCollector(label string) *collector {
	return &collector{label: label}
}

// add records s, received at now. s.sinceStart is computed here.
func (c *collector) add(now time.Time, s sample) {
	if c.start.IsZero() {
		c.start = now
	}
	s.sinceStart = now.Sub(c.start)
	s.arrivedUnixNano = now.UnixNano()
	c.samples = append(c.samples, s)
	c.bytes += int64(s.bytes)
}

// elapsed is the wall time from the first ledger to the last. A single ledger
// spans no time, so rates are reported only for two or more.
func (c *collector) elapsed() time.Duration {
	if len(c.samples) == 0 {
		return 0
	}
	return c.samples[len(c.samples)-1].sinceStart
}

func (c *collector) interArrivals() []time.Duration {
	if len(c.samples) < 2 {
		return nil
	}
	gaps := make([]time.Duration, 0, len(c.samples)-1)
	for i := 1; i < len(c.samples); i++ {
		gaps = append(gaps, c.samples[i].sinceStart-c.samples[i-1].sinceStart)
	}
	return gaps
}

// durations filters one optional per-sample metric out of the run.
func (c *collector) durations(metric func(sample) (time.Duration, bool)) []time.Duration {
	var ds []time.Duration
	for _, s := range c.samples {
		if d, ok := metric(s); ok {
			ds = append(ds, d)
		}
	}
	return ds
}

func (c *collector) deliveries() []time.Duration {
	return c.durations(func(s sample) (time.Duration, bool) { return s.delivery, s.hasDelivery })
}

func (c *collector) pipelines() []time.Duration {
	return c.durations(func(s sample) (time.Duration, bool) { return s.pipeline, s.hasPipeline })
}

func (c *collector) emits() []time.Duration {
	return c.durations(func(s sample) (time.Duration, bool) { return s.emit, s.hasEmit })
}

// fallbacks is how many partial deliveries were discarded across the run: the
// client-side count of server ring fallbacks.
func (c *collector) fallbacks() int {
	n := 0
	for _, s := range c.samples {
		n += s.discarded
	}
	return n
}

// chunked reports whether any ledger arrived as a chunk flow — false for the
// local mode, which has no stream and therefore no fallbacks to report.
func (c *collector) chunked() bool {
	for _, s := range c.samples {
		if s.chunks > 0 {
			return true
		}
	}
	return false
}

// summary renders the run as plain text.
func (c *collector) summary() string {
	out := fmt.Sprintf("\n=== %s ===\n", c.label)
	if len(c.samples) == 0 {
		return out + "no ledgers received\n"
	}
	elapsed := c.elapsed()
	mib := float64(c.bytes) / (1 << 20)
	out += fmt.Sprintf("ledgers      %d (%d..%d)\n", len(c.samples), c.samples[0].seq, c.samples[len(c.samples)-1].seq)
	out += fmt.Sprintf("bytes        %.2f MiB (mean %.1f KiB/ledger)\n", mib, float64(c.bytes)/float64(len(c.samples))/1024)
	out += fmt.Sprintf("elapsed      %s\n", elapsed.Round(time.Millisecond))
	if elapsed > 0 {
		secs := elapsed.Seconds()
		out += fmt.Sprintf("throughput   %.1f ledgers/s, %.1f MiB/s\n", float64(len(c.samples)-1)/secs, mib/secs)
	}
	out += percentileLine("inter-arrival", c.interArrivals())

	if ds := c.deliveries(); len(ds) == 0 {
		out += "delivery     no emit stamps in this mode (nothing to measure against)\n"
	} else {
		if emits := c.emits(); len(emits) > 0 {
			out += percentileLine("t_emit", emits)
		}
		out += percentileLine("delivery", ds)
		if ps := c.pipelines(); len(ps) > 0 {
			out += percentileLine("pipeline", ps)
		}
		if missing := len(c.samples) - len(ds); missing > 0 {
			out += fmt.Sprintf("             %d ledger(s) excluded: replayed from retention, no emit stamp\n", missing)
		}
	}
	if c.chunked() {
		// Stated even at zero: the acceptance gate is zero fallbacks, so the
		// report has to show the count was measured, not merely omitted.
		out += fmt.Sprintf("fallbacks    %d partial deliveries discarded (live flow abandoned, ring redelivered)\n",
			c.fallbacks())
	}
	return out
}

func percentileLine(label string, ds []time.Duration) string {
	if len(ds) == 0 {
		return fmt.Sprintf("%-13s n/a\n", label)
	}
	sorted := slices.Clone(ds)
	slices.Sort(sorted)
	return fmt.Sprintf("%-13s p50 %s  p90 %s  p99 %s  max %s\n", label,
		fmtDur(percentile(sorted, 50)), fmtDur(percentile(sorted, 90)),
		fmtDur(percentile(sorted, 99)), fmtDur(sorted[len(sorted)-1]))
}

// percentile returns the p-th percentile of an already sorted slice, using
// nearest-rank so no value is invented by interpolation: the value at rank
// ceil(p*n/100), which for n=10 makes p50 the 5th value and p99 the 10th.
func percentile(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := (p*len(sorted)+99)/100 - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func fmtDur(d time.Duration) string {
	switch {
	case d < time.Microsecond:
		return fmt.Sprintf("%dns", d.Nanoseconds())
	case d < time.Millisecond:
		return fmt.Sprintf("%.1fµs", float64(d.Nanoseconds())/1e3)
	default:
		return fmt.Sprintf("%.2fms", float64(d.Nanoseconds())/1e6)
	}
}

// writeCSV writes one row per ledger.
func (c *collector) writeCSV(w io.Writer) error {
	cw := csv.NewWriter(w)
	header := []string{
		"sequence", "bytes", "chunks", "since_start_ns", "inter_arrival_ns",
		"emit_ns", "delivery_ns", "pipeline_ns", "discarded_partials",
		"arrived_unix_ns",
	}
	if err := cw.Write(header); err != nil {
		return err
	}
	optional := func(d time.Duration, ok bool) string {
		if !ok {
			return ""
		}
		return strconv.FormatInt(d.Nanoseconds(), 10)
	}
	for i, s := range c.samples {
		var gap time.Duration
		if i > 0 {
			gap = s.sinceStart - c.samples[i-1].sinceStart
		}
		row := []string{
			strconv.FormatUint(uint64(s.seq), 10),
			strconv.Itoa(s.bytes),
			strconv.Itoa(s.chunks),
			strconv.FormatInt(s.sinceStart.Nanoseconds(), 10),
			strconv.FormatInt(gap.Nanoseconds(), 10),
			optional(s.emit, s.hasEmit),
			optional(s.delivery, s.hasDelivery),
			optional(s.pipeline, s.hasPipeline),
			strconv.Itoa(s.discarded),
			strconv.FormatInt(s.arrivedUnixNano, 10),
		}
		if err := cw.Write(row); err != nil {
			return err
		}
	}
	cw.Flush()
	return cw.Error()
}
