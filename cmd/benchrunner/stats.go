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
	// bytes is the ledger payload size.
	bytes int
	// sinceStart is the offset from the first ledger of the run, which is what
	// inter-arrival gaps are computed from.
	sinceStart time.Duration
	// delivery is receive minus the server's emit stamp. Absent for a locally
	// consumed ledger and for one replayed from the server's retention.
	delivery    time.Duration
	hasDelivery bool
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

// add records a ledger of size n bytes, received now, with an optional delivery
// latency.
func (c *collector) add(seq uint32, n int, now time.Time, delivery time.Duration, hasDelivery bool) {
	if c.start.IsZero() {
		c.start = now
	}
	c.samples = append(c.samples, sample{
		seq:         seq,
		bytes:       n,
		sinceStart:  now.Sub(c.start),
		delivery:    delivery,
		hasDelivery: hasDelivery,
	})
	c.bytes += int64(n)
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

func (c *collector) deliveries() []time.Duration {
	var ds []time.Duration
	for _, s := range c.samples {
		if s.hasDelivery {
			ds = append(ds, s.delivery)
		}
	}
	return ds
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

	ds := c.deliveries()
	if len(ds) == 0 {
		out += "delivery     no emit stamps in this mode (nothing to measure against)\n"
		return out
	}
	out += percentileLine("delivery", ds)
	if missing := len(c.samples) - len(ds); missing > 0 {
		out += fmt.Sprintf("             %d ledger(s) excluded: replayed from retention, no emit stamp\n", missing)
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
// nearest-rank so no value is invented by interpolation.
func percentile(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := (p * len(sorted)) / 100
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
	if err := cw.Write([]string{"sequence", "bytes", "since_start_ns", "inter_arrival_ns", "delivery_ns"}); err != nil {
		return err
	}
	for i, s := range c.samples {
		var gap time.Duration
		if i > 0 {
			gap = s.sinceStart - c.samples[i-1].sinceStart
		}
		delivery := ""
		if s.hasDelivery {
			delivery = strconv.FormatInt(s.delivery.Nanoseconds(), 10)
		}
		row := []string{
			strconv.FormatUint(uint64(s.seq), 10),
			strconv.Itoa(s.bytes),
			strconv.FormatInt(s.sinceStart.Nanoseconds(), 10),
			strconv.FormatInt(gap.Nanoseconds(), 10),
			delivery,
		}
		if err := cw.Write(row); err != nil {
			return err
		}
	}
	cw.Flush()
	return cw.Error()
}
