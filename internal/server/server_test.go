package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"

	"github.com/stellar-experimental/remote-core-poc/internal/store"
	"github.com/stellar-experimental/remote-core-poc/internal/wire"
	"github.com/stellar-experimental/remote-core-poc/remoteledger"
)

// consumer collects sequences, delivery metadata, payload copies and any error
// from one subscription. It is the assertion surface of the end-to-end tests:
// what a real consumer of the seam sees.
type consumer struct {
	seqs     []uint32
	stamps   []int64 // emit-end stamps; zero means the ledger was served from the retention ring
	infos    []remoteledger.LedgerInfo
	payloads [][]byte
	err      error
}

// discarded is the total partial assemblies thrown away across the run: the
// client-side count of the server's live-flow abandons.
func (c *consumer) discarded() int {
	n := 0
	for _, info := range c.infos {
		n += info.DiscardedPartials
	}
	return n
}

// consume subscribes to h over rng and reads until the stream ends, limit
// ledgers have arrived (limit > 0), or onLedger asks it to stop. The deadline
// turns a stream that wedges — a lost wakeup, a subscriber asleep forever —
// into a test failure instead of a package timeout.
func consume(
	ctx context.Context, t *testing.T, url string, rng ledgerbackend.Range, limit int, onLedger func(seq uint32),
	opts ...remoteledger.Option,
) *consumer {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	c := &consumer{}
	var last remoteledger.LedgerInfo
	opts = append(opts, remoteledger.WithObserver(func(info remoteledger.LedgerInfo) {
		last = info
	}))
	stream := remoteledger.New(url, opts...)
	for raw, err := range stream.RawLedgers(ctx, rng) {
		if err != nil {
			c.err = err
			return c
		}
		c.seqs = append(c.seqs, last.Sequence)
		c.stamps = append(c.stamps, last.EmitEndUnixNano)
		c.infos = append(c.infos, last)
		c.payloads = append(c.payloads, bytes.Clone(raw))
		if onLedger != nil {
			onLedger(last.Sequence)
		}
		if limit > 0 && len(c.seqs) >= limit {
			break
		}
	}
	return c
}

// stallOnFirstLedger returns a consume callback that parks the subscriber on
// its first ledger until the harness's source loop has finished, so whatever
// the subscriber still needs must come out of the retention ring.
func stallOnFirstLedger(t *testing.T, h *harness) func(uint32) {
	return func(seq uint32) {
		if seq == 1 {
			if err := h.wait(t); err != nil {
				t.Errorf("source loop: %v", err)
			}
		}
	}
}

// wantContiguous checks the delivered sequences are exactly from..to, in order.
func wantContiguous(t *testing.T, seqs []uint32, from, to uint32) {
	t.Helper()
	want := int(to-from) + 1
	if len(seqs) != want {
		t.Fatalf("received %d ledgers (%v), want %d covering [%d,%d]", len(seqs), seqs, want, from, to)
	}
	for i, seq := range seqs {
		if seq != from+uint32(i) {
			t.Fatalf("received sequences %v, want [%d,%d] with no gap or duplicate", seqs, from, to)
			return
		}
	}
}

func TestNewValidation(t *testing.T) {
	src := PacedSource(NewSyntheticStream(SyntheticConfig{}), 0, 0)
	tests := []struct {
		name string
		cfg  Config
	}{
		{"no source", Config{Range: ledgerbackend.UnboundedRange(1)}},
		{"no store", Config{Source: src, Range: ledgerbackend.UnboundedRange(1)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(tt.cfg); err == nil {
				t.Error("New succeeded, want an error")
			}
		})
	}

	// A zero start ledger has no counter to run from: the server numbers
	// ledgers from the range it asked for.
	ring, err := store.Open(t.TempDir(), 10)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	if _, err := New(Config{Source: src, Range: ledgerbackend.UnboundedRange(0), Store: ring}); err == nil {
		t.Error("New with a zero start ledger succeeded, want an error")
	}

	// The chunk-size bounds are an interop contract with every client's read
	// limit, so they bind library embedders, not just the CLI flag parsers.
	for _, bad := range []int{1024, wire.MaxChunkSize + 1} {
		cfg := Config{Source: src, Range: ledgerbackend.UnboundedRange(1), Store: ring, ChunkSize: bad}
		if _, err := New(cfg); err == nil {
			t.Errorf("New accepted chunk size %d, outside the protocol bounds", bad)
		}
	}
}

func TestBoundedReplayFromRetention(t *testing.T) {
	h := startHarness(t, harnessOpts{count: 10, size: 512})
	if err := h.wait(t); err != nil {
		t.Fatalf("source loop: %v", err)
	}

	c := consume(t.Context(), t, h.url, ledgerbackend.BoundedRange(3, 7), 0, nil)
	if c.err != nil {
		t.Fatalf("stream error: %v", c.err)
	}
	wantContiguous(t, c.seqs, 3, 7)
	for i, seq := range c.seqs {
		if !bytes.Equal(c.payloads[i], h.payload(seq)) {
			t.Errorf("ledger %d payload does not match what the source produced", seq)
		}
	}
}

func TestReplayToLiveHandoff(t *testing.T) {
	h := startHarness(t, harnessOpts{interval: 2 * time.Millisecond, size: 256})
	// Let the source get ahead, so the subscription starts behind the tip and
	// must cross from retention into the live stream.
	h.waitForLedger(t, 5)

	c := consume(t.Context(), t, h.url, ledgerbackend.BoundedRange(1, 25), 0, nil)
	if c.err != nil {
		t.Fatalf("stream error: %v", c.err)
	}
	wantContiguous(t, c.seqs, 1, 25)
	for i, seq := range c.seqs {
		if !bytes.Equal(c.payloads[i], h.payload(seq)) {
			t.Errorf("ledger %d payload does not match what the source produced", seq)
		}
	}
}

func TestTooFarBehind(t *testing.T) {
	h := startHarness(t, harnessOpts{count: 10, retention: 3})
	if err := h.wait(t); err != nil {
		t.Fatalf("source loop: %v", err)
	}

	c := consume(t.Context(), t, h.url, ledgerbackend.BoundedRange(1, 10), 0, nil)
	if !errors.Is(c.err, remoteledger.ErrTooFarBehind) {
		t.Fatalf("error = %v, want ErrTooFarBehind", c.err)
	}
	var tfb *remoteledger.TooFarBehindError
	if !errors.As(c.err, &tfb) {
		t.Fatalf("error %v does not carry a TooFarBehindError", c.err)
	}
	if tfb.Oldest != 8 || tfb.Latest != 10 {
		t.Errorf("reported retention [%d,%d], want [8,10]", tfb.Oldest, tfb.Latest)
	}
	if tfb.Requested != 1 {
		t.Errorf("reported request %d, want 1", tfb.Requested)
	}
	if len(c.seqs) != 0 {
		t.Errorf("received %d ledgers before the refusal, want none", len(c.seqs))
	}
}

func TestBoundedRangeBeyondTheSourceIsTruncated(t *testing.T) {
	// The source only ever produces 5 ledgers, so a subscription through 20 can
	// never be satisfied. Ending it quietly would tell the consumer it had the
	// whole range; it must be an error.
	h := startHarness(t, harnessOpts{count: 5})
	if err := h.wait(t); err != nil {
		t.Fatalf("source loop: %v", err)
	}

	c := consume(t.Context(), t, h.url, ledgerbackend.BoundedRange(1, 20), 0, nil)
	if !errors.Is(c.err, remoteledger.ErrTruncated) {
		t.Fatalf("error = %v, want ErrTruncated", c.err)
	}
	wantContiguous(t, c.seqs, 1, 5)
	for _, want := range []string{"got through ledger 5", "requested through 20"} {
		if !strings.Contains(c.err.Error(), want) {
			t.Errorf("error %v does not say %q", c.err, want)
		}
	}
}

func TestBoundedRangeStrandedMidStreamIsTruncated(t *testing.T) {
	// Same shortfall, reached the other way: the subscriber is live on the tip
	// when the source runs out, rather than finding it already finished.
	h := startHarness(t, harnessOpts{count: 6, interval: 5 * time.Millisecond})

	c := consume(t.Context(), t, h.url, ledgerbackend.BoundedRange(1, 30), 0, nil)
	if !errors.Is(c.err, remoteledger.ErrTruncated) {
		t.Fatalf("error = %v, want ErrTruncated", c.err)
	}
	wantContiguous(t, c.seqs, 1, 6)
}

func TestUnboundedSubscriptionEndsCleanlyWhenSourceEnds(t *testing.T) {
	// An unbounded consumer asked for "everything from here", so the source
	// running out is the end of the stream, not a shortfall.
	h := startHarness(t, harnessOpts{count: 5})
	if err := h.wait(t); err != nil {
		t.Fatalf("source loop: %v", err)
	}

	c := consume(t.Context(), t, h.url, ledgerbackend.UnboundedRange(1), 0, nil)
	if c.err != nil {
		t.Fatalf("stream error: %v", c.err)
	}
	wantContiguous(t, c.seqs, 1, 5)
}

func TestReplayTerminatesAtTheSequenceCeiling(t *testing.T) {
	// The retained window ends at math.MaxUint32. A uint32 cursor stepping past
	// it would wrap to 0 and re-anchor at the tip, re-serving ledgers forever;
	// the cursor is wider than a sequence precisely so serving the ceiling
	// leaves it one past MaxUint32 and the subscription just waits out the end.
	const first = uint32(math.MaxUint32 - 2)
	h := startHarness(t, harnessOpts{start: first, count: 3})
	if err := h.wait(t); err != nil {
		t.Fatalf("source loop: %v", err)
	}
	if _, latest, _ := h.store.Bounds(); latest != math.MaxUint32 {
		t.Fatalf("retained latest = %d, want the ceiling %d", latest, uint32(math.MaxUint32))
	}

	// Unbounded, and read to the end of the stream: the replay cursor runs to the
	// ceiling instead of stopping at a requested end, and the subscription then
	// finishes on the source ending, with no error.
	c := consume(t.Context(), t, h.url, ledgerbackend.UnboundedRange(first), 0, nil)
	if c.err != nil {
		t.Fatalf("stream error after replaying through the ceiling: %v", c.err)
	}
	wantContiguous(t, c.seqs, first, math.MaxUint32)
}

func TestSourceLoopRejectsAnOversizedLedger(t *testing.T) {
	// No subscriber could read a ledger past the protocol cap — every client's
	// read limit is that cap — so the source loop must fail loudly instead of
	// publishing it.
	original := maxPayloadSize
	maxPayloadSize = 128
	t.Cleanup(func() { maxPayloadSize = original })

	ring, err := store.Open(t.TempDir(), 10)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	srv, err := New(Config{
		Source: PacedSource(NewSyntheticStream(SyntheticConfig{Size: 4096}), 0, 0),
		Range:  ledgerbackend.BoundedRange(1, 3),
		Store:  ring,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	err = srv.Run(t.Context())
	if err == nil {
		t.Fatal("the source loop published a ledger over the payload cap")
	}
	for _, want := range []string{"ledger 1", "4096", "128"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %v does not mention %q", err, want)
		}
	}
	if _, _, filled := ring.Bounds(); filled {
		t.Error("the oversized ledger was retained anyway")
	}
}

func TestReplayLosingALedgerToPruningIsTooFarBehind(t *testing.T) {
	h := startHarness(t, harnessOpts{count: 5})
	if err := h.wait(t); err != nil {
		t.Fatalf("source loop: %v", err)
	}

	// Stand in for pruning catching up with a replay in progress: the ledger is
	// inside the retained bounds, but its file is gone.
	if err := os.Remove(filepath.Join(h.dir, "ledger-3.xdr")); err != nil {
		t.Fatalf("remove ledger 3: %v", err)
	}

	c := consume(t.Context(), t, h.url, ledgerbackend.BoundedRange(1, 5), 0, nil)
	if !errors.Is(c.err, remoteledger.ErrTooFarBehind) {
		t.Fatalf("error = %v, want ErrTooFarBehind", c.err)
	}
	wantContiguous(t, c.seqs, 1, 2)
}

func TestStartAheadOfRetentionWaitsForLive(t *testing.T) {
	h := startHarness(t, harnessOpts{interval: time.Millisecond})
	h.waitForLedger(t, 3)

	// Ledger 20 is in the future: nothing to replay, so the subscriber waits.
	c := consume(t.Context(), t, h.url, ledgerbackend.BoundedRange(20, 24), 0, nil)
	if c.err != nil {
		t.Fatalf("stream error: %v", c.err)
	}
	wantContiguous(t, c.seqs, 20, 24)
}

func TestTipSubscriptionSkipsReplay(t *testing.T) {
	h := startHarness(t, harnessOpts{interval: 2 * time.Millisecond})
	h.waitForLedger(t, 4)

	// From 0 means "the next live ledger": the retained history is skipped.
	c := consume(t.Context(), t, h.url, ledgerbackend.UnboundedRange(0), 3, nil)
	if c.err != nil {
		t.Fatalf("stream error: %v", c.err)
	}
	if len(c.seqs) != 3 {
		t.Fatalf("received %d ledgers, want 3", len(c.seqs))
	}
	if c.seqs[0] <= 4 {
		t.Errorf("first ledger = %d, want one published after the subscription (>4)", c.seqs[0])
	}
	for i := 1; i < len(c.seqs); i++ {
		if c.seqs[i] != c.seqs[i-1]+1 {
			t.Errorf("sequences %v are not contiguous", c.seqs)
		}
	}
}

func TestUnboundedStreamCancelTearsDown(t *testing.T) {
	h := startHarness(t, harnessOpts{interval: time.Millisecond})
	before := runtime.NumGoroutine()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	got := 0
	c := consume(ctx, t, h.url, ledgerbackend.UnboundedRange(1), 0, func(uint32) {
		got++
		if got == 3 {
			cancel()
		}
	})
	if !errors.Is(c.err, context.Canceled) {
		t.Fatalf("error after cancel = %v, want context.Canceled", c.err)
	}
	if got < 3 {
		t.Fatalf("received %d ledgers before cancelling, want at least 3", got)
	}

	// The server side unwinds asynchronously; give it a moment before deciding
	// something leaked.
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > before+2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if after := runtime.NumGoroutine(); after > before+2 {
		t.Errorf("goroutines grew from %d to %d after teardown", before, after)
	}
}

func TestStalledSubscriberCatchesUpWithoutHurtingOthers(t *testing.T) {
	// Payloads big enough to fill the socket buffers make the stall real: the
	// server cannot write ahead, so the subscriber genuinely falls behind the
	// tip and has to be served the difference from the retention ring.
	h := startHarness(t, harnessOpts{
		count: 80, size: 256 << 10, interval: time.Millisecond, retention: 200,
	})

	healthy := make(chan *consumer, 1)
	go func() {
		healthy <- consume(t.Context(), t, h.url, ledgerbackend.UnboundedRange(1), 0, nil)
	}()

	// The stall leaves the subscriber far behind the tip when it resumes;
	// retention still covers everything, so it must catch up, not be dropped.
	stalled := consume(t.Context(), t, h.url, ledgerbackend.UnboundedRange(1), 0, stallOnFirstLedger(t, h))
	if stalled.err != nil {
		t.Fatalf("stalled subscriber error = %v, want a clean catch-up to the end", stalled.err)
	}
	wantContiguous(t, stalled.seqs, 1, 80)
	if !slices.Contains(stalled.stamps, 0) {
		t.Error("every ledger arrived with an emit stamp: the catch-up was never served from the retention ring")
	}

	// The healthy subscriber reads the whole run: had the stalled one wedged
	// the source loop, this one would sit short of 80 until consume's deadline.
	other := <-healthy
	if other.err != nil {
		t.Errorf("the subscriber that kept up failed: %v", other.err)
	}
	wantContiguous(t, other.seqs, 1, 80)
}

func TestStalledSubscriberFallingOutOfRetentionIsTooFarBehind(t *testing.T) {
	// Retention far smaller than the run: by the time the stalled subscriber
	// resumes, the ledgers it still needs are pruned. The stream must end with
	// the too-far-behind refusal — a gap disguised as a clean close would make
	// the consumer trust a stream it did not receive. Retention is 20, not
	// smaller, so ledger 1 stays retained long enough for the subscriber to
	// receive it and stall on it; and the payloads are large enough that the
	// socket buffers cannot swallow the whole run while nobody reads.
	h := startHarness(t, harnessOpts{
		count: 100, size: 256 << 10, interval: time.Millisecond, retention: 20,
	})

	c := consume(t.Context(), t, h.url, ledgerbackend.UnboundedRange(1), 0, stallOnFirstLedger(t, h))
	if !errors.Is(c.err, remoteledger.ErrTooFarBehind) {
		t.Fatalf("error = %v, want ErrTooFarBehind", c.err)
	}
	var tfb *remoteledger.TooFarBehindError
	if !errors.As(c.err, &tfb) {
		t.Fatalf("error %v does not carry a TooFarBehindError", c.err)
	}
	// The exact bounds depend on when the cursor lost the pruning race, so
	// assert the shape: a full-width window that has moved past ledger 1.
	if tfb.Latest-tfb.Oldest+1 != 20 || tfb.Oldest <= 1 {
		t.Errorf("reported retention [%d,%d], want a 20-ledger window past ledger 1", tfb.Oldest, tfb.Latest)
	}
	// Whatever arrived before the refusal must be gapless from the start.
	if len(c.seqs) == 0 {
		t.Fatal("received no ledgers, want a stream starting at ledger 1")
	}
	wantContiguous(t, c.seqs, 1, uint32(len(c.seqs)))
	if len(c.seqs) >= 100 {
		t.Errorf("received all %d ledgers, want the stream cut short by pruning", len(c.seqs))
	}
}

func TestManySubscribersShareTheStream(t *testing.T) {
	// Every subscriber shares the flow's chunk messages, and the race detector
	// only sees that sharing when many goroutines do it at once. Payload
	// equality is the corruption check. The emit window keeps flows open long
	// enough that subscribers reliably join some of them mid-emission — the
	// stamped live path — as well as after completion (the stampless path).
	h := startHarness(t, harnessOpts{count: 50, size: 4 << 10, interval: time.Millisecond, emitWindow: 2 * time.Millisecond})

	const subscribers = 8
	results := make(chan *consumer, subscribers)
	for range subscribers {
		go func() {
			results <- consume(t.Context(), t, h.url, ledgerbackend.UnboundedRange(1), 0, nil)
		}()
	}
	anyStamped := false
	for range subscribers {
		c := <-results
		if c.err != nil {
			t.Errorf("subscriber failed: %v", c.err)
			continue
		}
		wantContiguous(t, c.seqs, 1, 50)
		for i, seq := range c.seqs {
			if !bytes.Equal(c.payloads[i], h.payload(seq)) {
				t.Errorf("ledger %d payload does not match what the source produced", seq)
				break
			}
			anyStamped = anyStamped || c.stamps[i] != 0
		}
	}
	// If no ledger anywhere carried a stamp, nobody was ever served the shared
	// tip message and this test silently stopped exercising the sharing.
	if !anyStamped {
		t.Error("no subscriber received a stamped ledger: the tip fast path was never taken")
	}
}

func TestLargeLedgerRoundTrips(t *testing.T) {
	// Well past the SDK's 10 MiB pipe read buffer and the client's default
	// 32 KiB WebSocket read limit, which the client raises for exactly this.
	const size = 12 << 20
	h := startHarness(t, harnessOpts{count: 2, size: size})
	if err := h.wait(t); err != nil {
		t.Fatalf("source loop: %v", err)
	}

	c := consume(t.Context(), t, h.url, ledgerbackend.BoundedRange(1, 2), 0, nil)
	if c.err != nil {
		t.Fatalf("stream error: %v", c.err)
	}
	wantContiguous(t, c.seqs, 1, 2)
	for i, seq := range c.seqs {
		if len(c.payloads[i]) != size {
			t.Fatalf("ledger %d arrived with %d bytes, want %d", seq, len(c.payloads[i]), size)
		}
		if !bytes.Equal(c.payloads[i], h.payload(seq)) {
			t.Errorf("ledger %d payload does not match what the source produced", seq)
		}
	}
}

func TestCompletedFlowServedToLateArrivalIsStampless(t *testing.T) {
	// The current flow is retained in memory after it completes. A subscriber
	// that reaches it only then — the README's own "bound the range just
	// ahead of latest" methodology produces exactly this — was not following
	// live, so handing it the flow's original stamps would record the flow's
	// age (up to a whole cadence) as a delivery latency. It must arrive
	// stampless, like any other redelivery of history.
	h := startHarness(t, harnessOpts{count: 5})
	if err := h.wait(t); err != nil {
		t.Fatalf("source loop: %v", err)
	}

	c := consume(t.Context(), t, h.url, ledgerbackend.BoundedRange(5, 5), 0, nil)
	if c.err != nil {
		t.Fatalf("stream error: %v", c.err)
	}
	wantContiguous(t, c.seqs, 5, 5)
	if !bytes.Equal(c.payloads[0], h.payload(5)) {
		t.Error("the stampless redelivery does not match what the source produced")
	}
	if c.stamps[0] != 0 {
		t.Errorf("ledger 5 arrived with emit stamp %d; a post-completion arrival must be stampless", c.stamps[0])
	}
	if _, ok := c.infos[0].Delivery(); ok {
		t.Error("a post-completion arrival contributed a delivery sample")
	}
}

func TestHealthz(t *testing.T) {
	h := startHarness(t, harnessOpts{count: 5})
	if err := h.wait(t); err != nil {
		t.Fatalf("source loop: %v", err)
	}

	resp, err := http.Get(h.url + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var got struct {
		Oldest      uint32 `json:"oldest"`
		Latest      uint32 `json:"latest"`
		Subscribers int    `json:"subscribers"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if got.Oldest != 1 || got.Latest != 5 {
		t.Errorf("retained [%d,%d], want [1,5]", got.Oldest, got.Latest)
	}
	if got.Subscribers != 0 {
		t.Errorf("subscribers = %d, want 0", got.Subscribers)
	}
}

func TestStreamRejectsBadQuery(t *testing.T) {
	h := startHarness(t, harnessOpts{count: 1})
	for _, query := range []string{
		"?start=abc", "?end=xyz", "?start=9&end=4", "?end=0",
		// The protocol has no sequence wrap, so the last representable ledger is
		// not streamable from either parameter.
		"?start=4294967295", "?end=4294967295", "?start=4294967295&end=4294967295",
	} {
		resp, err := http.Get(h.url + "/stream" + query)
		if err != nil {
			t.Fatalf("GET %s: %v", query, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("GET %s status = %d, want 400", query, resp.StatusCode)
		}
	}
}

func TestLiveDeliveryCarriesFlowStamps(t *testing.T) {
	// The whole point of chunking: a live ledger arrives as many chunks with both
	// emit stamps, and T_emit spans most of the emission window — proof the
	// transfer overlapped emission instead of following it.
	const (
		size       = 64 << 10
		chunkSize  = 4 << 10
		emitWindow = 30 * time.Millisecond
	)
	h := startHarness(t, harnessOpts{
		size: size, chunkSize: chunkSize, emitWindow: emitWindow, interval: time.Millisecond,
	})

	c := consume(t.Context(), t, h.url, ledgerbackend.UnboundedRange(0), 3, nil)
	if c.err != nil {
		t.Fatalf("stream error: %v", c.err)
	}
	if len(c.infos) != 3 {
		t.Fatalf("received %d ledgers, want 3", len(c.infos))
	}
	for _, info := range c.infos {
		if !bytes.Equal(c.payloads[slices.Index(c.seqs, info.Sequence)], h.payload(info.Sequence)) {
			t.Errorf("ledger %d payload does not match what the source produced", info.Sequence)
		}
		if want := size / chunkSize; info.Chunks != want {
			t.Errorf("ledger %d arrived in %d chunks, want %d", info.Sequence, info.Chunks, want)
		}
		if _, ok := info.Delivery(); !ok {
			t.Errorf("ledger %d carries no delivery measurement; it was received live", info.Sequence)
		}
		emit, ok := info.EmitWindow()
		if !ok {
			t.Errorf("ledger %d carries no emit window", info.Sequence)
			continue
		}
		if emit < emitWindow/2 || emit > 10*emitWindow {
			t.Errorf("ledger %d T_emit = %s, want roughly the %s window", info.Sequence, emit, emitWindow)
		}
		if pipeline, ok := info.Pipeline(); !ok || pipeline < emit {
			t.Errorf("ledger %d pipeline = (%s, %v), want at least T_emit %s", info.Sequence, pipeline, ok, emit)
		}
	}
}

func TestSlowSubscriberIsAbandonedMidLedgerAndRecoversFromTheRing(t *testing.T) {
	// Ledgers far bigger than any socket buffering, emitted over a window: the
	// stalled subscriber's server-side writes block mid-ledger, the source
	// runs ahead regardless, and on resume the flow has moved past the ledger
	// the subscriber was inside — so its live flow is abandoned and the ledger
	// is redelivered complete from the ring. The client must discard the
	// partial and end up with every ledger intact.
	const count = 5
	h := startHarness(t, harnessOpts{
		count: count, size: 8 << 20, interval: time.Millisecond, emitWindow: 50 * time.Millisecond,
		retention: 100,
	})

	c := consume(t.Context(), t, h.url, ledgerbackend.UnboundedRange(1), 0, func(seq uint32) {
		if seq == 1 {
			// Stall until the source has produced everything: the ledger the
			// server is currently writing to us is by then far behind the flow.
			if err := h.wait(t); err != nil {
				t.Errorf("source loop: %v", err)
			}
		}
	})
	if c.err != nil {
		t.Fatalf("stream error: %v", c.err)
	}
	wantContiguous(t, c.seqs, 1, count)
	for i, seq := range c.seqs {
		if !bytes.Equal(c.payloads[i], h.payload(seq)) {
			t.Errorf("ledger %d payload does not match what the source produced", seq)
		}
	}
	if c.discarded() == 0 {
		t.Error("no partial assembly was discarded: the abandon path was never exercised")
	}
	if h.srv.LiveAbandons() == 0 {
		t.Error("the server counted no live-flow abandons")
	}
	for i, info := range c.infos {
		if info.DiscardedPartials > 0 && c.stamps[i] != 0 {
			t.Errorf("ledger %d replaced a partial but carries an emit stamp; a ring redelivery must not", info.Sequence)
		}
	}
}

func TestParseStreamRequest(t *testing.T) {
	tests := []struct {
		name    string
		query   map[string][]string
		want    streamRequest
		wantErr bool
	}{
		{"empty is a tip subscription", nil, streamRequest{}, false},
		{"start only", map[string][]string{"start": {"7"}}, streamRequest{start: 7}, false},
		{"start and end", map[string][]string{"start": {"7"}, "end": {"9"}},
			streamRequest{start: 7, end: 9, bounded: true}, false},
		{"tip with an end", map[string][]string{"end": {"9"}}, streamRequest{end: 9, bounded: true}, false},
		{"blank values", map[string][]string{"start": {""}, "end": {""}}, streamRequest{}, false},
		{"end before start", map[string][]string{"start": {"9"}, "end": {"4"}}, streamRequest{}, true},
		{"zero end", map[string][]string{"end": {"0"}}, streamRequest{}, true},
		{"unparseable start", map[string][]string{"start": {"x"}}, streamRequest{}, true},
		{"start too large", map[string][]string{"start": {"4294967296"}}, streamRequest{}, true},
		{"start at the ceiling", map[string][]string{"start": {"4294967295"}}, streamRequest{}, true},
		{"end at the ceiling", map[string][]string{"start": {"1"}, "end": {"4294967295"}}, streamRequest{}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseStreamRequest(tt.query)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parseStreamRequest(%v) succeeded, want an error", tt.query)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseStreamRequest(%v): %v", tt.query, err)
			}
			if got != tt.want {
				t.Errorf("got %+v, want %+v", got, tt.want)
			}
		})
	}
}
