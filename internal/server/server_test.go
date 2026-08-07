package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"

	"github.com/stellar/remote-core-poc/internal/store"
	"github.com/stellar/remote-core-poc/remoteledger"
)

// consumer collects sequences, payload copies and any error from one
// subscription. It is the assertion surface of the end-to-end tests: what a real
// consumer of the seam sees.
type consumer struct {
	seqs     []uint32
	payloads [][]byte
	err      error
}

// consume subscribes to h over rng and reads until the stream ends, limit
// ledgers have arrived (limit > 0), or onLedger asks it to stop.
func consume(
	ctx context.Context, t *testing.T, url string, rng ledgerbackend.Range, limit int, onLedger func(seq uint32),
) *consumer {
	t.Helper()
	c := &consumer{}
	var seq uint32
	stream := remoteledger.New(url, remoteledger.WithObserver(func(info remoteledger.LedgerInfo) {
		seq = info.Sequence
	}))
	for raw, err := range stream.RawLedgers(ctx, rng) {
		if err != nil {
			c.err = err
			return c
		}
		c.seqs = append(c.seqs, seq)
		c.payloads = append(c.payloads, bytes.Clone(raw))
		if onLedger != nil {
			onLedger(seq)
		}
		if limit > 0 && len(c.seqs) >= limit {
			break
		}
	}
	return c
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
	src := NewSyntheticStream(SyntheticConfig{})
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

func TestSlowSubscriberIsDroppedWithoutHurtingOthers(t *testing.T) {
	// A queue of one and payloads big enough to fill the socket buffers is what
	// makes falling behind unavoidable for the subscriber that stops reading.
	h := startHarness(t, harnessOpts{
		count: 80, size: 256 << 10, interval: time.Millisecond, buffer: 1, retention: 200,
	})

	healthy := make(chan *consumer, 1)
	go func() {
		healthy <- consume(t.Context(), t, h.url, ledgerbackend.UnboundedRange(1), 5, nil)
	}()

	stalled := consume(t.Context(), t, h.url, ledgerbackend.UnboundedRange(1), 0, func(seq uint32) {
		if seq == 1 {
			// Long enough for the source to overrun a one-deep queue.
			time.Sleep(500 * time.Millisecond)
		}
	})
	if !errors.Is(stalled.err, remoteledger.ErrSlowConsumer) {
		t.Fatalf("stalled subscriber error = %v, want ErrSlowConsumer", stalled.err)
	}
	for i := 1; i < len(stalled.seqs); i++ {
		if stalled.seqs[i] != stalled.seqs[i-1]+1 {
			t.Fatalf("stalled subscriber saw a gap before being dropped: %v", stalled.seqs)
		}
	}

	other := <-healthy
	if other.err != nil {
		t.Errorf("the subscriber that kept up failed: %v", other.err)
	}
	if len(other.seqs) != 5 {
		t.Errorf("the subscriber that kept up received %d ledgers, want 5", len(other.seqs))
	}
	for i := 1; i < len(other.seqs); i++ {
		if other.seqs[i] != other.seqs[i-1]+1 {
			t.Errorf("the subscriber that kept up saw a gap: %v", other.seqs)
		}
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
		resp, err := http.Get(h.url + "/v1/stream" + query)
		if err != nil {
			t.Fatalf("GET %s: %v", query, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("GET %s status = %d, want 400", query, resp.StatusCode)
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
