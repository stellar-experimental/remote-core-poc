package remoteledger

import (
	"bytes"
	"context"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"

	"github.com/stellar/remote-core-poc/internal/wire"
)

// The seam this whole prototype exists to satisfy.
var _ ledgerbackend.LedgerStream = (*Stream)(nil)

// stub is a server that sends exactly what a test tells it to, including the
// malformed and hostile cases a real corestreamd never produces.
func stub(t *testing.T, serve func(ctx context.Context, conn *websocket.Conn, q url.Values)) string {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("stub accept: %v", err)
			return
		}
		defer conn.CloseNow()
		serve(r.Context(), conn, r.URL.Query())
	}))
	t.Cleanup(ts.Close)
	return ts.URL
}

// sendLedgers writes one message per sequence, with the given payload size.
func sendLedgers(t *testing.T, conn *websocket.Conn, ctx context.Context, size int, seqs ...uint32) {
	t.Helper()
	for _, seq := range seqs {
		payload := bytes.Repeat([]byte{byte(seq)}, size)
		msg := wire.AppendLedger(nil, seq, time.Now().UnixNano(), payload)
		if err := conn.Write(ctx, websocket.MessageBinary, msg); err != nil {
			return // the consumer went away; that is the test's business
		}
	}
}

// drain consumes a stream and reports the sequences seen (by payload byte),
// payload sizes, and the error iteration ended with.
type result struct {
	sizes []int
	first byte
	err   error
	count int
}

func drain(ctx context.Context, s *Stream, rng ledgerbackend.Range) result {
	var r result
	for raw, err := range s.RawLedgers(ctx, rng) {
		if err != nil {
			r.err = err
			return r
		}
		if r.count == 0 && len(raw) > 0 {
			r.first = raw[0]
		}
		r.sizes = append(r.sizes, len(raw))
		r.count++
	}
	return r
}

func TestRawLedgersDeliversBoundedRange(t *testing.T) {
	url := stub(t, func(ctx context.Context, conn *websocket.Conn, q url.Values) {
		if got := q.Get("start"); got != "5" {
			t.Errorf("start parameter = %q, want 5", got)
		}
		if got := q.Get("end"); got != "7" {
			t.Errorf("end parameter = %q, want 7", got)
		}
		sendLedgers(t, conn, ctx, 64, 5, 6, 7)
	})

	r := drain(t.Context(), New(url), ledgerbackend.BoundedRange(5, 7))
	if r.err != nil {
		t.Fatalf("stream error: %v", r.err)
	}
	if r.count != 3 {
		t.Errorf("received %d ledgers, want 3", r.count)
	}
	if r.first != 5 {
		t.Errorf("first payload byte = %d, want 5", r.first)
	}
}

func TestRawLedgersStopsAtRangeEnd(t *testing.T) {
	// The server keeps going; a bounded range must stop the consumer anyway.
	url := stub(t, func(ctx context.Context, conn *websocket.Conn, _ url.Values) {
		sendLedgers(t, conn, ctx, 32, 1, 2, 3, 4, 5)
	})
	r := drain(t.Context(), New(url), ledgerbackend.BoundedRange(1, 3))
	if r.err != nil {
		t.Fatalf("stream error: %v", r.err)
	}
	if r.count != 3 {
		t.Errorf("received %d ledgers, want 3", r.count)
	}
}

func TestRawLedgersDetectsGap(t *testing.T) {
	url := stub(t, func(ctx context.Context, conn *websocket.Conn, _ url.Values) {
		sendLedgers(t, conn, ctx, 32, 5, 6, 8)
	})
	r := drain(t.Context(), New(url), ledgerbackend.UnboundedRange(5))
	if !errors.Is(r.err, ErrGap) {
		t.Fatalf("error = %v, want ErrGap", r.err)
	}
	if r.count != 2 {
		t.Errorf("received %d ledgers before the gap, want 2", r.count)
	}
	if !strings.Contains(r.err.Error(), "expected 7") {
		t.Errorf("error %v does not say which ledger was expected", r.err)
	}
}

func TestRawLedgersDetectsWrongFirstLedger(t *testing.T) {
	url := stub(t, func(ctx context.Context, conn *websocket.Conn, _ url.Values) {
		sendLedgers(t, conn, ctx, 32, 11)
	})
	r := drain(t.Context(), New(url), ledgerbackend.UnboundedRange(10))
	if !errors.Is(r.err, ErrGap) {
		t.Fatalf("error = %v, want ErrGap", r.err)
	}
}

func TestRawLedgersFromTipAcceptsAnyFirstLedger(t *testing.T) {
	url := stub(t, func(ctx context.Context, conn *websocket.Conn, q url.Values) {
		if got := q.Get("start"); got != "0" {
			t.Errorf("start parameter = %q, want 0", got)
		}
		if _, ok := q["end"]; ok {
			t.Error("an unbounded range sent an end parameter")
		}
		sendLedgers(t, conn, ctx, 32, 900, 901)
		_ = conn.Close(websocket.StatusNormalClosure, "")
	})
	r := drain(t.Context(), New(url), ledgerbackend.UnboundedRange(0))
	if r.err != nil {
		t.Fatalf("stream error: %v", r.err)
	}
	if r.count != 2 {
		t.Errorf("received %d ledgers, want 2", r.count)
	}
}

func TestRawLedgersNormalCloseEndsIteration(t *testing.T) {
	url := stub(t, func(ctx context.Context, conn *websocket.Conn, _ url.Values) {
		sendLedgers(t, conn, ctx, 16, 1, 2)
		_ = conn.Close(websocket.StatusNormalClosure, "source ended")
	})
	r := drain(t.Context(), New(url), ledgerbackend.UnboundedRange(1))
	if r.err != nil {
		t.Fatalf("a normal close surfaced as an error: %v", r.err)
	}
	if r.count != 2 {
		t.Errorf("received %d ledgers, want 2", r.count)
	}
}

func TestRawLedgersTruncatedBoundedRange(t *testing.T) {
	// A clean close is not the end of the story for a bounded range: the contract
	// is every ledger in it, so a short delivery must not look like completion.
	url := stub(t, func(ctx context.Context, conn *websocket.Conn, _ url.Values) {
		sendLedgers(t, conn, ctx, 32, 1, 2)
		_ = conn.Close(websocket.StatusNormalClosure, "source ended")
	})
	r := drain(t.Context(), New(url), ledgerbackend.BoundedRange(1, 5))
	if !errors.Is(r.err, ErrTruncated) {
		t.Fatalf("error = %v, want ErrTruncated", r.err)
	}
	if r.count != 2 {
		t.Errorf("received %d ledgers, want the 2 that arrived", r.count)
	}
	for _, want := range []string{"got through ledger 2", "requested through 5"} {
		if !strings.Contains(r.err.Error(), want) {
			t.Errorf("error %v does not say %q", r.err, want)
		}
	}
}

func TestRawLedgersTruncatedWithNothingDelivered(t *testing.T) {
	url := stub(t, func(_ context.Context, conn *websocket.Conn, _ url.Values) {
		_ = conn.Close(websocket.StatusNormalClosure, "source ended")
	})
	r := drain(t.Context(), New(url), ledgerbackend.BoundedRange(7, 9))
	if !errors.Is(r.err, ErrTruncated) {
		t.Fatalf("error = %v, want ErrTruncated", r.err)
	}
	for _, want := range []string{"received no ledgers", "[7,9]"} {
		if !strings.Contains(r.err.Error(), want) {
			t.Errorf("error %v does not say %q", r.err, want)
		}
	}
}

func TestRawLedgersTruncatedOnGoingAway(t *testing.T) {
	// What corestreamd itself sends when its source ends mid-range.
	url := stub(t, func(ctx context.Context, conn *websocket.Conn, _ url.Values) {
		sendLedgers(t, conn, ctx, 32, 4)
		_ = conn.Close(websocket.StatusGoingAway, "source ended before range complete")
	})
	r := drain(t.Context(), New(url), ledgerbackend.BoundedRange(4, 6))
	if !errors.Is(r.err, ErrTruncated) {
		t.Fatalf("error = %v, want ErrTruncated", r.err)
	}
	if !strings.Contains(r.err.Error(), "got through ledger 4") {
		t.Errorf("error %v does not name the last ledger received", r.err)
	}
}

func TestRawLedgersCompleteBoundedRangeHasNoError(t *testing.T) {
	// The regression the truncation check must not break: a range delivered in
	// full ends silently, even when the server closes right after it.
	url := stub(t, func(ctx context.Context, conn *websocket.Conn, _ url.Values) {
		sendLedgers(t, conn, ctx, 32, 1, 2, 3)
		_ = conn.Close(websocket.StatusNormalClosure, "range complete")
	})
	r := drain(t.Context(), New(url), ledgerbackend.BoundedRange(1, 3))
	if r.err != nil {
		t.Fatalf("a fully delivered range yielded %v", r.err)
	}
	if r.count != 3 {
		t.Errorf("received %d ledgers, want 3", r.count)
	}
}

func TestRawLedgersTipBoundedCloseStaysClean(t *testing.T) {
	// From 0 the consumer never named a first ledger, so a clean close with
	// nothing delivered describes no shortfall and is reported as the end.
	url := stub(t, func(_ context.Context, conn *websocket.Conn, _ url.Values) {
		_ = conn.Close(websocket.StatusNormalClosure, "source ended")
	})
	r := drain(t.Context(), New(url), ledgerbackend.BoundedRange(0, 5))
	if r.err != nil {
		t.Fatalf("error = %v, want the close treated as the end", r.err)
	}
	if r.count != 0 {
		t.Errorf("received %d ledgers, want none", r.count)
	}
}

func TestRawLedgersAbruptCloseIsAnError(t *testing.T) {
	// No close frame at all: not truncation, but it must not pass for completion.
	url := stub(t, func(ctx context.Context, conn *websocket.Conn, _ url.Values) {
		sendLedgers(t, conn, ctx, 32, 1)
		_ = conn.CloseNow()
	})
	r := drain(t.Context(), New(url), ledgerbackend.BoundedRange(1, 4))
	if r.err == nil {
		t.Fatal("a dropped connection ended iteration with no error")
	}
}

func TestRawLedgersTooFarBehind(t *testing.T) {
	url := stub(t, func(_ context.Context, conn *websocket.Conn, _ url.Values) {
		_ = conn.Close(wire.StatusTooFarBehind, wire.TooFarBehindReason(500, 900))
	})
	r := drain(t.Context(), New(url), ledgerbackend.UnboundedRange(100))
	if !errors.Is(r.err, ErrTooFarBehind) {
		t.Fatalf("error = %v, want ErrTooFarBehind", r.err)
	}
	var tfb *TooFarBehindError
	if !errors.As(r.err, &tfb) {
		t.Fatalf("error %v does not carry a TooFarBehindError", r.err)
	}
	if tfb.Requested != 100 || tfb.Oldest != 500 || tfb.Latest != 900 {
		t.Errorf("got requested %d retention [%d,%d], want 100 and [500,900]", tfb.Requested, tfb.Oldest, tfb.Latest)
	}
	if !strings.Contains(tfb.Error(), "[500,900]") {
		t.Errorf("message %q does not state the retained range", tfb.Error())
	}
}

func TestRawLedgersTooFarBehindWithoutBounds(t *testing.T) {
	url := stub(t, func(_ context.Context, conn *websocket.Conn, _ url.Values) {
		_ = conn.Close(wire.StatusTooFarBehind, "nope")
	})
	r := drain(t.Context(), New(url), ledgerbackend.UnboundedRange(100))
	var tfb *TooFarBehindError
	if !errors.As(r.err, &tfb) {
		t.Fatalf("error = %v, want a TooFarBehindError", r.err)
	}
	if tfb.Oldest != 0 || tfb.Latest != 0 {
		t.Errorf("bounds = [%d,%d], want them left unknown", tfb.Oldest, tfb.Latest)
	}
	if !strings.Contains(tfb.Error(), "requested 100") {
		t.Errorf("message %q does not state what was requested", tfb.Error())
	}
}

func TestRawLedgersSlowConsumer(t *testing.T) {
	url := stub(t, func(ctx context.Context, conn *websocket.Conn, _ url.Values) {
		sendLedgers(t, conn, ctx, 16, 1)
		_ = conn.Close(wire.StatusSlowConsumer, "subscriber too slow")
	})
	r := drain(t.Context(), New(url), ledgerbackend.UnboundedRange(1))
	if !errors.Is(r.err, ErrSlowConsumer) {
		t.Fatalf("error = %v, want ErrSlowConsumer", r.err)
	}
	if r.count != 1 {
		t.Errorf("received %d ledgers before being dropped, want 1", r.count)
	}
}

func TestRawLedgersRejectsBadMessages(t *testing.T) {
	tests := []struct {
		name string
		send func(ctx context.Context, conn *websocket.Conn)
		want string
	}{
		{"text message", func(ctx context.Context, conn *websocket.Conn) {
			_ = conn.Write(ctx, websocket.MessageText, []byte("hello"))
		}, "unexpected"},
		{"truncated header", func(ctx context.Context, conn *websocket.Conn) {
			_ = conn.Write(ctx, websocket.MessageBinary, []byte{0x01, 0x01})
		}, "shorter than header"},
		{"wrong version", func(ctx context.Context, conn *websocket.Conn) {
			msg := wire.AppendLedger(nil, 1, 0, []byte("x"))
			msg[0] = 0x09
			_ = conn.Write(ctx, websocket.MessageBinary, msg)
		}, "version"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			url := stub(t, func(ctx context.Context, conn *websocket.Conn, _ url.Values) {
				tt.send(ctx, conn)
			})
			r := drain(t.Context(), New(url), ledgerbackend.UnboundedRange(1))
			if r.err == nil {
				t.Fatal("stream accepted a malformed message")
			}
			if !strings.Contains(r.err.Error(), tt.want) {
				t.Errorf("error = %v, want it to mention %q", r.err, tt.want)
			}
		})
	}
}

func TestRawLedgersYieldsBorrowedSlices(t *testing.T) {
	url := stub(t, func(ctx context.Context, conn *websocket.Conn, _ url.Values) {
		sendLedgers(t, conn, ctx, 64, 1, 2)
	})
	var borrowed []byte
	var copied []byte
	for raw, err := range New(url).RawLedgers(t.Context(), ledgerbackend.BoundedRange(1, 2)) {
		if err != nil {
			t.Fatalf("stream error: %v", err)
		}
		if borrowed == nil {
			borrowed = raw
			copied = bytes.Clone(raw)
		}
	}
	if bytes.Equal(borrowed, copied) {
		t.Error("the yielded slice survived the next iteration step; it is documented as borrowed")
	}
}

func TestRawLedgersHonoursMaxMessageSize(t *testing.T) {
	url := stub(t, func(ctx context.Context, conn *websocket.Conn, _ url.Values) {
		sendLedgers(t, conn, ctx, 4096, 1)
	})
	r := drain(t.Context(), New(url, WithMaxMessageSize(512)), ledgerbackend.UnboundedRange(1))
	if r.err == nil {
		t.Fatal("a message over the limit was accepted")
	}
}

func TestRawLedgersObserverSeesStamps(t *testing.T) {
	const emit int64 = 1700000000000000000
	url := stub(t, func(ctx context.Context, conn *websocket.Conn, _ url.Values) {
		_ = conn.Write(ctx, websocket.MessageBinary, wire.AppendLedger(nil, 4, emit, make([]byte, 100)))
		_ = conn.Write(ctx, websocket.MessageBinary, wire.AppendLedger(nil, 5, 0, make([]byte, 100)))
		_ = conn.Close(websocket.StatusNormalClosure, "")
	})

	var infos []LedgerInfo
	stream := New(url, WithObserver(func(info LedgerInfo) { infos = append(infos, info) }))
	if r := drain(t.Context(), stream, ledgerbackend.UnboundedRange(4)); r.err != nil {
		t.Fatalf("stream error: %v", r.err)
	}
	if len(infos) != 2 {
		t.Fatalf("observer saw %d ledgers, want 2", len(infos))
	}
	if infos[0].Sequence != 4 || infos[0].EmitUnixNano != emit || infos[0].Size != 100 {
		t.Errorf("first ledger info = %+v, want sequence 4, the emit stamp and 100 bytes", infos[0])
	}
	if d, ok := infos[0].Delivery(); !ok || d != time.Duration(infos[0].ReceivedUnixNano-emit) {
		t.Errorf("delivery = (%s, %v), want the receive-minus-emit difference", d, ok)
	}
	// A replayed ledger carries no stamp, so it has no delivery latency.
	if _, ok := infos[1].Delivery(); ok {
		t.Error("a ledger without an emit stamp reported a delivery latency")
	}
}

func TestRawLedgersCancelTearsDown(t *testing.T) {
	url := stub(t, func(ctx context.Context, conn *websocket.Conn, _ url.Values) {
		sendLedgers(t, conn, ctx, 32, 1)
		<-ctx.Done() // hold the stream open until the client leaves
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	got := 0
	var streamErr error
	for _, err := range New(url).RawLedgers(ctx, ledgerbackend.UnboundedRange(1)) {
		if err != nil {
			streamErr = err
			break
		}
		got++
		cancel()
	}
	if got != 1 {
		t.Errorf("received %d ledgers, want 1", got)
	}
	if !errors.Is(streamErr, context.Canceled) {
		t.Errorf("error after cancel = %v, want context.Canceled", streamErr)
	}
}

func TestRawLedgersIgnoresStreamOptions(t *testing.T) {
	// StreamOption mutates the SDK's unexported streamConfig, so an outside
	// implementation cannot read one. Accepting and ignoring them is documented,
	// and must not break the stream.
	url := stub(t, func(ctx context.Context, conn *websocket.Conn, _ url.Values) {
		sendLedgers(t, conn, ctx, 32, 1, 2)
	})
	count := 0
	for _, err := range New(url).RawLedgers(t.Context(), ledgerbackend.BoundedRange(1, 2),
		ledgerbackend.WithStreamMetrics(nil, "test")) {
		if err != nil {
			t.Fatalf("stream error with options: %v", err)
		}
		count++
	}
	if count != 2 {
		t.Errorf("received %d ledgers with options passed, want 2", count)
	}
}

func TestDialFailures(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{"bad scheme", "ftp://example.invalid"},
		{"no host", "ws://"},
		{"unparseable", "ws://[::1"},
		{"nothing listening", "ws://127.0.0.1:1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := drain(t.Context(), New(tt.url, WithDialTimeout(2*time.Second)), ledgerbackend.UnboundedRange(1))
			if r.err == nil {
				t.Fatalf("New(%q) streamed successfully, want a dial error", tt.url)
			}
		})
	}
}

func TestEndpointScrubsStaleRangeParams(t *testing.T) {
	// The range owns start and end. A URL configured with either must not leak
	// into a call that asked for something else; unrelated parameters survive.
	stream := New("ws://box:8462/v1/stream?start=9&end=5&token=abc")

	got, err := stream.endpoint(ledgerbackend.UnboundedRange(10))
	if err != nil {
		t.Fatalf("endpoint: %v", err)
	}
	if want := "ws://box:8462/v1/stream?start=10&token=abc"; got != want {
		t.Errorf("endpoint = %q, want %q", got, want)
	}

	got, err = stream.endpoint(ledgerbackend.BoundedRange(2, 3))
	if err != nil {
		t.Fatalf("endpoint: %v", err)
	}
	if want := "ws://box:8462/v1/stream?end=3&start=2&token=abc"; got != want {
		t.Errorf("endpoint = %q, want %q", got, want)
	}
}

func TestEndpointRejects(t *testing.T) {
	tests := []struct {
		name string
		url  string
		rng  ledgerbackend.Range
	}{
		{"bad scheme", "ftp://box", ledgerbackend.UnboundedRange(1)},
		{"no host", "ws://", ledgerbackend.UnboundedRange(1)},
		{"unparseable", "ws://[::1", ledgerbackend.UnboundedRange(1)},
		// Sequences are uint32 with no wrap: the last representable ledger has no
		// successor to ask for, so the edge is refused rather than half-served.
		{"start at the ceiling", "ws://box", ledgerbackend.UnboundedRange(math.MaxUint32)},
		{"end at the ceiling", "ws://box", ledgerbackend.BoundedRange(5, math.MaxUint32)},
		{"single ledger at the ceiling", "ws://box", ledgerbackend.SingleLedgerRange(math.MaxUint32)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, err := New(tt.url).endpoint(tt.rng); err == nil {
				t.Errorf("endpoint = %q, want an error", got)
			}
		})
	}
}

func TestRawLedgersRejectsCeilingRangeBeforeDialing(t *testing.T) {
	// Nothing is listening: the range must be refused before any connection.
	r := drain(t.Context(), New("ws://127.0.0.1:1"), ledgerbackend.BoundedRange(5, math.MaxUint32))
	if r.err == nil {
		t.Fatal("a range reaching the sequence ceiling was accepted")
	}
	if strings.Contains(r.err.Error(), "dial") {
		t.Errorf("error = %v, want the range refused before dialling", r.err)
	}
}

func TestRawLedgersRejectsSequenceWrap(t *testing.T) {
	// A tip subscription accepts any first ledger, including the ceiling. What it
	// must not accept is a ledger after it: expected wraps to 0 there, the value
	// that means "any first ledger", so a wrap would re-open the stream.
	url := stub(t, func(ctx context.Context, conn *websocket.Conn, _ url.Values) {
		sendLedgers(t, conn, ctx, 32, math.MaxUint32, 1)
	})
	r := drain(t.Context(), New(url), ledgerbackend.UnboundedRange(0))
	if !errors.Is(r.err, ErrGap) {
		t.Fatalf("error = %v, want ErrGap", r.err)
	}
	if r.count != 1 {
		t.Errorf("received %d ledgers, want the 1 before the wrap", r.count)
	}
	if !strings.Contains(r.err.Error(), "last representable") {
		t.Errorf("error %v does not explain the wrap", r.err)
	}
}

func TestReadLimitAdmitsTheLargestPayload(t *testing.T) {
	// The read limit is a whole-message ceiling, so capping it at the payload size
	// would reject the largest ledger by exactly the header.
	if got, want := New("ws://box").readLimit(), wire.DefaultMaxPayloadSize+wire.HeaderSize; got != want {
		t.Errorf("default read limit = %d, want %d (256 MiB payload plus header)", got, want)
	}
	if got := New("ws://box", WithMaxMessageSize(4096)).readLimit(); got != 4096+wire.HeaderSize {
		t.Errorf("read limit for a 4096-byte cap = %d, want %d", got, 4096+wire.HeaderSize)
	}
	if got := New("ws://box", WithMaxMessageSize(-1)).readLimit(); got != -1 {
		t.Errorf("read limit for a negative cap = %d, want -1 (no limit)", got)
	}
}

func TestReadLimitSaturatesInsteadOfOverflowing(t *testing.T) {
	// Adding the header to a cap near the int64 ceiling would wrap to a negative
	// number, which is how the library spells "no limit at all" — the one thing an
	// enormous cap must not turn into.
	tests := []struct {
		name string
		cap  int64
		want int64
	}{
		{"the int64 ceiling", math.MaxInt64, math.MaxInt64},
		{"one header short of it", math.MaxInt64 - wire.HeaderSize, math.MaxInt64},
		{"two headers short of it", math.MaxInt64 - wire.HeaderSize - 1, math.MaxInt64 - 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := New("ws://box", WithMaxMessageSize(tt.cap)).readLimit()
			if got != tt.want {
				t.Errorf("read limit for a cap of %d = %d, want %d", tt.cap, got, tt.want)
			}
			if got < 0 {
				t.Errorf("read limit for a cap of %d is negative, which disables the cap", tt.cap)
			}
		})
	}
}

func TestRawLedgersPayloadAtTheLimit(t *testing.T) {
	const limit = 64 << 10
	tests := []struct {
		name    string
		size    int
		wantErr bool
	}{
		{"exactly at the limit", limit, false},
		{"one byte over", limit + 1, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			url := stub(t, func(ctx context.Context, conn *websocket.Conn, _ url.Values) {
				sendLedgers(t, conn, ctx, tt.size, 1)
			})
			r := drain(t.Context(), New(url, WithMaxMessageSize(limit)), ledgerbackend.SingleLedgerRange(1))
			if tt.wantErr {
				if r.err == nil {
					t.Fatalf("a %d-byte payload passed a %d-byte cap", tt.size, limit)
				}
				return
			}
			if r.err != nil {
				t.Fatalf("a %d-byte payload was rejected by a %d-byte cap: %v", tt.size, limit, r.err)
			}
			if r.count != 1 || r.sizes[0] != tt.size {
				t.Errorf("received %d ledgers with sizes %v, want one of %d", r.count, r.sizes, tt.size)
			}
		})
	}
}

func TestEndpoint(t *testing.T) {
	tests := []struct {
		name string
		url  string
		rng  ledgerbackend.Range
		want string
	}{
		{"origin gets the stream path", "ws://box:8462", ledgerbackend.UnboundedRange(10),
			"ws://box:8462/v1/stream?start=10"},
		{"trailing slash", "ws://box:8462/", ledgerbackend.UnboundedRange(10),
			"ws://box:8462/v1/stream?start=10"},
		{"http is rewritten", "http://box:8462", ledgerbackend.UnboundedRange(10),
			"ws://box:8462/v1/stream?start=10"},
		{"https is rewritten", "https://box", ledgerbackend.UnboundedRange(10),
			"wss://box/v1/stream?start=10"},
		{"explicit path is kept", "ws://box/custom", ledgerbackend.UnboundedRange(1),
			"ws://box/custom?start=1"},
		{"bounded range", "ws://box", ledgerbackend.BoundedRange(4, 9),
			"ws://box/v1/stream?end=9&start=4"},
		{"single ledger", "ws://box", ledgerbackend.SingleLedgerRange(7),
			"ws://box/v1/stream?end=7&start=7"},
		{"tip", "ws://box", ledgerbackend.UnboundedRange(0),
			"ws://box/v1/stream?start=0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := New(tt.url).endpoint(tt.rng)
			if err != nil {
				t.Fatalf("endpoint: %v", err)
			}
			if got != tt.want {
				t.Errorf("endpoint = %q, want %q", got, tt.want)
			}
		})
	}
}
