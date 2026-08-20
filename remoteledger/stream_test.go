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

	"github.com/cespare/xxhash/v2"
	"github.com/coder/websocket"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"

	"github.com/stellar-experimental/remote-core-poc/internal/wire"
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

// send writes one binary message, ignoring a send on a departed consumer:
// that is the test's business, not the stub's.
func send(ctx context.Context, conn *websocket.Conn, msg []byte) {
	_ = conn.Write(ctx, websocket.MessageBinary, msg)
}

// sendFlow writes one complete, correct flow for seq: BEGIN, one CHUNK per
// piece, and an END whose counts and checksum match.
func sendFlow(ctx context.Context, conn *websocket.Conn, seq uint32, emitStart, emitEnd int64, pieces ...[]byte) {
	send(ctx, conn, wire.AppendBegin(nil, seq, emitStart))
	hasher := xxhash.New()
	total := 0
	for i, p := range pieces {
		hasher.Write(p)
		total += len(p)
		send(ctx, conn, wire.AppendChunk(nil, seq, uint32(i), p))
	}
	send(ctx, conn, wire.AppendEnd(nil, seq, uint32(len(pieces)), uint64(total), emitEnd, hasher.Sum64()))
}

// sendLedgers writes one single-chunk flow per sequence, with the given
// payload size, each payload filled with its sequence's low byte.
func sendLedgers(ctx context.Context, conn *websocket.Conn, size int, seqs ...uint32) {
	for _, seq := range seqs {
		sendFlow(ctx, conn, seq, time.Now().UnixNano(), time.Now().UnixNano(),
			bytes.Repeat([]byte{byte(seq)}, size))
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
		sendLedgers(ctx, conn, 64, 5, 6, 7)
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

func TestRawLedgersReassemblesChunkFlows(t *testing.T) {
	const emitStart, emitEnd int64 = 1700000000000000000, 1700000000015000000
	url := stub(t, func(ctx context.Context, conn *websocket.Conn, _ url.Values) {
		sendFlow(ctx, conn, 5, emitStart, emitEnd, []byte("hello, "), []byte("world"))
		sendFlow(ctx, conn, 6, 0, 0, []byte("ring-replayed"))
	})

	var infos []LedgerInfo
	var payloads [][]byte
	s := New(url, WithObserver(func(info LedgerInfo) { infos = append(infos, info) }))
	for raw, err := range s.RawLedgers(t.Context(), ledgerbackend.BoundedRange(5, 6)) {
		if err != nil {
			t.Fatalf("stream error: %v", err)
		}
		payloads = append(payloads, bytes.Clone(raw))
	}
	if len(payloads) != 2 {
		t.Fatalf("received %d ledgers, want 2", len(payloads))
	}
	if string(payloads[0]) != "hello, world" || string(payloads[1]) != "ring-replayed" {
		t.Fatalf("assembled %q and %q", payloads[0], payloads[1])
	}

	live := infos[0]
	if live.Sequence != 5 || live.Chunks != 2 || live.Size != len("hello, world") {
		t.Errorf("live info = %+v, want ledger 5 in 2 chunks", live)
	}
	if live.EmitStartUnixNano != emitStart || live.EmitEndUnixNano != emitEnd {
		t.Errorf("stamps = (%d, %d), want the flow's", live.EmitStartUnixNano, live.EmitEndUnixNano)
	}
	if d, ok := live.Delivery(); !ok || d != time.Duration(live.ReceivedUnixNano-emitEnd) {
		t.Errorf("delivery = (%s, %v), want received minus emit-end", d, ok)
	}
	if w, ok := live.EmitWindow(); !ok || w != time.Duration(emitEnd-emitStart) {
		t.Errorf("emit window = (%s, %v), want emit-end minus emit-start", w, ok)
	}
	if p, ok := live.Pipeline(); !ok || p != time.Duration(live.ReceivedUnixNano-emitStart) {
		t.Errorf("pipeline = (%s, %v), want received minus emit-start", p, ok)
	}

	replayed := infos[1]
	if _, ok := replayed.Delivery(); ok {
		t.Error("a ring-replayed flow reported a delivery latency")
	}
	if replayed.DiscardedPartials != 0 {
		t.Errorf("replayed ledger discarded %d partials, want 0", replayed.DiscardedPartials)
	}
}

func TestRawLedgersStopsAtRangeEnd(t *testing.T) {
	// The server keeps going; a bounded range must stop the consumer anyway.
	url := stub(t, func(ctx context.Context, conn *websocket.Conn, _ url.Values) {
		sendLedgers(ctx, conn, 32, 1, 2, 3, 4, 5)
	})
	r := drain(t.Context(), New(url), ledgerbackend.BoundedRange(1, 3))
	if r.err != nil {
		t.Fatalf("stream error: %v", r.err)
	}
	if r.count != 3 {
		t.Errorf("received %d ledgers, want 3", r.count)
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
		sendLedgers(ctx, conn, 32, 900, 901)
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

func TestRawLedgersSequenceRules(t *testing.T) {
	tests := []struct {
		name string
		rng  ledgerbackend.Range
		send func(ctx context.Context, conn *websocket.Conn)
		want error
	}{
		{"gap between ledgers", ledgerbackend.UnboundedRange(5),
			func(ctx context.Context, conn *websocket.Conn) {
				sendLedgers(ctx, conn, 32, 5, 6, 8)
			}, ErrGap},
		{"wrong first ledger", ledgerbackend.UnboundedRange(10),
			func(ctx context.Context, conn *websocket.Conn) {
				sendLedgers(ctx, conn, 32, 11)
			}, ErrGap},
		{"BEGIN for another ledger mid-assembly", ledgerbackend.UnboundedRange(1),
			func(ctx context.Context, conn *websocket.Conn) {
				send(ctx, conn, wire.AppendBegin(nil, 1, 0))
				send(ctx, conn, wire.AppendBegin(nil, 2, 0))
			}, ErrProtocol},
		// A tip subscription accepts any first ledger, including the ceiling.
		// What it must not accept is a ledger after it: expected wraps to 0
		// there, the value that means "any first ledger", so a wrap would
		// silently re-open the stream.
		{"wrap past the ceiling", ledgerbackend.UnboundedRange(0),
			func(ctx context.Context, conn *websocket.Conn) {
				sendLedgers(ctx, conn, 32, math.MaxUint32, 1)
			}, ErrGap},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			url := stub(t, func(ctx context.Context, conn *websocket.Conn, _ url.Values) {
				tt.send(ctx, conn)
			})
			r := drain(t.Context(), New(url), tt.rng)
			if !errors.Is(r.err, tt.want) {
				t.Fatalf("error = %v, want %v", r.err, tt.want)
			}
		})
	}
}

func TestRawLedgersNormalCloseEndsIteration(t *testing.T) {
	url := stub(t, func(ctx context.Context, conn *websocket.Conn, _ url.Values) {
		sendLedgers(ctx, conn, 16, 1, 2)
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
		sendLedgers(ctx, conn, 32, 1, 2)
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

func TestRawLedgersTruncatedMidAssembly(t *testing.T) {
	// A clean close in the middle of a flow: the partial is dropped and the
	// bounded shortfall reported, exactly as if the flow had never begun.
	url := stub(t, func(ctx context.Context, conn *websocket.Conn, _ url.Values) {
		sendFlow(ctx, conn, 1, 0, 0, []byte("whole"))
		send(ctx, conn, wire.AppendBegin(nil, 2, 0))
		send(ctx, conn, wire.AppendChunk(nil, 2, 0, []byte("partial")))
		_ = conn.Close(websocket.StatusNormalClosure, "source ended")
	})
	r := drain(t.Context(), New(url), ledgerbackend.BoundedRange(1, 3))
	if !errors.Is(r.err, ErrTruncated) {
		t.Fatalf("error = %v, want ErrTruncated", r.err)
	}
	if r.count != 1 {
		t.Errorf("received %d ledgers, want the 1 complete one", r.count)
	}
	if !strings.Contains(r.err.Error(), "mid-assembly of ledger 2") {
		t.Errorf("error %v does not name the ledger cut short", r.err)
	}
}

func TestRawLedgersUnboundedCloseMidAssemblyIsAnError(t *testing.T) {
	// The RPC live-ingest shape: an unbounded range, where a clean close is
	// normally just the end. Mid-assembly it is not — BEGIN announced a ledger
	// the close did not deliver, and a server crash mid-emission sends exactly
	// this orderly close. Silence here would vanish the ledger.
	url := stub(t, func(ctx context.Context, conn *websocket.Conn, _ url.Values) {
		sendFlow(ctx, conn, 1, 0, 0, []byte("whole"))
		send(ctx, conn, wire.AppendBegin(nil, 2, 0))
		send(ctx, conn, wire.AppendChunk(nil, 2, 0, []byte("partial")))
		_ = conn.Close(websocket.StatusNormalClosure, "source ended")
	})
	r := drain(t.Context(), New(url), ledgerbackend.UnboundedRange(1))
	if !errors.Is(r.err, ErrTruncated) {
		t.Fatalf("error = %v, want ErrTruncated", r.err)
	}
	if r.count != 1 {
		t.Errorf("received %d ledgers, want the 1 complete one", r.count)
	}
	if !strings.Contains(r.err.Error(), "mid-assembly of ledger 2") {
		t.Errorf("error %v does not name the ledger cut short", r.err)
	}
}

func TestRawLedgersTruncatedOnGoingAway(t *testing.T) {
	// What corestreamd itself sends when its source ends mid-range.
	url := stub(t, func(ctx context.Context, conn *websocket.Conn, _ url.Values) {
		sendLedgers(ctx, conn, 32, 4)
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
		sendLedgers(ctx, conn, 32, 1, 2, 3)
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
		sendLedgers(ctx, conn, 32, 1)
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

func TestRawLedgersDiscardsPartialOnRingRedelivery(t *testing.T) {
	// The slow-subscriber story from the client's side: a live flow opens,
	// some chunks land, and then a fresh BEGIN for the SAME sequence arrives —
	// the server abandoned the live flow and the ring is redelivering. The
	// partial must be thrown away, and the eventual ledger reported with a
	// discard count and no stamps.
	url := stub(t, func(ctx context.Context, conn *websocket.Conn, _ url.Values) {
		send(ctx, conn, wire.AppendBegin(nil, 9, time.Now().UnixNano()))
		send(ctx, conn, wire.AppendChunk(nil, 9, 0, []byte("partial that must v")))
		send(ctx, conn, wire.AppendChunk(nil, 9, 1, []byte("anish")))
		sendFlow(ctx, conn, 9, 0, 0, []byte("the complete"), []byte(" ledger"))
	})

	var infos []LedgerInfo
	var got []byte
	s := New(url, WithObserver(func(info LedgerInfo) { infos = append(infos, info) }))
	for raw, err := range s.RawLedgers(t.Context(), ledgerbackend.SingleLedgerRange(9)) {
		if err != nil {
			t.Fatalf("stream error: %v", err)
		}
		got = bytes.Clone(raw)
	}
	if string(got) != "the complete ledger" {
		t.Fatalf("assembled %q, want the redelivered ledger, not the partial", got)
	}
	if len(infos) != 1 || infos[0].DiscardedPartials != 1 {
		t.Fatalf("infos = %+v, want one ledger with one discarded partial", infos)
	}
	if _, ok := infos[0].Delivery(); ok {
		t.Error("a ring-redelivered ledger reported a delivery latency")
	}
}

func TestRawLedgersProtocolViolations(t *testing.T) {
	// Every violation the END-time verification exists to catch. None of these
	// may pass silently: an unverifiable ledger must never be handed up.
	tests := []struct {
		name string
		send func(ctx context.Context, conn *websocket.Conn)
		want string
	}{
		{"chunk out of order", func(ctx context.Context, conn *websocket.Conn) {
			send(ctx, conn, wire.AppendBegin(nil, 1, 0))
			send(ctx, conn, wire.AppendChunk(nil, 1, 1, []byte("x")))
		}, "expected chunk 0"},
		{"chunk without a flow", func(ctx context.Context, conn *websocket.Conn) {
			send(ctx, conn, wire.AppendChunk(nil, 1, 0, []byte("x")))
		}, "outside its flow"},
		{"chunk for another ledger", func(ctx context.Context, conn *websocket.Conn) {
			send(ctx, conn, wire.AppendBegin(nil, 1, 0))
			send(ctx, conn, wire.AppendChunk(nil, 2, 0, []byte("x")))
		}, "outside its flow"},
		{"end without a flow", func(ctx context.Context, conn *websocket.Conn) {
			send(ctx, conn, wire.AppendEnd(nil, 1, 0, 0, 0, 0))
		}, "outside its flow"},
		{"chunk count mismatch", func(ctx context.Context, conn *websocket.Conn) {
			send(ctx, conn, wire.AppendBegin(nil, 1, 0))
			send(ctx, conn, wire.AppendChunk(nil, 1, 0, []byte("x")))
			send(ctx, conn, wire.AppendEnd(nil, 1, 2, 1, 0, xxhash.Sum64([]byte("x"))))
		}, "declares 2 chunks"},
		{"length mismatch", func(ctx context.Context, conn *websocket.Conn) {
			send(ctx, conn, wire.AppendBegin(nil, 1, 0))
			send(ctx, conn, wire.AppendChunk(nil, 1, 0, []byte("x")))
			send(ctx, conn, wire.AppendEnd(nil, 1, 1, 2, 0, xxhash.Sum64([]byte("x"))))
		}, "declares 2 bytes"},
		{"checksum mismatch", func(ctx context.Context, conn *websocket.Conn) {
			send(ctx, conn, wire.AppendBegin(nil, 1, 0))
			send(ctx, conn, wire.AppendChunk(nil, 1, 0, []byte("x")))
			send(ctx, conn, wire.AppendEnd(nil, 1, 1, 1, 0, 0xbad))
		}, "checksum mismatch"},
		{"truncated header", func(ctx context.Context, conn *websocket.Conn) {
			send(ctx, conn, []byte{wire.Version, wire.TypeBegin})
		}, "shorter than header"},
		{"retired version byte", func(ctx context.Context, conn *websocket.Conn) {
			msg := wire.AppendBegin(nil, 1, 0)
			msg[0] = 0x01
			send(ctx, conn, msg)
		}, "version"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			url := stub(t, func(ctx context.Context, conn *websocket.Conn, _ url.Values) {
				tt.send(ctx, conn)
			})
			r := drain(t.Context(), New(url), ledgerbackend.UnboundedRange(1))
			if r.err == nil {
				t.Fatal("the stream accepted a protocol violation")
			}
			if r.count != 0 {
				t.Errorf("%d ledgers were yielded from a broken flow", r.count)
			}
			if !strings.Contains(r.err.Error(), tt.want) {
				t.Errorf("error = %v, want it to mention %q", r.err, tt.want)
			}
		})
	}
}

func TestRawLedgersRejectsTextMessages(t *testing.T) {
	url := stub(t, func(ctx context.Context, conn *websocket.Conn, _ url.Values) {
		_ = conn.Write(ctx, websocket.MessageText, []byte("hello"))
	})
	r := drain(t.Context(), New(url), ledgerbackend.UnboundedRange(1))
	if r.err == nil || !strings.Contains(r.err.Error(), "unexpected") {
		t.Fatalf("error = %v, want a text message rejected", r.err)
	}
}

func TestRawLedgersYieldsBorrowedSlices(t *testing.T) {
	url := stub(t, func(ctx context.Context, conn *websocket.Conn, _ url.Values) {
		sendLedgers(ctx, conn, 64, 1, 2)
	})
	var borrowed, copied []byte
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

func TestRawLedgersAssemblyCap(t *testing.T) {
	// The per-ledger cap is an assembly-buffer cap: chunks may be individually
	// small and still add up past it — and a ledger of exactly the cap passes.
	t.Run("over the cap", func(t *testing.T) {
		url := stub(t, func(ctx context.Context, conn *websocket.Conn, _ url.Values) {
			send(ctx, conn, wire.AppendBegin(nil, 1, 0))
			send(ctx, conn, wire.AppendChunk(nil, 1, 0, bytes.Repeat([]byte{1}, 100)))
			send(ctx, conn, wire.AppendChunk(nil, 1, 1, bytes.Repeat([]byte{2}, 100)))
		})
		r := drain(t.Context(), New(url, WithMaxMessageSize(150)), ledgerbackend.UnboundedRange(1))
		if !errors.Is(r.err, ErrProtocol) {
			t.Fatalf("error = %v, want the payload cap violation", r.err)
		}
		if !strings.Contains(r.err.Error(), "payload cap") {
			t.Errorf("error %v does not name the cap", r.err)
		}
	})
	t.Run("exactly at the cap", func(t *testing.T) {
		url := stub(t, func(ctx context.Context, conn *websocket.Conn, _ url.Values) {
			sendFlow(ctx, conn, 1, 0, 0, bytes.Repeat([]byte{1}, 100), bytes.Repeat([]byte{2}, 50))
		})
		r := drain(t.Context(), New(url, WithMaxMessageSize(150)), ledgerbackend.SingleLedgerRange(1))
		if r.err != nil {
			t.Fatalf("a ledger of exactly the cap was rejected: %v", r.err)
		}
		if r.count != 1 || r.sizes[0] != 150 {
			t.Errorf("received %d ledgers with sizes %v, want one of 150", r.count, r.sizes)
		}
	})
}

func TestRawLedgersChunkOverTheReadLimitIsRejected(t *testing.T) {
	url := stub(t, func(ctx context.Context, conn *websocket.Conn, _ url.Values) {
		send(ctx, conn, wire.AppendBegin(nil, 1, 0))
		send(ctx, conn, wire.AppendChunk(nil, 1, 0, bytes.Repeat([]byte{1}, 8<<10)))
	})
	r := drain(t.Context(), New(url, WithMaxChunkSize(4<<10)), ledgerbackend.UnboundedRange(1))
	if r.err == nil {
		t.Fatal("a chunk over the read limit was accepted")
	}
}

func TestRawLedgersCancelTearsDown(t *testing.T) {
	url := stub(t, func(ctx context.Context, conn *websocket.Conn, _ url.Values) {
		sendLedgers(ctx, conn, 32, 1)
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
		sendLedgers(ctx, conn, 32, 1, 2)
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

func TestReadLimitIsChunkSized(t *testing.T) {
	// The connection admits one chunk message, not one whole ledger: the
	// ledger cap lives on the assembly buffer instead.
	if got, want := New("ws://box").readLimit(), int64(wire.MaxChunkSize+wire.ChunkHeaderSize); got != want {
		t.Errorf("default read limit = %d, want %d (max chunk plus header)", got, want)
	}
	if got := New("ws://box", WithMaxChunkSize(64<<10)).readLimit(); got != 64<<10+wire.ChunkHeaderSize {
		t.Errorf("read limit for a 64 KiB chunk cap = %d, want %d", got, 64<<10+wire.ChunkHeaderSize)
	}
	// Zero means the default, not a 34-byte connection.
	if got, want := New("ws://box", WithMaxChunkSize(0)).readLimit(), int64(wire.MaxChunkSize+wire.ChunkHeaderSize); got != want {
		t.Errorf("read limit for a zero chunk cap = %d, want the default %d", got, want)
	}
	// Negative disables the limit, mirroring WithMaxMessageSize — it must not
	// collapse to a tiny cap that rejects every real chunk.
	if got := New("ws://box", WithMaxChunkSize(-1)).readLimit(); got != -1 {
		t.Errorf("read limit for a negative chunk cap = %d, want -1 (no limit)", got)
	}
	// An enormous cap must saturate BELOW MaxInt64 and stay a real limit.
	// coder/websocket increments whatever it is handed, so MaxInt64 wraps to
	// MinInt64 and its reader reads a negative limit as "unlimited" — the cap
	// would silently disappear and one hostile message could OOM the client.
	got := New("ws://box", WithMaxChunkSize(math.MaxInt64)).readLimit()
	if got <= 0 {
		t.Errorf("read limit for the int64 ceiling = %d, want a positive limit", got)
	}
	if got+1 <= 0 {
		t.Errorf("read limit %d wraps negative once the library increments it — that is 'no limit'", got)
	}
}

func TestEndpointScrubsStaleRangeParams(t *testing.T) {
	// The range owns start and end. A URL configured with either must not leak
	// into a call that asked for something else; unrelated parameters survive.
	stream := New("ws://box:8462/stream?start=9&end=5&token=abc")

	got, err := stream.endpoint(ledgerbackend.UnboundedRange(10))
	if err != nil {
		t.Fatalf("endpoint: %v", err)
	}
	if want := "ws://box:8462/stream?start=10&token=abc"; got != want {
		t.Errorf("endpoint = %q, want %q", got, want)
	}

	got, err = stream.endpoint(ledgerbackend.BoundedRange(2, 3))
	if err != nil {
		t.Fatalf("endpoint: %v", err)
	}
	if want := "ws://box:8462/stream?end=3&start=2&token=abc"; got != want {
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

func TestEndpoint(t *testing.T) {
	tests := []struct {
		name string
		url  string
		rng  ledgerbackend.Range
		want string
	}{
		{"origin gets the stream path", "ws://box:8462", ledgerbackend.UnboundedRange(10),
			"ws://box:8462/stream?start=10"},
		{"trailing slash", "ws://box:8462/", ledgerbackend.UnboundedRange(10),
			"ws://box:8462/stream?start=10"},
		{"http is rewritten", "http://box:8462", ledgerbackend.UnboundedRange(10),
			"ws://box:8462/stream?start=10"},
		{"https is rewritten", "https://box", ledgerbackend.UnboundedRange(10),
			"wss://box/stream?start=10"},
		{"explicit path is kept", "ws://box/custom", ledgerbackend.UnboundedRange(1),
			"ws://box/custom?start=1"},
		{"bounded range", "ws://box", ledgerbackend.BoundedRange(4, 9),
			"ws://box/stream?end=9&start=4"},
		{"single ledger", "ws://box", ledgerbackend.SingleLedgerRange(7),
			"ws://box/stream?end=7&start=7"},
		{"tip", "ws://box", ledgerbackend.UnboundedRange(0),
			"ws://box/stream?start=0"},
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
