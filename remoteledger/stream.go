// Package remoteledger consumes a corestreamd server as a
// ledgerbackend.LedgerStream.
//
// This is the seam stellar-rpc v2 full-history ingest already pulls on: swap the
// captive-core stream for a Stream pointed at a corestreamd URL and the ingest
// hot loop is unchanged, with core running on another machine. See
// docs/rpc-wiring.md.
//
// The client reassembles each ledger from the server's chunk flow and
// verifies its xxhash64 before yielding it, so the seam above stays
// byte-identical to captive core's.
package remoteledger

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"iter"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/coder/websocket"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"

	"github.com/stellar-experimental/remote-core-poc/internal/wire"
)

// Stream errors, each distinguishable from a transport failure with errors.Is.
var (
	// ErrTooFarBehind means the requested start ledger is older than the
	// server's retention. Use errors.As with *TooFarBehindError to read the
	// bounds the server reported.
	ErrTooFarBehind = errors.New("remoteledger: requested ledger is behind the server's retention")

	// ErrGap means the server delivered a sequence that does not continue the
	// stream. Ledgers must arrive in order with none missing.
	ErrGap = errors.New("remoteledger: ledger sequence gap")

	// ErrTruncated means the stream closed cleanly before the whole bounded range
	// arrived. A LedgerStream promises every ledger in the range it was asked
	// for, so a short delivery is an error even when the close itself was
	// orderly: the server's source ended, or something between the two ends
	// closed the connection politely.
	ErrTruncated = errors.New("remoteledger: stream ended before the requested range was delivered")

	// ErrProtocol means the server broke the message grammar: a chunk out of
	// order or outside a flow, an END whose counts disagree with what arrived,
	// or a checksum mismatch. Violations end the stream — a ledger that cannot
	// be verified must never be handed up as if it had been.
	ErrProtocol = errors.New("remoteledger: protocol violation")
)

// TooFarBehindError carries the retained range the server reported when it
// refused a subscription. Oldest and Latest are zero when the server's close
// reason did not state them.
type TooFarBehindError struct {
	Requested uint32
	Oldest    uint32
	Latest    uint32
}

func (e *TooFarBehindError) Error() string {
	if e.Oldest == 0 && e.Latest == 0 {
		return fmt.Sprintf("%s: requested %d", ErrTooFarBehind, e.Requested)
	}
	return fmt.Sprintf("%s: requested %d, server retains [%d,%d]",
		ErrTooFarBehind, e.Requested, e.Oldest, e.Latest)
}

func (e *TooFarBehindError) Unwrap() error { return ErrTooFarBehind }

// Stream is a LedgerStream served by a remote corestreamd. It holds no
// connection of its own: each RawLedgers call dials, streams, and tears the
// connection down, so a Stream is reusable and safe to keep in a config.
type Stream struct {
	rawURL string
	// maxPayloadSize caps the assembled ledger bytes accepted. Negative means
	// no cap.
	maxPayloadSize int64
	// maxChunkSize caps a single chunk's payload; the read limit adds the
	// chunk header on top.
	maxChunkSize int64
	httpClient   *http.Client
	dialTimeout  time.Duration
	observe      func(LedgerInfo)
}

// readLimit is the whole-message ceiling: one chunk plus its header (an END,
// at wire.EndSize bytes, always fits under it). A negative cap disables the
// limit; zero means the default. Saturate rather than wrap: an enormous chunk
// cap plus the header would overflow to a negative limit, which is how the
// library spells "no limit at all" — the one thing a huge cap must not
// silently become.
func (s *Stream) readLimit() int64 {
	n := s.maxChunkSize
	if n < 0 {
		return -1 // coder/websocket's "unlimited"
	}
	if n == 0 {
		n = wire.MaxChunkSize
	}
	if n > math.MaxInt64-wire.ChunkHeaderSize {
		return math.MaxInt64
	}
	return max(n+wire.ChunkHeaderSize, wire.EndSize)
}

// LedgerInfo is the delivery metadata of one received ledger. It exists because
// the LedgerStream seam carries payloads only: the benchmark harness needs the
// server's emit stamps, and a consumer measuring its own transport is the only
// party that can pair them with a receive stamp.
type LedgerInfo struct {
	// Sequence is the ledger this flow carried.
	Sequence uint32

	// EmitStartUnixNano is the server's wall clock at the FIRST byte of the
	// ledger from its source. It is zero for a ledger replayed from the
	// server's retention.
	EmitStartUnixNano int64

	// EmitEndUnixNano is the server's wall clock at the LAST byte of the ledger
	// from its source. It is zero for a ledger replayed from retention: the
	// emission times are not persisted, so no delivery latency can be derived.
	EmitEndUnixNano int64

	// ReceivedUnixNano is this client's wall clock once the ledger is usable:
	// assembly complete, checksum verified. Comparing it with the emit stamps
	// assumes the two clocks agree — true on one host, close under NTP across
	// hosts.
	ReceivedUnixNano int64

	// Size is the ledger's byte count, excluding all protocol framing.
	Size int

	// Chunks is how many CHUNK messages carried the ledger.
	Chunks int

	// DiscardedPartials counts partial assemblies of this same sequence thrown
	// away before this complete delivery: each one is a live chunk flow the
	// server abandoned mid-ledger and then redelivered from its retention ring.
	// A measurement window must show zero of these.
	DiscardedPartials int
}

// Delivery is ReceivedUnixNano - EmitEndUnixNano — the headline metric: how
// long after the source finished emitting the ledger it was usable here. ok is
// false for a replayed ledger, which carries no stamps.
func (l LedgerInfo) Delivery() (d time.Duration, ok bool) {
	if l.EmitEndUnixNano == 0 {
		return 0, false
	}
	return time.Duration(l.ReceivedUnixNano - l.EmitEndUnixNano), true
}

// Pipeline is ReceivedUnixNano - EmitStartUnixNano: the whole path from the
// source's first byte. ok is false when there is no emit-start stamp (a
// replayed ledger).
func (l LedgerInfo) Pipeline() (d time.Duration, ok bool) {
	if l.EmitStartUnixNano == 0 {
		return 0, false
	}
	return time.Duration(l.ReceivedUnixNano - l.EmitStartUnixNano), true
}

// EmitWindow is EmitEndUnixNano - EmitStartUnixNano: T_emit, how long the
// source took to emit the ledger, measured on the server's clock alone. ok is
// false when either stamp is missing.
func (l LedgerInfo) EmitWindow() (d time.Duration, ok bool) {
	if l.EmitStartUnixNano == 0 || l.EmitEndUnixNano == 0 {
		return 0, false
	}
	return time.Duration(l.EmitEndUnixNano - l.EmitStartUnixNano), true
}

var _ ledgerbackend.LedgerStream = (*Stream)(nil)

// DefaultDialTimeout bounds the WebSocket handshake.
const DefaultDialTimeout = 30 * time.Second

// Option configures a Stream.
type Option func(*Stream)

// WithHTTPClient dials with client instead of the default. Use it for a custom
// transport, timeouts, or TLS.
func WithHTTPClient(client *http.Client) Option {
	return func(s *Stream) { s.httpClient = client }
}

// WithMaxMessageSize caps the ledger the client will accept, in bytes — the
// whole assembled ledger; a limit of n accepts a ledger of exactly n bytes. The default is wire.DefaultMaxPayloadSize,
// 256 MiB, which matches the SDK's captive-core frame cap. Negative disables
// the cap.
func WithMaxMessageSize(n int64) Option {
	return func(s *Stream) { s.maxPayloadSize = n }
}

// WithMaxChunkSize caps a single chunk's payload, in bytes; the WebSocket
// read limit is this plus the chunk header, which is what keeps the
// connection's admission at chunk size rather than whole-ledger size. Zero
// means the default — wire.MaxChunkSize, the largest chunk a server flag can
// configure, so defaults on both ends always interoperate — and negative
// disables the per-message limit entirely, mirroring WithMaxMessageSize (the
// assembled-ledger cap still applies). A positive cap must be at least the
// server's --chunk-size or every ledger's first chunk is rejected as
// oversized.
func WithMaxChunkSize(n int64) Option {
	return func(s *Stream) { s.maxChunkSize = n }
}

// WithDialTimeout bounds the handshake. Zero or negative means no timeout beyond
// the caller's context.
func WithDialTimeout(d time.Duration) Option {
	return func(s *Stream) { s.dialTimeout = d }
}

// WithObserver calls fn for each ledger, on the iterating goroutine, just before
// the ledger is yielded. It is how the benchmark harness reads delivery stamps;
// fn runs in the hot path, so it must not block. Without an observer the client
// never samples the clock.
func WithObserver(fn func(LedgerInfo)) Option {
	return func(s *Stream) { s.observe = fn }
}

// New returns a Stream that reads from the corestreamd at rawURL.
//
// rawURL may be a bare origin ("ws://box:8462", or http:// which is rewritten to
// ws://), in which case the stream path is appended, or the full endpoint URL.
func New(rawURL string, opts ...Option) *Stream {
	s := &Stream{
		rawURL:         rawURL,
		maxPayloadSize: wire.DefaultMaxPayloadSize,
		maxChunkSize:   wire.MaxChunkSize,
		dialTimeout:    DefaultDialTimeout,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// RawLedgers yields the raw XDR of each ledger in ledgerRange, in order, read
// from the remote server.
//
// The connection is dialled on the first pull and closed when iteration ends —
// completion, break, error, or ctx cancellation. Each yielded slice is BORROWED:
// it is the assembly buffer and the next iteration step overwrites it, so a
// consumer that retains a ledger must copy it.
//
// A bounded range that ends early yields ErrTruncated rather than ending
// quietly: the contract is every ledger in the range, so an orderly close short
// of it is still a failure to deliver.
//
// opts are accepted and ignored. StreamOption mutates the SDK's unexported
// streamConfig, so no implementation outside the SDK can read one, let alone
// apply it. Metrics for this stream belong to its consumer.
func (s *Stream) RawLedgers(
	ctx context.Context, ledgerRange ledgerbackend.Range, _ ...ledgerbackend.StreamOption,
) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		endpoint, err := s.endpoint(ledgerRange)
		if err != nil {
			yield(nil, err)
			return
		}

		dialCtx := ctx
		if s.dialTimeout > 0 {
			var cancel context.CancelFunc
			dialCtx, cancel = context.WithTimeout(ctx, s.dialTimeout)
			defer cancel()
		}
		conn, _, err := websocket.Dial(dialCtx, endpoint, &websocket.DialOptions{
			HTTPClient: s.httpClient,
			// Explicitly no permessage-deflate: the codec's milliseconds are the
			// kind of tail this transport exists to avoid.
			CompressionMode: websocket.CompressionDisabled,
		})
		if err != nil {
			yield(nil, fmt.Errorf("remoteledger: dial %s: %w", endpoint, err))
			return
		}
		// CloseNow is the teardown for every exit path; a graceful close on
		// normal completion runs before it and makes it a no-op.
		defer func() { _ = conn.CloseNow() }()
		conn.SetReadLimit(s.readLimit())

		s.stream(ctx, conn, ledgerRange, yield)
	}
}

// seqTracker enforces the in-order, gapless delivery both protocols promise.
// A tip subscription (expected 0) accepts whatever ledger arrives first and
// holds the server to continuity from there. The last representable sequence
// has no successor, so nothing may follow it: expected would wrap to 0 there —
// the value that means "accept any first ledger" — which is why the wrap is a
// tracked state rather than an arithmetic accident.
type seqTracker struct {
	expected  uint32
	exhausted bool
}

// accept checks that seq may arrive now.
func (t *seqTracker) accept(seq uint32) error {
	if t.exhausted {
		return fmt.Errorf("%w: ledger %d cannot follow %d, the last representable sequence",
			ErrGap, seq, uint32(math.MaxUint32))
	}
	if t.expected != 0 && seq != t.expected {
		return fmt.Errorf("%w: got ledger %d, expected %d", ErrGap, seq, t.expected)
	}
	return nil
}

// advance records seq as delivered.
func (t *seqTracker) advance(seq uint32) {
	t.exhausted = seq == math.MaxUint32
	t.expected = seq + 1
}

// deliver yields one complete ledger and reports whether iteration should
// continue, closing the connection when the consumer stops pulling or the
// bounded range is complete.
func deliver(
	conn *websocket.Conn, ledgerRange ledgerbackend.Range, seq uint32, payload []byte,
	yield func([]byte, error) bool,
) bool {
	if !yield(payload, nil) {
		_ = conn.Close(websocket.StatusNormalClosure, "consumer done")
		return false
	}
	if ledgerRange.Bounded() && seq >= ledgerRange.To() {
		_ = conn.Close(websocket.StatusNormalClosure, "range complete")
		return false
	}
	return true
}

// readMessage pulls one binary message into buf, replacing its contents.
func readMessage(ctx context.Context, conn *websocket.Conn, buf *bytes.Buffer) error {
	typ, reader, err := conn.Reader(ctx)
	if err != nil {
		return err
	}
	if typ != websocket.MessageBinary {
		return fmt.Errorf("remoteledger: unexpected %v message", typ)
	}
	buf.Reset()
	if _, err := buf.ReadFrom(reader); err != nil {
		return fmt.Errorf("remoteledger: read message body: %w", err)
	}
	return nil
}

// finishClose classifies a read error once iteration is over, yielding
// whatever the consumer should see. expected is the sequence still owed.
func finishClose(
	readErr error, ledgerRange ledgerbackend.Range, expected uint32, yield func([]byte, error) bool,
) {
	if done, cerr := classifyClose(readErr, expected); done {
		if cerr == nil {
			// An orderly close is only the end of the story when the range was
			// actually delivered.
			cerr = truncated(ledgerRange, expected)
		}
		if cerr != nil {
			yield(nil, cerr)
		}
		return
	}
	yield(nil, fmt.Errorf("remoteledger: read: %w", readErr))
}

// stream is the read loop: reassemble BEGIN/CHUNK*/END into the complete raw
// ledger, verify it, and yield it. The seam above cannot tell the difference —
// only the observer sees the flow's timing.
func (s *Stream) stream(
	ctx context.Context, conn *websocket.Conn, ledgerRange ledgerbackend.Range, yield func([]byte, error) bool,
) {
	var msgBuf bytes.Buffer
	// asm is the assembly in progress. Its buffer is reused across ledgers,
	// which is what makes the yielded slice a borrow.
	asm := struct {
		active    bool
		seq       uint32
		emitStart int64
		buf       []byte
		chunks    uint32
		discarded int
	}{}
	hasher := xxhash.New()
	track := seqTracker{expected: ledgerRange.From()}

	for {
		if err := readMessage(ctx, conn, &msgBuf); err != nil {
			if asm.active {
				if done, cerr := classifyClose(err, track.expected); done && cerr == nil {
					// An orderly close in the middle of an announced ledger is
					// a truncation even on an unbounded range: BEGIN promised a
					// ledger the close did not deliver, and treating it as a
					// clean end would make a server crash mid-emission
					// indistinguishable from shutdown.
					yield(nil, fmt.Errorf("%w: stream closed mid-assembly of ledger %d", ErrTruncated, asm.seq))
					return
				}
			}
			finishClose(err, ledgerRange, track.expected, yield)
			return
		}
		m, err := wire.Decode(msgBuf.Bytes())
		if err != nil {
			yield(nil, fmt.Errorf("remoteledger: %w", err))
			return
		}

		switch m.Type {
		case wire.TypeBegin:
			if asm.active {
				if m.Seq != asm.seq {
					yield(nil, fmt.Errorf("%w: BEGIN for ledger %d while assembling %d", ErrProtocol, m.Seq, asm.seq))
					return
				}
				// A ring redelivery of the ledger being assembled: the server
				// abandoned this subscriber's live flow mid-ledger. The partial
				// is worthless — start over from the replacement.
				asm.discarded++
			} else if err := track.accept(m.Seq); err != nil {
				yield(nil, err)
				return
			}
			asm.active, asm.seq, asm.emitStart = true, m.Seq, m.EmitStartUnixNano
			asm.buf, asm.chunks = asm.buf[:0], 0
			hasher.Reset()

		case wire.TypeChunk:
			if !asm.active || m.Seq != asm.seq {
				yield(nil, fmt.Errorf("%w: CHUNK for ledger %d outside its flow", ErrProtocol, m.Seq))
				return
			}
			if m.ChunkIndex != asm.chunks {
				yield(nil, fmt.Errorf("%w: ledger %d chunk %d arrived, expected chunk %d",
					ErrProtocol, m.Seq, m.ChunkIndex, asm.chunks))
				return
			}
			if s.maxPayloadSize >= 0 && int64(len(asm.buf))+int64(len(m.Payload)) > s.maxPayloadSize {
				yield(nil, fmt.Errorf("%w: ledger %d exceeds the %d-byte payload cap",
					ErrProtocol, m.Seq, s.maxPayloadSize))
				return
			}
			_, _ = hasher.Write(m.Payload) // xxhash's Write cannot fail
			asm.buf = append(asm.buf, m.Payload...)
			asm.chunks++

		case wire.TypeEnd:
			if !asm.active || m.Seq != asm.seq {
				yield(nil, fmt.Errorf("%w: END for ledger %d outside its flow", ErrProtocol, m.Seq))
				return
			}
			if m.ChunkCount != asm.chunks {
				yield(nil, fmt.Errorf("%w: ledger %d END declares %d chunks, %d arrived",
					ErrProtocol, m.Seq, m.ChunkCount, asm.chunks))
				return
			}
			if m.TotalLen != uint64(len(asm.buf)) {
				yield(nil, fmt.Errorf("%w: ledger %d END declares %d bytes, %d assembled",
					ErrProtocol, m.Seq, m.TotalLen, len(asm.buf)))
				return
			}
			if sum := hasher.Sum64(); sum != m.Checksum {
				yield(nil, fmt.Errorf("%w: ledger %d checksum mismatch: got %#016x, want %#016x",
					ErrProtocol, m.Seq, sum, m.Checksum))
				return
			}
			// The receive stamp is taken only now: the metric is when the
			// ledger became usable — assembled and verified — not when its last
			// frame hit the socket.
			var received int64
			if s.observe != nil {
				received = time.Now().UnixNano()
			}

			track.advance(m.Seq)

			if s.observe != nil {
				s.observe(LedgerInfo{
					Sequence:          m.Seq,
					EmitStartUnixNano: asm.emitStart,
					EmitEndUnixNano:   m.EmitEndUnixNano,
					ReceivedUnixNano:  received,
					Size:              len(asm.buf),
					Chunks:            int(asm.chunks),
					DiscardedPartials: asm.discarded,
				})
			}
			asm.active, asm.discarded = false, 0

			if !deliver(conn, ledgerRange, m.Seq, asm.buf, yield) {
				return
			}
		}
	}
}

// truncated reports the stream having ended short of a bounded range, given the
// sequence it was still expecting. It returns nil for a range that was delivered
// and for an unbounded one, where a clean close is simply the end.
//
// A tip subscription that received nothing is not reported: with a start of 0 the
// consumer never named a first ledger, so there is no shortfall to describe.
func truncated(ledgerRange ledgerbackend.Range, expected uint32) error {
	if !ledgerRange.Bounded() || expected == 0 || expected > ledgerRange.To() {
		return nil
	}
	if expected == ledgerRange.From() {
		return fmt.Errorf("%w: received no ledgers, requested %s", ErrTruncated, ledgerRange)
	}
	return fmt.Errorf("%w: got through ledger %d, requested through %d",
		ErrTruncated, expected-1, ledgerRange.To())
}

// classifyClose turns a read error into the outcome the consumer should see.
// done reports that iteration is over; err is what to yield, nil when the stream
// ended the way it was supposed to.
func classifyClose(readErr error, requested uint32) (done bool, err error) {
	switch websocket.CloseStatus(readErr) {
	case websocket.StatusNormalClosure, websocket.StatusGoingAway:
		return true, nil
	case wire.StatusTooFarBehind:
		tfb := &TooFarBehindError{Requested: requested}
		var ce websocket.CloseError
		if errors.As(readErr, &ce) {
			if oldest, latest, ok := wire.ParseTooFarBehindReason(ce.Reason); ok {
				tfb.Oldest, tfb.Latest = oldest, latest
			}
		}
		return true, tfb
	default:
		return false, nil
	}
}

// endpoint builds the subscription URL for a range.
func (s *Stream) endpoint(ledgerRange ledgerbackend.Range) (string, error) {
	u, err := url.Parse(s.rawURL)
	if err != nil {
		return "", fmt.Errorf("remoteledger: invalid url %q: %w", s.rawURL, err)
	}
	switch u.Scheme {
	case "ws", "wss":
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	default:
		return "", fmt.Errorf("remoteledger: url %q must use ws, wss, http or https", s.rawURL)
	}
	if u.Host == "" {
		return "", fmt.Errorf("remoteledger: url %q has no host", s.rawURL)
	}
	if u.Path == "" || u.Path == "/" {
		u.Path = wire.StreamPath
	}

	// Sequences are uint32 and the protocol has no wrap: the last representable
	// ledger has no successor to ask for next, so the edge is refused here rather
	// than producing a subscription that cannot continue.
	if ledgerRange.From() == math.MaxUint32 || (ledgerRange.Bounded() && ledgerRange.To() == math.MaxUint32) {
		return "", fmt.Errorf("remoteledger: range %s reaches ledger %d, the last representable sequence",
			ledgerRange, uint32(math.MaxUint32))
	}

	q := u.Query()
	// The range owns these two parameters. Anything the configured URL carried is
	// stale and would contradict what this call asked for.
	q.Del("start")
	q.Del("end")
	q.Set("start", strconv.FormatUint(uint64(ledgerRange.From()), 10))
	if ledgerRange.Bounded() {
		if ledgerRange.To() < ledgerRange.From() {
			return "", fmt.Errorf("remoteledger: range %s ends before it starts", ledgerRange)
		}
		q.Set("end", strconv.FormatUint(uint64(ledgerRange.To()), 10))
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}
