// Package remoteledger consumes a corestreamd server as a
// ledgerbackend.LedgerStream.
//
// This is the seam stellar-rpc v2 full-history ingest already pulls on: swap the
// captive-core stream for a Stream pointed at a corestreamd URL and the ingest
// hot loop is unchanged, with core running on another machine. See
// docs/rpc-wiring.md.
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
	// maxPayloadSize caps the ledger bytes accepted; the read limit adds the
	// protocol header on top. Negative means no cap.
	maxPayloadSize int64
	httpClient     *http.Client
	dialTimeout    time.Duration
	observe        func(LedgerInfo)
}

// readLimit is the whole-message ceiling for the configured payload cap.
func (s *Stream) readLimit() int64 {
	if s.maxPayloadSize < 0 {
		return -1 // coder/websocket's "unlimited"
	}
	// Saturate rather than wrap: a cap near math.MaxInt64 plus the header would
	// overflow to a negative limit, which is how the library spells "no limit at
	// all" — turning an enormous cap into no cap.
	if s.maxPayloadSize > math.MaxInt64-wire.HeaderSize {
		return math.MaxInt64
	}
	return s.maxPayloadSize + wire.HeaderSize
}

// LedgerInfo is the delivery metadata of one received ledger. It exists because
// the LedgerStream seam carries payloads only: the benchmark harness needs the
// server's emit stamp, and a consumer measuring its own transport is the only
// party that can pair it with a receive stamp.
type LedgerInfo struct {
	// Sequence is the ledger this message carried.
	Sequence uint32

	// EmitUnixNano is the server's wall clock when the ledger arrived from its
	// source. It is zero for a ledger replayed from the server's retention: the
	// arrival time is not persisted, so no delivery latency can be derived.
	EmitUnixNano int64

	// ReceivedUnixNano is this client's wall clock when the message finished
	// arriving. Comparing it with EmitUnixNano assumes the two clocks agree —
	// true on one host, close enough under NTP across hosts.
	ReceivedUnixNano int64

	// Size is the ledger payload's byte count, excluding the protocol header.
	Size int
}

// Delivery is ReceivedUnixNano - EmitUnixNano, and ok is false for a replayed
// ledger, which carries no emit stamp.
func (l LedgerInfo) Delivery() (d time.Duration, ok bool) {
	if l.EmitUnixNano == 0 {
		return 0, false
	}
	return time.Duration(l.ReceivedUnixNano - l.EmitUnixNano), true
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

// WithMaxMessageSize caps the ledger PAYLOAD the client will accept, in bytes;
// the protocol header is admitted on top of it, so a limit of n accepts a ledger
// of exactly n bytes. The default is wire.DefaultMaxPayloadSize, 256 MiB, which
// matches the SDK's captive-core frame cap. Negative disables the cap.
func WithMaxMessageSize(n int64) Option {
	return func(s *Stream) { s.maxPayloadSize = n }
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
// it is the read buffer and the next iteration step overwrites it, so a consumer
// that retains a ledger must copy it.
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
		conn, _, err := websocket.Dial(dialCtx, endpoint, &websocket.DialOptions{HTTPClient: s.httpClient})
		if err != nil {
			yield(nil, fmt.Errorf("remoteledger: dial %s: %w", endpoint, err))
			return
		}
		// CloseNow is the teardown for every exit path; a graceful close on
		// normal completion runs before it and makes it a no-op.
		defer func() { _ = conn.CloseNow() }()
		conn.SetReadLimit(s.readLimit())

		// One buffer, reused: this is what makes the yielded slice a borrow.
		var buf bytes.Buffer
		expected := ledgerRange.From()
		// Set once the last representable sequence has been delivered. Nothing can
		// follow it: expected wraps to 0 there, which is the value that means
		// "accept any first ledger", so without this flag a wrap would silently
		// re-open the stream to any sequence at all.
		var exhausted bool

		for {
			typ, reader, err := conn.Reader(ctx)
			if err != nil {
				if done, cerr := classifyClose(err, expected); done {
					if cerr == nil {
						// An orderly close is only the end of the story when the
						// range was actually delivered.
						cerr = truncated(ledgerRange, expected)
					}
					if cerr != nil {
						yield(nil, cerr)
					}
					return
				}
				yield(nil, fmt.Errorf("remoteledger: read: %w", err))
				return
			}
			if typ != websocket.MessageBinary {
				yield(nil, fmt.Errorf("remoteledger: unexpected %v message", typ))
				return
			}

			buf.Reset()
			if _, err := buf.ReadFrom(reader); err != nil {
				yield(nil, fmt.Errorf("remoteledger: read message body: %w", err))
				return
			}
			var received int64
			if s.observe != nil {
				received = time.Now().UnixNano()
			}
			header, payload, err := wire.Decode(buf.Bytes())
			if err != nil {
				yield(nil, fmt.Errorf("remoteledger: %w", err))
				return
			}

			// The server promises in-order, gapless delivery within a
			// subscription. A tip subscription (from 0) accepts whatever ledger
			// arrives first and holds the server to continuity from there.
			if exhausted {
				yield(nil, fmt.Errorf("%w: ledger %d cannot follow %d, the last representable sequence",
					ErrGap, header.Sequence, uint32(math.MaxUint32)))
				return
			}
			if expected != 0 && header.Sequence != expected {
				yield(nil, fmt.Errorf("%w: got ledger %d, expected %d", ErrGap, header.Sequence, expected))
				return
			}
			exhausted = header.Sequence == math.MaxUint32
			expected = header.Sequence + 1

			if s.observe != nil {
				s.observe(LedgerInfo{
					Sequence:         header.Sequence,
					EmitUnixNano:     header.EmitUnixNano,
					ReceivedUnixNano: received,
					Size:             len(payload),
				})
			}

			if !yield(payload, nil) {
				_ = conn.Close(websocket.StatusNormalClosure, "consumer done")
				return
			}
			if ledgerRange.Bounded() && header.Sequence >= ledgerRange.To() {
				_ = conn.Close(websocket.StatusNormalClosure, "range complete")
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
