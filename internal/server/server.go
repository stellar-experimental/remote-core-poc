// Package server owns captive stellar-core (or a synthetic stand-in) on one
// machine and streams its raw ledger XDR to remote subscribers over WebSocket.
//
// A single source loop pulls ledgers from a ledgerbackend.LedgerStream, copies
// each one out of the borrowed iterator slice, appends it to a retention ring
// and fans it out to the connected subscribers. Nothing a subscriber does can
// slow that loop down: a consumer that falls behind its queue is disconnected.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"

	"github.com/stellar/remote-core-poc/internal/store"
	"github.com/stellar/remote-core-poc/internal/wire"
)

// DefaultPingInterval is how often the server pings an idle subscriber, so a
// connection that died without a FIN is noticed.
const DefaultPingInterval = 15 * time.Second

// writeTimeout bounds a single message write. The read side of a subscriber
// closing cleanly cancels the handler's context, but a peer that vanished
// without a reset would otherwise leave a handler blocked in write forever.
const writeTimeout = 2 * time.Minute

// maxPayloadSize is the largest ledger the protocol can carry. It is a variable
// only so tests can shrink it; the cap itself is protocol-defined, not an
// operator setting, which is why no flag exposes it.
var maxPayloadSize = wire.DefaultMaxPayloadSize

// Config describes a server.
type Config struct {
	// Source supplies ledgers. It is consumed exactly once, by Run.
	Source ledgerbackend.LedgerStream

	// Range is what Run asks the source for. Its start must be a concrete
	// ledger: the server counts sequences from it.
	Range ledgerbackend.Range

	// Store is the retention ring replays are served from.
	Store *store.Store

	// SubscriberBuffer is the per-subscriber queue depth. Zero means
	// DefaultSubscriberBuffer.
	SubscriberBuffer int

	// PingInterval is the keepalive period. Zero means DefaultPingInterval.
	PingInterval time.Duration

	// Logger receives structured logs. Zero means slog.Default().
	Logger *slog.Logger
}

// Server serves one source to many subscribers.
type Server struct {
	source ledgerbackend.LedgerStream
	rng    ledgerbackend.Range
	store  *store.Store
	b      *broadcaster
	log    *slog.Logger

	pingInterval time.Duration

	// sourceDone is closed when the source loop ends, which lets each handler
	// finish its subscriber off cleanly instead of hanging on a dead source.
	sourceDone chan struct{}
	doneOnce   sync.Once

	published atomic.Uint64
}

// New validates cfg and returns a server. It does not touch the source; Run
// does that.
func New(cfg Config) (*Server, error) {
	if cfg.Source == nil {
		return nil, errors.New("server: source is required")
	}
	if cfg.Store == nil {
		return nil, errors.New("server: store is required")
	}
	if cfg.Range.From() == 0 {
		return nil, errors.New("server: source range must start at a concrete ledger, not 0")
	}
	if cfg.Range.Bounded() && cfg.Range.To() < cfg.Range.From() {
		return nil, fmt.Errorf("server: source range %s ends before it starts", cfg.Range)
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	ping := cfg.PingInterval
	if ping <= 0 {
		ping = DefaultPingInterval
	}
	return &Server{
		source:       cfg.Source,
		rng:          cfg.Range,
		store:        cfg.Store,
		b:            newBroadcaster(cfg.SubscriberBuffer),
		log:          logger,
		pingInterval: ping,
		sourceDone:   make(chan struct{}),
	}, nil
}

// Subscribers is how many consumers are currently connected.
func (s *Server) Subscribers() int { return s.b.count() }

// Handler returns the HTTP surface: the stream endpoint and /healthz.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(wire.StreamPath, s.handleStream)
	mux.HandleFunc("/healthz", s.handleHealth)
	return mux
}

// Run drives the source loop until the source ends, it fails, or ctx is
// cancelled. It returns nil when a bounded source completed or when ctx was
// cancelled. Call it once: a LedgerStream is consumed by a single goroutine.
//
// When Run returns, every connected subscriber is finished off with a normal
// close — there is nothing more to stream, and a subscriber waiting on a dead
// source would hang.
func (s *Server) Run(ctx context.Context) error {
	defer s.doneOnce.Do(func() { close(s.sourceDone) })

	seq := s.rng.From()
	s.log.Info("source loop starting", "range", s.rng.String())

	for raw, err := range s.source.RawLedgers(ctx, s.rng) {
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil && errors.Is(err, ctxErr) {
				s.log.Info("source loop cancelled", "ledgers", s.published.Load())
				return nil
			}
			return fmt.Errorf("source failed at ledger %d: %w", seq, err)
		}

		// A ledger no subscriber could read is a failure here, not something to
		// hand out: every client's read limit is the protocol cap, so publishing
		// past it would disconnect all of them with a framing error instead of
		// telling the operator what is wrong. The SDK caps captive-core frames at
		// 256 MiB, so this fires only on a misconfigured synthetic source or a
		// future source with a larger frame.
		if int64(len(raw)) > maxPayloadSize {
			return fmt.Errorf("ledger %d is %d bytes, over the %d-byte protocol payload cap",
				seq, len(raw), maxPayloadSize)
		}

		// One allocation holds the header and our copy of the borrowed slice; the
		// store persists the copy, the broadcaster hands out the whole message.
		msg := wire.AppendLedger(make([]byte, 0, wire.HeaderSize+len(raw)), seq, time.Now().UnixNano(), raw)
		payload := msg[wire.HeaderSize:]

		reset, err := s.store.Put(seq, payload)
		if err != nil {
			return fmt.Errorf("retain ledger %d: %w", seq, err)
		}
		if reset {
			s.log.Warn("retention reset: ledger does not continue the retained range", "ledger", seq)
		}

		s.b.publish(liveLedger{seq: seq, msg: msg})
		s.published.Add(1)
		if seq%100 == 0 {
			oldest, latest, _ := s.store.Bounds()
			s.log.Info("streaming", "ledger", seq, "bytes", len(payload),
				"retained_oldest", oldest, "retained_latest", latest, "subscribers", s.b.count())
		}
		seq++
	}

	if err := ctx.Err(); err != nil {
		s.log.Info("source loop cancelled", "ledgers", s.published.Load())
		return nil
	}
	s.log.Info("source exhausted", "ledgers", s.published.Load(), "next_ledger", seq)
	return nil
}

// streamRequest is a parsed subscription.
type streamRequest struct {
	// start is the first ledger wanted, or 0 for the next live ledger.
	start uint32
	// end, when bounded, is the last ledger wanted.
	end     uint32
	bounded bool
}

func parseStreamRequest(q map[string][]string) (streamRequest, error) {
	var req streamRequest
	get := func(name string) (uint32, bool, error) {
		vals := q[name]
		if len(vals) == 0 || vals[0] == "" {
			return 0, false, nil
		}
		n, err := strconv.ParseUint(vals[0], 10, 32)
		if err != nil {
			return 0, false, fmt.Errorf("invalid %s parameter %q", name, vals[0])
		}
		return uint32(n), true, nil
	}
	start, _, err := get("start")
	if err != nil {
		return req, err
	}
	end, hasEnd, err := get("end")
	if err != nil {
		return req, err
	}
	if hasEnd && end == 0 {
		return req, errors.New("invalid end parameter: 0")
	}
	// Sequences are uint32 and the protocol has no wrap: serving the last
	// representable ledger would leave the next one unnameable, so the edge is
	// refused rather than half-supported.
	if start == math.MaxUint32 || end == math.MaxUint32 {
		return req, fmt.Errorf("ledger %d is the last representable sequence and is not streamable", uint32(math.MaxUint32))
	}
	if hasEnd && start > end {
		return req, fmt.Errorf("end %d is before start %d", end, start)
	}
	req.start, req.end, req.bounded = start, end, hasEnd
	return req, nil
}

func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	req, err := parseStreamRequest(r.URL.Query())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		s.log.Warn("websocket handshake failed", "remote", r.RemoteAddr, "error", err)
		return
	}
	defer conn.CloseNow()

	// Subscribers only receive. A low read limit caps what a misbehaving one can
	// make the server buffer, and CloseRead answers pings and notices the peer
	// going away — which is also what makes our own pings work.
	conn.SetReadLimit(1024)
	ctx := conn.CloseRead(r.Context())

	log := s.log.With("remote", r.RemoteAddr, "start", req.start, "end", req.end, "bounded", req.bounded)

	// Register before reading the retained bounds: a ledger published during
	// replay must be queued, not missed.
	sub := s.b.subscribe()
	defer s.b.unsubscribe(sub)
	log.Info("subscriber connected", "subscribers", s.b.count())

	var wg sync.WaitGroup
	pingCtx, stopPing := context.WithCancel(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.keepalive(pingCtx, conn)
	}()
	defer func() {
		stopPing()
		wg.Wait()
	}()

	code, reason, err := s.serve(ctx, conn, sub, req)
	if err != nil {
		log.Info("subscriber disconnected", "error", err)
		return
	}
	log.Info("closing subscriber", "code", int(code), "reason", reason)
	if cerr := conn.Close(code, reason); cerr != nil {
		log.Debug("close handshake did not complete", "error", cerr)
	}
}

// serve runs one subscription: replay from retention, then the live stream. It
// returns the close code to send, or an error when the connection is already
// broken and a close handshake is pointless.
func (s *Server) serve(
	ctx context.Context, conn *websocket.Conn, sub *subscriber, req streamRequest,
) (websocket.StatusCode, string, error) {
	// next is the sequence this subscriber expects. Zero means "whatever the
	// first live ledger turns out to be" — the tip subscription.
	next := req.start

	if req.start > 0 {
		done, err := s.replay(ctx, conn, req, &next)
		if err != nil {
			return 0, "", err
		}
		if done.set {
			return done.code, done.reason, nil
		}
	}

	deliver := func(l liveLedger) (bool, error) {
		if next != 0 && l.seq < next {
			return false, nil // already delivered from retention
		}
		if err := s.write(ctx, conn, l.msg); err != nil {
			return false, err
		}
		next = l.seq + 1
		return req.bounded && l.seq >= req.end, nil
	}

	for {
		// Being dropped outranks everything else that may be ready, including the
		// source having ended: closing a subscriber we dropped ledgers for with a
		// normal close would let it mistake a gap for the end of the stream.
		select {
		case <-sub.dropped:
			return wire.StatusSlowConsumer, "subscriber too slow", nil
		default:
		}

		select {
		case <-ctx.Done():
			return 0, "", ctx.Err()

		case <-sub.dropped:
			return wire.StatusSlowConsumer, "subscriber too slow", nil

		case <-s.sourceDone:
			// Deliver what is already queued, then finish: the source will not
			// produce more, so waiting is pointless.
			for {
				select {
				case <-sub.dropped:
					return wire.StatusSlowConsumer, "subscriber too slow", nil
				default:
				}
				select {
				case l := <-sub.ch:
					complete, err := deliver(l)
					if err != nil {
						return 0, "", err
					}
					if complete {
						return websocket.StatusNormalClosure, "", nil
					}
				default:
					// A bounded subscriber still waiting on ledgers is being cut
					// short, not served. Saying so on the wire keeps the close
					// honest; the client does not depend on it, because it
					// compares what arrived against what it asked for.
					if req.bounded && next <= req.end {
						return websocket.StatusGoingAway, "source ended before range complete", nil
					}
					return websocket.StatusNormalClosure, "source ended", nil
				}
			}

		case l := <-sub.ch:
			complete, err := deliver(l)
			if err != nil {
				return 0, "", err
			}
			if complete {
				return websocket.StatusNormalClosure, "", nil
			}
		}
	}
}

// closeOutcome is a decided close, so replay can tell "keep going" from "we are
// finished, close with this code".
type closeOutcome struct {
	set    bool
	code   websocket.StatusCode
	reason string
}

// replay sends the retained ledgers the request asks for and advances next past
// them. It reports an outcome when the subscription is already finished: the
// start ledger fell off retention, or a bounded range was served entirely from
// disk.
func (s *Server) replay(
	ctx context.Context, conn *websocket.Conn, req streamRequest, next *uint32,
) (closeOutcome, error) {
	oldest, latest, filled := s.store.Bounds()
	if !filled {
		return closeOutcome{}, nil
	}
	if req.start < oldest {
		return closeOutcome{true, wire.StatusTooFarBehind, wire.TooFarBehindReason(oldest, latest)}, nil
	}
	if req.start > latest {
		return closeOutcome{}, nil // nothing retained yet for this range
	}

	through := latest
	if req.bounded && req.end < through {
		through = req.end
	}
	buf := make([]byte, 0, wire.HeaderSize+64<<10)
	// Breaking after the last sequence instead of testing seq <= through keeps the
	// loop finite when through is math.MaxUint32, where seq++ would wrap to 0 and
	// the comparison would never end it.
	for seq := req.start; ; seq++ {
		raw, err := s.store.Get(seq)
		if err != nil {
			if errors.Is(err, store.ErrNotRetained) {
				// Pruning caught up with the replay: from here on this
				// subscriber is too far behind.
				o, l, _ := s.store.Bounds()
				return closeOutcome{true, wire.StatusTooFarBehind, wire.TooFarBehindReason(o, l)}, nil
			}
			return closeOutcome{}, err
		}
		// A replayed ledger carries no emit stamp: the arrival time is not
		// persisted, and the time we read the file is not a delivery latency.
		msg := wire.AppendLedger(buf[:0], seq, 0, raw)
		if err := s.write(ctx, conn, msg); err != nil {
			return closeOutcome{}, err
		}
		*next = seq + 1
		if seq == through {
			break
		}
	}
	if req.bounded && *next > req.end {
		return closeOutcome{true, websocket.StatusNormalClosure, ""}, nil
	}
	return closeOutcome{}, nil
}

func (s *Server) write(ctx context.Context, conn *websocket.Conn, msg []byte) error {
	wctx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	return conn.Write(wctx, websocket.MessageBinary, msg)
}

// keepalive pings the subscriber while it is idle. A failed ping means the peer
// is gone; closing the connection unblocks the handler.
func (s *Server) keepalive(ctx context.Context, conn *websocket.Conn) {
	ticker := time.NewTicker(s.pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pctx, cancel := context.WithTimeout(ctx, s.pingInterval)
			err := conn.Ping(pctx)
			cancel()
			if err != nil {
				if ctx.Err() == nil {
					s.log.Debug("keepalive ping failed", "error", err)
					conn.CloseNow()
				}
				return
			}
		}
	}
}

// health is the /healthz body.
type health struct {
	Oldest      uint32 `json:"oldest"`
	Latest      uint32 `json:"latest"`
	Subscribers int    `json:"subscribers"`
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	oldest, latest, _ := s.store.Bounds()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(health{
		Oldest:      oldest,
		Latest:      latest,
		Subscribers: s.Subscribers(),
	}); err != nil {
		s.log.Debug("healthz write failed", "error", err)
	}
}
