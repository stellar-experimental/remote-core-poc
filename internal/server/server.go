// Package server owns captive stellar-core (or a synthetic stand-in) on one
// machine and streams its raw ledger XDR to remote subscribers over WebSocket.
//
// A single source loop pulls ledgers from a ledgerbackend.LedgerStream, copies
// each one out of the borrowed iterator slice, appends it to a retention ring
// and moves the published tip forward. Each subscriber is a cursor over the
// ring: it reads from the store while behind the tip and is woken when the tip
// moves. Nothing a subscriber does can slow the source loop down, and a slow
// subscriber is never disconnected for lagging — it catches back up from the
// ring, and only falling out of retention entirely ends its stream.
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
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"

	"github.com/stellar-experimental/remote-core-poc/internal/store"
	"github.com/stellar-experimental/remote-core-poc/internal/wire"
)

// writeTimeout bounds a single message write, and is the server's liveness
// authority: a peer that vanished without a reset is noticed here once ledgers
// flow, or by the OS's TCP keepalive surfacing through the handler's read side
// while the stream is quiet. There is no WebSocket-level ping — a subscriber
// stalled mid-iteration cannot pong, and disconnecting it for that would break
// the promise that only falling out of retention ends a subscription.
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

	// Store is the retention ring replays and catch-ups are served from.
	Store *store.Store

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

	published   atomic.Uint64
	subscribers atomic.Int64
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
	return &Server{
		source: cfg.Source,
		rng:    cfg.Range,
		store:  cfg.Store,
		b:      newBroadcaster(),
		log:    logger,
	}, nil
}

// Subscribers is how many consumers are currently connected.
func (s *Server) Subscribers() int { return int(s.subscribers.Load()) }

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
	defer s.b.finish()

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
				"retained_oldest", oldest, "retained_latest", latest, "subscribers", s.Subscribers())
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
	// make the server buffer, and CloseRead notices the peer going away —
	// including via the OS's TCP keepalive when it vanished without a reset.
	conn.SetReadLimit(1024)
	ctx := conn.CloseRead(r.Context())

	log := s.log.With("remote", r.RemoteAddr, "start", req.start, "end", req.end, "bounded", req.bounded)

	log.Info("subscriber connected", "subscribers", s.subscribers.Add(1))
	defer s.subscribers.Add(-1)

	code, reason, err := s.serve(ctx, conn, req)
	if err != nil {
		log.Info("subscriber disconnected", "error", err)
		return
	}
	log.Info("closing subscriber", "code", int(code), "reason", reason)
	if cerr := conn.Close(code, reason); cerr != nil {
		log.Debug("close handshake did not complete", "error", cerr)
	}
}

// serve runs one subscription as a cursor over the retention ring: read from
// the store while behind, write the in-memory tip message when the cursor is
// exactly the tip, otherwise sleep until the tip moves. Catch-up is not a
// separate phase — it is what the loop does whenever the cursor is behind,
// including right after connecting. That is also why a subscriber that stalls
// is never disconnected for lagging: the ledgers it missed are read back from
// the ring, and only falling out of retention ends the subscription.
//
// It returns the close code to send, or an error when the connection is
// already broken and a close handshake is pointless.
func (s *Server) serve(
	ctx context.Context, conn *websocket.Conn, req streamRequest,
) (websocket.StatusCode, string, error) {
	// next is the sequence the subscriber expects, one past the last write. It
	// is a uint64 so serving the last representable ledger leaves it one past
	// math.MaxUint32 instead of wrapping to 0 and re-reading the ring. Zero
	// means the tip subscription, not yet anchored to a sequence.
	next := uint64(req.start)

	// A tip subscription starts after the tip observed at connect. When nothing
	// has been published yet it stays unanchored, and the loop anchors it to
	// the source's first ledger instead.
	if next == 0 {
		if tip, _, _ := s.b.watch(); tip.seq != 0 {
			next = uint64(tip.seq) + 1
		}
	}

	// buf is the scratch a ring read is framed into. It grows to the largest
	// ledger served and is then reused; a subscriber that never falls behind
	// never allocates it.
	var buf []byte

	for {
		tip, changed, ended := s.b.watch()
		if next == 0 {
			next = uint64(tip.seq) // the source's first ledger, or still unanchored
		}
		if next != 0 && req.bounded && next > uint64(req.end) {
			return websocket.StatusNormalClosure, "", nil
		}

		// Pick the message the cursor calls for: the shared in-memory tip when
		// the cursor is exactly there (the only copy that carries an emit
		// stamp), a ring read while behind it, nil when there is nothing to
		// send yet. Once the source has published, ring reads happen only
		// below the tip: Put and publish strictly alternate, so the ring's
		// latest is at most one past the tip, and that one ledger's stamped
		// publish is already in flight — sleeping on the captured channel
		// hands it out from memory instead of racing it to disk. Before the
		// first publish the ring alone decides: after a restart it holds
		// history the source has not re-published, and that must be served
		// rather than waited on.
		var msg []byte
		if next == uint64(tip.seq) && next != 0 {
			msg = tip.msg
		} else if next != 0 && (next < uint64(tip.seq) || tip.seq == 0) {
			if _, latest, filled := s.store.Bounds(); filled && next <= uint64(latest) {
				raw, err := s.store.Get(uint32(next))
				if err != nil {
					if errors.Is(err, store.ErrNotRetained) {
						// Off the ring's old edge, whether at connect or
						// because pruning caught up with the cursor: this
						// subscriber is too far behind.
						oldest, l, _ := s.store.Bounds()
						return wire.StatusTooFarBehind, wire.TooFarBehindReason(oldest, l), nil
					}
					return 0, "", err
				}
				// A ledger served from the ring carries no emit stamp: the
				// arrival time is not persisted, and the time we read the file
				// is not a delivery latency.
				buf = wire.AppendLedger(buf[:0], uint32(next), 0, raw)
				msg = buf
			}
		}

		if msg != nil {
			if err := s.write(ctx, conn, msg); err != nil {
				return 0, "", err
			}
			next++
			continue
		}

		// Nothing to send: caught up, or waiting for the source's first ledger.
		if ended {
			// A bounded subscriber still waiting on ledgers is being cut short,
			// not served. Saying so on the wire keeps the close honest; the
			// client does not depend on it, because it compares what arrived
			// against what it asked for.
			if req.bounded && next <= uint64(req.end) {
				return websocket.StatusGoingAway, "source ended before range complete", nil
			}
			return websocket.StatusNormalClosure, "source ended", nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return 0, "", ctx.Err()
		}
	}
}

func (s *Server) write(ctx context.Context, conn *websocket.Conn, msg []byte) error {
	wctx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	return conn.Write(wctx, websocket.MessageBinary, msg)
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
