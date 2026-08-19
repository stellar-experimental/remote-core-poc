// Package server owns a ledger source — captive stellar-core, a file dump
// replayer, or a synthetic stand-in — on one machine and streams its raw
// ledger XDR to remote subscribers over WebSocket.
//
// A single source loop reads each ledger's bytes incrementally from the
// source and publishes them as a chunk flow: the first chunk goes on the wire
// while the source is still emitting the rest, which is what lets transfer
// hide inside the source's own emission window. Each completed ledger is
// appended to a retention ring on disk. Each subscriber is a cursor over the
// ring: it reads from the store while behind the flow and is woken when the
// flow grows. Nothing a subscriber does can slow the source loop down, and a
// slow subscriber is never disconnected for lagging — it catches back up from
// the ring, and only falling out of retention entirely ends its stream.
//
// The one-message-per-ledger v1 protocol this design replaced is gone from
// the tree; measure it for a comparison row by building the pre-chunking
// revision from git history on the same pairing.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/coder/websocket"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"

	"github.com/stellar-experimental/remote-core-poc/internal/store"
	"github.com/stellar-experimental/remote-core-poc/internal/wire"
)

// chunkWriteTimeout bounds a single message write, and is the server's
// write-side liveness authority: chunks are small, so a healthy peer drains
// one far faster than this, and a dead one is noticed within one ledger. A
// merely slow peer is handled before this fires — its live flow is abandoned
// in favour of the ring the moment the flow moves past it. There is no
// WebSocket-level ping: a subscriber stalled mid-iteration cannot pong, and
// disconnecting it for that would break the promise that only falling out of
// retention ends a subscription.
const chunkWriteTimeout = 10 * time.Second

// maxPayloadSize is the largest ledger the protocol can carry — the
// assembly-buffer cap on both ends. It is a variable only so tests can shrink
// it; the cap itself is protocol-defined, not an operator setting, which is
// why no flag exposes it.
var maxPayloadSize = wire.DefaultMaxPayloadSize

// Config describes a server.
type Config struct {
	// Source supplies ledgers as incremental emissions — the seam a real core
	// meta pipe would plug into. Wrap a complete-ledger LedgerStream in
	// PacedSource to adapt (and optionally pace) it. It is consumed exactly
	// once, by Run.
	Source EmittingStream

	// ChunkSize is the chunk payload size ledgers are cut into on the wire.
	// Zero means wire.DefaultChunkSize.
	ChunkSize int

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
	source    EmittingStream
	rng       ledgerbackend.Range
	store     *store.Store
	b         *broadcaster
	log       *slog.Logger
	chunkSize int

	// ringSums caches each retained ledger's xxhash64, so a ring redelivery
	// reuses the sum the source loop already computed instead of rehashing
	// megabytes per catching-up subscriber. Best-effort: history left by a
	// previous process misses and is hashed on demand.
	ringSumsMu sync.Mutex
	ringSums   map[uint32]uint64

	published    atomic.Uint64
	subscribers  atomic.Int64
	liveAbandons atomic.Uint64
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
	chunkSize := cfg.ChunkSize
	if chunkSize == 0 {
		chunkSize = wire.DefaultChunkSize
	}
	if chunkSize < 0 {
		return nil, fmt.Errorf("server: chunk size must be positive, got %d", chunkSize)
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{
		source:    cfg.Source,
		rng:       cfg.Range,
		store:     cfg.Store,
		b:         newBroadcaster(),
		log:       logger,
		chunkSize: chunkSize,
		ringSums:  make(map[uint32]uint64),
	}, nil
}

// Subscribers is how many consumers are currently connected.
func (s *Server) Subscribers() int { return int(s.subscribers.Load()) }

// LiveAbandons is how many times a slow subscriber's live chunk flow was
// abandoned in favour of a ring redelivery. A measurement window must show
// zero of these: an abandoned ledger arrives complete but stampless, dropping
// out of the delivery samples.
func (s *Server) LiveAbandons() uint64 { return s.liveAbandons.Load() }

// Handler returns the HTTP surface: the stream endpoint and /healthz.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(wire.StreamPath, s.handleStream)
	mux.HandleFunc("/healthz", s.handleHealth)
	return mux
}

// Run drives the source loop until the source ends, it fails, or ctx is
// cancelled. It returns nil when a bounded source completed or when ctx was
// cancelled. Call it once: a source is consumed by a single goroutine.
//
// The loop's shape is what makes overlap possible: each chunk is read from
// the emission and published the moment it exists — no batching, no waiting
// for the ledger to complete — and the disk write to the retention ring
// happens AFTER the END is published, so a live subscriber's delivery never
// pays for it. The flow stays in memory until the next ledger's BEGIN, so the
// just-completed ledger is served from there; the ring only ever serves
// ledgers older than the current flow, and each Put completes before the next
// BEGIN — so those are always on disk by the time a cursor asks.
//
// When Run returns, every connected subscriber is finished off with a normal
// close — there is nothing more to stream, and a subscriber waiting on a dead
// source would hang.
func (s *Server) Run(ctx context.Context) error {
	defer s.b.finish()

	s.log.Info("source loop starting", "range", s.rng.String(), "chunk_size", s.chunkSize)

	var assembly []byte // reused across ledgers; the store copies it to disk
	// spare is the next CHUNK message's allocation, read into directly so a
	// chunk's bytes are copied once, not staged through an intermediate
	// buffer — the final chunk's staging copy would sit squarely on the
	// measured post-emission tail. Once published it belongs to subscribers
	// and a fresh one is allocated; a read that returns no data hands it back.
	var spare []byte
	hasher := xxhash.New()

	for em, err := range s.source.Emissions(ctx, s.rng) {
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil && errors.Is(err, ctxErr) {
				s.log.Info("source loop cancelled", "ledgers", s.published.Load())
				return nil
			}
			return fmt.Errorf("source failed at ledger %d: %w", em.Seq, err)
		}
		seq := em.Seq

		// A ledger no subscriber could assemble is a failure here, not
		// something to hand out: every client's assembly cap is the protocol
		// cap, so streaming past it would disconnect all of them with a
		// protocol error instead of telling the operator what is wrong.
		if em.Size > maxPayloadSize {
			return fmt.Errorf("ledger %d is %d bytes, over the %d-byte protocol payload cap",
				seq, em.Size, maxPayloadSize)
		}

		assembly = assembly[:0]
		if int64(cap(assembly)) < em.Size {
			assembly = make([]byte, 0, em.Size)
		}
		hasher.Reset()

		var chunkIdx uint32
		for {
			if spare == nil {
				spare = make([]byte, wire.ChunkHeaderSize+s.chunkSize)
			}
			n, rerr := em.Body.Read(spare[wire.ChunkHeaderSize:])
			if n > 0 {
				if int64(len(assembly))+int64(n) > maxPayloadSize {
					return fmt.Errorf("ledger %d ran past the %d-byte protocol payload cap mid-emission",
						seq, maxPayloadSize)
				}
				if chunkIdx == 0 {
					// The stamp is the first byte's arrival: T_emit and the
					// whole-pipeline metric both count from here.
					s.b.begin(seq, wire.AppendBegin(make([]byte, 0, wire.BeginSize), seq, time.Now().UnixNano()))
				}
				msg := spare[:wire.ChunkHeaderSize+n]
				spare = nil
				wire.PutChunkHeader(msg, seq, chunkIdx)
				data := msg[wire.ChunkHeaderSize:]
				_, _ = hasher.Write(data) // xxhash's Write cannot fail
				assembly = append(assembly, data...)
				s.b.chunk(msg)
				chunkIdx++
			}
			if rerr == io.EOF {
				break
			}
			if rerr != nil {
				if ctxErr := ctx.Err(); ctxErr != nil && errors.Is(rerr, ctxErr) {
					s.log.Info("source loop cancelled", "ledgers", s.published.Load())
					return nil
				}
				return fmt.Errorf("source failed emitting ledger %d: %w", seq, rerr)
			}
		}
		if chunkIdx == 0 {
			// A zero-byte ledger still opens its flow, so BEGIN/END stay paired.
			s.b.begin(seq, wire.AppendBegin(make([]byte, 0, wire.BeginSize), seq, time.Now().UnixNano()))
		}
		sum := hasher.Sum64()
		s.b.end(wire.AppendEnd(make([]byte, 0, wire.EndSize), seq, chunkIdx,
			uint64(len(assembly)), time.Now().UnixNano(), sum))

		reset, err := s.store.Put(seq, assembly)
		if err != nil {
			return fmt.Errorf("retain ledger %d: %w", seq, err)
		}
		if reset {
			s.log.Warn("retention reset: ledger does not continue the retained range", "ledger", seq)
		}
		s.cacheRingSum(seq, sum, reset)

		s.published.Add(1)
		if seq%100 == 0 {
			oldest, latest, _ := s.store.Bounds()
			s.log.Info("streaming", "ledger", seq, "bytes", len(assembly), "chunks", chunkIdx,
				"retained_oldest", oldest, "retained_latest", latest, "subscribers", s.Subscribers())
		}
	}

	if err := ctx.Err(); err != nil {
		s.log.Info("source loop cancelled", "ledgers", s.published.Load())
		return nil
	}
	s.log.Info("source exhausted", "ledgers", s.published.Load())
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

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		// Explicitly no permessage-deflate: a codec in the hot path adds
		// milliseconds of tail on the hardware this targets, and compression is
		// this design's documented small-NIC alternative, not a default.
		CompressionMode: websocket.CompressionDisabled,
	})
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

// anchorTip resolves a start-at-tip subscription against the current flow: an
// in-flight ledger can still be joined from its first chunk, a completed one
// is history and the subscription starts after it. Zero means the flow has
// not begun and the cursor stays unanchored.
func (s *Server) anchorTip() uint64 {
	snap, _, _ := s.b.watch()
	if snap.seq() == 0 {
		return 0
	}
	if snap.complete() {
		return uint64(snap.seq()) + 1
	}
	return uint64(snap.seq())
}

// cacheRingSum records ledger seq's checksum for ring redeliveries, evicting
// the entry that just left retention. A reset emptied the ring, so it empties
// the cache: a re-used sequence must never serve a stale sum.
func (s *Server) cacheRingSum(seq uint32, sum uint64, reset bool) {
	s.ringSumsMu.Lock()
	defer s.ringSumsMu.Unlock()
	if reset {
		clear(s.ringSums)
	}
	s.ringSums[seq] = sum
	if retention := uint64(s.store.Retention()); uint64(seq) > retention {
		delete(s.ringSums, uint32(uint64(seq)-retention))
	}
}

// ringSum is the cached checksum of retained ledger seq. A miss means the
// ledger predates this process; the caller hashes it itself.
func (s *Server) ringSum(seq uint32) (uint64, bool) {
	s.ringSumsMu.Lock()
	defer s.ringSumsMu.Unlock()
	sum, ok := s.ringSums[seq]
	return sum, ok
}

// readRing fetches ledger next from the retention ring — the catch-up path of
// both protocols. ok is false with no error when the ring cannot serve next
// yet (nothing retained, or next is beyond the retained range): the caller
// sleeps on the flow. A ledger missing INSIDE the bounds was pruned, so the
// subscriber is too far behind; tooFar carries the close reason for
// wire.StatusTooFarBehind.
func (s *Server) readRing(next uint64) (raw []byte, ok bool, tooFar string, err error) {
	if _, latest, filled := s.store.Bounds(); !filled || next > uint64(latest) {
		return nil, false, "", nil
	}
	raw, err = s.store.Get(uint32(next))
	if err != nil {
		if errors.Is(err, store.ErrNotRetained) {
			// Off the ring's old edge, whether at connect or because pruning
			// caught up with the cursor.
			oldest, latest, _ := s.store.Bounds()
			return nil, false, wire.TooFarBehindReason(oldest, latest), nil
		}
		return nil, false, "", err
	}
	return raw, true, "", nil
}

// endedClose is the close for a subscriber whose source has finished with
// nothing more to send it. A bounded subscriber still waiting on ledgers is
// being cut short, not served; saying so on the wire keeps the close honest,
// though the client does not depend on it — it compares what arrived against
// what it asked for.
func endedClose(req streamRequest, next uint64) (websocket.StatusCode, string) {
	if req.bounded && next <= uint64(req.end) {
		return websocket.StatusGoingAway, "source ended before range complete"
	}
	return websocket.StatusNormalClosure, "source ended"
}

// serve runs one subscription as a cursor over the retention ring plus
// the live chunk flow: follow the flow chunk-by-chunk when the cursor is
// exactly there, read complete ledgers from the store while behind, otherwise
// sleep until the flow grows. Catch-up is not a separate phase — it is what
// the loop does whenever the cursor is behind, including right after
// connecting. A subscriber that stalls is never disconnected for lagging: the
// ledgers it missed are read back from the ring, and only falling out of
// retention ends the subscription.
//
// The slow-subscriber policy lives here too: a subscriber mid-flow when the
// flow moves on to the next ledger has spent its budget — the rest of that
// ledger's emission — so its live flow is abandoned and the ledger is
// redelivered complete (and stampless) from the ring. The client discards its
// partial assembly on the fresh BEGIN. The source loop never notices any of
// this, which is the invariant that keeps one slow consumer from hurting the
// rest.
//
// It returns the close code to send, or an error when the connection is
// already broken and a close handshake is pointless.
func (s *Server) serve(
	ctx context.Context, conn *websocket.Conn, req streamRequest,
) (websocket.StatusCode, string, error) {
	// next is the sequence the subscriber expects. It is a uint64 so serving
	// the last representable ledger leaves it one past math.MaxUint32 instead
	// of wrapping to 0 and re-reading the ring. Zero means the tip
	// subscription, not yet anchored to a sequence.
	next := uint64(req.start)
	if next == 0 {
		next = s.anchorTip()
	}

	// live tracks progress through the flow the subscriber is mid-way into.
	// While begun holds, next stays pinned to the begun flow's ledger, so no
	// separate sequence field is needed to know which flow the progress is in.
	var live struct {
		begun bool // BEGIN written, END not yet
		sent  int  // chunks written
	}

	// buf is the scratch ring reads are framed into; it grows to one chunk
	// message and is then reused. A subscriber that never falls behind never
	// allocates it.
	var buf []byte

	for {
		snap, changed, ended := s.b.watch()
		if next == 0 {
			next = uint64(snap.seq()) // the source's first ledger, or still unanchored
		}
		if next != 0 && req.bounded && next > uint64(req.end) {
			return websocket.StatusNormalClosure, "", nil
		}

		if next != 0 && next == uint64(snap.seq()) {
			// The live flow: write whatever this snapshot holds beyond what has
			// already been sent, then re-watch — more may have been published
			// while these writes were in flight.
			wrote := false
			if !live.begun {
				if err := s.write(ctx, conn, snap.f.begin); err != nil {
					return 0, "", err
				}
				live.begun, live.sent = true, 0
				wrote = true
			}
			for live.sent < len(snap.chunks) {
				if err := s.write(ctx, conn, snap.chunks[live.sent]); err != nil {
					return 0, "", err
				}
				live.sent++
				wrote = true
			}
			if snap.complete() {
				if err := s.write(ctx, conn, snap.end); err != nil {
					return 0, "", err
				}
				live.begun = false
				next++
				continue
			}
			if wrote {
				continue
			}
			// Nothing new in this snapshot: fall through to sleep on the flow.
		} else if next != 0 && (next < uint64(snap.seq()) || snap.seq() == 0) {
			// Behind the flow: serve from the ring. Once the source has
			// published, everything below the current flow is on disk — each
			// Put completes before the next BEGIN. Before the first publish the
			// ring alone decides: after a restart it holds history the source
			// has not re-published, and that must be served rather than waited
			// on.
			if live.begun {
				// The flow moved past a ledger this subscriber was still
				// mid-way through: its live delivery is abandoned, the ring
				// redelivers it complete. The fresh BEGIN below is what tells
				// the client to discard its partial.
				live.begun = false
				s.liveAbandons.Add(1)
				s.log.Debug("live chunk flow abandoned; redelivering from the ring", "ledger", next)
			}
			raw, ok, tooFar, err := s.readRing(next)
			if err != nil {
				return 0, "", err
			}
			if tooFar != "" {
				return wire.StatusTooFarBehind, tooFar, nil
			}
			if ok {
				if err := s.writeRingFlow(ctx, conn, uint32(next), raw, &buf); err != nil {
					return 0, "", err
				}
				next++
				continue
			}
		}

		// Nothing to send: caught up mid-flow, or waiting for the source's
		// first ledger.
		if ended {
			code, reason := endedClose(req, next)
			return code, reason, nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return 0, "", ctx.Err()
		}
	}
}

// writeRingFlow delivers one complete ledger from the retention ring as a
// chunk flow with both emit stamps zero: the original emission times are not
// persisted, and a replayed ledger must never contribute a delivery sample.
func (s *Server) writeRingFlow(
	ctx context.Context, conn *websocket.Conn, seq uint32, raw []byte, buf *[]byte,
) error {
	b := *buf
	defer func() { *buf = b }()

	b = wire.AppendBegin(b[:0], seq, 0)
	if err := s.write(ctx, conn, b); err != nil {
		return err
	}
	var idx uint32
	for off := 0; off < len(raw); off += s.chunkSize {
		chunk := raw[off:min(off+s.chunkSize, len(raw))]
		b = wire.AppendChunk(b[:0], seq, idx, chunk)
		if err := s.write(ctx, conn, b); err != nil {
			return err
		}
		idx++
	}
	sum, ok := s.ringSum(seq)
	if !ok {
		sum = xxhash.Sum64(raw)
	}
	b = wire.AppendEnd(b[:0], seq, idx, uint64(len(raw)), 0, sum)
	return s.write(ctx, conn, b)
}

func (s *Server) write(ctx context.Context, conn *websocket.Conn, msg []byte) error {
	wctx, cancel := context.WithTimeout(ctx, chunkWriteTimeout)
	defer cancel()
	return conn.Write(wctx, websocket.MessageBinary, msg)
}

// health is the /healthz body.
type health struct {
	Oldest       uint32 `json:"oldest"`
	Latest       uint32 `json:"latest"`
	Subscribers  int    `json:"subscribers"`
	LiveAbandons uint64 `json:"live_abandons"`
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	oldest, latest, _ := s.store.Bounds()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(health{
		Oldest:       oldest,
		Latest:       latest,
		Subscribers:  s.Subscribers(),
		LiveAbandons: s.LiveAbandons(),
	}); err != nil {
		s.log.Debug("healthz write failed", "error", err)
	}
}
