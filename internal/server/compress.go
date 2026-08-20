package server

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/klauspost/compress/zstd"

	"github.com/stellar-experimental/remote-core-poc/internal/wire"
)

// Chunk compression. A 14.5 MiB ledger needs ~24 ms of wire on one TCP flow
// (EC2 caps a single flow at ~5 Gbit/s outside a cluster placement group,
// ~10 inside), against a source emission window measured at ~7.5 ms — so most
// of the transfer cannot hide behind emission and lands in delivery. Real
// pubnet meta compresses ~7.6x per 256 KiB chunk, which puts the same ledger
// at ~2 MB and ~3.2 ms of wire: it fits inside the window with room, and the
// link rate stops mattering.
//
// Frames are per chunk, and independent, because the encode is parallel — a
// zstd stream is serial, and one core does not keep up with the source — and
// because a ring replay compresses chunks in isolation, with no flow to share
// a window with. Compression is also opportunistic: a payload that does not
// shrink ships CodecRaw, which is what an incompressible ledger does end to
// end.
//
// It runs ONCE per chunk, not once per subscriber: the broadcaster publishes
// one immutable message that every subscriber writes to its own socket, so
// the cost is flat in fan-out.

const (
	// compressWorkersDefault is what the live path uses when a caller does
	// not choose. Core drains its meta into the pipe at ~1.9 GB/s and one
	// core compresses ~490 MiB/s of real meta, so four workers run at par
	// with the source (~1.97 GB/s) — which is why the raw fallback in
	// submit is a designed path and not an emergency one.
	compressWorkersDefault = 4

	// maxCompressWorkers is a sanity bound on operator input. The goroutines
	// are free; the encoder states are not (~9.8 MiB of pool at concurrency
	// 6, ~25 MiB at 66), and nothing on this hardware profits past a few.
	maxCompressWorkers = 64

	// pendingChunks is how many chunks may be queued for publication. It is
	// sized by the LEDGER, not by the worker count: the source must be able
	// to hand off a whole ledger's chunks without ever waiting on the queue,
	// because a source that waits stops draining core's meta pipe. A stress
	// ledger is ~58 chunks at the default size; 1024 slots is 8 KB once per
	// run and leaves the source unblocked at any chunk size worth using.
	pendingChunks = 1024

	// ringEncoderConcurrency is how many retention-ring replays may compress
	// at once. Replays are throughput work, not latency work: queueing among
	// themselves is fine, and each state costs ~256 KiB, so this stays small.
	ringEncoderConcurrency = 2

	// encodeSlots bounds the queue INTO the encoders (chunks actually in
	// flight are encodeSlots + workers). Past ~16 the
	// extra depth only defers compression into flush(), landing it on the
	// post-emission tail; below it, the raw fallback fires for chunks that
	// had time to compress.
	encodeSlots = 16
)

// newEncoder builds an encoder with room for concurrency concurrent
// EncodeAll calls. zstd.Encoder is itself a pool of encoder states gated by a
// channel — EncodeAll BLOCKS when they are all borrowed — so this number is
// not a hint: it is how many callers can encode at once.
//
// That is why the live pipeline and the retention ring get SEPARATE encoders.
// Sharing one made a subscriber able to slow the source loop, which the
// server's whole shape exists to prevent: catch-up replays borrowed states
// the live workers then waited for, so live chunks took submit's raw fallback
// — measured 13% of chunks at 8 catch-up subscribers, 37% at 16, precisely
// when the link is busiest and compression matters most.
func newEncoder(concurrency int) (*zstd.Encoder, error) {
	return zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithEncoderConcurrency(concurrency),
		// No per-frame checksum: END carries xxhash64 over the whole RAW
		// ledger and the client verifies it, so a frame CRC detects nothing
		// extra while costing the decoder ~23% (measured on real pubnet
		// meta: 2.55 -> 3.13 GB/s, ~1.1 ms of client CPU per stress ledger).
		zstd.WithEncoderCRC(false))
}

// encodeChunk frames one chunk message into dst, compressing the payload when
// that makes it smaller. dst is a scratch buffer the caller reuses, sized so
// neither codec grows it; the returned slice aliases it.
func encodeChunk(enc *zstd.Encoder, dst []byte, seq, idx uint32, raw []byte) []byte {
	if enc != nil {
		header := wire.AppendChunkHeader(dst[:0], seq, idx, wire.CodecZstd)
		if msg := enc.EncodeAll(raw, header); len(msg)-len(header) < len(raw) {
			return msg
		}
	}
	return wire.AppendChunk(dst[:0], seq, idx, wire.CodecRaw, raw)
}

// chunkPipeline compresses chunks concurrently and publishes them in
// submission order. It lives for the whole run: a per-ledger pipeline would
// churn goroutines every 600 ms, and — the reason this is not merely
// wasteful — every early return in the source loop would leak its publisher,
// which then publishes into a broadcaster that Run has already finished.
type chunkPipeline struct {
	enc *zstd.Encoder
	// fallbacks counts chunks shipped raw because every encoder was busy.
	// Silent degradation is the failure mode this path has: the codec is per
	// chunk, so a flow that quietly stops compressing looks identical on the
	// wire to one that never needed to.
	fallbacks *atomic.Uint64
	chunkSize int
	work      chan *chunkJob
	ordered   chan *chunkJob
	publish   func(msg []byte)
	inflight  sync.WaitGroup // submitted but not yet published
	workers   sync.WaitGroup
	pubDone   chan struct{}
	stop      sync.Once
}

// chunkJob is one chunk in flight. msg is the source's own buffer: a
// ChunkHeaderSize gap followed by the raw payload, which lets the raw path
// frame in place instead of copying. It must stay valid until done closes,
// which the per-ledger arena guarantees and flush() enforces before END.
type chunkJob struct {
	seq  uint32
	idx  uint32
	msg  []byte
	out  []byte
	done chan struct{}
}

// newChunkPipeline starts workers encoders and one publisher. enc is shared
// (see newEncoder); publish is called from the publisher goroutine only, in
// submission order.
func newChunkPipeline(enc *zstd.Encoder, workers, chunkSize int, fallbacks *atomic.Uint64, publish func(msg []byte)) *chunkPipeline {
	if enc == nil {
		// Not compressing: submit frames in place and publishes inline, so
		// there is no queue, no workers and nothing to drain. The caller
		// still holds a pipeline, which keeps the source loop single-path.
		return &chunkPipeline{publish: publish}
	}
	p := &chunkPipeline{
		enc:       enc,
		fallbacks: fallbacks,
		chunkSize: chunkSize,
		work:      make(chan *chunkJob, encodeSlots),
		ordered:   make(chan *chunkJob, pendingChunks),
		publish:   publish,
		pubDone:   make(chan struct{}),
	}
	for range workers {
		p.workers.Add(1)
		go func() {
			defer p.workers.Done()
			// One scratch buffer per worker: the message is encoded here and
			// then copied out at its exact size, so neither codec grows a
			// buffer mid-encode and the published allocation is never the
			// 4x-oversized fallback the compressed sizing would imply.
			// Sized by what zstd can actually emit, not by the payload: a
			// frame of an incompressible chunk is LARGER than its input, and
			// a scratch that has to grow is reallocated on every such chunk
			// (measured 47% slower for the sake of 29 bytes).
			scratch := make([]byte, 0, wire.ChunkHeaderSize+p.enc.MaxEncodedSize(p.chunkSize))
			for job := range p.work {
				msg := encodeChunk(p.enc, scratch, job.seq, job.idx, job.msg[wire.ChunkHeaderSize:])
				job.out = bytes.Clone(msg)
				close(job.done)
			}
		}()
	}
	go func() {
		defer close(p.pubDone)
		for job := range p.ordered {
			<-job.done
			p.publish(job.out)
			p.inflight.Done()
		}
	}()
	return p
}

// submit hands one chunk to the pipeline. msg is the source's buffer with a
// ChunkHeaderSize gap in front of the payload.
//
// It does not wait in practice: the ordered queue holds a whole ledger at the
// default chunk size, and when every encoder is busy the chunk is framed raw
// in place — a header write, no copy — so the source keeps draining core's
// pipe. Encoders running at par with the source (see compressWorkersDefault)
// make that the designed steady state, not an edge case.
func (p *chunkPipeline) submit(seq, idx uint32, msg []byte) {
	if p.enc == nil {
		wire.PutChunkHeader(msg, seq, idx, wire.CodecRaw)
		p.publish(msg)
		return
	}
	job := &chunkJob{seq: seq, idx: idx, msg: msg, done: make(chan struct{})}
	p.inflight.Add(1)
	p.ordered <- job
	select {
	case p.work <- job:
	default:
		wire.PutChunkHeader(msg, seq, idx, wire.CodecRaw)
		job.out = msg
		close(job.done)
		p.fallbacks.Add(1)
	}
}

// flush blocks until every chunk submitted so far has been published. The
// caller must call it before publishing the ledger's END, which announces the
// chunk count and the checksum those chunks must match. submit and flush are
// both called from the source goroutine, so every Add happens before this
// Wait in program order.
func (p *chunkPipeline) flush() { p.inflight.Wait() }

// close drains and stops the pipeline. Safe to call more than once, and with
// nothing submitted.
func (p *chunkPipeline) close() {
	if p.work == nil {
		return // pass-through pipeline: nothing was started
	}
	p.stop.Do(func() {
		close(p.work)
		p.workers.Wait()
		close(p.ordered)
		<-p.pubDone
	})
}

// validateCompressWorkers checks an operator-supplied worker count. It is
// called from New so a bad value fails before the listener binds, not after
// the daemon reports itself up.
func validateCompressWorkers(n int) (int, error) {
	switch {
	case n == 0:
		return compressWorkersDefault, nil
	case n < 0 || n > maxCompressWorkers:
		return 0, fmt.Errorf("compress workers %d is outside [1,%d]", n, maxCompressWorkers)
	default:
		return n, nil
	}
}
