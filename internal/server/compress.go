package server

import (
	"fmt"
	"sync"

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
	// core compresses ~450 MiB/s of real meta, so four workers keep the
	// pipeline ahead of the source; going wider adds goroutines without
	// buying window.
	compressWorkersDefault = 4

	// maxCompressWorkers bounds the flag: past this the goroutines cost more
	// than the parallelism buys.
	maxCompressWorkers = 64

	// pendingChunks is how many chunks may be queued for publication. It is
	// sized by the LEDGER, not by the worker count: the source must be able
	// to hand off a whole ledger's chunks without ever waiting on the queue,
	// because a source that waits stops draining core's meta pipe. A stress
	// ledger is ~58 chunks at the default size; 1024 slots is 8 KB once per
	// run and leaves the source unblocked at any chunk size worth using.
	pendingChunks = 1024

	// encodeSlots bounds chunks in flight INSIDE the encoders. Past ~16 the
	// extra depth only defers compression into flush(), landing it on the
	// post-emission tail; below it, the raw fallback fires for chunks that
	// had time to compress.
	encodeSlots = 16
)

// newEncoder builds the shared encoder: SpeedFastest, since the pipeline must
// stay ahead of the source, and a concurrency of workers+2 because
// zstd.Encoder is itself a pool of encoder states gated by a channel —
// EncodeAll borrows one per call, so the live workers and the retention-ring
// replays share this one object without contending for the same state, and
// the +2 keeps a replay from starving the live path.
func newEncoder(workers int) (*zstd.Encoder, error) {
	return zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithEncoderConcurrency(workers+2))
}

// encodeChunk frames one chunk message into dst, compressing the payload when
// that makes it smaller. dst is a scratch buffer the caller reuses; the
// returned slice aliases it.
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
	enc       *zstd.Encoder
	chunkSize int
	work      chan *chunkJob
	ordered   chan *chunkJob
	publish   func(msg []byte)
	inflight  sync.WaitGroup // submitted but not yet published
	workers   sync.WaitGroup
	pubDone   chan struct{}
	stop      sync.Once
}

// chunkJob is one chunk in flight: raw bytes in, framed message out. raw must
// stay valid until done closes, which the per-ledger arena guarantees and
// flush() enforces before END.
type chunkJob struct {
	seq  uint32
	idx  uint32
	raw  []byte
	out  []byte
	done chan struct{}
}

// newChunkPipeline starts workers encoders and one publisher. enc is shared
// (see newEncoder); publish is called from the publisher goroutine only, in
// submission order.
func newChunkPipeline(enc *zstd.Encoder, workers, chunkSize int, publish func(msg []byte)) *chunkPipeline {
	p := &chunkPipeline{
		enc:       enc,
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
			scratch := make([]byte, 0, wire.ChunkHeaderSize+p.chunkSize)
			for job := range p.work {
				msg := encodeChunk(p.enc, scratch, job.seq, job.idx, job.raw)
				if cap(msg) > cap(scratch) {
					scratch = msg[:0] // an oversized chunk grew it; keep the growth
				}
				job.out = append(make([]byte, 0, len(msg)), msg...)
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

// submit queues one chunk. It never waits: the ordered queue holds a whole
// ledger, and if every encoder is busy the chunk is framed raw on this
// goroutine, so the source keeps draining core's pipe.
func (p *chunkPipeline) submit(seq, idx uint32, raw []byte) {
	job := &chunkJob{seq: seq, idx: idx, raw: raw, done: make(chan struct{})}
	p.inflight.Add(1)
	p.ordered <- job
	select {
	case p.work <- job:
	default:
		msg := encodeChunk(nil, make([]byte, 0, wire.ChunkHeaderSize+len(raw)), seq, idx, raw)
		job.out = msg
		close(job.done)
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
