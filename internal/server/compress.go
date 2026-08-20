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
// Compression is per chunk and opportunistic (a payload that does not shrink
// ships CodecRaw), because a chunk must go out the moment it exists and the
// relay never sees a whole ledger before forwarding its first bytes. It is
// also done ONCE per chunk, not once per subscriber: the broadcaster
// publishes one immutable message that every subscriber writes to its own
// socket, so the cost is flat in fan-out.

// compressWorkersDefault is what the live path uses when a caller does not
// choose. Core drains its meta into the pipe at ~1.9 GB/s and one core
// compresses ~450 MiB/s, so four workers keep the pipeline ahead of the
// source; going wider adds goroutines without buying window.
const compressWorkersDefault = 4

// maxCompressWorkers bounds the flag: past this the goroutines and encoders
// cost more than the parallelism buys, and a fat-fingered value should be a
// startup error rather than tens of thousands of encoders.
const maxCompressWorkers = 64

// newEncoder builds the shared encoder configuration: SpeedFastest, since the
// pipeline must stay ahead of the source, and single-goroutine because the
// parallelism lives in the worker pool below (one encoder per worker).
func newEncoder() (*zstd.Encoder, error) {
	return zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithEncoderConcurrency(1))
}

// encodeChunk frames one chunk message, compressing the payload when that
// makes it smaller. It appends to dst and returns the framed message.
func encodeChunk(enc *zstd.Encoder, dst []byte, seq, idx uint32, raw []byte) []byte {
	if enc != nil {
		header := wire.AppendChunkHeader(dst, seq, idx, wire.CodecZstd)
		if msg := enc.EncodeAll(raw, header); len(msg)-len(header) < len(raw) {
			return msg
		}
		// Incompressible: zstd's frame is no smaller than the bytes it wraps,
		// so ship them verbatim. Reuse dst's original capacity rather than the
		// grown frame, so the fallback costs one append, not two.
	}
	return wire.AppendChunk(dst, seq, idx, wire.CodecRaw, raw)
}

// chunkPipeline compresses chunks concurrently and publishes them in
// submission order. It lives for the whole run, not per ledger: creating
// encoders per ledger would churn four zstd table sets and five goroutines
// every 600 ms, landing GC work in the measured post-emission tail.
//
// The source loop must never block on compression — a blocked drain backs up
// into core's meta pipe and eventually stalls ledger apply — so a submission
// that would wait for a busy encoder is framed raw inline instead. Ordering
// is preserved either way, because every chunk (compressed or not) passes
// through the same ordered queue.
type chunkPipeline struct {
	work    chan *chunkJob
	ordered chan *chunkJob
	publish func(msg []byte)
	wg      sync.WaitGroup
	pubDone chan struct{}
	stop    sync.Once
}

// chunkJob is one chunk in flight: raw bytes in, framed message out. raw must
// stay valid until done closes, which the per-ledger arena guarantees by
// carving disjoint regions and by flush() draining before the arena is reused.
type chunkJob struct {
	seq  uint32
	idx  uint32
	raw  []byte
	out  []byte
	done chan struct{}
	// flushed, when non-nil, marks a barrier rather than a chunk: the
	// publisher closes it once every job queued before it has been published.
	flushed chan struct{}
}

// newChunkPipeline starts workers encoders and one publisher. publish is
// called from the publisher goroutine only, in submission order.
func newChunkPipeline(workers int, publish func(msg []byte)) (*chunkPipeline, error) {
	if workers < 1 {
		workers = compressWorkersDefault
	}
	if workers > maxCompressWorkers {
		return nil, fmt.Errorf("compression: %d workers is over the %d cap", workers, maxCompressWorkers)
	}
	p := &chunkPipeline{
		work:    make(chan *chunkJob, workers),
		ordered: make(chan *chunkJob, workers*2),
		publish: publish,
		pubDone: make(chan struct{}),
	}
	encoders := make([]*zstd.Encoder, 0, workers)
	for range workers {
		enc, err := newEncoder()
		if err != nil {
			// Stop the workers already started before returning, or they
			// block on p.work forever with nothing to drain them.
			close(p.work)
			p.wg.Wait()
			for _, e := range encoders {
				e.Close()
			}
			return nil, fmt.Errorf("compression: %w", err)
		}
		encoders = append(encoders, enc)
		p.wg.Add(1)
		go func(enc *zstd.Encoder) {
			defer p.wg.Done()
			defer enc.Close()
			for job := range p.work {
				job.out = encodeChunk(enc, job.out[:0], job.seq, job.idx, job.raw)
				close(job.done)
			}
		}(enc)
	}
	go func() {
		defer close(p.pubDone)
		for job := range p.ordered {
			if job.flushed != nil {
				close(job.flushed)
				continue
			}
			<-job.done
			p.publish(job.out)
		}
	}()
	return p, nil
}

// submit queues one chunk. It never waits for an encoder: if every worker is
// busy the chunk is framed raw on this goroutine, which keeps the source
// draining core's pipe at the cost of one uncompressed chunk.
func (p *chunkPipeline) submit(seq, idx uint32, raw []byte) {
	job := &chunkJob{
		seq: seq, idx: idx, raw: raw,
		// One allocation per chunk, sized for the header plus a compressed
		// payload; the published message must outlive the ledger, so these
		// are never recycled.
		out:  make([]byte, 0, wire.ChunkHeaderSize+len(raw)/4+64),
		done: make(chan struct{}),
	}
	p.ordered <- job
	select {
	case p.work <- job:
	default:
		job.out = encodeChunk(nil, job.out[:0], seq, idx, raw)
		close(job.done)
	}
}

// flush blocks until every chunk submitted so far has been published. The
// caller must call it before publishing the ledger's END — which announces
// the chunk count — and before reusing the buffers the chunks were read into.
func (p *chunkPipeline) flush() {
	barrier := &chunkJob{flushed: make(chan struct{})}
	p.ordered <- barrier
	<-barrier.flushed
}

// close drains and stops the pipeline. It is safe to call more than once and
// safe to call with nothing submitted.
func (p *chunkPipeline) close() {
	p.stop.Do(func() {
		close(p.work)
		p.wg.Wait()
		close(p.ordered)
		<-p.pubDone
	})
}

// ringEncoder borrows an encoder for a retention-ring replay, or nil when the
// server ships raw chunks. Replays are not latency-critical, so they encode on
// the serving goroutine; pooling keeps a reconnect storm from allocating one
// encoder per subscriber per ledger.
func (s *Server) ringEncoder() *zstd.Encoder {
	if !s.compress {
		return nil
	}
	if enc, ok := s.ringEncPool.Get().(*zstd.Encoder); ok {
		return enc
	}
	enc, err := newEncoder()
	if err != nil {
		// Only invalid options can fail here, and they are compile-time
		// constants; ship raw rather than failing a replay.
		s.log.Warn("compression unavailable for ring replay", "error", err)
		return nil
	}
	return enc
}

func (s *Server) putRingEncoder(enc *zstd.Encoder) {
	if enc != nil {
		s.ringEncPool.Put(enc)
	}
}
