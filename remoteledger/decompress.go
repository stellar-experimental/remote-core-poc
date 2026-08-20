package remoteledger

import (
	"fmt"

	"github.com/klauspost/compress/zstd"

	"github.com/stellar-experimental/remote-core-poc/internal/wire"
)

// Chunk decompression. The server compresses each chunk independently and
// opportunistically (see internal/server/compress.go), so a flow can mix
// codecs and the client decides per chunk. Decompression appends straight
// into the assembly buffer: no intermediate copy, and the buffer keeps its
// capacity across ledgers.
//
// One core decompresses ~2.2 GB/s, against the ~1.9 GB/s a source can emit,
// so a single decoder keeps pace with a live flow.

// newDecoder builds the client's decoder. Frames expand into a scratch
// buffer, never straight into the assembly, so this limit bounds ONE chunk's
// expansion exactly — zstd's limit counts the whole output slice it is given,
// so decoding into a growing assembly would both reject legitimate ledgers
// (the bound fires once the LEDGER outgrows a chunk) and, for a frame that
// does not declare its content size, let a hostile peer overshoot before the
// caller's cap check runs.
func newDecoder(maxChunkBytes int64) (*zstd.Decoder, error) {
	if maxChunkBytes <= 0 || maxChunkBytes > wire.MaxChunkSize {
		maxChunkBytes = wire.MaxChunkSize
	}
	return zstd.NewReader(nil,
		zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderMaxMemory(uint64(maxChunkBytes)))
}

// appendChunkPayload appends one chunk's ledger bytes to dst, expanding it
// first when the chunk arrived compressed. scratch carries the expansion
// buffer between calls; it is grown as needed and never aliased by dst.
func appendChunkPayload(dec *zstd.Decoder, dst, payload []byte, codec byte, scratch *[]byte) ([]byte, error) {
	switch codec {
	case wire.CodecRaw:
		return append(dst, payload...), nil
	case wire.CodecZstd:
		out, err := dec.DecodeAll(payload, (*scratch)[:0])
		if err != nil {
			return nil, fmt.Errorf("decompress: %w", err)
		}
		*scratch = out
		return append(dst, out...), nil
	default:
		// wire.Decode rejects unknown codecs, so this is unreachable unless
		// the two ends disagree about the format.
		return nil, fmt.Errorf("unknown chunk codec 0x%02x", codec)
	}
}
