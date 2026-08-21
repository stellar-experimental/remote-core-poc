// Package wire encodes and decodes the corestreamd stream protocol.
//
// Each ledger travels as a BEGIN / CHUNK* / END message flow, so the server
// can put the first bytes on the wire while the source is still emitting the
// rest — the overlap that one-message-per-ledger framing (this protocol's
// retired predecessor) makes impossible:
//
//	BEGIN [1B ver=0x02][1B type=0x10][4B BE seq][8B BE emitStartUnixNano]
//	CHUNK [1B ver=0x02][1B type=0x11][4B BE seq][4B BE chunkIdx][1B codec][payload]
//	END   [1B ver=0x02][1B type=0x12][4B BE seq][4B BE chunkCount]
//	      [8B BE totalLen][8B BE emitEndUnixNano][8B BE xxhash64-of-raw]
//
// A chunk's codec byte says how its payload is encoded: CodecRaw carries the
// ledger's bytes verbatim, CodecZstd carries one independent zstd frame the
// receiver expands before appending. Compression is per chunk and
// opportunistic — a payload that does not shrink ships raw — because a chunk
// must go out the moment it exists, and a relay cannot know a ledger's
// compressibility before it has the ledger. The counts and the checksum in
// END always describe the RAW ledger, so integrity checking is identical
// whatever each chunk chose.
//
// emitStartUnixNano is the server clock at the FIRST byte of the ledger from
// its source, emitEndUnixNano at the LAST byte: together they make both the
// headline delivery metric (client-assembled minus emitEnd) and the source's
// emission window T_emit measurable. Both stamps are zero for a ledger
// replayed from the retention ring: the original emission times are not
// persisted, so replayed ledgers never contribute delivery samples. The
// checksum is xxhash64 over the raw ledger bytes; the client verifies it on
// END before handing the meta up, and a mismatch — like a totalLen or
// chunkCount mismatch — is a protocol error that closes the stream, never a
// silent drop.
package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Protocol constants.
const (
	// Version is the protocol version byte: a cheap "is this even our
	// protocol" check on the first byte of every message. It is not a
	// compatibility mechanism — both ends of this prototype always ship
	// together, and nothing negotiates.
	Version byte = 0x02

	// TypeBegin opens one ledger's chunk flow and carries its emit-start stamp.
	TypeBegin byte = 0x10
	// TypeChunk carries one contiguous piece of the ledger's raw bytes.
	TypeChunk byte = 0x11
	// TypeEnd closes the flow and carries the counts, the emit-end stamp and
	// the xxhash64 the client verifies the assembly against.
	TypeEnd byte = 0x12

	// BeginSize is the whole BEGIN message: ver, type, seq, emitStart.
	BeginSize = 1 + 1 + 4 + 8
	// ChunkHeaderSize is the fixed prefix of a CHUNK message: ver, type, seq,
	// chunkIdx, codec. The payload follows.
	ChunkHeaderSize = 1 + 1 + 4 + 4 + 1
	// EndSize is the whole END message: ver, type, seq, chunkCount, totalLen,
	// emitEnd, checksum.
	EndSize = 1 + 1 + 4 + 4 + 8 + 8 + 8

	// DefaultChunkSize is the chunk payload size the server cuts a ledger into.
	// At 12.5 Gbps a 256 KiB chunk is ~0.17 ms of wire time, and a 14.5 MiB
	// stress ledger is ~58 chunks — framing overhead stays trivial.
	DefaultChunkSize = 256 << 10

	// MinChunkSize and MaxChunkSize bound the server's --chunk-size flag.
	// MaxChunkSize is also the chunk payload a client admits by default, so
	// default-configured ends always interoperate whatever the server's flag.
	MinChunkSize = 4 << 10
	MaxChunkSize = 8 << 20

	// CodecRaw payloads are the ledger's bytes verbatim; CodecZstd payloads
	// are one independent zstd frame each.
	CodecRaw  byte = 0x00
	CodecZstd byte = 0x01

	// DefaultMaxPayloadSize is the per-LEDGER cap — the assembly-buffer
	// ceiling on both ends. It matches the SDK captive-core frame cap
	// (256 MiB), so any ledger core can emit fits.
	DefaultMaxPayloadSize int64 = 256 << 20
)

// StreamPath is the endpoint a subscriber connects to. Its query parameters
// are start (absent or 0 = the next live ledger) and end (absent = unbounded).
const StreamPath = "/stream"

// Application close codes, in the WebSocket private range.
const (
	// StatusTooFarBehind means the subscriber needs a ledger older than the
	// server's retention. The close reason carries the retained bounds.
	StatusTooFarBehind = 4001
)

// Decode errors.
var (
	ErrShortMessage = errors.New("wire: message shorter than header")
	ErrVersion      = errors.New("wire: unsupported protocol version")
	ErrCodec        = errors.New("wire: unknown chunk codec")
	ErrType         = errors.New("wire: unknown message type")
)

// TooFarBehindReason formats the close reason for StatusTooFarBehind. The
// client parses the bounds back out of it, so the two functions belong together.
func TooFarBehindReason(oldest, latest uint32) string {
	return fmt.Sprintf("too far behind: oldest=%d latest=%d", oldest, latest)
}

// ParseTooFarBehindReason reads the retained bounds out of a
// StatusTooFarBehind close reason. ok is false for a reason it cannot read,
// which leaves the bounds unknown rather than wrong.
func ParseTooFarBehindReason(reason string) (oldest, latest uint32, ok bool) {
	n, err := fmt.Sscanf(reason, "too far behind: oldest=%d latest=%d", &oldest, &latest)
	if err != nil || n != 2 {
		return 0, 0, false
	}
	return oldest, latest, true
}

// AppendBegin appends a BEGIN message to dst and returns the extended slice.
// Pass emitStartUnixNano zero for a ledger replayed from retention.
func AppendBegin(dst []byte, seq uint32, emitStartUnixNano int64) []byte {
	dst = append(dst, Version, TypeBegin)
	dst = binary.BigEndian.AppendUint32(dst, seq)
	return binary.BigEndian.AppendUint64(dst, uint64(emitStartUnixNano))
}

// AppendChunk appends a CHUNK message to dst and returns the extended slice.
// The payload is copied, so a caller may reuse its buffer; a caller that holds
// on to the result may keep the payload alive by slicing it at ChunkHeaderSize.
func AppendChunk(dst []byte, seq uint32, chunkIdx uint32, codec byte, payload []byte) []byte {
	dst = AppendChunkHeader(dst, seq, chunkIdx, codec)
	return append(dst, payload...)
}

// AppendChunkHeader appends a CHUNK message's fixed prefix to dst. It exists
// for the compressing path, which encodes the payload straight onto the
// returned slice instead of copying an already-built payload in.
func AppendChunkHeader(dst []byte, seq uint32, chunkIdx uint32, codec byte) []byte {
	dst = append(dst, Version, TypeChunk)
	dst = binary.BigEndian.AppendUint32(dst, seq)
	dst = binary.BigEndian.AppendUint32(dst, chunkIdx)
	return append(dst, codec)
}

// PutChunkHeader writes a CHUNK message's fixed prefix into msg's first
// ChunkHeaderSize bytes. It exists for the hot path that reads a chunk's
// payload directly into the message allocation and frames it in place,
// instead of copying the payload in behind an appended header.
func PutChunkHeader(msg []byte, seq uint32, chunkIdx uint32, codec byte) {
	msg[0], msg[1] = Version, TypeChunk
	binary.BigEndian.PutUint32(msg[2:6], seq)
	binary.BigEndian.PutUint32(msg[6:10], chunkIdx)
	msg[10] = codec
}

// AppendEnd appends an END message to dst and returns the extended slice.
// checksum is xxhash64 over the ledger's raw bytes; emitEndUnixNano is zero for
// a ledger replayed from retention.
func AppendEnd(dst []byte, seq uint32, chunkCount uint32, totalLen uint64, emitEndUnixNano int64, checksum uint64) []byte {
	dst = append(dst, Version, TypeEnd)
	dst = binary.BigEndian.AppendUint32(dst, seq)
	dst = binary.BigEndian.AppendUint32(dst, chunkCount)
	dst = binary.BigEndian.AppendUint64(dst, totalLen)
	dst = binary.BigEndian.AppendUint64(dst, uint64(emitEndUnixNano))
	return binary.BigEndian.AppendUint64(dst, checksum)
}

// ValidateChunkSize checks an operator-supplied chunk payload size against the
// protocol's bounds, so every flag that configures one rejects the same range.
func ValidateChunkSize(n int) error {
	if n < MinChunkSize || n > MaxChunkSize {
		return fmt.Errorf("chunk size %d is outside [%d,%d]", n, MinChunkSize, MaxChunkSize)
	}
	return nil
}

// Message is one decoded stream message. Type selects which fields are
// meaningful: EmitStartUnixNano for TypeBegin; ChunkIndex and Payload for
// TypeChunk; ChunkCount, TotalLen, EmitEndUnixNano and Checksum for TypeEnd.
// Seq is meaningful for all three.
type Message struct {
	Type byte
	Seq  uint32

	// BEGIN
	EmitStartUnixNano int64

	// CHUNK
	ChunkIndex uint32
	Codec      byte   // CodecRaw or CodecZstd; how Payload is encoded
	Payload    []byte // aliases the decoded message, living exactly as long

	// END
	ChunkCount      uint32
	TotalLen        uint64
	EmitEndUnixNano int64
	Checksum        uint64
}

// Decode splits msg into one stream message. A chunk's Payload aliases msg.
func Decode(msg []byte) (Message, error) {
	// Every message starts with version, type and sequence.
	const commonSize = 1 + 1 + 4
	if len(msg) < commonSize {
		return Message{}, fmt.Errorf("%w: got %d bytes, want at least %d", ErrShortMessage, len(msg), commonSize)
	}
	if msg[0] != Version {
		return Message{}, fmt.Errorf("%w: got 0x%02x, want 0x%02x", ErrVersion, msg[0], Version)
	}
	m := Message{Type: msg[1], Seq: binary.BigEndian.Uint32(msg[2:6])}
	switch m.Type {
	case TypeBegin:
		if len(msg) != BeginSize {
			return Message{}, fmt.Errorf("%w: BEGIN is %d bytes, want %d", ErrShortMessage, len(msg), BeginSize)
		}
		m.EmitStartUnixNano = int64(binary.BigEndian.Uint64(msg[6:14]))
	case TypeChunk:
		if len(msg) < ChunkHeaderSize {
			return Message{}, fmt.Errorf("%w: CHUNK is %d bytes, want at least %d", ErrShortMessage, len(msg), ChunkHeaderSize)
		}
		m.ChunkIndex = binary.BigEndian.Uint32(msg[6:10])
		m.Codec = msg[10]
		if m.Codec != CodecRaw && m.Codec != CodecZstd {
			return Message{}, fmt.Errorf("%w: 0x%02x", ErrCodec, m.Codec)
		}
		m.Payload = msg[ChunkHeaderSize:]
	case TypeEnd:
		if len(msg) != EndSize {
			return Message{}, fmt.Errorf("%w: END is %d bytes, want %d", ErrShortMessage, len(msg), EndSize)
		}
		m.ChunkCount = binary.BigEndian.Uint32(msg[6:10])
		m.TotalLen = binary.BigEndian.Uint64(msg[10:18])
		m.EmitEndUnixNano = int64(binary.BigEndian.Uint64(msg[18:26]))
		m.Checksum = binary.BigEndian.Uint64(msg[26:34])
	default:
		return Message{}, fmt.Errorf("%w: 0x%02x", ErrType, m.Type)
	}
	return m, nil
}
