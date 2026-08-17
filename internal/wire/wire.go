// Package wire encodes and decodes the corestreamd v1 stream protocol.
//
// A server-to-client ledger message is one binary WebSocket message:
//
//	[1B version=0x01][1B type=0x01][4B BE sequence][8B BE emitUnixNano][raw ledger XDR ...]
//
// emitUnixNano is the server wall clock at the moment the ledger arrived from
// the source. It is zero when the ledger was replayed from the retention store,
// because the original arrival time is not persisted: a replayed ledger carries
// no delivery-latency measurement. One asymmetry: the newest ledger is served
// from memory with its original stamp even to a subscriber replaying long
// after it was emitted, so a stamp measures delivery latency only on a stream
// being followed live.
package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Protocol constants.
const (
	// Version is the only protocol version this package speaks.
	Version byte = 0x01

	// TypeLedger marks a message whose payload is one raw ledger XDR blob.
	TypeLedger byte = 0x01

	// HeaderSize is the fixed byte count in front of every payload.
	HeaderSize = 14

	// DefaultMaxPayloadSize matches the SDK captive-core frame cap (256 MiB), so
	// any ledger core can emit fits in one message.
	DefaultMaxPayloadSize int64 = 256 << 20

	// DefaultMaxMessageSize is what a reader must admit to accept a
	// DefaultMaxPayloadSize ledger: the payload plus this protocol's header.
	// Capping a reader at the payload size alone would reject the largest ledger
	// by exactly HeaderSize bytes.
	DefaultMaxMessageSize = DefaultMaxPayloadSize + HeaderSize
)

// StreamPath is the endpoint a subscriber connects to. Its query parameters are
// start (absent or 0 = the next live ledger) and end (absent = unbounded).
const StreamPath = "/v1/stream"

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
	ErrType         = errors.New("wire: unknown message type")
)

// Header is the fixed-size prefix of a ledger message.
type Header struct {
	Version      byte
	Type         byte
	Sequence     uint32
	EmitUnixNano int64
}

// AppendLedger appends a complete ledger message for seq to dst and returns the
// extended slice. Pass emitUnixNano zero for a ledger replayed from retention.
//
// Callers that hold on to the result may keep the payload alive by slicing it
// at HeaderSize: the payload is copied into the same allocation as the header.
func AppendLedger(dst []byte, seq uint32, emitUnixNano int64, payload []byte) []byte {
	dst = AppendHeader(dst, Header{
		Version:      Version,
		Type:         TypeLedger,
		Sequence:     seq,
		EmitUnixNano: emitUnixNano,
	})
	return append(dst, payload...)
}

// AppendHeader appends h's 14 bytes to dst and returns the extended slice.
func AppendHeader(dst []byte, h Header) []byte {
	dst = append(dst, h.Version, h.Type)
	dst = binary.BigEndian.AppendUint32(dst, h.Sequence)
	return binary.BigEndian.AppendUint64(dst, uint64(h.EmitUnixNano))
}

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

// Decode splits msg into its header and payload. The payload aliases msg, so it
// lives exactly as long as msg does.
func Decode(msg []byte) (Header, []byte, error) {
	if len(msg) < HeaderSize {
		return Header{}, nil, fmt.Errorf("%w: got %d bytes, want at least %d", ErrShortMessage, len(msg), HeaderSize)
	}
	h := Header{
		Version:      msg[0],
		Type:         msg[1],
		Sequence:     binary.BigEndian.Uint32(msg[2:6]),
		EmitUnixNano: int64(binary.BigEndian.Uint64(msg[6:14])),
	}
	if h.Version != Version {
		return Header{}, nil, fmt.Errorf("%w: got 0x%02x, want 0x%02x", ErrVersion, h.Version, Version)
	}
	if h.Type != TypeLedger {
		return Header{}, nil, fmt.Errorf("%w: 0x%02x", ErrType, h.Type)
	}
	return h, msg[HeaderSize:], nil
}
