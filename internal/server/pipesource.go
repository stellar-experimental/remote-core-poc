package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"os/exec"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
)

// PipeSource taps a ledger source BELOW the complete-ledger seam: it runs
// command (via `sh -c`, so the operator controls quoting) with the write end
// of a pipe inherited as child fd 3, and yields each RFC 5531-framed
// LedgerCloseMeta record on that pipe as one Emission whose Body streams the
// frame's bytes as the child writes them. Pointing the child's
// METADATA_OUTPUT_STREAM at "fd:3" makes a real stellar-core the emitter —
// the deployment shape this PoC exists to prove — so unlike the captive
// source (which the SDK seam confines to complete metas at window zero),
// transfer overlaps core's actual pipe-write burst.
//
// The frame's 4-byte record marker supplies Emission.Size up front, and the
// ledger sequence is parsed from the meta's fixed prefix (see
// ledgerSeqFromMetaPrefix) — the relay never decodes the rest. The range
// argument is informational: sequences come from the metas themselves, and
// the stream ends at pipe EOF (the child exiting cleanly ends the range).
func PipeSource(command string) EmittingStream {
	return &pipeStream{command: command}
}

type pipeStream struct {
	command string
}

// seqPrefixLen bounds the bytes needed by ledgerSeqFromMetaPrefix: the walk
// crosses the meta discriminant, the (optional) close-meta ext, and the
// ledger header up to ledgerSeq — including a maximal upgrades vector and a
// signed StellarValue ext, that is well under 2 KiB.
const seqPrefixLen = 4096

func (p *pipeStream) Emissions(ctx context.Context, _ ledgerbackend.Range) iter.Seq2[Emission, error] {
	return func(yield func(Emission, error) bool) {
		r, w, err := os.Pipe()
		if err != nil {
			yield(Emission{}, fmt.Errorf("pipe source: %w", err))
			return
		}
		defer r.Close()

		cmd := exec.CommandContext(ctx, "sh", "-c", p.command)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		cmd.ExtraFiles = []*os.File{w} // fd 3 in the child
		if err := cmd.Start(); err != nil {
			w.Close()
			yield(Emission{}, fmt.Errorf("pipe source: start %q: %w", p.command, err))
			return
		}
		// The parent must not hold the write end open, or EOF never arrives.
		w.Close()
		// The child is killed by CommandContext on ctx cancel; reap it
		// exactly once on every exit path so a yield-stop cannot leak a
		// zombie and no path ever sees a second Wait's spurious error.
		var waitErr error
		waited := false
		wait := func() error {
			if !waited {
				waited = true
				waitErr = cmd.Wait()
			}
			return waitErr
		}
		// Close the read end BEFORE waiting: on an early stop (consumer quit
		// mid-frame, ctx intact) the child may be blocked writing into the
		// full pipe — closing our end turns its next write into EPIPE so it
		// exits and Wait returns, instead of deadlocking shutdown. The outer
		// r.Close is then a harmless double-close.
		defer func() {
			_ = r.Close()
			_ = wait()
		}()

		br := bufio.NewReaderSize(r, 1<<20)
		var hdr [4]byte
		prefix := make([]byte, seqPrefixLen)
		for {
			if _, err := io.ReadFull(br, hdr[:]); err != nil {
				if errors.Is(err, io.EOF) {
					// Clean frame boundary: the child finished its range.
					if werr := wait(); werr != nil && ctx.Err() == nil {
						yield(Emission{}, fmt.Errorf("pipe source: %q exited: %w", p.command, werr))
					}
					return
				}
				yield(Emission{}, fmt.Errorf("pipe source: read frame marker: %w", err))
				return
			}
			marker := binary.BigEndian.Uint32(hdr[:])
			if marker&0x80000000 == 0 {
				// Core writes each record as a single last-fragment; anything
				// else is a framing bug worth failing loudly on.
				yield(Emission{}, fmt.Errorf("pipe source: multi-fragment record (marker %#x)", marker))
				return
			}
			size := int64(marker &^ 0x80000000)

			n := min(size, seqPrefixLen)
			if _, err := io.ReadFull(br, prefix[:n]); err != nil {
				yield(Emission{}, fmt.Errorf("pipe source: read frame prefix: %w", err))
				return
			}
			seq, err := ledgerSeqFromMetaPrefix(prefix[:n])
			if err != nil {
				yield(Emission{}, fmt.Errorf("pipe source: parse ledger seq: %w", err))
				return
			}
			body := io.MultiReader(bytes.NewReader(prefix[:n]), &frameTail{r: br, remaining: size - n})
			if !yield(Emission{Seq: seq, Size: size, Body: body}, nil) {
				return
			}
		}
	}
}

// frameTail serves the frame bytes past the parsed prefix, converting an
// EOF before the marker-declared length into a loud ErrUnexpectedEOF — a
// child dying mid-frame must never read as a clean, shorter ledger.
type frameTail struct {
	r         *bufio.Reader
	remaining int64
}

func (f *frameTail) Read(p []byte) (int, error) {
	if f.remaining == 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > f.remaining {
		p = p[:f.remaining]
	}
	n, err := f.r.Read(p)
	f.remaining -= int64(n)
	if errors.Is(err, io.EOF) && f.remaining > 0 {
		err = fmt.Errorf("pipe source: frame truncated %d bytes short: %w", f.remaining, io.ErrUnexpectedEOF)
	}
	return n, err
}

// ledgerSeqFromMetaPrefix walks the fixed-shape prefix of a serialized
// LedgerCloseMeta (v0/v1/v2) to LedgerHeader.ledgerSeq without decoding the
// meta: discriminant, close-meta ext (v1+ only), then the header history
// entry — hash, ledgerVersion, previousLedgerHash, scpValue (txSetHash,
// closeTime, upgrades vector, basic-or-signed ext), txSetResultHash,
// bucketListHash, ledgerSeq. Every discriminant on the path is validated so
// a foreign or corrupt stream errors instead of yielding a garbage sequence.
func ledgerSeqFromMetaPrefix(b []byte) (uint32, error) {
	c := xdrCursor{b: b}
	metaV, err := c.u32()
	if err != nil {
		return 0, err
	}
	if metaV > 2 {
		return 0, fmt.Errorf("unknown LedgerCloseMeta version %d", metaV)
	}
	if metaV >= 1 {
		extV, err := c.u32()
		if err != nil {
			return 0, err
		}
		switch extV {
		case 0: // void
		case 1: // ExtensionPoint (u32 0) + sorobanFeeWrite1KB (int64)
			if err := c.skip(12); err != nil {
				return 0, err
			}
		default:
			return 0, fmt.Errorf("unknown LedgerCloseMetaExt version %d", extV)
		}
	}
	// LedgerHeaderHistoryEntry.hash, then LedgerHeader up to ledgerSeq.
	if err := c.skip(32 + 4 + 32 + 32 + 8); err != nil { // hash, ledgerVersion, prevHash, txSetHash, closeTime
		return 0, err
	}
	nUpgrades, err := c.u32()
	if err != nil {
		return 0, err
	}
	if nUpgrades > 6 {
		return 0, fmt.Errorf("upgrades vector claims %d entries (max 6)", nUpgrades)
	}
	for range nUpgrades {
		if err := c.skipOpaque(); err != nil {
			return 0, err
		}
	}
	scpExtV, err := c.u32()
	if err != nil {
		return 0, err
	}
	switch scpExtV {
	case 0: // STELLAR_VALUE_BASIC
	case 1: // STELLAR_VALUE_SIGNED: NodeID (key type + 32B ed25519) + Signature
		keyType, err := c.u32()
		if err != nil {
			return 0, err
		}
		if keyType != 0 {
			return 0, fmt.Errorf("unexpected NodeID key type %d", keyType)
		}
		if err := c.skip(32); err != nil {
			return 0, err
		}
		if err := c.skipOpaque(); err != nil {
			return 0, err
		}
	default:
		return 0, fmt.Errorf("unknown StellarValue ext %d", scpExtV)
	}
	if err := c.skip(32 + 32); err != nil { // txSetResultHash, bucketListHash
		return 0, err
	}
	return c.u32()
}

// xdrCursor is a bounds-checked forward reader over big-endian XDR bytes.
type xdrCursor struct {
	b   []byte
	off int
}

func (c *xdrCursor) u32() (uint32, error) {
	if c.off+4 > len(c.b) {
		return 0, errShortMetaPrefix
	}
	v := binary.BigEndian.Uint32(c.b[c.off:])
	c.off += 4
	return v, nil
}

func (c *xdrCursor) skip(n int) error {
	if c.off+n > len(c.b) {
		return errShortMetaPrefix
	}
	c.off += n
	return nil
}

// skipOpaque skips one variable-length opaque: u32 length, data, pad to 4.
func (c *xdrCursor) skipOpaque() error {
	n, err := c.u32()
	if err != nil {
		return err
	}
	return c.skip(int(n+3) &^ 3)
}

var errShortMetaPrefix = errors.New("meta prefix too short for header walk")
