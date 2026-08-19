package server

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"os/exec"
	"syscall"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/xdr"
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
// The frame's record marker (xdr.ReadFrameLength) supplies Emission.Size up
// front, and the ledger sequence comes from xdr.LedgerCloseMetaView's
// structural accessor over the meta's first seqPrefixLen bytes — the same
// no-full-decode path captive core's own consumer uses. The relay never
// decodes the rest. The range
// argument is informational: sequences come from the metas themselves, and
// the stream ends at pipe EOF (the child exiting cleanly ends the range).
func PipeSource(command string) EmittingStream {
	return &pipeStream{command: command}
}

type pipeStream struct {
	command string
}

// seqPrefixLen is how much of each meta is buffered before streaming the
// rest: enough for the view's walk to reach LedgerHeader.ledgerSeq, which
// crosses the meta discriminant, the optional close-meta ext, and the header
// through a maximal upgrades vector and a signed StellarValue ext — well
// under 2 KiB (pinned by TestPipeSource_SeqPrefixSuffices against real
// stress-sized core frames).
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
		// The command is arbitrary shell, so sh may fork rather than exec:
		// killing sh alone would leave a grandchild holding fd 3, the pipe
		// would never reach EOF, and shutdown would hang forever. Give the
		// child its own process group and signal the GROUP, then bound the
		// wait so a process ignoring SIGTERM cannot stall the daemon.
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		cmd.Cancel = func() error {
			return syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
		}
		cmd.WaitDelay = 5 * time.Second
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
			if cmd.Process != nil && ctx.Err() == nil {
				// An early stop with the context still live gets no Cancel
				// from CommandContext; tear the group down ourselves so no
				// grandchild survives holding the pipe.
				_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
			}
			_ = wait()
		}()

		br := bufio.NewReaderSize(r, 1<<20)
		prefix := make([]byte, seqPrefixLen)
		for {
			// ReadFrameLength enforces the last-fragment bit for us: core
			// writes each record as one fragment, and anything else is a
			// framing bug worth failing loudly on.
			length, err := xdr.ReadFrameLength(br)
			if err != nil {
				if errors.Is(err, io.EOF) {
					// Clean frame boundary: the child finished its range.
					if werr := wait(); werr != nil && ctx.Err() == nil {
						yield(Emission{}, fmt.Errorf("pipe source: %q exited: %w", p.command, werr))
					}
					return
				}
				if ctx.Err() != nil {
					return // shutdown mid-stream: the partial frame is expected
				}
				yield(Emission{}, fmt.Errorf("pipe source: read frame marker: %w", err))
				return
			}
			size := int64(length)

			n := min(size, seqPrefixLen)
			if _, err := io.ReadFull(br, prefix[:n]); err != nil {
				if ctx.Err() != nil {
					return // shutdown mid-frame
				}
				yield(Emission{}, fmt.Errorf("pipe source: read frame prefix: %w", err))
				return
			}
			seq, err := xdr.LedgerCloseMetaView(prefix[:n]).LedgerSequence()
			if err != nil {
				yield(Emission{}, fmt.Errorf("pipe source: read ledger seq: %w", err))
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
