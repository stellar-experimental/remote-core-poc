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
func PipeSource(command string, pipeBytes int) EmittingStream {
	return &pipeStream{command: command, pipeBytes: pipeBytes}
}

type pipeStream struct {
	command string
	// pipeBytes is the kernel pipe capacity to ask for. It bounds what one
	// read can return, and the source frames one chunk per read, so it is
	// really the chunk-size knob for this source. Zero keeps the default.
	pipeBytes int
}

// DefaultPipeBytes is the capacity PipeSource asks for when the caller does
// not choose. It is above the 256 KiB default chunk size so one read can fill
// a whole chunk with room over, and at or under the usual
// /proc/sys/fs/pipe-max-size of 1 MiB so an unprivileged daemon gets it.
const DefaultPipeBytes = 1 << 20

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
		// Sizing the pipe before the child inherits the write end: capacity
		// belongs to the pipe, not to an end, so this governs how much core
		// can hand over per read — and therefore how large a chunk gets. A
		// refusal (EPERM past pipe-max-size) is not fatal; the default
		// capacity still works, just in smaller pieces.
		want := p.pipeBytes
		if want == 0 {
			want = DefaultPipeBytes
		}
		if _, err := setPipeSize(r, want); err != nil {
			_ = err // best effort: the granted capacity is whatever it is
		}

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
		// Nor may a blocked read outlive the context. Everything below assumes
		// the pipe eventually EOFs, which assumes every writer eventually
		// exits — and a grandchild that escaped the process group does not:
		// it keeps the write end open and this read never returns, so the
		// defer that would close r is unreachable, Run never returns, and the
		// daemon lives on with its listener already shut. That is not
		// hypothetical; it left a corestreamd running for two days after an
		// ordinary SIGTERM, its stellar-core spinning at 100% of a core.
		// os.Pipe is poller-backed, so a deadline in the past unblocks the
		// read at once.
		stopUnblock := context.AfterFunc(ctx, func() {
			_ = r.SetReadDeadline(time.Now())
		})
		defer stopUnblock()
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
			if cmd.Process == nil {
				_ = wait()
				return
			}
			if ctx.Err() == nil {
				// An early stop with the context still live gets no Cancel
				// from CommandContext; tear the group down ourselves so no
				// grandchild survives holding the pipe.
				_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
			}
			_ = wait()
			// Whatever is still in the group outlived both the SIGTERM and
			// the child that led it. WaitDelay only escalates to the direct
			// child, so a grandchild ignoring SIGTERM — an apply-load mid-run
			// does exactly this — is left running with nothing to write to.
			// The group exists solely for this command, so sweeping it is
			// safe; on the ordinary path it is already empty and this is an
			// ESRCH no-op.
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
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
			body := io.MultiReader(bytes.NewReader(prefix[:n]), &frameTail{ctx: ctx, r: br, remaining: size - n})
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
	ctx       context.Context
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
	if err != nil && f.ctx.Err() != nil {
		// A shutdown that interrupts a body read reports the cancellation,
		// not the read deadline that implemented it: the consumer tells its
		// own shutdown from a source failure by unwrapping the context error,
		// and an i/o timeout would be logged as a failed source loop.
		return n, f.ctx.Err()
	}
	if errors.Is(err, io.EOF) && f.remaining > 0 {
		err = fmt.Errorf("pipe source: frame truncated %d bytes short: %w", f.remaining, io.ErrUnexpectedEOF)
	}
	return n, err
}
