//go:build linux

package server

import (
	"os"

	"golang.org/x/sys/unix"
)

// setPipeSize asks the kernel to resize the pipe f belongs to, and reports the
// capacity it actually got.
//
// This matters more than it looks. A read can never return more than the pipe
// holds, and the source frames one chunk per read — so with the 64 KiB default
// a 14.48 MiB ledger ships as ~232 undersized chunks instead of ~58 full ones,
// quadrupling every per-chunk cost on both ends: compression frames, WebSocket
// frames, decode calls, scheduling, assembly.
//
// A request above /proc/sys/fs/pipe-max-size fails with EPERM for an
// unprivileged process, so failure is not exceptional: the caller keeps the
// default capacity and carries on. Sizes are rounded up to a page and capped
// by the kernel, which is why the granted size is read back rather than
// assumed.
func setPipeSize(f *os.File, want int) (int, error) {
	// SyscallConn, never Fd: Fd takes the file out of the runtime poller and
	// puts it in blocking mode, which silently disables SetReadDeadline — the
	// one thing that lets a cancelled context unblock a parked read. Control
	// hands over the descriptor without disturbing any of that.
	rc, err := f.SyscallConn()
	if err != nil {
		return 0, err
	}
	var got int
	var opErr error
	if cerr := rc.Control(func(fd uintptr) {
		if want > 0 {
			if _, e := unix.FcntlInt(fd, unix.F_SETPIPE_SZ, want); e != nil {
				opErr = e
			}
		}
		g, e := unix.FcntlInt(fd, unix.F_GETPIPE_SZ, 0)
		if e != nil {
			if opErr == nil {
				opErr = e
			}
			return
		}
		got = g
	}); cerr != nil {
		return 0, cerr
	}
	return got, opErr
}
