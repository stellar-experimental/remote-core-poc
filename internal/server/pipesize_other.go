//go:build !linux

package server

import "os"

// setPipeSize is a no-op off Linux: F_SETPIPE_SZ is a Linux extension, and the
// pipe keeps whatever capacity the platform gives it.
func setPipeSize(_ *os.File, _ int) (int, error) { return 0, nil }
