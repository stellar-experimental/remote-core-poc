package server

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar-experimental/remote-core-poc/internal/wire"
)

// metaFixture marshals a LedgerCloseMeta with the SDK — the byte-level
// oracle ledgerSeqFromMetaPrefix must agree with.
func metaFixture(t *testing.T, version int32, seq uint32, signedSCP bool, upgrades int, extV1 bool) []byte {
	t.Helper()
	header := xdr.LedgerHeaderHistoryEntry{
		Hash: xdr.Hash{1, 2, 3},
		Header: xdr.LedgerHeader{
			LedgerVersion:      23,
			PreviousLedgerHash: xdr.Hash{4},
			ScpValue: xdr.StellarValue{
				TxSetHash: xdr.Hash{5},
				CloseTime: 1234567890,
			},
			TxSetResultHash: xdr.Hash{6},
			BucketListHash:  xdr.Hash{7},
			LedgerSeq:       xdr.Uint32(seq),
		},
	}
	for i := range upgrades {
		up := make(xdr.UpgradeType, 5+i) // odd lengths exercise XDR padding
		header.Header.ScpValue.Upgrades = append(header.Header.ScpValue.Upgrades, up)
	}
	if signedSCP {
		header.Header.ScpValue.Ext = xdr.StellarValueExt{
			V: xdr.StellarValueTypeStellarValueSigned,
			LcValueSignature: &xdr.LedgerCloseValueSignature{
				NodeId:    xdr.NodeId{Type: xdr.PublicKeyTypePublicKeyTypeEd25519, Ed25519: &xdr.Uint256{9}},
				Signature: xdr.Signature{1, 2, 3, 4, 5, 6, 7}, // odd length: padding again
			},
		}
	}

	var m xdr.LedgerCloseMeta
	switch version {
	case 0:
		m = xdr.LedgerCloseMeta{V: 0, V0: &xdr.LedgerCloseMetaV0{LedgerHeader: header}}
	case 1:
		v1 := &xdr.LedgerCloseMetaV1{
			LedgerHeader: header,
			// The parser never reaches TxSet (it stops at ledgerSeq inside
			// the header), but the SDK refuses to marshal an unset union.
			TxSet: xdr.GeneralizedTransactionSet{V: 1, V1TxSet: &xdr.TransactionSetV1{}},
		}
		if extV1 {
			fee := xdr.Int64(115)
			v1.Ext = xdr.LedgerCloseMetaExt{V: 1, V1: &xdr.LedgerCloseMetaExtV1{SorobanFeeWrite1Kb: fee}}
		}
		m = xdr.LedgerCloseMeta{V: 1, V1: v1}
	default:
		t.Fatalf("unsupported fixture version %d", version)
	}
	b, err := m.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestPipeSource_SeqPrefixSuffices pins the tap's one structural
// assumption: seqPrefixLen bytes are always enough for the SDK view to
// reach LedgerHeader.ledgerSeq, so the rest of a multi-MiB meta can stream
// without ever being buffered. Covers the shapes that lengthen the walk
// (signed SCP values, upgrade vectors, the v1 ext) and, when the capture is
// present, real stress-sized core frames.
func TestPipeSource_SeqPrefixSuffices(t *testing.T) {
	cases := []struct {
		name string
		meta []byte
		want uint32
	}{
		{"v0 basic", metaFixture(t, 0, 42, false, 0, false), 42},
		{"v1 plain", metaFixture(t, 1, 7_654_321, false, 0, false), 7_654_321},
		{"v1 extV1", metaFixture(t, 1, 99, false, 0, true), 99},
		{"v1 signed scp + upgrades", metaFixture(t, 1, 1_000_000, true, 3, true), 1_000_000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n := min(len(tc.meta), seqPrefixLen)
			got, err := xdr.LedgerCloseMetaView(tc.meta[:n]).LedgerSequence()
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("seq = %d, want %d", got, tc.want)
			}
		})
	}

	t.Run("real core frames", func(t *testing.T) {
		b, err := os.ReadFile("/mnt/nvme/tamir/metaprobe-run/meta-sample.xdr")
		if err != nil {
			t.Skip("no real-core capture on this machine")
		}
		off, frames := 0, 0
		for off+4 <= len(b) && frames < 20 {
			size := int(binary.BigEndian.Uint32(b[off:]) &^ 0x80000000)
			off += 4
			if off+size > len(b) {
				break
			}
			full := b[off : off+size]
			n := min(size, seqPrefixLen)
			prefixSeq, perr := xdr.LedgerCloseMetaView(full[:n]).LedgerSequence()
			if perr != nil {
				t.Fatalf("frame %d (%d bytes): prefix walk failed: %v", frames, size, perr)
			}
			fullSeq, ferr := xdr.LedgerCloseMetaView(full).LedgerSequence()
			if ferr != nil {
				t.Fatal(ferr)
			}
			if prefixSeq != fullSeq {
				t.Fatalf("frame %d: prefix says %d, full meta says %d", frames, prefixSeq, fullSeq)
			}
			off += size
			frames++
		}
		if frames == 0 {
			t.Fatal("capture yielded no complete frames")
		}
		t.Logf("%d real frames: prefix walk agrees with full-meta walk", frames)
	})
}

// TestPipeSource_StreamsFramedMetas runs the real exec path: a shell child
// writes RFC 5531-framed SDK-marshaled metas to fd 3, and the source must
// yield one Emission per frame with the parsed seq, the marker-declared
// size, and a Body that reproduces the frame bytes exactly.
func TestPipeSource_StreamsFramedMetas(t *testing.T) {
	dir := t.TempDir()
	var want [][]byte
	stream := make([]byte, 0, 1<<20)
	seqs := []uint32{101, 102, 103}
	for i, seq := range seqs {
		meta := metaFixture(t, 1, seq, i%2 == 0, i, true)
		var hdr [4]byte
		binary.BigEndian.PutUint32(hdr[:], uint32(len(meta))|0x80000000)
		stream = append(stream, hdr[:]...)
		stream = append(stream, meta...)
		want = append(want, meta)
	}
	path := filepath.Join(dir, "frames.bin")
	if err := os.WriteFile(path, stream, 0o644); err != nil {
		t.Fatal(err)
	}

	src := PipeSource(fmt.Sprintf("cat %s >&3", path), 0)
	var got []uint32
	for em, err := range src.Emissions(context.Background(), CountedRange(1, 0)) {
		if err != nil {
			t.Fatal(err)
		}
		body, rerr := io.ReadAll(em.Body)
		if rerr != nil {
			t.Fatal(rerr)
		}
		i := len(got)
		if em.Size != int64(len(want[i])) {
			t.Fatalf("frame %d: Size = %d, want %d", i, em.Size, len(want[i]))
		}
		if string(body) != string(want[i]) {
			t.Fatalf("frame %d: body diverges from the marshaled meta", i)
		}
		got = append(got, em.Seq)
	}
	if len(got) != len(seqs) {
		t.Fatalf("yielded %d emissions, want %d", len(got), len(seqs))
	}
	for i, seq := range seqs {
		if got[i] != seq {
			t.Fatalf("emission %d: seq = %d, want %d", i, got[i], seq)
		}
	}
}

// TestPipeSource_ChildFailureSurfaces pins the loud-exit contract: a child
// that dies mid-stream must surface an error, not a silent clean end.
func TestPipeSource_ChildFailureSurfaces(t *testing.T) {
	src := PipeSource("exit 7", 0)
	var sawErr bool
	for _, err := range src.Emissions(context.Background(), CountedRange(1, 0)) {
		if err != nil {
			sawErr = true
			break
		}
		t.Fatal("no emission expected from a childless stream")
	}
	if !sawErr {
		t.Fatal("want the child's exit status surfaced as an error")
	}
}

// TestPipeSource_TruncatedFrameSurfaces pins the integrity contract: a child
// dying mid-frame must surface ErrUnexpectedEOF from the Body, never a clean
// EOF that would let a truncated ledger publish as a valid shorter one.
func TestPipeSource_TruncatedFrameSurfaces(t *testing.T) {
	meta := metaFixture(t, 1, 55, false, 0, false)
	// Pad past the seq-parse prefix so the truncation lands in the body
	// tail (a shortfall inside the prefix read errors at emission level —
	// also loud, but a different path).
	padded := append(append([]byte{}, meta...), make([]byte, seqPrefixLen)...)
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(padded)+4096)|0x80000000) // declare more than exists
	stream := append(hdr[:], padded...)
	path := filepath.Join(t.TempDir(), "truncated.bin")
	if err := os.WriteFile(path, stream, 0o644); err != nil {
		t.Fatal(err)
	}
	src := PipeSource(fmt.Sprintf("cat %s >&3", path), 0)
	for em, err := range src.Emissions(context.Background(), CountedRange(1, 0)) {
		if err != nil {
			t.Fatalf("emission-level error before body read: %v", err)
		}
		_, rerr := io.ReadAll(em.Body)
		if !errors.Is(rerr, io.ErrUnexpectedEOF) {
			t.Fatalf("body read error = %v, want ErrUnexpectedEOF", rerr)
		}
		return
	}
	t.Fatal("no emission yielded")
}

// TestPipeSource_CancelUnblocksAnEscapedWriter pins the shutdown contract
// against the case that actually happened: a grandchild that escapes the
// process group keeps the pipe's write end open, so the read never EOFs. The
// source must still return when the context is cancelled — before this was
// fixed it did not, and the daemon stayed alive with its listener already
// shut down and a stellar-core spinning behind it for two days.
func TestPipeSource_CancelUnblocksAnEscapedWriter(t *testing.T) {
	// setsid puts the sleeper in its own session, so the source's group
	// SIGTERM cannot reach it; it inherits fd 3 and holds the pipe open. The
	// parent shell exits immediately, so the child is gone while the writer
	// is not — exactly the observed shape.
	src := PipeSource("setsid sleep 60 & exit 0", 0)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range src.Emissions(ctx, CountedRange(1, 0)) { //nolint:revive // draining is the point
		}
	}()

	// The read is blocked on a pipe nothing will ever write to or close.
	time.Sleep(200 * time.Millisecond)
	select {
	case <-done:
		t.Fatal("the source ended before the cancel: the fixture is not holding the pipe open")
	default:
	}

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("cancel did not unblock the pipe read: the source outlived its context")
	}
}

// TestSetPipeSize pins the mechanism the chunk size depends on for this
// source. A read cannot return more than the pipe holds, and the source frames
// one chunk per read, so the default 64 KiB capacity silently caps chunks at a
// quarter of the configured size no matter what -chunk-size says.
func TestSetPipeSize(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()

	before, err := setPipeSize(r, 0)
	if err != nil {
		t.Skipf("pipe sizing unavailable here: %v", err)
	}
	got, err := setPipeSize(r, DefaultPipeBytes)
	if err != nil {
		t.Skipf("kernel refused %d bytes (pipe-max-size?): %v", DefaultPipeBytes, err)
	}
	if got < wire.DefaultChunkSize {
		t.Fatalf("pipe capacity %d is under one chunk (%d): reads cannot fill a chunk",
			got, wire.DefaultChunkSize)
	}
	if got <= before {
		t.Fatalf("capacity did not grow: %d -> %d", before, got)
	}
	t.Logf("pipe capacity %d -> %d bytes", before, got)
}
