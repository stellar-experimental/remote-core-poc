package server

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
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

func TestLedgerSeqFromMetaPrefix(t *testing.T) {
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
			got, err := ledgerSeqFromMetaPrefix(tc.meta)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("seq = %d, want %d", got, tc.want)
			}
		})
	}

	t.Run("short prefix errors", func(t *testing.T) {
		meta := metaFixture(t, 1, 5, false, 0, false)
		if _, err := ledgerSeqFromMetaPrefix(meta[:16]); err == nil {
			t.Fatal("want error on truncated prefix")
		}
	})
	t.Run("garbage discriminant errors", func(t *testing.T) {
		if _, err := ledgerSeqFromMetaPrefix([]byte{0xEE, 0xEE, 0xEE, 0xEE, 0, 0, 0, 0}); err == nil {
			t.Fatal("want error on unknown meta version")
		}
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

	src := PipeSource(fmt.Sprintf("cat %s >&3", path))
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
	src := PipeSource("exit 7")
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
