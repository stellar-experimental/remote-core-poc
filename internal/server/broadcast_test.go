package server

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stellar-experimental/remote-core-poc/internal/wire"
)

func TestBroadcasterFlowLifecycle(t *testing.T) {
	b := newBroadcaster()
	snap, changed, ended := b.watch()
	if snap.seq() != 0 || ended {
		t.Fatalf("fresh broadcaster: seq=%d ended=%v, want 0 and false", snap.seq(), ended)
	}
	select {
	case <-changed:
		t.Fatal("notify channel closed before any publish")
	default:
	}

	// The channel handed out before each publish must be closed by it: this is
	// the take-the-channel-before-sleeping contract serve depends on.
	b.begin(7, []byte("begin-7"))
	select {
	case <-changed:
	default:
		t.Fatal("begin did not close the channel handed out before it")
	}

	snap, changed, _ = b.watch()
	if snap.seq() != 7 || string(snap.f.begin) != "begin-7" || snap.complete() {
		t.Fatalf("after begin: %+v, want an open flow for ledger 7", snap)
	}
	if len(snap.chunks) != 0 {
		t.Fatalf("a fresh flow already holds %d chunks", len(snap.chunks))
	}

	b.chunk([]byte("chunk-0"))
	select {
	case <-changed:
	default:
		t.Fatal("chunk did not close the channel handed out before it")
	}
	b.chunk([]byte("chunk-1"))

	snap, changed, _ = b.watch()
	if len(snap.chunks) != 2 || string(snap.chunks[1]) != "chunk-1" {
		t.Fatalf("after two chunks: %+v, want both visible", snap)
	}
	if snap.complete() {
		t.Fatal("the flow reads as complete before its end")
	}

	b.end([]byte("end-7"))
	select {
	case <-changed:
	default:
		t.Fatal("end did not close the channel handed out before it")
	}
	snap, _, _ = b.watch()
	if !snap.complete() || string(snap.end) != "end-7" {
		t.Fatalf("after end: %+v, want a complete flow", snap)
	}
}

func TestBroadcasterSnapshotIsStable(t *testing.T) {
	// A snapshot taken mid-flow must keep showing exactly the prefix it saw,
	// even as the writer appends and even after the next ledger replaces the
	// flow: subscribers write from snapshots while the source runs ahead.
	b := newBroadcaster()
	b.begin(1, []byte("begin"))
	b.chunk([]byte("alpha"))

	snap, _, _ := b.watch()

	b.chunk([]byte("beta"))
	b.end([]byte("end"))
	b.begin(2, []byte("begin-2"))

	if snap.seq() != 1 || len(snap.chunks) != 1 || string(snap.chunks[0]) != "alpha" || snap.complete() {
		t.Fatalf("snapshot changed under its holder: %+v", snap)
	}
}

func TestBroadcasterFinishIsFinalAndIdempotent(t *testing.T) {
	b := newBroadcaster()
	b.begin(3, nil)
	b.end([]byte("end"))
	_, changed, _ := b.watch()

	b.finish()

	select {
	case <-changed:
	default:
		t.Fatal("finish did not wake the watcher")
	}
	if snap, _, ended := b.watch(); !ended || snap.seq() != 3 {
		t.Fatalf("after finish: seq=%d ended=%v, want 3 and true", snap.seq(), ended)
	}
	b.finish()
	if _, _, ended := b.watch(); !ended {
		t.Fatal("a second finish undid the first")
	}
}

func TestBroadcasterGuardPanicsDoNotHoldTheLock(t *testing.T) {
	tests := []struct {
		name      string
		setup     func(b *broadcaster)
		publish   func(b *broadcaster)
		wantPanic string
	}{
		{
			"begin after finish",
			func(b *broadcaster) { b.finish() },
			func(b *broadcaster) { b.begin(1, nil) },
			"server: ledger begun after the stream was finished",
		},
		{
			"begin over an open flow",
			func(b *broadcaster) { b.begin(1, nil) },
			func(b *broadcaster) { b.begin(2, nil) },
			"server: ledger begun while the previous flow is still open",
		},
		{
			"chunk outside a flow",
			func(b *broadcaster) {},
			func(b *broadcaster) { b.chunk(nil) },
			"server: chunk published outside an open flow",
		},
		{
			"end outside a flow",
			func(b *broadcaster) { b.begin(1, nil); b.end([]byte("end")) },
			func(b *broadcaster) { b.end([]byte("end")) },
			"server: end published outside an open flow",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := newBroadcaster()
			tt.setup(b)
			defer func() {
				// The named panic, not an accidental one: this pins the guard.
				if got := recover(); got != tt.wantPanic {
					t.Fatalf("recovered %v, want %q", got, tt.wantPanic)
				}
				// The panic must not leave b.mu held: a recover upstream would
				// otherwise turn a crash into every watcher deadlocking.
				done := make(chan struct{})
				go func() { b.watch(); close(done) }()
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Fatal("watch deadlocked after the guard panic")
				}
			}()
			tt.publish(b)
		})
	}
}

func TestBroadcasterConcurrentWatchers(t *testing.T) {
	b := newBroadcaster()
	const flows = 300
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var lastSeq uint32
			var lastChunks int
			for {
				snap, changed, ended := b.watch()
				if snap.seq() < lastSeq {
					t.Errorf("flow went backwards: %d after %d", snap.seq(), lastSeq)
					return
				}
				if snap.seq() == lastSeq && len(snap.chunks) < lastChunks {
					t.Errorf("ledger %d chunk prefix shrank from %d to %d", snap.seq(), lastChunks, len(snap.chunks))
					return
				}
				// Chunk contents must read consistently while the writer runs:
				// the race detector is the real assertion here.
				for i, c := range snap.chunks {
					if string(c[wire.ChunkHeaderSize:]) != fmt.Sprintf("%d/%d", snap.seq(), i) {
						t.Errorf("ledger %d chunk %d holds %q", snap.seq(), i, c)
						return
					}
				}
				lastSeq, lastChunks = snap.seq(), len(snap.chunks)
				if ended {
					return
				}
				<-changed
			}
		}()
	}
	for seq := uint32(1); seq <= flows; seq++ {
		b.begin(seq, wire.AppendBegin(nil, seq, 1))
		for i := range 3 {
			b.chunk(wire.AppendChunk(nil, seq, uint32(i), fmt.Appendf(nil, "%d/%d", seq, i)))
		}
		b.end(wire.AppendEnd(nil, seq, 3, 0, int64(seq), 0))
	}
	b.finish()
	wg.Wait()
}
