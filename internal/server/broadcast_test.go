package server

import (
	"sync"
	"testing"
	"time"
)

func TestBroadcasterPublishWakesEarlierWatch(t *testing.T) {
	b := newBroadcaster()
	tip, changed, ended := b.watch()
	if tip.seq != 0 || ended {
		t.Fatalf("fresh broadcaster: tip=%d ended=%v, want 0 and false", tip.seq, ended)
	}
	select {
	case <-changed:
		t.Fatal("notify channel closed before any publish")
	default:
	}

	b.publish(liveLedger{seq: 7, msg: []byte("seven")})

	// The channel handed out before the publish must be closed by it: this is
	// the take-the-channel-before-sleeping contract serve depends on.
	select {
	case <-changed:
	default:
		t.Fatal("publish did not close the channel handed out before it")
	}
	tip, changed, ended = b.watch()
	if tip.seq != 7 || string(tip.msg) != "seven" || ended {
		t.Fatalf("after publish: tip=%d msg=%q ended=%v, want 7 %q false", tip.seq, tip.msg, ended, "seven")
	}
	select {
	case <-changed:
		t.Fatal("the channel handed out after the publish is already closed")
	default:
	}
}

func TestBroadcasterFinishIsFinalAndIdempotent(t *testing.T) {
	b := newBroadcaster()
	b.publish(liveLedger{seq: 3})
	_, changed, _ := b.watch()

	b.finish()

	select {
	case <-changed:
	default:
		t.Fatal("finish did not wake the watcher")
	}
	if tip, _, ended := b.watch(); !ended || tip.seq != 3 {
		t.Fatalf("after finish: tip=%d ended=%v, want 3 and true", tip.seq, ended)
	}
	b.finish()
	if _, _, ended := b.watch(); !ended {
		t.Fatal("a second finish undid the first")
	}
}

func TestBroadcasterPublishAfterFinishPanicsWithoutHoldingTheLock(t *testing.T) {
	b := newBroadcaster()
	b.finish()
	defer func() {
		// The named panic, not the accidental "close of closed channel" the
		// guard exists to pre-empt: this pins the guard itself.
		if got := recover(); got != "server: ledger published after the stream was finished" {
			t.Fatalf("recovered %v, want the named publish-after-finish panic", got)
		}
		// The panic must not leave b.mu held: a recover upstream would
		// otherwise turn a crash into every watcher deadlocking.
		done := make(chan struct{})
		go func() { b.watch(); close(done) }()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("watch deadlocked after the publish panic")
		}
	}()
	b.publish(liveLedger{seq: 1})
}

func TestBroadcasterConcurrentWatchers(t *testing.T) {
	b := newBroadcaster()
	const publishes = 1000
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var last uint32
			for {
				tip, changed, ended := b.watch()
				if tip.seq < last {
					t.Errorf("tip went backwards: %d after %d", tip.seq, last)
					return
				}
				last = tip.seq
				if ended {
					return
				}
				<-changed
			}
		}()
	}
	for seq := uint32(1); seq <= publishes; seq++ {
		b.publish(liveLedger{seq: seq})
	}
	b.finish()
	wg.Wait()
}
