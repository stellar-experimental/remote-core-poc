package server

import (
	"sync"
)

// liveLedger is one encoded wire message plus the sequence it carries: the tip
// of the stream as the source loop last published it.
type liveLedger struct {
	seq uint32
	msg []byte
}

// broadcaster is a single-value watch over the most recently published ledger.
// Subscribers are cursors over the retention store; the only things pushed at
// them are the current tip and a wakeup when it moves. There is no
// per-subscriber queue to overflow: a subscriber that falls behind reads what
// it missed back out of the store.
type broadcaster struct {
	mu     sync.Mutex
	latest liveLedger    // seq 0 until the first publish
	notify chan struct{} // closed on every publish, and once more by finish
	ended  bool          // the tip is final; no publish may follow
}

func newBroadcaster() *broadcaster {
	return &broadcaster{notify: make(chan struct{})}
}

// publish makes l the tip and wakes every watcher. It never blocks: waking is
// closing a channel, so nothing a subscriber does can stall the source loop,
// and its cost does not grow with the subscriber count.
func (b *broadcaster) publish(l liveLedger) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.ended {
		// Named panic, and the deferred unlock releases the mutex: a recover
		// upstream must not leave every watcher deadlocked on b.mu.
		panic("server: ledger published after the stream was finished")
	}
	b.latest = l
	close(b.notify)
	b.notify = make(chan struct{})
}

// finish marks the tip final and wakes every watcher one last time.
func (b *broadcaster) finish() {
	b.mu.Lock()
	if !b.ended {
		b.ended = true
		close(b.notify)
	}
	b.mu.Unlock()
}

// watch returns the current tip, a channel that is closed the next time
// anything changes, and whether the tip is final. Take the channel before
// deciding to sleep: a publish landing after watch returns has already closed
// it, so the sleep wakes immediately instead of missing the ledger. Because
// finish shares the tip's lock, an ended watch has already seen the last
// ledger the source will ever produce.
func (b *broadcaster) watch() (liveLedger, <-chan struct{}, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.latest, b.notify, b.ended
}
