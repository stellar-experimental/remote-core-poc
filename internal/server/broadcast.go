package server

import (
	"sync"
)

// flow is one ledger's chunk flow as the source loop publishes it: the encoded
// BEGIN, the encoded CHUNK messages so far, and the encoded END once the
// source has emitted the last byte. seq and begin are immutable from the
// moment the flow is published, chunks is append-only, and end is written
// once — every published message is immutable, which is what lets snapshots
// be handed to subscribers without copies.
type flow struct {
	seq    uint32
	begin  []byte
	chunks [][]byte
	end    []byte // nil while the ledger is still being emitted
}

// flowSnap is one subscriber's view of the current flow: the shared flow plus
// the chunk prefix and end published by watch time. seq and begin are read
// through the flow because they are immutable; chunks and end are copied under
// the broadcaster's lock because the writer may still be appending.
type flowSnap struct {
	f      *flow
	chunks [][]byte
	end    []byte
}

// seq is the flow's ledger, or 0 before the first begin.
func (s flowSnap) seq() uint32 {
	if s.f == nil {
		return 0
	}
	return s.f.seq
}

// complete reports whether the flow's END had been published by watch time.
func (s flowSnap) complete() bool { return s.end != nil }

// broadcaster is a watch over the in-flight ledger's chunk flow. Subscribers
// are cursors over the retention store; the only things pushed at them are the
// current flow's published prefix and a wakeup whenever it grows. There is no
// per-subscriber queue to overflow: a subscriber that falls behind reads what
// it missed back out of the store.
//
// The current flow — complete or not — is retained until the next begin
// replaces it, so a subscriber arriving mid-emission replays its chunks from
// memory and joins the live tail, and the just-completed ledger is served from
// memory with its real stamps rather than raced to disk.
type broadcaster struct {
	mu     sync.Mutex
	cur    *flow         // nil until the first begin
	notify chan struct{} // closed on every publish, and once more by finish
	ended  bool          // the flow is final; nothing may follow
	srcErr error         // the source failure finish recorded, if any
}

func newBroadcaster() *broadcaster {
	return &broadcaster{notify: make(chan struct{})}
}

// begin opens ledger seq's flow, replacing the previous ledger's, and wakes
// every watcher. Like every publish, it never blocks: waking is closing a
// channel, so nothing a subscriber does can stall the source loop, and its
// cost does not grow with the subscriber count.
func (b *broadcaster) begin(seq uint32, beginMsg []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.ended {
		// Named panic, and the deferred unlock releases the mutex: a recover
		// upstream must not leave every watcher deadlocked on b.mu.
		panic("server: ledger begun after the stream was finished")
	}
	if b.cur != nil && b.cur.end == nil {
		panic("server: ledger begun while the previous flow is still open")
	}
	b.cur = &flow{seq: seq, begin: beginMsg}
	b.wakeLocked()
}

// chunk appends one encoded CHUNK message to the open flow and wakes every
// watcher.
func (b *broadcaster) chunk(chunkMsg []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.cur == nil || b.cur.end != nil {
		panic("server: chunk published outside an open flow")
	}
	b.cur.chunks = append(b.cur.chunks, chunkMsg)
	b.wakeLocked()
}

// end closes the open flow with its encoded END message and wakes every
// watcher.
func (b *broadcaster) end(endMsg []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.cur == nil || b.cur.end != nil {
		panic("server: end published outside an open flow")
	}
	b.cur.end = endMsg
	b.wakeLocked()
}

// finish marks the stream final and wakes every watcher one last time. A
// non-nil srcErr is the source's failure: subscribers must be able to tell
// "the source finished" from "the source broke", or an unbounded consumer
// ends its iteration with a nil error and silently stops ingesting.
func (b *broadcaster) finish(srcErr error) {
	b.mu.Lock()
	if !b.ended {
		b.ended = true
		b.srcErr = srcErr
		close(b.notify)
	}
	b.mu.Unlock()
}

// failure reports the source failure recorded by finish, once ended.
func (b *broadcaster) failure() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.srcErr
}

func (b *broadcaster) wakeLocked() {
	close(b.notify)
	b.notify = make(chan struct{})
}

// watch returns the current flow's published prefix (snap.seq() is 0 before
// the first begin), a channel that is closed the next time anything changes,
// and whether the stream is final. Take the channel before deciding to sleep:
// a publish landing after watch returns has already closed it, so the sleep
// wakes immediately instead of missing the event. Because finish shares the
// lock, an ended watch has already seen everything the source will ever
// produce.
func (b *broadcaster) watch() (flowSnap, <-chan struct{}, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var snap flowSnap
	if b.cur != nil {
		snap = flowSnap{f: b.cur, chunks: b.cur.chunks, end: b.cur.end}
	}
	return snap, b.notify, b.ended
}
