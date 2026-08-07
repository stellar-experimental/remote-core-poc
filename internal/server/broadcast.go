package server

import (
	"sync"
)

// DefaultSubscriberBuffer is how many live ledgers a subscriber may fall behind
// before the server gives up on it.
const DefaultSubscriberBuffer = 64

// liveLedger is one encoded wire message plus the sequence it carries, queued
// for delivery to a subscriber.
type liveLedger struct {
	seq uint32
	msg []byte
}

// subscriber is one connected consumer's queue. The source loop fills ch and
// never blocks on it: a full queue means this consumer cannot keep up, which
// closes dropped and makes its handler disconnect it.
type subscriber struct {
	ch      chan liveLedger
	dropped chan struct{}
	once    sync.Once
}

func (s *subscriber) drop() {
	s.once.Do(func() { close(s.dropped) })
}

// broadcaster fans one source out to every subscriber.
type broadcaster struct {
	buffer int

	mu   sync.Mutex
	subs map[*subscriber]struct{}
}

func newBroadcaster(buffer int) *broadcaster {
	if buffer <= 0 {
		buffer = DefaultSubscriberBuffer
	}
	return &broadcaster{buffer: buffer, subs: make(map[*subscriber]struct{})}
}

func (b *broadcaster) subscribe() *subscriber {
	s := &subscriber{
		ch:      make(chan liveLedger, b.buffer),
		dropped: make(chan struct{}),
	}
	b.mu.Lock()
	b.subs[s] = struct{}{}
	b.mu.Unlock()
	return s
}

func (b *broadcaster) unsubscribe(s *subscriber) {
	b.mu.Lock()
	delete(b.subs, s)
	b.mu.Unlock()
}

// publish queues l for every subscriber. It never blocks: a subscriber whose
// queue is full is dropped instead, so one slow consumer cannot stall the
// source loop or the consumers keeping up.
func (b *broadcaster) publish(l liveLedger) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for s := range b.subs {
		select {
		case <-s.dropped:
			// Already given up on. Queuing more would hand this subscriber a
			// ledger that does not follow the last one it received, turning a
			// clean "too slow" close into an apparent sequence gap.
			continue
		default:
		}
		select {
		case s.ch <- l:
		default:
			s.drop()
		}
	}
}

func (b *broadcaster) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.subs)
}
