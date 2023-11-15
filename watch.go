package kvdb

import (
	"sync"
	"time"
)

type WatchActionType = byte

const (
	WatchActionPut WatchActionType = iota
	WatchActionDelete
)

type Event struct {
	Action  WatchActionType
	Key     []byte
	Value   []byte
	BatchId uint64
}

type Watcher struct {
	queue *CircularQueue[*Event]
	mu    sync.RWMutex
}

// NewWatcher 创建一个监视器的实例
func NewWatcher(capacity uint64) *Watcher {
	return &Watcher{
		queue: NewCircularQueue[*Event](capacity),
		mu:    sync.RWMutex{},
	}
}

func (w *Watcher) putEvent(e *Event) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.queue.Put(e)
	if w.queue.Full() {
		w.queue.MoveFrontOneStep()
	}
}

func (w *Watcher) getEvent() *Event {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.queue.Empty() {
		return nil
	}
	return w.queue.Front()
}

func (w *Watcher) sendEvent(c chan *Event) {
	for {
		event := w.getEvent()
		if event == nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		c <- event
	}
}
