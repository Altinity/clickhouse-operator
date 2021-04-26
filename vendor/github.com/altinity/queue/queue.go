package queue

import "context"

type PriorityQueue interface {
	Insert(item PriorityQueueItem)
	Get() (item PriorityQueueItem, ctx context.Context, ok bool)
	Done(item PriorityQueueItem)
	Len() int
	Close()
	Closed() bool
}

type priorityQueue struct {
	items       PriorityQueueItems
	waiting     Map
	inProgress  Set
	cancelFns   Map
	c           Conditioner
	closed      bool
	drainClosed bool
}

// New
func New() PriorityQueue {
	return &priorityQueue{
		items:       NewHeapPriorityQueueItems(),
		waiting:     NewSimpleMap(),
		inProgress:  NewMapSet(),
		cancelFns:   NewSimpleMap(),
		c:           NewCond(),
		closed:      false,
		drainClosed: false,
	}
}

// Insert
func (q *priorityQueue) Insert(item PriorityQueueItem) {
	q.c.Lock()
	defer q.c.Unlock()

	if q.closed {
		// Do not accept items into closed queue
		return
	}

	// All waiting/progress/cancel activities are done with the handle
	handle := item.Handle()

	// Place item as waiting
	q.waiting.Insert(handle, item)

	if q.inProgress.Has(handle) {
		// In case item is already being processed it's enough to just place it into waiting,
		// it will be prioritised when Done() is called

		// Now ask current processor to complete/abort this task ASAP
		fn := q.cancelFns.Get(handle)
		fn.(context.CancelFunc)()
		return
	}

	// Completely new item, let's prioritize it and signal for waiting readers to pick it up
	q.items.Insert(item)
	q.c.Signal()
}

// Get
func (q *priorityQueue) Get() (item PriorityQueueItem, ctx context.Context, ok bool) {
	q.c.Lock()
	defer q.c.Unlock()

	for (q.items.Len() == 0) && !q.closed {
		// Wait for items to come or the queue being closed
		q.c.Wait()
	}

	switch {
	case q.closed && q.drainClosed:
		if q.items.Len() == 0 {
			// Queue is closed and drained, we are done
			return nil, nil, false
		}
		// Queue is closed, but not drained yet, continue to fetch items
	case q.closed:
		// Queue is closed and no need to drain it
		return nil, nil, false
	}

	if item, ok = q.items.Get(); !ok {
		return nil, nil, false
	}

	// All waiting/progress/cancel activities are done with the handle
	handle := item.Handle()

	// Move item from waiting to in progress
	q.waiting.Delete(handle)
	q.inProgress.Insert(handle)
	// Returned items is accompanied by cancellable context
	c, fn := context.WithCancel(context.Background())
	q.cancelFns.Insert(handle, fn)

	return item, c, true
}

// Done informs the queue that worker has done with the item.
// In case this item was re-inserted into the queue while being processed, it has to be prioritised again.
func (q *priorityQueue) Done(item PriorityQueueItem) {
	q.c.Lock()
	defer q.c.Unlock()

	// All waiting/progress/cancel activities are done with the handle
	handle := item.Handle()

	// Item is done for now, it should not be cancelled
	q.inProgress.Delete(handle)
	q.cancelFns.Delete(handle)

	// In case this item is again waiting for processing (meaning it was re-added during being processed),
	// let's prioritize it and signal for waiters to pick it up
	if q.waiting.Has(handle) {
		q.items.Insert(q.waiting.Get(handle).(PriorityQueueItem))
		q.c.Signal()
	}
}

// Len
func (q *priorityQueue) Len() int {
	q.c.Lock()
	defer q.c.Unlock()
	return q.items.Len()
}

// Close
func (q *priorityQueue) Close() {
	q.c.Lock()
	defer q.c.Unlock()
	q.closed = true
	// Notify all waiters to start shutdown process
	q.c.Broadcast()
}

// Closed reports whether the queue is closed
func (q *priorityQueue) Closed() bool {
	q.c.Lock()
	defer q.c.Unlock()
	return q.closed
}
