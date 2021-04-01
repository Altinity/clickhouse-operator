package queue

type Prioritier interface {
	Priority() int
}

type PriorityQueueItem interface {
	Prioritier
	Handle() T
}

type PriorityQueueItems interface {
	// Insert inserts item into the queue
	Insert(PriorityQueueItem)
	// Get gets item if available and reports whether item is returned successfully
	Get() (PriorityQueueItem, bool)
	// Len returns length of the queue
	Len() int
}
