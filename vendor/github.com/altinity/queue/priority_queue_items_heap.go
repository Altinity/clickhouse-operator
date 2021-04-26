package queue

import (
	"container/heap"
)

type HeapPriorityQueueItems struct {
	items *Heap
}

// Ensure implements PriorityQueueItems interface
var _ PriorityQueueItems = NewHeapPriorityQueueItems()

func NewHeapPriorityQueueItems() *HeapPriorityQueueItems {
	i := &HeapPriorityQueueItems{
		items: NewHeap(),
	}
	heap.Init(i.items)
	return i
}

func (i *HeapPriorityQueueItems) Insert(item PriorityQueueItem) {
	heap.Push(i.items, item)
}

func (i *HeapPriorityQueueItems) Get() (item PriorityQueueItem, ok bool) {
	if i.items.Len() > 0 {
		return heap.Pop(i.items).(PriorityQueueItem), true
	}
	return nil, false
}

func (i *HeapPriorityQueueItems) Len() int {
	return i.items.Len()
}
