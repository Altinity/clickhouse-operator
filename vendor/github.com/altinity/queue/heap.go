package queue

import "container/heap"

// Slice-based min-heap implementation
type Heap []Prioritier

// Ensure heap implements container/heap Interface
var _ heap.Interface = NewHeap()

func NewHeap(l ...int) *Heap {
	var q Heap
	if len(l) > 0 {
		q = make(Heap, l[0])
	} else {
		q = make(Heap, 0)
	}
	return &q
}

// Implement container/heap Interface

func (h Heap) Len() int {
	return len(h)
}

func (h Heap) Less(i, j int) bool {
	return h[i].Priority() < h[j].Priority()
}

func (h Heap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *Heap) Push(x interface{}) {
	*h = append(*h, x.(Prioritier))
}

func (h *Heap) Pop() interface{} {
	n := len(*h)
	if n <= 0 {
		return nil
	}
	item := (*h)[n-1]
	// Explicitly exclude unused entry before shrinking slice.
	// If not excluded this entry would be stuck until re-written or array GC-ed
	(*h)[n-1] = nil
	*h = (*h)[0 : n-1]
	return item
}
