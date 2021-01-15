package queue

type Set interface {
	Has(item T) bool
	// Insert overwrites item in the set
	Insert(item T)
	Delete(item T)
}
