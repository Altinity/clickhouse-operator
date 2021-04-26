package queue

type Map interface {
	Has(item T) bool
	// Insert overwrites item's value
	Insert(item T, value T)
	Get(item T) T
	Delete(item T)
}

type SimpleMap map[T]T

func NewSimpleMap() SimpleMap {
	return make(SimpleMap)
}

// IMPORTANT
// item T has to have equality defined

func (s SimpleMap) Has(item T) bool {
	_, exists := s[item]
	return exists
}

func (s SimpleMap) Get(item T) T {
	return s[item]
}

func (s SimpleMap) Insert(item, value T) {
	s[item] = value
}

func (s SimpleMap) Delete(item T) {
	delete(s, item)
}
