package queue

type MapSet map[T]empty
type empty struct{}

func NewMapSet() Set {
	return make(MapSet)
}

// IMPORTANT
// item T has to have equality defined

func (s MapSet) Has(item T) bool {
	_, exists := s[item]
	return exists
}

func (s MapSet) Insert(item T) {
	s[item] = empty{}
}

func (s MapSet) Delete(item T) {
	delete(s, item)
}
