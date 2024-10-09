package rest

// IDSet is a set of IDs.
type IDSet map[ID]struct{}

// Has returns true iff id is contained in the set.
func (s IDSet) Has(id ID) bool {
	_, ok := s[id]
	return ok
}
