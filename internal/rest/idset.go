package rest

// IDSet is a set of IDs.
type IDSet map[ID]struct{}

// NewIDSet returns a new IDSet, populated with ids.
func NewIDSet(ids ...ID) IDSet {
	m := make(IDSet)
	for _, id := range ids {
		m[id] = struct{}{}
	}

	return m
}

// Has returns true iff id is contained in the set.
func (s IDSet) Has(id ID) bool {
	_, ok := s[id]
	return ok
}

// Merge adds the blobs in other to the current set.
func (s IDSet) Merge(other IDSet) {
	for id := range other {
		s.Insert(id)
	}
}

// Insert adds id to the set.
func (s IDSet) Insert(id ID) {
	s[id] = struct{}{}
}

// Sub returns a new set containing all IDs that are present in s but not in
// other.
func (s IDSet) Sub(other IDSet) (result IDSet) {
	result = NewIDSet()
	for id := range s {
		if !other.Has(id) {
			result.Insert(id)
		}
	}

	return result
}
