package rest

// BlobSet is a set of blobs.
type BlobSet map[BlobHandle]struct{}

// NewBlobSet returns a new BlobSet, populated with ids.
func NewBlobSet(handles ...BlobHandle) BlobSet {
	m := make(BlobSet)
	for _, h := range handles {
		m[h] = struct{}{}
	}

	return m
}

// Has returns true iff id is contained in the set.
func (s BlobSet) Has(h BlobHandle) bool {
	_, ok := s[h]
	return ok
}

// Insert adds id to the set.
func (s BlobSet) Insert(h BlobHandle) {
	s[h] = struct{}{}
}

// Delete removes id from the set.
func (s BlobSet) Delete(h BlobHandle) {
	delete(s, h)
}
