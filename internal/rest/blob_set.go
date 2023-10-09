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
