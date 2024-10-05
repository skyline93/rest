package rest

type BlobSet map[BlobHandle]struct{}

func NewBlobSet(handles ...BlobHandle) BlobSet {
	m := make(BlobSet)
	for _, h := range handles {
		m[h] = struct{}{}
	}

	return m
}
