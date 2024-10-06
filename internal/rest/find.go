package rest

import "context"

// Loader loads a blob from a repository.
type Loader interface {
	LoadBlob(context.Context, BlobType, ID, []byte) ([]byte, error)
	LookupBlobSize(tpe BlobType, id ID) (uint, bool)
	Connections() uint
}
