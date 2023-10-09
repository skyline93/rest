package rest

// BlobHandle identifies a blob of a given type.
type BlobHandle struct {
	ID   ID
	Type BlobType
}

// These are the blob types that can be stored in a pack.
const (
	InvalidBlob BlobType = iota
	DataBlob
	TreeBlob
	NumBlobTypes // Number of types. Must be last in this enumeration.
)

// Blob is one part of a file or a tree.
type Blob struct {
	BlobHandle
	Length             uint
	Offset             uint
	UncompressedLength uint
}
