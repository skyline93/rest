package rest

// BlobType specifies what a blob stored in a pack is.
type BlobType uint8

const (
	InvalidBlob BlobType = iota
	DataBlob
	TreeBlob
	NumBlobTypes // Number of types. Must be last in this enumeration.
)

type BlobHandle struct {
	ID   ID
	Type BlobType
}

type Blob struct {
	BlobHandle
	Length             uint
	Offset             uint
	UncompressedLength uint
}
