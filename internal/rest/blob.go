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

// PackedBlob is a blob stored within a file.
type PackedBlob struct {
	Blob
	PackID ID
}

func (t BlobType) IsMetadata() bool {
	switch t {
	case TreeBlob:
		return true
	default:
		return false
	}
}

func (b Blob) IsCompressed() bool {
	return b.UncompressedLength != 0
}
