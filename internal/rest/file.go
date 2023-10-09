package rest

// FileType is the type of a file in the backend.
type FileType uint8

// These are the different data types a backend can store.
const (
	PackFile FileType = 1 + iota
	KeyFile
	LockFile
	SnapshotFile
	IndexFile
	ConfigFile
)

// BlobType specifies what a blob stored in a pack is.
type BlobType uint8

// Handle is used to store and access data in a backend.
type Handle struct {
	Type              FileType
	ContainedBlobType BlobType
	Name              string
}
