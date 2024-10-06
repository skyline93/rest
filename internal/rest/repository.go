package rest

import (
	"context"
	"errors"

	"github.com/skyline93/rest/internal/backend"
)

// ErrInvalidData is used to report that a file is corrupted
var ErrInvalidData = errors.New("invalid data returned")

type FileType = backend.FileType

// These are the different data types a backend can store.
const (
	PackFile     FileType = backend.PackFile
	KeyFile      FileType = backend.KeyFile
	LockFile     FileType = backend.LockFile
	SnapshotFile FileType = backend.SnapshotFile
	IndexFile    FileType = backend.IndexFile
	ConfigFile   FileType = backend.ConfigFile
)

// Lister allows listing files in a backend.
type Lister interface {
	List(ctx context.Context, t FileType, fn func(ID, int64) error) error
}

// SaverUnpacked allows saving a blob not stored in a pack file
type SaverUnpacked interface {
	// Connections returns the maximum number of concurrent backend operations
	Connections() uint
	SaveUnpacked(ctx context.Context, t FileType, buf []byte) (ID, error)
}

// LoaderUnpacked allows loading a blob not stored in a pack file
type LoaderUnpacked interface {
	// Connections returns the maximum number of concurrent backend operations
	Connections() uint
	LoadUnpacked(ctx context.Context, t FileType, id ID) (data []byte, err error)
}

type ListerLoaderUnpacked interface {
	Lister
	LoaderUnpacked
}
