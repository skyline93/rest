package rest

import (
	"context"

	"github.com/pkg/errors"
)

// ErrInvalidData is used to report that a file is corrupted
var ErrInvalidData = errors.New("invalid data returned")

// SaverUnpacked allows saving a blob not stored in a pack file
type SaverUnpacked interface {
	// Connections returns the maximum number of concurrent backend operations
	Connections() uint
	SaveUnpacked(context.Context, FileType, []byte) (ID, error)
}

// Lister allows listing files in a backend.
type Lister interface {
	List(context.Context, FileType, func(FileInfo) error) error
}

// LoaderUnpacked allows loading a blob not stored in a pack file
type LoaderUnpacked interface {
	// Connections returns the maximum number of concurrent backend operations
	Connections() uint
	LoadUnpacked(ctx context.Context, t FileType, id ID) (data []byte, err error)
}
