package rest

import "context"

// SaverUnpacked allows saving a blob not stored in a pack file
type SaverUnpacked interface {
	// Connections returns the maximum number of concurrent backend operations
	Connections() uint
	SaveUnpacked(context.Context, FileType, []byte) (ID, error)
}
