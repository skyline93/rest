package index

import (
	"sync"

	"github.com/skyline93/rest/internal/rest"
)

// MasterIndex is a collection of indexes and IDs of chunks that are in the process of being saved.
type MasterIndex struct {
	idx          []*Index
	pendingBlobs rest.BlobSet
	idxMutex     sync.RWMutex
	compress     bool
}

// NewMasterIndex creates a new master index.
func NewMasterIndex() *MasterIndex {
	// Always add an empty final index, such that MergeFinalIndexes can merge into this.
	// Note that removing this index could lead to a race condition in the rare
	// sitation that only two indexes exist which are saved and merged concurrently.
	idx := []*Index{NewIndex()}
	idx[0].Finalize()
	return &MasterIndex{idx: idx, pendingBlobs: rest.NewBlobSet()}
}

func (mi *MasterIndex) MarkCompressed() {
	mi.compress = true
}
