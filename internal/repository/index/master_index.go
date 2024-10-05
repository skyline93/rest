package index

import (
	"sync"

	"github.com/skyline93/rest/internal/rest"
)

type MasterIndex struct {
	idx          []*Index
	pendingBlobs rest.BlobSet
	idxMutex     sync.RWMutex
}

func NewMasterIndex() *MasterIndex {
	mi := &MasterIndex{pendingBlobs: rest.NewBlobSet()}
	mi.clear()
	return mi
}

func (mi *MasterIndex) clear() {
	// Always add an empty final index, such that MergeFinalIndexes can merge into this.
	mi.idx = []*Index{NewIndex()}
	mi.idx[0].Finalize()
}
