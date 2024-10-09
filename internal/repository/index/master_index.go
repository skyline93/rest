package index

import (
	"context"
	"fmt"
	"sync"

	log "github.com/sirupsen/logrus"
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

// SaveIndex saves all new indexes in the backend.
func (mi *MasterIndex) SaveIndex(ctx context.Context, r rest.SaverUnpacked) error {
	return mi.saveIndex(ctx, r, mi.finalizeNotFinalIndexes()...)
}

// saveIndex saves all indexes in the backend.
func (mi *MasterIndex) saveIndex(ctx context.Context, r rest.SaverUnpacked, indexes ...*Index) error {
	for i, idx := range indexes {
		log.Printf("Saving index %d", i)

		sid, err := idx.SaveIndex(ctx, r)
		if err != nil {
			return err
		}

		log.Printf("Saved index %d as %v", i, sid)
	}

	return mi.MergeFinalIndexes()
}

// finalizeNotFinalIndexes finalizes all indexes that
// have not yet been saved and returns that list
func (mi *MasterIndex) finalizeNotFinalIndexes() []*Index {
	mi.idxMutex.Lock()
	defer mi.idxMutex.Unlock()

	var list []*Index

	for _, idx := range mi.idx {
		if !idx.Final() {
			idx.Finalize()
			list = append(list, idx)
		}
	}

	log.Printf("return %d indexes", len(list))
	return list
}

// MergeFinalIndexes merges all final indexes together.
// After calling, there will be only one big final index in MasterIndex
// containing all final index contents.
// Indexes that are not final are left untouched.
// This merging can only be called after all index files are loaded - as
// removing of superseded index contents is only possible for unmerged indexes.
func (mi *MasterIndex) MergeFinalIndexes() error {
	mi.idxMutex.Lock()
	defer mi.idxMutex.Unlock()

	// The first index is always final and the one to merge into
	newIdx := mi.idx[:1]
	for i := 1; i < len(mi.idx); i++ {
		idx := mi.idx[i]
		// clear reference in masterindex as it may become stale
		mi.idx[i] = nil
		// do not merge indexes that have no id set
		ids, _ := idx.IDs()
		if !idx.Final() || len(ids) == 0 {
			newIdx = append(newIdx, idx)
		} else {
			err := mi.idx[0].merge(idx)
			if err != nil {
				return fmt.Errorf("MergeFinalIndexes: %w", err)
			}
		}
	}
	mi.idx = newIdx

	return nil
}

// Lookup queries all known Indexes for the ID and returns all matches.
func (mi *MasterIndex) Lookup(bh rest.BlobHandle) (pbs []rest.PackedBlob) {
	mi.idxMutex.RLock()
	defer mi.idxMutex.RUnlock()

	for _, idx := range mi.idx {
		pbs = idx.Lookup(bh, pbs)
	}

	return pbs
}

// LookupSize queries all known Indexes for the ID and returns the first match.
func (mi *MasterIndex) LookupSize(bh rest.BlobHandle) (uint, bool) {
	mi.idxMutex.RLock()
	defer mi.idxMutex.RUnlock()

	for _, idx := range mi.idx {
		if size, found := idx.LookupSize(bh); found {
			return size, found
		}
	}

	return 0, false
}

// AddPending adds a given blob to list of pending Blobs
// Before doing so it checks if this blob is already known.
// Returns true if adding was successful and false if the blob
// was already known
func (mi *MasterIndex) AddPending(bh rest.BlobHandle) bool {

	mi.idxMutex.Lock()
	defer mi.idxMutex.Unlock()

	// Check if blob is pending or in index
	if mi.pendingBlobs.Has(bh) {
		return false
	}

	for _, idx := range mi.idx {
		if idx.Has(bh) {
			return false
		}
	}

	// really not known -> insert
	mi.pendingBlobs.Insert(bh)
	return true
}

// StorePack remembers the id and pack in the index.
func (mi *MasterIndex) StorePack(id rest.ID, blobs []rest.Blob) {
	mi.idxMutex.Lock()
	defer mi.idxMutex.Unlock()

	// delete blobs from pending
	for _, blob := range blobs {
		mi.pendingBlobs.Delete(rest.BlobHandle{Type: blob.Type, ID: blob.ID})
	}

	for _, idx := range mi.idx {
		if !idx.Final() {
			idx.StorePack(id, blobs)
			return
		}
	}

	newIdx := NewIndex()
	newIdx.StorePack(id, blobs)
	mi.idx = append(mi.idx, newIdx)
}

// StorePack remembers the ids of all blobs of a given pack
// in the index
func (idx *Index) StorePack(id rest.ID, blobs []rest.Blob) {
	idx.m.Lock()
	defer idx.m.Unlock()

	if idx.final {
		panic("store new item in finalized index")
	}

	log.Debugf("%v", blobs)
	packIndex := idx.addToPacks(id)

	for _, blob := range blobs {
		idx.store(packIndex, blob)
	}
}

// SaveFullIndex saves all full indexes in the backend.
func (mi *MasterIndex) SaveFullIndex(ctx context.Context, r rest.SaverUnpacked) error {
	return mi.saveIndex(ctx, r, mi.finalizeFullIndexes()...)
}

// finalizeFullIndexes finalizes all indexes that are full and returns that list.
func (mi *MasterIndex) finalizeFullIndexes() []*Index {
	mi.idxMutex.Lock()
	defer mi.idxMutex.Unlock()

	var list []*Index

	log.Debugf("checking %d indexes", len(mi.idx))
	for _, idx := range mi.idx {
		if idx.Final() {
			continue
		}

		if IndexFull(idx) {
			log.Debugf("index %p is full", idx)
			idx.Finalize()
			list = append(list, idx)
		} else {
			log.Debugf("index %p not full", idx)
		}
	}

	log.Debugf("return %d indexes", len(list))
	return list
}
