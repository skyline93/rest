package index

import (
	"log"
	"sync"
	"time"

	"github.com/skyline93/rest/internal/rest"
)

// In large repositories, millions of blobs are stored in the repository
// and restic needs to store an index entry for each blob in memory for
// most operations.
// Hence the index data structure defined here is one of the main contributions
// to the total memory requirements of restic.
//
// We store the index entries in indexMaps. In these maps, entries take 56
// bytes each, plus 8/4 = 2 bytes of unused pointers on average, not counting
// malloc and header struct overhead and ignoring duplicates (those are only
// present in edge cases and are also removed by prune runs).
//
// In the index entries, we need to reference the packID. As one pack may
// contain many blobs the packIDs are saved in a separate array and only the index
// within this array is saved in the indexEntry
//
// We assume on average a minimum of 8 blobs per pack; BP=8.
// (Note that for large files there should be 3 blobs per pack as the average chunk
// size is 1.5 MB and the minimum pack size is 4 MB)
//
// We have the following sizes:
// indexEntry:  56 bytes  (on amd64)
// each packID: 32 bytes
//
// To save N index entries, we therefore need:
// N * (56 + 2) bytes + N * 32 bytes / BP = N * 62 bytes,
// i.e., fewer than 64 bytes per blob in an index.

// Index holds lookup tables for id -> pack.
type Index struct {
	m      sync.Mutex
	byType [rest.NumBlobTypes]indexMap
	packs  rest.IDs

	final      bool     // set to true for all indexes read from the backend ("finalized")
	ids        rest.IDs // set to the IDs of the contained finalized indexes
	supersedes rest.IDs
	created    time.Time
}

// NewIndex returns a new index.
func NewIndex() *Index {
	return &Index{
		created: time.Now(),
	}
}

// Finalize sets the index to final.
func (idx *Index) Finalize() {
	log.Println("finalizing index")
	idx.m.Lock()
	defer idx.m.Unlock()

	idx.final = true
}
