package index

import (
	"hash/maphash"

	"github.com/skyline93/rest/internal/rest"
)

// An indexMap is a chained hash table that maps blob IDs to indexEntries.
// It allows storing multiple entries with the same key.
//
// IndexMap uses some optimizations that are not compatible with supporting
// deletions.
//
// The buckets in this hash table contain only pointers, rather than inlined
// key-value pairs like the standard Go map. This way, only a pointer array
// needs to be resized when the table grows, preventing memory usage spikes.
type indexMap struct {
	// The number of buckets is always a power of two and never zero.
	buckets    []uint
	numentries uint

	mh maphash.Hash

	blockList hashedArrayTree
}

type hashedArrayTree struct {
	mask      uint
	maskShift uint
	blockSize uint

	size      uint
	blockList [][]indexEntry
}

type indexEntry struct {
	id                 rest.ID
	next               uint
	packIndex          int // Position in containing Index's packs field.
	offset             uint32
	length             uint32
	uncompressedLength uint32
}
