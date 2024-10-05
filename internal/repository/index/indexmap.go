package index

import (
	"hash/maphash"

	"github.com/skyline93/rest/internal/rest"
)

type indexEntry struct {
	id                 rest.ID
	next               uint
	packIndex          int // Position in containing Index's packs field.
	offset             uint32
	length             uint32
	uncompressedLength uint32
}

type hashedArrayTree struct {
	mask      uint
	maskShift uint
	blockSize uint

	size      uint
	blockList [][]indexEntry
}

type indexMap struct {
	// The number of buckets is always a power of two and never zero.
	buckets    []uint
	numentries uint

	mh maphash.Hash

	blockList hashedArrayTree
}
