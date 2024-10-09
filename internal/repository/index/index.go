package index

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/skyline93/rest/internal/crypto"
	"github.com/skyline93/rest/internal/rest"
)

type Index struct {
	m      sync.RWMutex
	byType [rest.NumBlobTypes]indexMap
	packs  rest.IDs

	final   bool     // set to true for all indexes read from the backend ("finalized")
	ids     rest.IDs // set to the IDs of the contained finalized indexes
	created time.Time
}

func NewIndex() *Index {
	return &Index{
		created: time.Now(),
	}
}

func (idx *Index) Finalize() {
	log.Infof("finalizing index")
	idx.m.Lock()
	defer idx.m.Unlock()

	idx.final = true
}

// SaveIndex saves an index in the repository.
func (idx *Index) SaveIndex(ctx context.Context, repo rest.SaverUnpacked) (rest.ID, error) {
	buf := bytes.NewBuffer(nil)

	err := idx.Encode(buf)
	if err != nil {
		return rest.ID{}, err
	}

	id, err := repo.SaveUnpacked(ctx, rest.IndexFile, buf.Bytes())
	ierr := idx.SetID(id)
	if ierr != nil {
		// logic bug
		panic(ierr)
	}
	return id, err
}

// Encode writes the JSON serialization of the index to the writer w.
func (idx *Index) Encode(w io.Writer) error {
	log.Info("encoding index")
	idx.m.RLock()
	defer idx.m.RUnlock()

	list, err := idx.generatePackList()
	if err != nil {
		return err
	}

	enc := json.NewEncoder(w)
	idxJSON := jsonIndex{
		Packs: list,
	}
	return enc.Encode(idxJSON)
}

// SetID sets the ID the index has been written to. This requires that
// Finalize() has been called before, otherwise an error is returned.
func (idx *Index) SetID(id rest.ID) error {
	idx.m.Lock()
	defer idx.m.Unlock()

	if !idx.final {
		return errors.New("index is not final")
	}

	if len(idx.ids) > 0 {
		return errors.New("ID already set")
	}

	log.Printf("ID set to %v", id)
	idx.ids = append(idx.ids, id)

	return nil
}

// generatePackList returns a list of packs.
func (idx *Index) generatePackList() ([]packJSON, error) {
	list := make([]packJSON, 0, len(idx.packs))
	packs := make(map[rest.ID]int, len(list)) // Maps to index in list.

	for typ := range idx.byType {
		m := &idx.byType[typ]
		m.foreach(func(e *indexEntry) bool {
			packID := idx.packs[e.packIndex]
			if packID.IsNull() {
				panic("null pack id")
			}

			i, ok := packs[packID]
			if !ok {
				i = len(list)
				list = append(list, packJSON{ID: packID})
				packs[packID] = i
			}
			p := &list[i]

			// add blob
			p.Blobs = append(p.Blobs, blobJSON{
				ID:                 e.id,
				Type:               rest.BlobType(typ),
				Offset:             uint(e.offset),
				Length:             uint(e.length),
				UncompressedLength: uint(e.uncompressedLength),
			})

			return true
		})
	}

	return list, nil
}

type packJSON struct {
	ID    rest.ID    `json:"id"`
	Blobs []blobJSON `json:"blobs"`
}

type blobJSON struct {
	ID                 rest.ID       `json:"id"`
	Type               rest.BlobType `json:"type"`
	Offset             uint          `json:"offset"`
	Length             uint          `json:"length"`
	UncompressedLength uint          `json:"uncompressed_length,omitempty"`
}

type jsonIndex struct {
	// removed: Supersedes restic.IDs `json:"supersedes,omitempty"`
	Packs []packJSON `json:"packs"`
}

// Final returns true iff the index is already written to the repository, it is
// finalized.
func (idx *Index) Final() bool {
	idx.m.RLock()
	defer idx.m.RUnlock()

	return idx.final
}

// IDs returns the IDs of the index, if available. If the index is not yet
// finalized, an error is returned.
func (idx *Index) IDs() (rest.IDs, error) {
	idx.m.RLock()
	defer idx.m.RUnlock()

	if !idx.final {
		return nil, errors.New("index not finalized")
	}

	return idx.ids, nil
}

// merge() merges indexes, i.e. idx.merge(idx2) merges the contents of idx2 into idx.
// During merging exact duplicates are removed;  idx2 is not changed by this method.
func (idx *Index) merge(idx2 *Index) error {
	idx.m.Lock()
	defer idx.m.Unlock()
	idx2.m.Lock()
	defer idx2.m.Unlock()

	if !idx2.final {
		return errors.New("index to merge is not final")
	}

	packlen := len(idx.packs)
	// first append packs as they might be accessed when looking for duplicates below
	idx.packs = append(idx.packs, idx2.packs...)

	// copy all index entries of idx2 to idx
	for typ := range idx2.byType {
		m2 := &idx2.byType[typ]
		m := &idx.byType[typ]

		// helper func to test if identical entry is contained in idx
		hasIdenticalEntry := func(e2 *indexEntry) (found bool) {
			m.foreachWithID(e2.id, func(e *indexEntry) {
				b := idx.toPackedBlob(e, rest.BlobType(typ))
				b2 := idx2.toPackedBlob(e2, rest.BlobType(typ))
				if b == b2 {
					found = true
				}
			})
			return found
		}

		m2.foreach(func(e2 *indexEntry) bool {
			if !hasIdenticalEntry(e2) {
				// packIndex needs to be changed as idx2.pack was appended to idx.pack, see above
				m.add(e2.id, e2.packIndex+packlen, e2.offset, e2.length, e2.uncompressedLength)
			}
			return true
		})
	}

	idx.ids = append(idx.ids, idx2.ids...)

	return nil
}

func (idx *Index) toPackedBlob(e *indexEntry, t rest.BlobType) rest.PackedBlob {
	return rest.PackedBlob{
		Blob: rest.Blob{
			BlobHandle: rest.BlobHandle{
				ID:   e.id,
				Type: t},
			Length:             uint(e.length),
			Offset:             uint(e.offset),
			UncompressedLength: uint(e.uncompressedLength),
		},
		PackID: idx.packs[e.packIndex],
	}
}

// Lookup queries the index for the blob ID and returns all entries including
// duplicates. Adds found entries to blobs and returns the result.
func (idx *Index) Lookup(bh rest.BlobHandle, pbs []rest.PackedBlob) []rest.PackedBlob {
	idx.m.RLock()
	defer idx.m.RUnlock()

	idx.byType[bh.Type].foreachWithID(bh.ID, func(e *indexEntry) {
		pbs = append(pbs, idx.toPackedBlob(e, bh.Type))
	})

	return pbs
}

// LookupSize returns the length of the plaintext content of the blob with the
// given id.
func (idx *Index) LookupSize(bh rest.BlobHandle) (plaintextLength uint, found bool) {
	idx.m.RLock()
	defer idx.m.RUnlock()

	e := idx.byType[bh.Type].get(bh.ID)
	if e == nil {
		return 0, false
	}
	if e.uncompressedLength != 0 {
		return uint(e.uncompressedLength), true
	}
	return uint(crypto.PlaintextLength(int(e.length))), true
}

// Has returns true iff the id is listed in the index.
func (idx *Index) Has(bh rest.BlobHandle) bool {
	idx.m.RLock()
	defer idx.m.RUnlock()

	return idx.byType[bh.Type].get(bh.ID) != nil
}

// addToPacks saves the given pack ID and return the index.
// This procedere allows to use pack IDs which can be easily garbage collected after.
func (idx *Index) addToPacks(id rest.ID) int {
	idx.packs = append(idx.packs, id)
	return len(idx.packs) - 1
}

func (idx *Index) store(packIndex int, blob rest.Blob) {
	// assert that offset and length fit into uint32!
	if blob.Offset > math.MaxUint32 || blob.Length > math.MaxUint32 || blob.UncompressedLength > math.MaxUint32 {
		panic("offset or length does not fit in uint32. You have packs > 4GB!")
	}

	m := &idx.byType[blob.Type]
	m.add(blob.ID, packIndex, uint32(blob.Offset), uint32(blob.Length), uint32(blob.UncompressedLength))
}

const (
	indexMaxBlobs = 50000
	indexMaxAge   = 10 * time.Minute
)

// IndexFull returns true iff the index is "full enough" to be saved as a preliminary index.
var IndexFull = func(idx *Index) bool {
	idx.m.RLock()
	defer idx.m.RUnlock()

	log.Debugf("checking whether index %p is full", idx)

	var blobs uint
	for typ := range idx.byType {
		blobs += idx.byType[typ].len()
	}
	age := time.Since(idx.created)

	switch {
	case age >= indexMaxAge:
		log.Debugf("index %p is old enough", idx, age)
		return true
	case blobs >= indexMaxBlobs:
		log.Debugf("index %p has %d blobs", idx, blobs)
		return true
	}

	log.Debugf("index %p only has %d blobs and is too young (%v)", idx, blobs, age)
	return false

}
