package index

import (
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	"io"
	"log"
	"sync"
	"time"

	"github.com/skyline93/rest/internal/crypto"
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

// Has returns true iff the id is listed in the index.
func (idx *Index) Has(bh rest.BlobHandle) bool {
	idx.m.Lock()
	defer idx.m.Unlock()

	return idx.byType[bh.Type].get(bh.ID) != nil
}

// Each passes all blobs known to the index to the callback fn. This blocks any
// modification of the index.
func (idx *Index) Each(ctx context.Context, fn func(rest.PackedBlob)) {
	idx.m.Lock()
	defer idx.m.Unlock()

	for typ := range idx.byType {
		m := &idx.byType[typ]
		m.foreach(func(e *indexEntry) bool {
			if ctx.Err() != nil {
				return false
			}
			fn(idx.toPackedBlob(e, rest.BlobType(typ)))
			return true
		})
	}
}

// Lookup queries the index for the blob ID and returns all entries including
// duplicates. Adds found entries to blobs and returns the result.
func (idx *Index) Lookup(bh rest.BlobHandle, pbs []rest.PackedBlob) []rest.PackedBlob {
	idx.m.Lock()
	defer idx.m.Unlock()

	idx.byType[bh.Type].foreachWithID(bh.ID, func(e *indexEntry) {
		pbs = append(pbs, idx.toPackedBlob(e, bh.Type))
	})

	return pbs
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

// Final returns true iff the index is already written to the repository, it is
// finalized.
func (idx *Index) Final() bool {
	idx.m.Lock()
	defer idx.m.Unlock()

	return idx.final
}

// IDs returns the IDs of the index, if available. If the index is not yet
// finalized, an error is returned.
func (idx *Index) IDs() (rest.IDs, error) {
	idx.m.Lock()
	defer idx.m.Unlock()

	if !idx.final {
		return nil, errors.New("index not finalized")
	}

	return idx.ids, nil
}

// AddToSupersedes adds the ids to the list of indexes superseded by this
// index. If the index has already been finalized, an error is returned.
func (idx *Index) AddToSupersedes(ids ...rest.ID) error {
	idx.m.Lock()
	defer idx.m.Unlock()

	if idx.final {
		return errors.New("index already finalized")
	}

	idx.supersedes = append(idx.supersedes, ids...)
	return nil
}

type EachByPackResult struct {
	PackID rest.ID
	Blobs  []rest.Blob
}

// EachByPack returns a channel that yields all blobs known to the index
// grouped by packID but ignoring blobs with a packID in packPlacklist for
// finalized indexes.
// This filtering is used when rebuilding the index where we need to ignore packs
// from the finalized index which have been re-read into a non-finalized index.
// When the  context is cancelled, the background goroutine
// terminates. This blocks any modification of the index.
func (idx *Index) EachByPack(ctx context.Context, packBlacklist rest.IDSet) <-chan EachByPackResult {
	idx.m.Lock()

	ch := make(chan EachByPackResult)

	go func() {
		defer idx.m.Unlock()
		defer close(ch)

		byPack := make(map[rest.ID][rest.NumBlobTypes][]*indexEntry)

		for typ := range idx.byType {
			m := &idx.byType[typ]
			m.foreach(func(e *indexEntry) bool {
				packID := idx.packs[e.packIndex]
				if !idx.final || !packBlacklist.Has(packID) {
					v := byPack[packID]
					v[typ] = append(v[typ], e)
					byPack[packID] = v
				}
				return true
			})
		}

		for packID, packByType := range byPack {
			var result EachByPackResult
			result.PackID = packID
			for typ, pack := range packByType {
				for _, e := range pack {
					result.Blobs = append(result.Blobs, idx.toPackedBlob(e, rest.BlobType(typ)).Blob)
				}
			}
			// allow GC once entry is no longer necessary
			delete(byPack, packID)
			select {
			case <-ctx.Done():
				return
			case ch <- result:
			}
		}
	}()

	return ch
}

// StorePack remembers the ids of all blobs of a given pack
// in the index
func (idx *Index) StorePack(id rest.ID, blobs []rest.Blob) {
	idx.m.Lock()
	defer idx.m.Unlock()

	if idx.final {
		panic("store new item in finalized index")
	}

	log.Printf("%v", blobs)
	packIndex := idx.addToPacks(id)

	for _, blob := range blobs {
		idx.store(packIndex, blob)
	}
}

// addToPacks saves the given pack ID and return the index.
// This procedere allows to use pack IDs which can be easily garbage collected after.
func (idx *Index) addToPacks(id rest.ID) int {
	idx.packs = append(idx.packs, id)
	return len(idx.packs) - 1
}

const maxuint32 = 1<<32 - 1

func (idx *Index) store(packIndex int, blob rest.Blob) {
	// assert that offset and length fit into uint32!
	if blob.Offset > maxuint32 || blob.Length > maxuint32 || blob.UncompressedLength > maxuint32 {
		panic("offset or length does not fit in uint32. You have packs > 4GB!")
	}

	m := &idx.byType[blob.Type]
	m.add(blob.ID, packIndex, uint32(blob.Offset), uint32(blob.Length), uint32(blob.UncompressedLength))
}

// Encode writes the JSON serialization of the index to the writer w.
func (idx *Index) Encode(w io.Writer) error {
	log.Println("encoding index")
	idx.m.Lock()
	defer idx.m.Unlock()

	list, err := idx.generatePackList()
	if err != nil {
		return err
	}

	enc := json.NewEncoder(w)
	idxJSON := jsonIndex{
		Supersedes: idx.supersedes,
		Packs:      list,
	}
	return enc.Encode(idxJSON)
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
	Supersedes rest.IDs   `json:"supersedes,omitempty"`
	Packs      []packJSON `json:"packs"`
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

// isErrOldIndex returns true if the error may be caused by an old index
// format.
func isErrOldIndex(err error) bool {
	e, ok := err.(*json.UnmarshalTypeError)
	return ok && e.Value == "array"
}

// DecodeIndex unserializes an index from buf.
func DecodeIndex(buf []byte, id rest.ID) (idx *Index, oldFormat bool, err error) {
	log.Println("Start decoding index")
	idxJSON := &jsonIndex{}

	err = json.Unmarshal(buf, idxJSON)
	if err != nil {
		log.Printf("Error %v", err)

		if isErrOldIndex(err) {
			log.Println("index is probably old format, trying that")
			idx, err = decodeOldIndex(buf)
			return idx, err == nil, err
		}

		return nil, false, errors.Wrap(err, "DecodeIndex")
	}

	idx = NewIndex()
	for _, pack := range idxJSON.Packs {
		packID := idx.addToPacks(pack.ID)

		for _, blob := range pack.Blobs {
			idx.store(packID, rest.Blob{
				BlobHandle: rest.BlobHandle{
					Type: blob.Type,
					ID:   blob.ID},
				Offset:             blob.Offset,
				Length:             blob.Length,
				UncompressedLength: blob.UncompressedLength,
			})
		}
	}
	idx.supersedes = idxJSON.Supersedes
	idx.ids = append(idx.ids, id)
	idx.final = true

	log.Println("done")
	return idx, false, nil
}

// DecodeOldIndex loads and unserializes an index in the old format from rd.
func decodeOldIndex(buf []byte) (idx *Index, err error) {
	log.Println("Start decoding old index")
	list := []*packJSON{}

	err = json.Unmarshal(buf, &list)
	if err != nil {
		log.Printf("Error %#v", err)
		return nil, errors.Wrap(err, "Decode")
	}

	idx = NewIndex()
	for _, pack := range list {
		packID := idx.addToPacks(pack.ID)

		for _, blob := range pack.Blobs {
			idx.store(packID, rest.Blob{
				BlobHandle: rest.BlobHandle{
					Type: blob.Type,
					ID:   blob.ID},
				Offset: blob.Offset,
				Length: blob.Length,
				// no compressed length in the old index format
			})
		}
	}
	idx.final = true

	log.Println("done")
	return idx, nil
}

// LookupSize returns the length of the plaintext content of the blob with the
// given id.
func (idx *Index) LookupSize(bh rest.BlobHandle) (plaintextLength uint, found bool) {
	idx.m.Lock()
	defer idx.m.Unlock()

	e := idx.byType[bh.Type].get(bh.ID)
	if e == nil {
		return 0, false
	}
	if e.uncompressedLength != 0 {
		return uint(e.uncompressedLength), true
	}
	return uint(crypto.PlaintextLength(int(e.length))), true
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
	idx.supersedes = append(idx.supersedes, idx2.supersedes...)

	return nil
}

// Packs returns all packs in this index
func (idx *Index) Packs() rest.IDSet {
	idx.m.Lock()
	defer idx.m.Unlock()

	packs := rest.NewIDSet()
	for _, packID := range idx.packs {
		packs.Insert(packID)
	}

	return packs
}
