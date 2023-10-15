package repository

import (
	"bytes"
	"context"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"log"
	"os"
	"runtime"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/restic/chunker"
	"github.com/skyline93/rest/internal/backend"
	"github.com/skyline93/rest/internal/cache"
	"github.com/skyline93/rest/internal/crypto"
	"github.com/skyline93/rest/internal/index"
	"github.com/skyline93/rest/internal/pack"
	"github.com/skyline93/rest/internal/rest"
	"golang.org/x/sync/errgroup"
)

const MinPackSize = 4 * 1024 * 1024
const DefaultPackSize = 16 * 1024 * 1024
const MaxPackSize = 128 * 1024 * 1024

type Options struct {
	Compression CompressionMode
	PackSize    uint
}

// CompressionMode configures if data should be compressed.
type CompressionMode uint

// Constants for the different compression levels.
const (
	CompressionAuto    CompressionMode = 0
	CompressionOff     CompressionMode = 1
	CompressionMax     CompressionMode = 2
	CompressionInvalid CompressionMode = 3
)

// Repository is used to access a repository in a backend.
type Repository struct {
	be    rest.Backend
	cfg   rest.Config
	key   *crypto.Key
	keyID rest.ID
	idx   *index.MasterIndex
	Cache *cache.Cache

	opts Options

	noAutoIndexUpdate bool

	packerWg *errgroup.Group
	uploader *packerUploader
	treePM   *packerManager
	dataPM   *packerManager

	allocEnc sync.Once
	allocDec sync.Once
	enc      *zstd.Encoder
	dec      *zstd.Decoder
}

// New returns a new repository with backend be.
func New(be rest.Backend, opts Options) (*Repository, error) {
	if opts.Compression == CompressionInvalid {
		return nil, errors.New("invalid compression mode")
	}

	if opts.PackSize == 0 {
		opts.PackSize = DefaultPackSize
	}
	if opts.PackSize > MaxPackSize {
		return nil, fmt.Errorf("pack size larger than limit of %v MiB", MaxPackSize/1024/1024)
	} else if opts.PackSize < MinPackSize {
		return nil, fmt.Errorf("pack size smaller than minimum of %v MiB", MinPackSize/1024/1024)
	}

	repo := &Repository{
		be:   be,
		opts: opts,
		idx:  index.NewMasterIndex(),
	}

	return repo, nil
}

// Init creates a new master key with the supplied password, initializes and
// saves the repository config.
func (r *Repository) Init(ctx context.Context, version uint, password string, chunkerPolynomial *chunker.Pol) error {
	if version > rest.MaxRepoVersion {
		return fmt.Errorf("repository version %v too high", version)
	}

	if version < rest.MinRepoVersion {
		return fmt.Errorf("repository version %v too low", version)
	}

	_, err := r.be.Stat(ctx, rest.Handle{Type: rest.ConfigFile})
	if err != nil && !r.be.IsNotExist(err) {
		return err
	}
	if err == nil {
		return errors.New("repository master key and config already initialized")
	}

	cfg, err := rest.CreateConfig(version)
	if err != nil {
		return err
	}
	if chunkerPolynomial != nil {
		cfg.ChunkerPolynomial = *chunkerPolynomial
	}

	return r.init(ctx, password, cfg)
}

// init creates a new master key with the supplied password and uses it to save
// the config into the repo.
func (r *Repository) init(ctx context.Context, password string, cfg rest.Config) error {
	key, err := createMasterKey(ctx, r, password)
	if err != nil {
		return err
	}

	r.key = key.master
	r.keyID = key.ID()
	r.setConfig(cfg)
	return rest.SaveConfig(ctx, r, cfg)
}

// setConfig assigns the given config and updates the repository parameters accordingly
func (r *Repository) setConfig(cfg rest.Config) {
	r.cfg = cfg
	if r.cfg.Version >= 2 {
		r.idx.MarkCompressed()
	}
}

func (r *Repository) Connections() uint {
	return r.be.Connections()
}

// SaveUnpacked encrypts data and stores it in the backend. Returned is the
// storage hash.
func (r *Repository) SaveUnpacked(ctx context.Context, t rest.FileType, p []byte) (id rest.ID, err error) {
	if t != rest.ConfigFile {
		p, err = r.compressUnpacked(p)
		if err != nil {
			return rest.ID{}, err
		}
	}

	ciphertext := crypto.NewBlobBuffer(len(p))
	ciphertext = ciphertext[:0]
	nonce := crypto.NewRandomNonce()
	ciphertext = append(ciphertext, nonce...)

	ciphertext = r.key.Seal(ciphertext, nonce, p, nil)

	if t == rest.ConfigFile {
		id = rest.ID{}
	} else {
		id = rest.Hash(ciphertext)
	}
	h := rest.Handle{Type: t, Name: id.String()}

	err = r.be.Save(ctx, h, rest.NewByteReader(ciphertext, r.be.Hasher()))
	if err != nil {
		log.Printf("error saving blob %v: %v", h, err)
		return rest.ID{}, err
	}

	log.Printf("blob %v saved", h)
	return id, nil
}

func (r *Repository) compressUnpacked(p []byte) ([]byte, error) {
	// compression is only available starting from version 2
	if r.cfg.Version < 2 {
		return p, nil
	}

	// version byte
	out := []byte{2}
	out = r.getZstdEncoder().EncodeAll(p, out)
	return out, nil
}

func (r *Repository) getZstdEncoder() *zstd.Encoder {
	r.allocEnc.Do(func() {
		level := zstd.SpeedDefault
		if r.opts.Compression == CompressionMax {
			level = zstd.SpeedBestCompression
		}

		opts := []zstd.EOption{
			// Set the compression level configured.
			zstd.WithEncoderLevel(level),
			// Disable CRC, we have enough checks in place, makes the
			// compressed data four bytes shorter.
			zstd.WithEncoderCRC(false),
			// Set a window of 512kbyte, so we have good lookbehind for usual
			// blob sizes.
			zstd.WithWindowSize(512 * 1024),
		}

		enc, err := zstd.NewWriter(nil, opts...)
		if err != nil {
			panic(err)
		}
		r.enc = enc
	})
	return r.enc
}

// Backend returns the backend for the repository.
func (r *Repository) Backend() rest.Backend {
	return r.be
}

// List runs fn for all files of type t in the repo.
func (r *Repository) List(ctx context.Context, t rest.FileType, fn func(rest.ID, int64) error) error {
	return r.be.List(ctx, t, func(fi rest.FileInfo) error {
		id, err := rest.ParseID(fi.Name)
		if err != nil {
			log.Printf("unable to parse %v as an ID", fi.Name)
			return nil
		}
		return fn(id, fi.Size)
	})
}

// SearchKey finds a key with the supplied password, afterwards the config is
// read and parsed. It tries at most maxKeys key files in the repo.
func (r *Repository) SearchKey(ctx context.Context, password string, maxKeys int, keyHint string) error {
	key, err := SearchKey(ctx, r, password, maxKeys, keyHint)
	if err != nil {
		return err
	}

	r.key = key.master
	r.keyID = key.ID()
	cfg, err := rest.LoadConfig(ctx, r)
	if err == crypto.ErrUnauthenticated {
		return fmt.Errorf("config or key %v is damaged: %w", key.ID(), err)
	} else if err != nil {
		return fmt.Errorf("config cannot be loaded: %w", err)
	}

	r.setConfig(cfg)
	return nil
}

// LoadUnpacked loads and decrypts the file with the given type and ID.
func (r *Repository) LoadUnpacked(ctx context.Context, t rest.FileType, id rest.ID) ([]byte, error) {
	log.Printf("load %v with id %v", t, id)

	if t == rest.ConfigFile {
		id = rest.ID{}
	}

	ctx, cancel := context.WithCancel(ctx)

	h := rest.Handle{Type: t, Name: id.String()}
	retriedInvalidData := false
	var dataErr error
	wr := new(bytes.Buffer)

	err := r.be.Load(ctx, h, 0, 0, func(rd io.Reader) error {
		// make sure this call is idempotent, in case an error occurs
		wr.Reset()
		_, cerr := io.Copy(wr, rd)
		if cerr != nil {
			return cerr
		}

		buf := wr.Bytes()
		if t != rest.ConfigFile && !rest.Hash(buf).Equal(id) {
			log.Printf("retry loading broken blob %v", h)
			if !retriedInvalidData {
				retriedInvalidData = true
			} else {
				// with a canceled context there is not guarantee which error will
				// be returned by `be.Load`.
				dataErr = fmt.Errorf("load(%v): %w", h, rest.ErrInvalidData)
				cancel()
			}
			return rest.ErrInvalidData

		}
		return nil
	})

	if dataErr != nil {
		return nil, dataErr
	}
	if err != nil {
		return nil, err
	}

	buf := wr.Bytes()
	nonce, ciphertext := buf[:r.key.NonceSize()], buf[r.key.NonceSize():]
	plaintext, err := r.key.Open(ciphertext[:0], nonce, ciphertext, nil)
	if err != nil {
		return nil, err
	}
	if t != rest.ConfigFile {
		return r.decompressUnpacked(plaintext)
	}

	return plaintext, nil
}

func (r *Repository) decompressUnpacked(p []byte) ([]byte, error) {
	// compression is only available starting from version 2
	if r.cfg.Version < 2 {
		return p, nil
	}

	if len(p) == 0 {
		// too short for version header
		return p, nil
	}
	if p[0] == '[' || p[0] == '{' {
		// probably raw JSON
		return p, nil
	}
	// version
	if p[0] != 2 {
		return nil, errors.New("not supported encoding format")
	}

	return r.getZstdDecoder().DecodeAll(p[1:], nil)
}

func (r *Repository) getZstdDecoder() *zstd.Decoder {
	r.allocDec.Do(func() {
		opts := []zstd.DOption{
			// Use all available cores.
			zstd.WithDecoderConcurrency(0),
			// Limit the maximum decompressed memory. Set to a very high,
			// conservative value.
			zstd.WithDecoderMaxMemory(16 * 1024 * 1024 * 1024),
		}

		dec, err := zstd.NewReader(nil, opts...)
		if err != nil {
			panic(err)
		}
		r.dec = dec
	})
	return r.dec
}

// Key returns the current master key.
func (r *Repository) Key() *crypto.Key {
	return r.key
}

// Index returns the currently used MasterIndex.
func (r *Repository) Index() rest.MasterIndex {
	return r.idx
}

// SetIndex instructs the repository to use the given index.
func (r *Repository) SetIndex(i rest.MasterIndex) error {
	r.idx = i.(*index.MasterIndex)
	return r.prepareCache()
}

// LoadIndex loads all index files from the backend in parallel and stores them
// in the master index. The first error that occurred is returned.
func (r *Repository) LoadIndex(ctx context.Context) error {
	log.Println("Loading index")

	err := index.ForAllIndexes(ctx, r, func(id rest.ID, idx *index.Index, oldFormat bool, err error) error {
		if err != nil {
			return err
		}
		r.idx.Insert(idx)
		return nil
	})

	if err != nil {
		return err
	}

	err = r.idx.MergeFinalIndexes()
	if err != nil {
		return err
	}

	// Trigger GC to reset garbage collection threshold
	runtime.GC()

	if r.cfg.Version < 2 {
		// sanity check
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		invalidIndex := false
		r.idx.Each(ctx, func(blob rest.PackedBlob) {
			if blob.IsCompressed() {
				invalidIndex = true
			}
		})
		if invalidIndex {
			return errors.New("index uses feature not supported by repository version 1")
		}
	}

	// remove index files from the cache which have been removed in the repo
	return r.prepareCache()
}

// prepareCache initializes the local cache. indexIDs is the list of IDs of
// index files still present in the repo.
func (r *Repository) prepareCache() error {
	if r.Cache == nil {
		return nil
	}

	indexIDs := r.idx.IDs()
	log.Printf("prepare cache with %d index files", len(indexIDs))

	// clear old index files
	err := r.Cache.Clear(rest.IndexFile, indexIDs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error clearing index files in cache: %v\n", err)
	}

	packs := r.idx.Packs(rest.NewIDSet())

	// clear old packs
	err = r.Cache.Clear(rest.PackFile, packs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error clearing pack files in cache: %v\n", err)
	}

	return nil
}

// LookupBlobSize returns the size of blob id.
func (r *Repository) LookupBlobSize(id rest.ID, tpe rest.BlobType) (uint, bool) {
	return r.idx.LookupSize(rest.BlobHandle{ID: id, Type: tpe})
}

// Config returns the repository configuration.
func (r *Repository) Config() rest.Config {
	return r.cfg
}

// PackSize return the target size of a pack file when uploading
func (r *Repository) PackSize() uint {
	return r.opts.PackSize
}

// ListPack returns the list of blobs saved in the pack id and the length of
// the the pack header.
func (r *Repository) ListPack(ctx context.Context, id rest.ID, size int64) ([]rest.Blob, uint32, error) {
	h := rest.Handle{Type: rest.PackFile, Name: id.String()}

	return pack.List(r.Key(), backend.ReaderAt(ctx, r.Backend(), h), size)
}

var zeroChunkOnce sync.Once
var zeroChunkID rest.ID

// ZeroChunk computes and returns (cached) the ID of an all-zero chunk with size chunker.MinSize
func ZeroChunk() rest.ID {
	zeroChunkOnce.Do(func() {
		zeroChunkID = rest.Hash(make([]byte, chunker.MinSize))
	})
	return zeroChunkID
}

// SaveBlob saves a blob of type t into the repository.
// It takes care that no duplicates are saved; this can be overwritten
// by setting storeDuplicate to true.
// If id is the null id, it will be computed and returned.
// Also returns if the blob was already known before.
// If the blob was not known before, it returns the number of bytes the blob
// occupies in the repo (compressed or not, including encryption overhead).
func (r *Repository) SaveBlob(ctx context.Context, t rest.BlobType, buf []byte, id rest.ID, storeDuplicate bool) (newID rest.ID, known bool, size int, err error) {

	// compute plaintext hash if not already set
	if id.IsNull() {
		// Special case the hash calculation for all zero chunks. This is especially
		// useful for sparse files containing large all zero regions. For these we can
		// process chunks as fast as we can read the from disk.
		if len(buf) == chunker.MinSize && rest.ZeroPrefixLen(buf) == chunker.MinSize {
			newID = ZeroChunk()
		} else {
			newID = rest.Hash(buf)
		}
	} else {
		newID = id
	}

	// first try to add to pending blobs; if not successful, this blob is already known
	known = !r.idx.AddPending(rest.BlobHandle{ID: newID, Type: t})

	// only save when needed or explicitly told
	if !known || storeDuplicate {
		size, err = r.saveAndEncrypt(ctx, t, buf, newID)
	}

	return newID, known, size, err
}

// LoadBlob loads a blob of type t from the repository.
// It may use all of buf[:cap(buf)] as scratch space.
func (r *Repository) LoadBlob(ctx context.Context, t rest.BlobType, id rest.ID, buf []byte) ([]byte, error) {
	log.Printf("load %v with id %v (buf len %v, cap %d)", t, id, len(buf), cap(buf))

	// lookup packs
	blobs := r.idx.Lookup(rest.BlobHandle{ID: id, Type: t})
	if len(blobs) == 0 {
		log.Printf("id %v not found in index", id)
		return nil, errors.Errorf("id %v not found in repository", id)
	}

	// try cached pack files first
	sortCachedPacksFirst(r.Cache, blobs)

	var lastError error
	for _, blob := range blobs {
		log.Printf("blob %v/%v found: %v", t, id, blob)

		if blob.Type != t {
			log.Printf("blob %v has wrong block type, want %v", blob, t)
		}

		// load blob from pack
		h := rest.Handle{Type: rest.PackFile, Name: blob.PackID.String(), ContainedBlobType: t}

		switch {
		case cap(buf) < int(blob.Length):
			buf = make([]byte, blob.Length)
		case len(buf) != int(blob.Length):
			buf = buf[:blob.Length]
		}

		n, err := backend.ReadAt(ctx, r.be, h, int64(blob.Offset), buf)
		if err != nil {
			log.Printf("error loading blob %v: %v", blob, err)
			lastError = err
			continue
		}

		if uint(n) != blob.Length {
			lastError = errors.Errorf("error loading blob %v: wrong length returned, want %d, got %d",
				id.Str(), blob.Length, uint(n))
			log.Printf("lastError: %v", lastError)
			continue
		}

		// decrypt
		nonce, ciphertext := buf[:r.key.NonceSize()], buf[r.key.NonceSize():]
		plaintext, err := r.key.Open(ciphertext[:0], nonce, ciphertext, nil)
		if err != nil {
			lastError = errors.Errorf("decrypting blob %v failed: %v", id, err)
			continue
		}

		if blob.IsCompressed() {
			plaintext, err = r.getZstdDecoder().DecodeAll(plaintext, make([]byte, 0, blob.DataLength()))
			if err != nil {
				lastError = errors.Errorf("decompressing blob %v failed: %v", id, err)
				continue
			}
		}

		// check hash
		if !rest.Hash(plaintext).Equal(id) {
			lastError = errors.Errorf("blob %v returned invalid hash", id)
			continue
		}

		if len(plaintext) > cap(buf) {
			return plaintext, nil
		}
		// move decrypted data to the start of the buffer
		buf = buf[:len(plaintext)]
		copy(buf, plaintext)
		return buf, nil
	}

	if lastError != nil {
		return nil, lastError
	}

	return nil, errors.Errorf("loading blob %v from %v packs failed", id.Str(), len(blobs))
}

// Flush saves all remaining packs and the index
func (r *Repository) Flush(ctx context.Context) error {
	if err := r.flushPacks(ctx); err != nil {
		return err
	}

	// Save index after flushing only if noAutoIndexUpdate is not set
	if r.noAutoIndexUpdate {
		return nil
	}
	return r.idx.SaveIndex(ctx, r)
}

func (r *Repository) StartPackUploader(ctx context.Context, wg *errgroup.Group) {
	if r.packerWg != nil {
		panic("uploader already started")
	}

	innerWg, ctx := errgroup.WithContext(ctx)
	r.packerWg = innerWg
	r.uploader = newPackerUploader(ctx, innerWg, r, r.be.Connections())
	r.treePM = newPackerManager(r.key, rest.TreeBlob, r.PackSize(), r.uploader.QueuePacker)
	r.dataPM = newPackerManager(r.key, rest.DataBlob, r.PackSize(), r.uploader.QueuePacker)

	wg.Go(func() error {
		return innerWg.Wait()
	})
}

// saveAndEncrypt encrypts data and stores it to the backend as type t. If data
// is small enough, it will be packed together with other small blobs. The
// caller must ensure that the id matches the data. Returned is the size data
// occupies in the repo (compressed or not, including the encryption overhead).
func (r *Repository) saveAndEncrypt(ctx context.Context, t rest.BlobType, data []byte, id rest.ID) (size int, err error) {
	log.Printf("save id %v (%v, %d bytes)", id, t, len(data))

	uncompressedLength := 0
	if r.cfg.Version > 1 {

		// we have a repo v2, so compression is available. if the user opts to
		// not compress, we won't compress any data, but everything else is
		// compressed.
		if r.opts.Compression != CompressionOff || t != rest.DataBlob {
			uncompressedLength = len(data)
			data = r.getZstdEncoder().EncodeAll(data, nil)
		}
	}

	nonce := crypto.NewRandomNonce()

	ciphertext := make([]byte, 0, crypto.CiphertextLength(len(data)))
	ciphertext = append(ciphertext, nonce...)

	// encrypt blob
	ciphertext = r.key.Seal(ciphertext, nonce, data, nil)

	// find suitable packer and add blob
	var pm *packerManager

	switch t {
	case rest.TreeBlob:
		pm = r.treePM
	case rest.DataBlob:
		pm = r.dataPM
	default:
		panic(fmt.Sprintf("invalid type: %v", t))
	}

	return pm.SaveBlob(ctx, t, id, ciphertext, uncompressedLength)
}

type haver interface {
	Has(rest.Handle) bool
}

// sortCachedPacksFirst moves all cached pack files to the front of blobs.
func sortCachedPacksFirst(cache haver, blobs []rest.PackedBlob) {
	if cache == nil {
		return
	}

	// no need to sort a list with one element
	if len(blobs) == 1 {
		return
	}

	cached := blobs[:0]
	noncached := make([]rest.PackedBlob, 0, len(blobs)/2)

	for _, blob := range blobs {
		if cache.Has(rest.Handle{Type: rest.PackFile, Name: blob.PackID.String()}) {
			cached = append(cached, blob)
			continue
		}
		noncached = append(noncached, blob)
	}

	copy(blobs[len(cached):], noncached)
}

// FlushPacks saves all remaining packs.
func (r *Repository) flushPacks(ctx context.Context) error {
	if r.packerWg == nil {
		return nil
	}

	err := r.treePM.Flush(ctx)
	if err != nil {
		return err
	}
	err = r.dataPM.Flush(ctx)
	if err != nil {
		return err
	}
	r.uploader.TriggerShutdown()
	err = r.packerWg.Wait()

	r.treePM = nil
	r.dataPM = nil
	r.uploader = nil
	r.packerWg = nil

	return err
}
