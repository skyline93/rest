package repository

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"sync"

	"github.com/skyline93/rest/internal/backend"
	"github.com/skyline93/rest/internal/backend/cache"
	"github.com/skyline93/rest/internal/crypto"
	"github.com/skyline93/rest/internal/errors"
	"github.com/skyline93/rest/internal/repository/index"
	"github.com/skyline93/rest/internal/rest"

	"github.com/klauspost/compress/zstd"
	"github.com/restic/chunker"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// CompressionMode configures if data should be compressed.
type CompressionMode uint

// Constants for the different compression levels.
const (
	CompressionAuto    CompressionMode = 0
	CompressionOff     CompressionMode = 1
	CompressionMax     CompressionMode = 2
	CompressionInvalid CompressionMode = 3
)

const MinPackSize = 4 * 1024 * 1024
const DefaultPackSize = 16 * 1024 * 1024
const MaxPackSize = 128 * 1024 * 1024

type Repository struct {
	be    backend.Backend
	cfg   rest.Config
	key   *crypto.Key
	keyID rest.ID
	idx   *index.MasterIndex
	Cache *cache.Cache

	opts Options

	packerWg *errgroup.Group
	uploader *packerUploader
	treePM   *packerManager
	dataPM   *packerManager

	allocEnc sync.Once
	allocDec sync.Once
	enc      *zstd.Encoder
	dec      *zstd.Decoder
}

func New(be backend.Backend, opts Options) (*Repository, error) {
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

type Options struct {
	Compression   CompressionMode
	PackSize      uint
	NoExtraVerify bool
}

func (r *Repository) Init(ctx context.Context, version uint, password string, chunkerPolynomial *chunker.Pol) error {
	if version > rest.MaxRepoVersion {
		return fmt.Errorf("repository version %v too high", version)
	}

	if version < rest.MinRepoVersion {
		return fmt.Errorf("repository version %v too low", version)
	}

	_, err := r.be.Stat(ctx, backend.Handle{Type: rest.ConfigFile})
	if err != nil && !r.be.IsNotExist(err) {
		return err
	}
	if err == nil {
		return errors.New("repository master key and config already initialized")
	}
	// double check to make sure that a repository is not accidentally reinitialized
	// if the backend somehow fails to stat the config file. An initialized repository
	// must always contain at least one key file.
	if err := r.List(ctx, rest.KeyFile, func(_ rest.ID, _ int64) error {
		return errors.New("repository already contains keys")
	}); err != nil {
		return err
	}
	// Also check for snapshots to detect repositories with a misconfigured retention
	// policy that deletes files older than x days. For such repositories usually the
	// config and key files are removed first and therefore the check would not detect
	// the old repository.
	if err := r.List(ctx, rest.SnapshotFile, func(_ rest.ID, _ int64) error {
		return errors.New("repository already contains snapshots")
	}); err != nil {
		return err
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

func (r *Repository) List(ctx context.Context, t rest.FileType, fn func(rest.ID, int64) error) error {
	return r.be.List(ctx, t, func(fi backend.FileInfo) error {
		id, err := rest.ParseID(fi.Name)
		if err != nil {
			log.Infof("unable to parse %v as an ID", fi.Name)
			return nil
		}
		return fn(id, fi.Size)
	})
}

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
}

func (r *Repository) Connections() uint {
	return r.be.Connections()
}

// SaveUnpacked encrypts data and stores it in the backend. Returned is the
// storage hash.
func (r *Repository) SaveUnpacked(ctx context.Context, t rest.FileType, buf []byte) (id rest.ID, err error) {
	p := buf
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

	if err := r.verifyUnpacked(ciphertext, t, buf); err != nil {
		//nolint:revive // ignore linter warnings about error message spelling
		return rest.ID{}, fmt.Errorf("Detected data corruption while saving file of type %v: %w\nCorrupted data is either caused by hardware issues or software bugs. Please open an issue at https://github.com/restic/restic/issues/new/choose for further troubleshooting.", t, err)
	}

	if t == rest.ConfigFile {
		id = rest.ID{}
	} else {
		id = rest.Hash(ciphertext)
	}
	h := backend.Handle{Type: t, Name: id.String()}

	err = r.be.Save(ctx, h, backend.NewByteReader(ciphertext, r.be.Hasher()))
	if err != nil {
		log.Infof("error saving blob %v: %v", h, err)
		return rest.ID{}, err
	}

	log.Infof("blob %v saved", h)
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

func (r *Repository) verifyUnpacked(buf []byte, t rest.FileType, expected []byte) error {
	if r.opts.NoExtraVerify {
		return nil
	}

	nonce, ciphertext := buf[:r.key.NonceSize()], buf[r.key.NonceSize():]
	plaintext, err := r.key.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return fmt.Errorf("decryption failed: %w", err)
	}
	if t != rest.ConfigFile {
		plaintext, err = r.decompressUnpacked(plaintext)
		if err != nil {
			return fmt.Errorf("decompression failed: %w", err)
		}
	}

	if !bytes.Equal(plaintext, expected) {
		return errors.New("data mismatch")
	}
	return nil
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

func (r *Repository) SearchKey(ctx context.Context, password string, maxKeys int, keyHint string) error {
	key, err := SearchKey(ctx, r, password, maxKeys, keyHint)
	if err != nil {
		return err
	}

	oldKey := r.key
	oldKeyID := r.keyID

	r.key = key.master
	r.keyID = key.ID()
	cfg, err := rest.LoadConfig(ctx, r)
	if err != nil {
		r.key = oldKey
		r.keyID = oldKeyID

		if err == crypto.ErrUnauthenticated {
			return fmt.Errorf("config or key %v is damaged: %w", key.ID(), err)
		}
		return fmt.Errorf("config cannot be loaded: %w", err)
	}

	r.setConfig(cfg)
	return nil
}

// LoadUnpacked loads and decrypts the file with the given type and ID.
func (r *Repository) LoadUnpacked(ctx context.Context, t rest.FileType, id rest.ID) ([]byte, error) {
	log.Infof("load %v with id %v", t, id)

	if t == rest.ConfigFile {
		id = rest.ID{}
	}

	buf, err := r.LoadRaw(ctx, t, id)
	if err != nil {
		return nil, err
	}

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

// Config returns the repository configuration.
func (r *Repository) Config() rest.Config {
	return r.cfg
}

// Flush saves all remaining packs and the index
func (r *Repository) Flush(ctx context.Context) error {
	if err := r.flushPacks(ctx); err != nil {
		return err
	}

	return r.idx.SaveIndex(ctx, r)
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

func (r *Repository) LookupBlob(tpe rest.BlobType, id rest.ID) []rest.PackedBlob {
	return r.idx.Lookup(rest.BlobHandle{Type: tpe, ID: id})
}

type byteReader struct {
	buf []byte
}

func newByteReader(buf []byte) *byteReader {
	return &byteReader{
		buf: buf,
	}
}

func (b *byteReader) Discard(n int) (discarded int, err error) {
	if len(b.buf) < n {
		return 0, io.ErrUnexpectedEOF
	}
	b.buf = b.buf[n:]
	return n, nil
}

func (b *byteReader) ReadFull(n int) (buf []byte, err error) {
	if len(b.buf) < n {
		return nil, io.ErrUnexpectedEOF
	}
	buf = b.buf[:n]
	b.buf = b.buf[n:]
	return buf, nil
}

func (r *Repository) loadBlob(ctx context.Context, blobs []rest.PackedBlob, buf []byte) ([]byte, error) {
	var lastError error
	for _, blob := range blobs {
		log.Infof("blob %v found: %v", blob.BlobHandle, blob)
		// load blob from pack
		h := backend.Handle{Type: rest.PackFile, Name: blob.PackID.String(), IsMetadata: blob.Type.IsMetadata()}

		switch {
		case cap(buf) < int(blob.Length):
			buf = make([]byte, blob.Length)
		case len(buf) != int(blob.Length):
			buf = buf[:blob.Length]
		}

		_, err := backend.ReadAt(ctx, r.be, h, int64(blob.Offset), buf)
		if err != nil {
			log.Infof("error loading blob %v: %v", blob, err)
			lastError = err
			continue
		}

		it := newPackBlobIterator(blob.PackID, newByteReader(buf), uint(blob.Offset), []rest.Blob{blob.Blob}, r.key, r.getZstdDecoder())
		pbv, err := it.Next()

		if err == nil {
			err = pbv.Err
		}
		if err != nil {
			log.Infof("error decoding blob %v: %v", blob, err)
			lastError = err
			continue
		}

		plaintext := pbv.Plaintext
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

	return nil, errors.Errorf("loading %v from %v packs failed", blobs[0].BlobHandle, len(blobs))
}

func newPackBlobIterator(packID rest.ID, rd discardReader, currentOffset uint,
	blobs []rest.Blob, key *crypto.Key, dec *zstd.Decoder) *packBlobIterator {
	return &packBlobIterator{
		packID:        packID,
		rd:            rd,
		currentOffset: currentOffset,
		blobs:         blobs,
		key:           key,
		dec:           dec,
	}
}

// discardReader allows the PackBlobIterator to perform zero copy
// reads if the underlying data source is a byte slice.
type discardReader interface {
	Discard(n int) (discarded int, err error)
	// ReadFull reads the next n bytes into a byte slice. The caller must not
	// retain a reference to the byte. Modifications are only allowed within
	// the boundaries of the returned slice.
	ReadFull(n int) (buf []byte, err error)
}

type packBlobIterator struct {
	packID        rest.ID
	rd            discardReader
	currentOffset uint

	blobs []rest.Blob
	key   *crypto.Key
	dec   *zstd.Decoder

	decode []byte
}

type packBlobValue struct {
	Handle    rest.BlobHandle
	Plaintext []byte
	Err       error
}

var errPackEOF = errors.New("reached EOF of pack file")

// Next returns the next blob, an error or ErrPackEOF if all blobs were read
func (b *packBlobIterator) Next() (packBlobValue, error) {
	if len(b.blobs) == 0 {
		return packBlobValue{}, errPackEOF
	}

	entry := b.blobs[0]
	b.blobs = b.blobs[1:]

	skipBytes := int(entry.Offset - b.currentOffset)
	if skipBytes < 0 {
		return packBlobValue{}, fmt.Errorf("overlapping blobs in pack %v", b.packID)
	}

	_, err := b.rd.Discard(skipBytes)
	if err != nil {
		return packBlobValue{}, err
	}
	b.currentOffset = entry.Offset

	h := rest.BlobHandle{ID: entry.ID, Type: entry.Type}
	log.Infof("  process blob %v, skipped %d, %v", h, skipBytes, entry)

	buf, err := b.rd.ReadFull(int(entry.Length))
	if err != nil {
		log.Infof("    read error %v", err)
		return packBlobValue{}, fmt.Errorf("readFull: %w", err)
	}

	b.currentOffset = entry.Offset + entry.Length

	if int(entry.Length) <= b.key.NonceSize() {
		log.Infof("%v", b.blobs)
		return packBlobValue{}, fmt.Errorf("invalid blob length %v", entry)
	}

	// decryption errors are likely permanent, give the caller a chance to skip them
	nonce, ciphertext := buf[:b.key.NonceSize()], buf[b.key.NonceSize():]
	plaintext, err := b.key.Open(ciphertext[:0], nonce, ciphertext, nil)
	if err != nil {
		err = fmt.Errorf("decrypting blob %v from %v failed: %w", h, b.packID.Str(), err)
	}
	if err == nil && entry.IsCompressed() {
		// DecodeAll will allocate a slice if it is not large enough since it
		// knows the decompressed size (because we're using EncodeAll)
		b.decode, err = b.dec.DecodeAll(plaintext, b.decode[:0])
		plaintext = b.decode
		if err != nil {
			err = fmt.Errorf("decompressing blob %v from %v failed: %w", h, b.packID.Str(), err)
		}
	}
	if err == nil {
		id := rest.Hash(plaintext)
		if !id.Equal(entry.ID) {
			log.Infof("read blob %v/%v from %v: wrong data returned, hash is %v",
				h.Type, h.ID, b.packID.Str(), id)
			err = fmt.Errorf("read blob %v from %v: wrong data returned, hash is %v",
				h, b.packID.Str(), id)
		}
	}

	return packBlobValue{entry.BlobHandle, plaintext, err}, nil
}

// LoadBlob loads a blob of type t from the repository.
// It may use all of buf[:cap(buf)] as scratch space.
func (r *Repository) LoadBlob(ctx context.Context, t rest.BlobType, id rest.ID, buf []byte) ([]byte, error) {
	log.Infof("load %v with id %v (buf len %v, cap %d)", t, id, len(buf), cap(buf))

	// lookup packs
	blobs := r.idx.Lookup(rest.BlobHandle{ID: id, Type: t})
	if len(blobs) == 0 {
		log.Infof("id %v not found in index", id)
		return nil, errors.Errorf("id %v not found in repository", id)
	}

	// try cached pack files first
	sortCachedPacksFirst(r.Cache, blobs)

	buf, err := r.loadBlob(ctx, blobs, buf)
	if err != nil {
		if r.Cache != nil {
			for _, blob := range blobs {
				h := backend.Handle{Type: rest.PackFile, Name: blob.PackID.String(), IsMetadata: blob.Type.IsMetadata()}
				// ignore errors as there's not much we can do here
				_ = r.Cache.Forget(h)
			}
		}

		buf, err = r.loadBlob(ctx, blobs, buf)
	}
	return buf, err
}

type haver interface {
	Has(backend.Handle) bool
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
		if cache.Has(backend.Handle{Type: rest.PackFile, Name: blob.PackID.String()}) {
			cached = append(cached, blob)
			continue
		}
		noncached = append(noncached, blob)
	}

	copy(blobs[len(cached):], noncached)
}

// LookupBlobSize returns the size of blob id.
func (r *Repository) LookupBlobSize(tpe rest.BlobType, id rest.ID) (uint, bool) {
	return r.idx.LookupSize(rest.BlobHandle{Type: tpe, ID: id})
}

// SaveBlob saves a blob of type t into the repository.
// It takes care that no duplicates are saved; this can be overwritten
// by setting storeDuplicate to true.
// If id is the null id, it will be computed and returned.
// Also returns if the blob was already known before.
// If the blob was not known before, it returns the number of bytes the blob
// occupies in the repo (compressed or not, including encryption overhead).
func (r *Repository) SaveBlob(ctx context.Context, t rest.BlobType, buf []byte, id rest.ID, storeDuplicate bool) (newID rest.ID, known bool, size int, err error) {

	if int64(len(buf)) > math.MaxUint32 {
		return rest.ID{}, false, 0, fmt.Errorf("blob is larger than 4GB")
	}

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

var zeroChunkOnce sync.Once
var zeroChunkID rest.ID

// ZeroChunk computes and returns (cached) the ID of an all-zero chunk with size chunker.MinSize
func ZeroChunk() rest.ID {
	zeroChunkOnce.Do(func() {
		zeroChunkID = rest.Hash(make([]byte, chunker.MinSize))
	})
	return zeroChunkID
}

// saveAndEncrypt encrypts data and stores it to the backend as type t. If data
// is small enough, it will be packed together with other small blobs. The
// caller must ensure that the id matches the data. Returned is the size data
// occupies in the repo (compressed or not, including the encryption overhead).
func (r *Repository) saveAndEncrypt(ctx context.Context, t rest.BlobType, data []byte, id rest.ID) (size int, err error) {
	log.Infof("save id %v (%v, %d bytes)", id, t, len(data))

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

	if err := r.verifyCiphertext(ciphertext, uncompressedLength, id); err != nil {
		//nolint:revive // ignore linter warnings about error message spelling
		return 0, fmt.Errorf("Detected data corruption while saving blob %v: %w\nCorrupted blobs are either caused by hardware issues or software bugs. Please open an issue at https://github.com/restic/restic/issues/new/choose for further troubleshooting.", id, err)
	}

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

func (r *Repository) verifyCiphertext(buf []byte, uncompressedLength int, id rest.ID) error {
	if r.opts.NoExtraVerify {
		return nil
	}

	nonce, ciphertext := buf[:r.key.NonceSize()], buf[r.key.NonceSize():]
	plaintext, err := r.key.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return fmt.Errorf("decryption failed: %w", err)
	}
	if uncompressedLength != 0 {
		// DecodeAll will allocate a slice if it is not large enough since it
		// knows the decompressed size (because we're using EncodeAll)
		plaintext, err = r.getZstdDecoder().DecodeAll(plaintext, nil)
		if err != nil {
			return fmt.Errorf("decompression failed: %w", err)
		}
	}
	if !rest.Hash(plaintext).Equal(id) {
		return errors.New("hash mismatch")
	}

	return nil
}

func (r *Repository) StartPackUploader(ctx context.Context, wg *errgroup.Group) {
	if r.packerWg != nil {
		panic("uploader already started")
	}

	innerWg, ctx := errgroup.WithContext(ctx)
	r.packerWg = innerWg
	r.uploader = newPackerUploader(ctx, innerWg, r, r.be.Connections())
	r.treePM = newPackerManager(r.key, rest.TreeBlob, r.packSize(), r.uploader.QueuePacker)
	r.dataPM = newPackerManager(r.key, rest.DataBlob, r.packSize(), r.uploader.QueuePacker)

	wg.Go(func() error {
		return innerWg.Wait()
	})
}

// packSize return the target size of a pack file when uploading
func (r *Repository) packSize() uint {
	return r.opts.PackSize
}
