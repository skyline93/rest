package repository

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/restic/chunker"
	"github.com/skyline93/rest/internal/crypto"
	"github.com/skyline93/rest/internal/index"
	"github.com/skyline93/rest/internal/rest"
	"golang.org/x/sync/errgroup"
	"honnef.co/go/tools/lintcmd/cache"
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
