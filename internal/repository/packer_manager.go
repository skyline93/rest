package repository

import (
	"bufio"
	"context"
	"crypto/sha256"
	"github.com/pkg/errors"
	"github.com/skyline93/rest/internal/fs"
	"github.com/skyline93/rest/internal/hashing"
	"io"
	"log"
	"os"
	"runtime"
	"sync"

	"github.com/skyline93/rest/internal/crypto"
	"github.com/skyline93/rest/internal/pack"
	"github.com/skyline93/rest/internal/rest"
)

// Packer holds a pack.Packer together with a hash writer.
type Packer struct {
	*pack.Packer
	tmpfile *os.File
	bufWr   *bufio.Writer
}

// packerManager keeps a list of open packs and creates new on demand.
type packerManager struct {
	tpe     rest.BlobType
	key     *crypto.Key
	queueFn func(ctx context.Context, t rest.BlobType, p *Packer) error

	pm       sync.Mutex
	packer   *Packer
	packSize uint
}

// newPackerManager returns an new packer manager which writes temporary files
// to a temporary directory
func newPackerManager(key *crypto.Key, tpe rest.BlobType, packSize uint, queueFn func(ctx context.Context, t rest.BlobType, p *Packer) error) *packerManager {
	return &packerManager{
		tpe:      tpe,
		key:      key,
		queueFn:  queueFn,
		packSize: packSize,
	}
}

// savePacker stores p in the backend.
func (r *Repository) savePacker(ctx context.Context, t rest.BlobType, p *Packer) error {
	log.Printf("save packer for %v with %d blobs (%d bytes)\n", t, p.Packer.Count(), p.Packer.Size())
	err := p.Packer.Finalize()
	if err != nil {
		return err
	}
	err = p.bufWr.Flush()
	if err != nil {
		return err
	}

	// calculate sha256 hash in a second pass
	var rd io.Reader
	rd, err = rest.NewFileReader(p.tmpfile, nil)
	if err != nil {
		return err
	}
	beHasher := r.be.Hasher()
	var beHr *hashing.Reader
	if beHasher != nil {
		beHr = hashing.NewReader(rd, beHasher)
		rd = beHr
	}

	hr := hashing.NewReader(rd, sha256.New())
	_, err = io.Copy(io.Discard, hr)
	if err != nil {
		return err
	}

	id := rest.IDFromHash(hr.Sum(nil))
	h := rest.Handle{Type: rest.PackFile, Name: id.String(), ContainedBlobType: t}
	var beHash []byte
	if beHr != nil {
		beHash = beHr.Sum(nil)
	}
	rrd, err := rest.NewFileReader(p.tmpfile, beHash)
	if err != nil {
		return err
	}

	err = r.be.Save(ctx, h, rrd)
	if err != nil {
		log.Printf("Save(%v) error: %v", h, err)
		return err
	}

	log.Printf("saved as %v", h)

	err = p.tmpfile.Close()
	if err != nil {
		return errors.Wrap(err, "close tempfile")
	}

	// on windows the tempfile is automatically deleted on close
	if runtime.GOOS != "windows" {
		err = fs.RemoveIfExists(p.tmpfile.Name())
		if err != nil {
			return errors.WithStack(err)
		}
	}

	// update blobs in the index
	log.Printf("  updating blobs %v to pack %v", p.Packer.Blobs(), id)
	r.idx.StorePack(id, p.Packer.Blobs())

	// Save index if full
	if r.noAutoIndexUpdate {
		return nil
	}
	return r.idx.SaveFullIndex(ctx, r)
}

func (r *packerManager) SaveBlob(ctx context.Context, t rest.BlobType, id rest.ID, ciphertext []byte, uncompressedLength int) (int, error) {
	r.pm.Lock()
	defer r.pm.Unlock()

	var err error
	packer := r.packer
	if r.packer == nil {
		packer, err = r.newPacker()
		if err != nil {
			return 0, err
		}
	}
	// remember packer
	r.packer = packer

	// save ciphertext
	// Add only appends bytes in memory to avoid being a scaling bottleneck
	size, err := packer.Add(t, id, ciphertext, uncompressedLength)
	if err != nil {
		return 0, err
	}

	// if the pack and header is not full enough, put back to the list
	if packer.Size() < r.packSize && !packer.HeaderFull() {
		log.Printf("pack is not full enough (%d bytes)", packer.Size())
		return size, nil
	}
	// forget full packer
	r.packer = nil

	// call while holding lock to prevent findPacker from creating new packers if the uploaders are busy
	// else write the pack to the backend
	err = r.queueFn(ctx, t, packer)
	if err != nil {
		return 0, err
	}

	return size + packer.HeaderOverhead(), nil
}

// findPacker returns a packer for a new blob of size bytes. Either a new one is
// created or one is returned that already has some blobs.
func (r *packerManager) newPacker() (packer *Packer, err error) {
	log.Printf("create new pack")
	tmpfile, err := fs.TempFile("", "restic-temp-pack-")
	if err != nil {
		return nil, errors.WithStack(err)
	}

	bufWr := bufio.NewWriter(tmpfile)
	p := pack.NewPacker(r.key, bufWr)
	packer = &Packer{
		Packer:  p,
		tmpfile: tmpfile,
		bufWr:   bufWr,
	}

	return packer, nil
}

func (r *packerManager) Flush(ctx context.Context) error {
	r.pm.Lock()
	defer r.pm.Unlock()

	if r.packer != nil {
		log.Printf("manually flushing pending pack")
		err := r.queueFn(ctx, r.tpe, r.packer)
		if err != nil {
			return err
		}
		r.packer = nil
	}
	return nil
}
