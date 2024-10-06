package repository

import (
	"bufio"
	"context"
	"crypto/sha256"
	"io"
	"os"
	"runtime"
	"sync"

	log "github.com/sirupsen/logrus"
	"github.com/skyline93/rest/internal/backend"
	"github.com/skyline93/rest/internal/crypto"
	"github.com/skyline93/rest/internal/errors"
	"github.com/skyline93/rest/internal/fs"
	"github.com/skyline93/rest/internal/repository/hashing"
	"github.com/skyline93/rest/internal/repository/pack"
	"github.com/skyline93/rest/internal/rest"
)

type packer struct {
	*pack.Packer
	tmpfile *os.File
	bufWr   *bufio.Writer
}

type packerManager struct {
	tpe     rest.BlobType
	key     *crypto.Key
	queueFn func(ctx context.Context, t rest.BlobType, p *packer) error

	pm       sync.Mutex
	packer   *packer
	packSize uint
}

func (r *packerManager) Flush(ctx context.Context) error {
	r.pm.Lock()
	defer r.pm.Unlock()

	if r.packer != nil {
		log.Info("manually flushing pending pack")
		err := r.queueFn(ctx, r.tpe, r.packer)
		if err != nil {
			return err
		}
		r.packer = nil
	}
	return nil
}

func (r *packerManager) SaveBlob(ctx context.Context, t rest.BlobType, id rest.ID, ciphertext []byte, uncompressedLength int) (int, error) {
	r.pm.Lock()
	defer r.pm.Unlock()

	var err error
	packer := r.packer
	// use separate packer if compressed length is larger than the packsize
	// this speeds up the garbage collection of oversized blobs and reduces the cache size
	// as the oversize blobs are only downloaded if necessary
	if len(ciphertext) >= int(r.packSize) || r.packer == nil {
		packer, err = r.newPacker()
		if err != nil {
			return 0, err
		}
		// don't store packer for oversized blob
		if r.packer == nil {
			r.packer = packer
		}
	}

	// save ciphertext
	// Add only appends bytes in memory to avoid being a scaling bottleneck
	size, err := packer.Add(t, id, ciphertext, uncompressedLength)
	if err != nil {
		return 0, err
	}

	// if the pack and header is not full enough, put back to the list
	if packer.Size() < r.packSize && !packer.HeaderFull() {
		log.Infof("pack is not full enough (%d bytes)", packer.Size())
		return size, nil
	}
	if packer == r.packer {
		// forget full packer
		r.packer = nil
	}

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
func (r *packerManager) newPacker() (pck *packer, err error) {
	log.Infof("create new pack")
	tmpfile, err := fs.TempFile("", "restic-temp-pack-")
	if err != nil {
		return nil, errors.WithStack(err)
	}

	bufWr := bufio.NewWriter(tmpfile)
	p := pack.NewPacker(r.key, bufWr)
	pck = &packer{
		Packer:  p,
		tmpfile: tmpfile,
		bufWr:   bufWr,
	}

	return pck, nil
}

// newPackerManager returns a new packer manager which writes temporary files
// to a temporary directory
func newPackerManager(key *crypto.Key, tpe rest.BlobType, packSize uint, queueFn func(ctx context.Context, t rest.BlobType, p *packer) error) *packerManager {
	return &packerManager{
		tpe:      tpe,
		key:      key,
		queueFn:  queueFn,
		packSize: packSize,
	}
}

// savePacker stores p in the backend.
func (r *Repository) savePacker(ctx context.Context, t rest.BlobType, p *packer) error {
	log.Infof("save packer for %v with %d blobs (%d bytes)\n", t, p.Packer.Count(), p.Packer.Size())
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
	rd, err = backend.NewFileReader(p.tmpfile, nil)
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
	h := backend.Handle{Type: backend.PackFile, Name: id.String(), IsMetadata: t.IsMetadata()}
	var beHash []byte
	if beHr != nil {
		beHash = beHr.Sum(nil)
	}
	rrd, err := backend.NewFileReader(p.tmpfile, beHash)
	if err != nil {
		return err
	}

	err = r.be.Save(ctx, h, rrd)
	if err != nil {
		log.Debugf("Save(%v) error: %v", h, err)
		return err
	}

	log.Debugf("saved as %v", h)

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
	log.Debugf("  updating blobs %v to pack %v", p.Packer.Blobs(), id)
	r.idx.StorePack(id, p.Packer.Blobs())

	// Save index if full
	return r.idx.SaveFullIndex(ctx, r)
}
