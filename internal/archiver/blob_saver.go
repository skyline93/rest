package archiver

import (
	"context"
	"fmt"

	log "github.com/sirupsen/logrus"
	"github.com/skyline93/rest/internal/rest"
	"golang.org/x/sync/errgroup"
)

// Saver allows saving a blob.
type Saver interface {
	SaveBlob(ctx context.Context, t rest.BlobType, data []byte, id rest.ID, storeDuplicate bool) (rest.ID, bool, int, error)
}

type saveBlobJob struct {
	rest.BlobType
	buf *Buffer
	fn  string
	cb  func(res SaveBlobResponse)
}

// BlobSaver concurrently saves incoming blobs to the repo.
type BlobSaver struct {
	repo Saver
	ch   chan<- saveBlobJob
}

type SaveBlobResponse struct {
	id         rest.ID
	length     int
	sizeInRepo int
	known      bool
}

func (s *BlobSaver) TriggerShutdown() {
	close(s.ch)
}

// NewBlobSaver returns a new blob. A worker pool is started, it is stopped
// when ctx is cancelled.
func NewBlobSaver(ctx context.Context, wg *errgroup.Group, repo Saver, workers uint) *BlobSaver {
	ch := make(chan saveBlobJob)
	s := &BlobSaver{
		repo: repo,
		ch:   ch,
	}

	for i := uint(0); i < workers; i++ {
		wg.Go(func() error {
			return s.worker(ctx, ch)
		})
	}

	return s
}

func (s *BlobSaver) worker(ctx context.Context, jobs <-chan saveBlobJob) error {
	for {
		var job saveBlobJob
		var ok bool
		select {
		case <-ctx.Done():
			return nil
		case job, ok = <-jobs:
			if !ok {
				return nil
			}
		}

		res, err := s.saveBlob(ctx, job.BlobType, job.buf.Data)
		if err != nil {
			log.Infof("saveBlob returned error, exiting: %v", err)
			return fmt.Errorf("failed to save blob from file %q: %w", job.fn, err)
		}
		job.cb(res)
		job.buf.Release()
	}
}

func (s *BlobSaver) saveBlob(ctx context.Context, t rest.BlobType, buf []byte) (SaveBlobResponse, error) {
	id, known, sizeInRepo, err := s.repo.SaveBlob(ctx, t, buf, rest.ID{}, false)

	if err != nil {
		return SaveBlobResponse{}, err
	}

	return SaveBlobResponse{
		id:         id,
		length:     len(buf),
		sizeInRepo: sizeInRepo,
		known:      known,
	}, nil
}

// Save stores a blob in the repo. It checks the index and the known blobs
// before saving anything. It takes ownership of the buffer passed in.
func (s *BlobSaver) Save(ctx context.Context, t rest.BlobType, buf *Buffer, filename string, cb func(res SaveBlobResponse)) {
	select {
	case s.ch <- saveBlobJob{BlobType: t, buf: buf, fn: filename, cb: cb}:
	case <-ctx.Done():
		log.Infof("not sending job, context is cancelled")
	}
}