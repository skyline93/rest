package repository

import (
	"context"

	"github.com/skyline93/rest/internal/rest"
	"golang.org/x/sync/errgroup"
)

type uploadTask struct {
	packer *packer
	tpe    rest.BlobType
}

type packerUploader struct {
	uploadQueue chan uploadTask
}

func (pu *packerUploader) TriggerShutdown() {
	close(pu.uploadQueue)
}

// savePacker implements saving a pack in the repository.
type savePacker interface {
	savePacker(ctx context.Context, t rest.BlobType, p *packer) error
}

func newPackerUploader(ctx context.Context, wg *errgroup.Group, repo savePacker, connections uint) *packerUploader {
	pu := &packerUploader{
		uploadQueue: make(chan uploadTask),
	}

	for i := 0; i < int(connections); i++ {
		wg.Go(func() error {
			for {
				select {
				case t, ok := <-pu.uploadQueue:
					if !ok {
						return nil
					}
					err := repo.savePacker(ctx, t.tpe, t.packer)
					if err != nil {
						return err
					}
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		})
	}

	return pu
}

func (pu *packerUploader) QueuePacker(ctx context.Context, t rest.BlobType, p *packer) (err error) {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case pu.uploadQueue <- uploadTask{tpe: t, packer: p}:
	}

	return nil
}
