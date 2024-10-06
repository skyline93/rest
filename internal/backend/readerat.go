package backend

import (
	"context"
	"io"

	log "github.com/sirupsen/logrus"
	"github.com/skyline93/rest/internal/errors"
)

// ReadAt reads from the backend handle h at the given position.
func ReadAt(ctx context.Context, be Backend, h Handle, offset int64, p []byte) (n int, err error) {
	log.Infof("ReadAt(%v) at %v, len %v", h, offset, len(p))

	err = be.Load(ctx, h, len(p), offset, func(rd io.Reader) (ierr error) {
		n, ierr = io.ReadFull(rd, p)

		return ierr
	})
	if err != nil {
		return 0, errors.Wrapf(err, "ReadFull(%v)", h)
	}

	log.Infof("ReadAt(%v) ReadFull returned %v bytes", h, n)
	return n, nil
}
