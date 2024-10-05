package util

import (
	"context"
	"io"

	"github.com/skyline93/rest/internal/backend"
)

// DefaultLoad implements Backend.Load using lower-level openReader func
func DefaultLoad(ctx context.Context, h backend.Handle, length int, offset int64,
	openReader func(ctx context.Context, h backend.Handle, length int, offset int64) (io.ReadCloser, error),
	fn func(rd io.Reader) error) error {

	rd, err := openReader(ctx, h, length, offset)
	if err != nil {
		return err
	}
	err = fn(rd)
	if err != nil {
		_ = rd.Close() // ignore secondary errors closing the reader
		return err
	}
	return rd.Close()
}
