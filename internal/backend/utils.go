package backend

import (
	"context"
	"io"

	"github.com/skyline93/rest/internal/rest"
)

// DefaultLoad implements Backend.Load using lower-level openReader func
func DefaultLoad(ctx context.Context, h rest.Handle, length int, offset int64,
	openReader func(ctx context.Context, h rest.Handle, length int, offset int64) (io.ReadCloser, error),
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

// LimitedReadCloser wraps io.LimitedReader and exposes the Close() method.
type LimitedReadCloser struct {
	io.Closer
	io.LimitedReader
}

// LimitReadCloser returns a new reader wraps r in an io.LimitedReader, but also
// exposes the Close() method.
func LimitReadCloser(r io.ReadCloser, n int64) *LimitedReadCloser {
	return &LimitedReadCloser{Closer: r, LimitedReader: io.LimitedReader{R: r, N: n}}
}
