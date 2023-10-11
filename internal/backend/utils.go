package backend

import (
	"bytes"
	"context"
	"io"
	"log"

	"github.com/pkg/errors"
	"github.com/skyline93/rest/internal/rest"
)

// LoadAll reads all data stored in the backend for the handle into the given
// buffer, which is truncated. If the buffer is not large enough or nil, a new
// one is allocated.
func LoadAll(ctx context.Context, buf []byte, be rest.Backend, h rest.Handle) ([]byte, error) {
	retriedInvalidData := false
	err := be.Load(ctx, h, 0, 0, func(rd io.Reader) error {
		// make sure this is idempotent, in case an error occurs this function may be called multiple times!
		wr := bytes.NewBuffer(buf[:0])
		_, cerr := io.Copy(wr, rd)
		if cerr != nil {
			return cerr
		}
		buf = wr.Bytes()

		// retry loading damaged data only once. If a file fails to download correctly
		// the second time, then it  is likely corrupted at the backend. Return the data
		// to the caller in that case to let it decide what to do with the data.
		if !retriedInvalidData && h.Type != rest.ConfigFile {
			id, err := rest.ParseID(h.Name)
			if err == nil && !rest.Hash(buf).Equal(id) {
				log.Printf("retry loading broken blob %v", h)
				retriedInvalidData = true
				return errors.Errorf("loadAll(%v): invalid data returned", h)
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return buf, nil
}

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
