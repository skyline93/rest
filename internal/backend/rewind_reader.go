package backend

import (
	"bytes"
	"hash"
	"io"
)

type RewindReader interface {
	io.Reader

	// Rewind rewinds the reader so the same data can be read again from the
	// start.
	Rewind() error

	// Length returns the number of bytes that can be read from the Reader
	// after calling Rewind.
	Length() int64

	// Hash return a hash of the data if requested by the backed.
	Hash() []byte
}

// NewByteReader prepares a ByteReader that can then be used to read buf.
func NewByteReader(buf []byte, hasher hash.Hash) *ByteReader {
	var hash []byte
	if hasher != nil {
		// must never fail according to interface
		_, err := hasher.Write(buf)
		if err != nil {
			panic(err)
		}
		hash = hasher.Sum(nil)
	}
	return &ByteReader{
		Reader: bytes.NewReader(buf),
		Len:    int64(len(buf)),
		hash:   hash,
	}
}

// ByteReader implements a RewindReader for a byte slice.
type ByteReader struct {
	*bytes.Reader
	Len  int64
	hash []byte
}

// Rewind restarts the reader from the beginning of the data.
func (b *ByteReader) Rewind() error {
	_, err := b.Reader.Seek(0, io.SeekStart)
	return err
}

// Length returns the number of bytes read from the reader after Rewind is
// called.
func (b *ByteReader) Length() int64 {
	return b.Len
}

// Hash return a hash of the data if requested by the backed.
func (b *ByteReader) Hash() []byte {
	return b.hash
}
