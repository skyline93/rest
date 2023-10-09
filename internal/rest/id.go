package rest

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"io"
)

// idSize contains the size of an ID, in bytes.
const idSize = sha256.Size

// ID references content within a repository.
type ID [idSize]byte

// NewRandomID returns a randomly generated ID. When reading from rand fails,
// the function panics.
func NewRandomID() ID {
	id := ID{}
	_, err := io.ReadFull(rand.Reader, id[:])
	if err != nil {
		panic(err)
	}
	return id
}

func (id ID) String() string {
	return hex.EncodeToString(id[:])
}

// Hash returns the ID for data.
func Hash(data []byte) ID {
	return sha256.Sum256(data)
}
