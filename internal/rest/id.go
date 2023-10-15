package rest

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
)

// idSize contains the size of an ID, in bytes.
const idSize = sha256.Size

// ID references content within a repository.
type ID [idSize]byte

// ParseID converts the given string to an ID.
func ParseID(s string) (ID, error) {
	if len(s) != hex.EncodedLen(idSize) {
		return ID{}, fmt.Errorf("invalid length for ID: %q", s)
	}

	b, err := hex.DecodeString(s)
	if err != nil {
		return ID{}, fmt.Errorf("invalid ID: %s", err)
	}

	id := ID{}
	copy(id[:], b)

	return id, nil
}

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

const shortStr = 4

// Str returns the shortened string version of id.
func (id *ID) Str() string {
	if id == nil {
		return "[nil]"
	}

	if id.IsNull() {
		return "[null]"
	}

	return hex.EncodeToString(id[:shortStr])
}

func (id ID) String() string {
	return hex.EncodeToString(id[:])
}

// IsNull returns true iff id only consists of null bytes.
func (id ID) IsNull() bool {
	var nullID ID

	return id == nullID
}

// Equal compares an ID to another other.
func (id ID) Equal(other ID) bool {
	return id == other
}

// Hash returns the ID for data.
func Hash(data []byte) ID {
	return sha256.Sum256(data)
}

// IDFromHash returns the ID for the hash.
func IDFromHash(hash []byte) (id ID) {
	if len(hash) != idSize {
		panic("invalid hash type, not enough/too many bytes")
	}

	copy(id[:], hash)
	return id
}
