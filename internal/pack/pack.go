package pack

import (
	"io"
	"sync"

	"github.com/skyline93/rest/internal/crypto"
	"github.com/skyline93/rest/internal/rest"
)

// Packer is used to create a new Pack.
type Packer struct {
	blobs []rest.Blob

	bytes uint
	k     *crypto.Key
	wr    io.Writer

	m sync.Mutex
}