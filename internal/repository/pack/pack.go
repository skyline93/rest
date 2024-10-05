package pack

import (
	"io"
	"sync"

	"github.com/skyline93/rest/internal/crypto"
	"github.com/skyline93/rest/internal/rest"
)

type Packer struct {
	blobs []rest.Blob

	bytes uint
	k     *crypto.Key
	wr    io.Writer

	m sync.Mutex
}
