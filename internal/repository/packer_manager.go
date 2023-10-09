package repository

import (
	"bufio"
	"context"
	"os"
	"sync"

	"github.com/skyline93/rest/internal/crypto"
	"github.com/skyline93/rest/internal/pack"
	"github.com/skyline93/rest/internal/rest"
)

// Packer holds a pack.Packer together with a hash writer.
type Packer struct {
	*pack.Packer
	tmpfile *os.File
	bufWr   *bufio.Writer
}

// packerManager keeps a list of open packs and creates new on demand.
type packerManager struct {
	tpe     rest.BlobType
	key     *crypto.Key
	queueFn func(ctx context.Context, t rest.BlobType, p *Packer) error

	pm       sync.Mutex
	packer   *Packer
	packSize uint
}
