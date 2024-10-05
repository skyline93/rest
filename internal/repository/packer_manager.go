package repository

import (
	"bufio"
	"context"
	"os"
	"sync"

	"github.com/skyline93/rest/internal/crypto"
	"github.com/skyline93/rest/internal/repository/pack"
	"github.com/skyline93/rest/internal/rest"
)

type packer struct {
	*pack.Packer
	tmpfile *os.File
	bufWr   *bufio.Writer
}

type packerManager struct {
	tpe     rest.BlobType
	key     *crypto.Key
	queueFn func(ctx context.Context, t rest.BlobType, p *packer) error

	pm       sync.Mutex
	packer   *packer
	packSize uint
}
