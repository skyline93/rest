package index

import (
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/skyline93/rest/internal/rest"
)

type Index struct {
	m      sync.RWMutex
	byType [rest.NumBlobTypes]indexMap
	packs  rest.IDs

	final   bool     // set to true for all indexes read from the backend ("finalized")
	ids     rest.IDs // set to the IDs of the contained finalized indexes
	created time.Time
}

func NewIndex() *Index {
	return &Index{
		created: time.Now(),
	}
}

func (idx *Index) Finalize() {
	log.Infof("finalizing index")
	idx.m.Lock()
	defer idx.m.Unlock()

	idx.final = true
}
