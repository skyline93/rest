package index

import (
	"context"
	"runtime"
	"sync"

	"github.com/skyline93/rest/internal/rest"
)

// ForAllIndexes loads all index files in parallel and calls the given callback.
// It is guaranteed that the function is not run concurrently. If the callback
// returns an error, this function is cancelled and also returns that error.
func ForAllIndexes(ctx context.Context, repo rest.Repository,
	fn func(id rest.ID, index *Index, oldFormat bool, err error) error) error {

	// decoding an index can take quite some time such that this can be both CPU- or IO-bound
	// as the whole index is kept in memory anyways, a few workers too much don't matter
	workerCount := repo.Connections() + uint(runtime.GOMAXPROCS(0))

	var m sync.Mutex
	return rest.ParallelList(ctx, repo.Backend(), rest.IndexFile, workerCount, func(ctx context.Context, id rest.ID, size int64) error {
		var err error
		var idx *Index
		oldFormat := false

		buf, err := repo.LoadUnpacked(ctx, rest.IndexFile, id)
		if err == nil {
			idx, oldFormat, err = DecodeIndex(buf, id)
		}

		m.Lock()
		defer m.Unlock()
		return fn(id, idx, oldFormat, err)
	})
}
