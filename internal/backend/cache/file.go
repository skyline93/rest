package cache

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/skyline93/rest/internal/backend"
	"github.com/skyline93/rest/internal/fs"
)

func (c *Cache) Forget(h backend.Handle) error {
	h.IsMetadata = false

	if _, ok := c.forgotten.Load(h); ok {
		// Delete a file at most once while restic runs.
		// This prevents repeatedly caching and forgetting broken files
		return fmt.Errorf("circuit breaker prevents repeated deletion of cached file %v", h)
	}

	removed, err := c.remove(h)
	if removed {
		c.forgotten.Store(h, struct{}{})
	}
	return err
}

// remove deletes a file. When the file is not cached, no error is returned.
func (c *Cache) remove(h backend.Handle) (bool, error) {
	if !c.canBeCached(h.Type) {
		return false, nil
	}

	err := fs.Remove(c.filename(h))
	removed := err == nil
	if errors.Is(err, os.ErrNotExist) {
		err = nil
	}
	return removed, err
}

func (c *Cache) canBeCached(t backend.FileType) bool {
	if c == nil {
		return false
	}

	_, ok := cacheLayoutPaths[t]
	return ok
}

func (c *Cache) filename(h backend.Handle) string {
	if len(h.Name) < 2 {
		panic("Name is empty or too short")
	}
	subdir := h.Name[:2]
	return filepath.Join(c.path, cacheLayoutPaths[h.Type], subdir, h.Name)
}

// Has returns true if the file is cached.
func (c *Cache) Has(h backend.Handle) bool {
	if !c.canBeCached(h.Type) {
		return false
	}

	_, err := fs.Stat(c.filename(h))
	return err == nil
}
