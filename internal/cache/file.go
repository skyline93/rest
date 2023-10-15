package cache

import (
	"github.com/pkg/errors"
	"github.com/skyline93/rest/internal/fs"
	"github.com/skyline93/rest/internal/rest"
	"log"
	"os"
	"path/filepath"
)

func (c *Cache) canBeCached(t rest.FileType) bool {
	if c == nil {
		return false
	}

	_, ok := cacheLayoutPaths[t]
	return ok
}

func (c *Cache) filename(h rest.Handle) string {
	if len(h.Name) < 2 {
		panic("Name is empty or too short")
	}
	subdir := h.Name[:2]
	return filepath.Join(c.path, cacheLayoutPaths[h.Type], subdir, h.Name)
}

// Clear removes all files of type t from the cache that are not contained in
// the set valid.
func (c *Cache) Clear(t rest.FileType, valid rest.IDSet) error {
	log.Printf("Clearing cache for %v: %v valid files", t, len(valid))
	if !c.canBeCached(t) {
		return nil
	}

	list, err := c.list(t)
	if err != nil {
		return err
	}

	for id := range list {
		if valid.Has(id) {
			continue
		}

		if err = fs.Remove(c.filename(rest.Handle{Type: t, Name: id.String()})); err != nil {
			return err
		}
	}

	return nil
}

func isFile(fi os.FileInfo) bool {
	return fi.Mode()&(os.ModeType|os.ModeCharDevice) == 0
}

// list returns a list of all files of type T in the cache.
func (c *Cache) list(t rest.FileType) (rest.IDSet, error) {
	if !c.canBeCached(t) {
		return nil, errors.New("cannot be cached")
	}

	list := rest.NewIDSet()
	dir := filepath.Join(c.path, cacheLayoutPaths[t])
	err := filepath.Walk(dir, func(name string, fi os.FileInfo, err error) error {
		if err != nil {
			return errors.Wrap(err, "Walk")
		}

		if !isFile(fi) {
			return nil
		}

		id, err := rest.ParseID(filepath.Base(name))
		if err != nil {
			return nil
		}

		list.Insert(id)
		return nil
	})

	return list, err
}

// Has returns true if the file is cached.
func (c *Cache) Has(h rest.Handle) bool {
	if !c.canBeCached(h.Type) {
		return false
	}

	_, err := fs.Stat(c.filename(h))
	return err == nil
}
