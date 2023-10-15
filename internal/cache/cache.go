package cache

import "github.com/skyline93/rest/internal/rest"

// Cache manages a local cache.
type Cache struct {
	path    string
	Base    string
	Created bool
}

var cacheLayoutPaths = map[rest.FileType]string{
	rest.PackFile:     "data",
	rest.SnapshotFile: "snapshots",
	rest.IndexFile:    "index",
}
