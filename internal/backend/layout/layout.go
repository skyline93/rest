package layout

import (
	"context"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/skyline93/rest/internal/backend"
	"github.com/skyline93/rest/internal/fs"
)

type LocalFilesystem struct {
}

// ReadDir returns all entries of a directory.
func (l *LocalFilesystem) ReadDir(_ context.Context, dir string) ([]os.FileInfo, error) {
	f, err := fs.Open(dir)
	if err != nil {
		return nil, err
	}

	entries, err := f.Readdir(-1)
	if err != nil {
		return nil, errors.Wrap(err, "Readdir")
	}

	err = f.Close()
	if err != nil {
		return nil, errors.Wrap(err, "Close")
	}

	return entries, nil
}

// Join combines several path components to one.
func (l *LocalFilesystem) Join(paths ...string) string {
	return filepath.Join(paths...)
}

// IsNotExist returns true for errors that are caused by not existing files.
func (l *LocalFilesystem) IsNotExist(err error) bool {
	return os.IsNotExist(err)
}

type Filesystem interface {
	Join(...string) string
	ReadDir(context.Context, string) ([]os.FileInfo, error)
	IsNotExist(error) bool
}

type Layout interface {
	Filename(backend.Handle) string
	Dirname(backend.Handle) string
	Basedir(backend.FileType) (dir string, subdirs bool)
	Paths() []string
	Name() string
}
