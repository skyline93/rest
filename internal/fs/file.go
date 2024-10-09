package fs

import (
	"fmt"
	"os"
)

func Open(name string) (File, error) {
	return os.Open(fixpath(name))
}

func Stat(name string) (os.FileInfo, error) {
	return os.Stat(fixpath(name))
}

func Lstat(name string) (os.FileInfo, error) {
	return os.Lstat(fixpath(name))
}

func MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(fixpath(path), perm)
}

func RemoveAll(path string) error {
	return os.RemoveAll(fixpath(path))
}

func Remove(name string) error {
	return os.Remove(fixpath(name))
}

// Readdirnames returns a list of file in a directory. Flags are passed to fs.OpenFile. O_RDONLY is implied.
func Readdirnames(filesystem FS, dir string, flags int) ([]string, error) {
	f, err := filesystem.OpenFile(dir, O_RDONLY|flags, 0)
	if err != nil {
		return nil, fmt.Errorf("openfile for readdirnames failed: %w", err)
	}

	entries, err := f.Readdirnames(-1)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("readdirnames %v failed: %w", dir, err)
	}

	err = f.Close()
	if err != nil {
		return nil, err
	}

	return entries, nil
}

// RemoveIfExists removes a file, returning no error if it does not exist.
func RemoveIfExists(filename string) error {
	err := os.Remove(filename)
	if err != nil && os.IsNotExist(err) {
		err = nil
	}
	return err
}

// Readlink returns the destination of the named symbolic link.
// If there is an error, it will be of type *PathError.
func Readlink(name string) (string, error) {
	return os.Readlink(fixpath(name))
}
