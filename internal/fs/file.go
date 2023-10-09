package fs

import "os"

// Stat returns a FileInfo structure describing the named file.
// If there is an error, it will be of type *PathError.
func Stat(name string) (os.FileInfo, error) {
	return os.Stat(fixpath(name))
}

// Lstat returns the FileInfo structure describing the named file.
// If the file is a symbolic link, the returned FileInfo
// describes the symbolic link.  Lstat makes no attempt to follow the link.
// If there is an error, it will be of type *PathError.
func Lstat(name string) (os.FileInfo, error) {
	return os.Lstat(fixpath(name))
}

// MkdirAll creates a directory named path, along with any necessary parents,
// and returns nil, or else returns an error. The permission bits perm are used
// for all directories that MkdirAll creates. If path is already a directory,
// MkdirAll does nothing and returns nil.
func MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(fixpath(path), perm)
}

// Open opens a file for reading.
func Open(name string) (File, error) {
	return os.Open(fixpath(name))
}

// Remove removes the named file or directory.
// If there is an error, it will be of type *PathError.
func Remove(name string) error {
	return os.Remove(fixpath(name))
}

// RemoveAll removes path and any children it contains.
// It removes everything it can but returns the first error
// it encounters.  If the path does not exist, RemoveAll
// returns nil (no error).
func RemoveAll(path string) error {
	return os.RemoveAll(fixpath(path))
}
