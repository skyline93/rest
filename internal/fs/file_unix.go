package fs

import (
	"os"
	"syscall"
)

func fixpath(name string) string {
	return name
}

// Chmod changes the mode of the named file to mode.
func Chmod(name string, mode os.FileMode) error {
	err := os.Chmod(fixpath(name), mode)

	// ignore the error if the FS does not support setting this mode (e.g. CIFS with gvfs on Linux)
	if err != nil && isNotSupported(err) {
		return nil
	}

	return err
}

// isNotSupported returns true if the error is caused by an unsupported file system feature.
func isNotSupported(err error) bool {
	if perr, ok := err.(*os.PathError); ok && perr.Err == syscall.ENOTSUP {
		return true
	}
	return false
}

// TempFile creates a temporary file which has already been deleted (on
// supported platforms)
func TempFile(dir, prefix string) (f *os.File, err error) {
	f, err = os.CreateTemp(dir, prefix)
	if err != nil {
		return nil, err
	}

	if err = os.Remove(f.Name()); err != nil {
		return nil, err
	}

	return f, nil
}
