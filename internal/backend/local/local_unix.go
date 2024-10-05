package local

import (
	"errors"
	"os"
	"runtime"
	"syscall"

	"github.com/skyline93/rest/internal/fs"
)

func isMacENOTTY(err error) bool {
	return runtime.GOOS == "darwin" && errors.Is(err, syscall.ENOTTY)
}

// fsyncDir flushes changes to the directory dir.
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}

	err = d.Sync()
	if err != nil &&
		(errors.Is(err, syscall.ENOTSUP) || errors.Is(err, syscall.ENOENT) ||
			errors.Is(err, syscall.EINVAL) || isMacENOTTY(err)) {
		err = nil
	}

	cerr := d.Close()
	if err == nil {
		err = cerr
	}

	return err
}

// set file to readonly
func setFileReadonly(f string, mode os.FileMode) error {
	return fs.Chmod(f, mode&^0222)
}
