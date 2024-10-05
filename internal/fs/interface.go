package fs

import (
	"io"
	"os"
)

type File interface {
	io.Reader
	io.Closer

	Fd() uintptr
	Readdirnames(n int) ([]string, error)
	Readdir(int) ([]os.FileInfo, error)
	Seek(int64, int) (int64, error)
	Stat() (os.FileInfo, error)
	Name() string
}
