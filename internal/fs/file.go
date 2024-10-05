package fs

import "os"

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
