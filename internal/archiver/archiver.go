package archiver

import (
	"log"
	"os"
	"sort"

	"github.com/pkg/errors"
	"github.com/skyline93/rest/internal/fs"
)

// SelectByNameFunc returns true for all items that should be included (files and
// dirs). If false is returned, files are ignored and dirs are not even walked.
type SelectByNameFunc func(item string) bool

// SelectFunc returns true for all items that should be included (files and
// dirs). If false is returned, files are ignored and dirs are not even walked.
type SelectFunc func(item string, fi os.FileInfo) bool

// ErrorFunc is called when an error during archiving occurs. When nil is
// returned, the archiver continues, otherwise it aborts and passes the error
// up the call stack.
type ErrorFunc func(file string, err error) error

// resolveRelativeTargets replaces targets that only contain relative
// directories ("." or "../../") with the contents of the directory. Each
// element of target is processed with fs.Clean().
func resolveRelativeTargets(filesys fs.FS, targets []string) ([]string, error) {
	log.Printf("targets before resolving: %v", targets)
	result := make([]string, 0, len(targets))
	for _, target := range targets {
		target = filesys.Clean(target)
		pc, _ := pathComponents(filesys, target, false)
		if len(pc) > 0 {
			result = append(result, target)
			continue
		}

		log.Printf("replacing %q with readdir(%q)", target, target)
		entries, err := readdirnames(filesys, target, fs.O_NOFOLLOW)
		if err != nil {
			return nil, err
		}
		sort.Strings(entries)

		for _, name := range entries {
			result = append(result, filesys.Join(target, name))
		}
	}

	log.Printf("targets after resolving: %v", result)
	return result, nil
}

// flags are passed to fs.OpenFile. O_RDONLY is implied.
func readdirnames(filesystem fs.FS, dir string, flags int) ([]string, error) {
	f, err := filesystem.OpenFile(dir, fs.O_RDONLY|flags, 0)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	entries, err := f.Readdirnames(-1)
	if err != nil {
		_ = f.Close()
		return nil, errors.Wrapf(err, "Readdirnames %v failed", dir)
	}

	err = f.Close()
	if err != nil {
		return nil, err
	}

	return entries, nil
}
