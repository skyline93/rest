package archiver

import (
	"context"
	"fmt"
	"os"
	"path"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/skyline93/rest/internal/errors"
	"github.com/skyline93/rest/internal/fs"
	"github.com/skyline93/rest/internal/rest"
	"golang.org/x/sync/errgroup"
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
	log.Debugf("targets before resolving: %v", targets)
	result := make([]string, 0, len(targets))
	for _, target := range targets {
		if target != "" && filesys.VolumeName(target) == target {
			// special case to allow users to also specify a volume name "C:" instead of a path "C:\"
			target = target + filesys.Separator()
		} else {
			target = filesys.Clean(target)
		}
		pc, _ := pathComponents(filesys, target, false)
		if len(pc) > 0 {
			result = append(result, target)
			continue
		}

		log.Debugf("replacing %q with readdir(%q)", target, target)
		entries, err := fs.Readdirnames(filesys, target, fs.O_NOFOLLOW)
		if err != nil {
			return nil, err
		}
		sort.Strings(entries)

		for _, name := range entries {
			result = append(result, filesys.Join(target, name))
		}
	}

	log.Debugf("targets after resolving: %v", result)
	return result, nil
}

type archiverRepo interface {
	rest.Loader
	rest.BlobSaver
	rest.SaverUnpacked

	Config() rest.Config
	StartPackUploader(ctx context.Context, wg *errgroup.Group)
	Flush(ctx context.Context) error
}

// Options is used to configure the archiver.
type Options struct {
	// ReadConcurrency sets how many files are read in concurrently. If
	// it's set to zero, at most two files are read in concurrently (which
	// turned out to be a good default for most situations).
	ReadConcurrency uint

	// SaveBlobConcurrency sets how many blobs are hashed and saved
	// concurrently. If it's set to zero, the default is the number of CPUs
	// available in the system.
	SaveBlobConcurrency uint

	// SaveTreeConcurrency sets how many trees are marshalled and saved to the
	// repo concurrently.
	SaveTreeConcurrency uint
}

// Archiver saves a directory structure to the repo.
type Archiver struct {
	Repo         archiverRepo
	SelectByName SelectByNameFunc
	Select       SelectFunc
	FS           fs.FS
	Options      Options

	blobSaver *BlobSaver
	fileSaver *FileSaver
	treeSaver *TreeSaver
	mu        sync.Mutex
	summary   *Summary

	// Error is called for all errors that occur during backup.
	Error ErrorFunc

	// CompleteItem is called for all files and dirs once they have been
	// processed successfully. The parameter item contains the path as it will
	// be in the snapshot after saving. s contains some statistics about this
	// particular file/dir.
	//
	// Once reading a file has completed successfully (but not saving it yet),
	// CompleteItem will be called with current == nil.
	//
	// CompleteItem may be called asynchronously from several different
	// goroutines!
	CompleteItem func(item string, previous, current *rest.Node, s ItemStats, d time.Duration)

	// StartFile is called when a file is being processed by a worker.
	StartFile func(filename string)

	// CompleteBlob is called for all saved blobs for files.
	CompleteBlob func(bytes uint64)

	// WithAtime configures if the access time for files and directories should
	// be saved. Enabling it may result in much metadata, so it's off by
	// default.
	WithAtime bool

	// Flags controlling change detection. See doc/040_backup.rst for details.
	ChangeIgnoreFlags uint
}

// ApplyDefaults returns a copy of o with the default options set for all unset
// fields.
func (o Options) ApplyDefaults() Options {
	if o.ReadConcurrency == 0 {
		// two is a sweet spot for almost all situations. We've done some
		// experiments documented here:
		// https://github.com/borgbackup/borg/issues/3500
		o.ReadConcurrency = 2
	}

	if o.SaveBlobConcurrency == 0 {
		// blob saving is CPU bound due to hash checking and encryption
		// the actual upload is handled by the repository itself
		o.SaveBlobConcurrency = uint(runtime.GOMAXPROCS(0))
	}

	if o.SaveTreeConcurrency == 0 {
		// can either wait for a file, wait for a tree, serialize a tree or wait for saveblob
		// the last two are cpu-bound and thus mutually exclusive.
		// Also allow waiting for FileReadConcurrency files, this is the maximum of FutureFiles
		// which currently can be in progress. The main backup loop blocks when trying to queue
		// more files to read.
		o.SaveTreeConcurrency = uint(runtime.GOMAXPROCS(0)) + o.ReadConcurrency
	}

	return o
}

// ItemStats collects some statistics about a particular file or directory.
type ItemStats struct {
	DataBlobs      int    // number of new data blobs added for this item
	DataSize       uint64 // sum of the sizes of all new data blobs
	DataSizeInRepo uint64 // sum of the bytes added to the repo (including compression and crypto overhead)
	TreeBlobs      int    // number of new tree blobs added for this item
	TreeSize       uint64 // sum of the sizes of all new tree blobs
	TreeSizeInRepo uint64 // sum of the bytes added to the repo (including compression and crypto overhead)
}

// New initializes a new archiver.
func New(repo archiverRepo, fs fs.FS, opts Options) *Archiver {
	arch := &Archiver{
		Repo:         repo,
		SelectByName: func(_ string) bool { return true },
		Select:       func(_ string, _ os.FileInfo) bool { return true },
		FS:           fs,
		Options:      opts.ApplyDefaults(),

		CompleteItem: func(string, *rest.Node, *rest.Node, ItemStats, time.Duration) {},
		StartFile:    func(string) {},
		CompleteBlob: func(uint64) {},
	}

	return arch
}

type futureNodeResult struct {
	snPath, target string

	node  *rest.Node
	stats ItemStats
	err   error
}

type Summary struct {
	Files, Dirs    ChangeStats
	ProcessedBytes uint64
	ItemStats
}

type ChangeStats struct {
	New       uint
	Changed   uint
	Unchanged uint
}

// SnapshotOptions collect attributes for a new snapshot.
type SnapshotOptions struct {
	Tags           rest.TagList
	Hostname       string
	Excludes       []string
	BackupStart    time.Time
	Time           time.Time
	ParentSnapshot *rest.Snapshot
	ProgramVersion string
	// SkipIfUnchanged omits the snapshot creation if it is identical to the parent snapshot.
	SkipIfUnchanged bool
}

// Snapshot saves several targets and returns a snapshot.
func (arch *Archiver) Snapshot(ctx context.Context, targets []string, opts SnapshotOptions) (*rest.Snapshot, rest.ID, *Summary, error) {
	arch.summary = &Summary{}

	cleanTargets, err := resolveRelativeTargets(arch.FS, targets)
	if err != nil {
		return nil, rest.ID{}, nil, err
	}

	atree, err := NewTree(arch.FS, cleanTargets)
	if err != nil {
		return nil, rest.ID{}, nil, err
	}

	var rootTreeID rest.ID

	wgUp, wgUpCtx := errgroup.WithContext(ctx)
	arch.Repo.StartPackUploader(wgUpCtx, wgUp)

	wgUp.Go(func() error {
		wg, wgCtx := errgroup.WithContext(wgUpCtx)
		start := time.Now()

		wg.Go(func() error {
			arch.runWorkers(wgCtx, wg)

			log.Debugf("starting snapshot")
			fn, nodeCount, err := arch.saveTree(wgCtx, "/", atree, arch.loadParentTree(wgCtx, opts.ParentSnapshot), func(_ *rest.Node, is ItemStats) {
				arch.trackItem("/", nil, nil, is, time.Since(start))
			})
			if err != nil {
				return err
			}

			fnr := fn.take(wgCtx)
			if fnr.err != nil {
				return fnr.err
			}

			if wgCtx.Err() != nil {
				return wgCtx.Err()
			}

			if nodeCount == 0 {
				return errors.New("snapshot is empty")
			}

			rootTreeID = *fnr.node.Subtree
			arch.stopWorkers()
			return nil
		})

		err = wg.Wait()
		log.Debugf("err is %v", err)

		if err != nil {
			log.Debugf("error while saving tree: %v", err)
			return err
		}

		return arch.Repo.Flush(ctx)
	})
	err = wgUp.Wait()
	if err != nil {
		return nil, rest.ID{}, nil, err
	}

	if opts.ParentSnapshot != nil && opts.SkipIfUnchanged {
		ps := opts.ParentSnapshot
		if ps.Tree != nil && rootTreeID.Equal(*ps.Tree) {
			return nil, rest.ID{}, arch.summary, nil
		}
	}

	sn, err := rest.NewSnapshot(targets, opts.Tags, opts.Hostname, opts.Time)
	if err != nil {
		return nil, rest.ID{}, nil, err
	}

	sn.ProgramVersion = opts.ProgramVersion
	sn.Excludes = opts.Excludes
	if opts.ParentSnapshot != nil {
		sn.Parent = opts.ParentSnapshot.ID()
	}
	sn.Tree = &rootTreeID
	sn.Summary = &rest.SnapshotSummary{
		BackupStart: opts.BackupStart,
		BackupEnd:   time.Now(),

		FilesNew:            arch.summary.Files.New,
		FilesChanged:        arch.summary.Files.Changed,
		FilesUnmodified:     arch.summary.Files.Unchanged,
		DirsNew:             arch.summary.Dirs.New,
		DirsChanged:         arch.summary.Dirs.Changed,
		DirsUnmodified:      arch.summary.Dirs.Unchanged,
		DataBlobs:           arch.summary.ItemStats.DataBlobs,
		TreeBlobs:           arch.summary.ItemStats.TreeBlobs,
		DataAdded:           arch.summary.ItemStats.DataSize + arch.summary.ItemStats.TreeSize,
		DataAddedPacked:     arch.summary.ItemStats.DataSizeInRepo + arch.summary.ItemStats.TreeSizeInRepo,
		TotalFilesProcessed: arch.summary.Files.New + arch.summary.Files.Changed + arch.summary.Files.Unchanged,
		TotalBytesProcessed: arch.summary.ProcessedBytes,
	}

	id, err := rest.SaveSnapshot(ctx, arch.Repo, sn)
	if err != nil {
		return nil, rest.ID{}, nil, err
	}

	return sn, id, arch.summary, nil
}

// runWorkers starts the worker pools, which are stopped when the context is cancelled.
func (arch *Archiver) runWorkers(ctx context.Context, wg *errgroup.Group) {
	arch.blobSaver = NewBlobSaver(ctx, wg, arch.Repo, arch.Options.SaveBlobConcurrency)

	arch.fileSaver = NewFileSaver(ctx, wg,
		arch.blobSaver.Save,
		arch.Repo.Config().ChunkerPolynomial,
		arch.Options.ReadConcurrency, arch.Options.SaveBlobConcurrency)
	arch.fileSaver.CompleteBlob = arch.CompleteBlob
	arch.fileSaver.NodeFromFileInfo = arch.nodeFromFileInfo

	arch.treeSaver = NewTreeSaver(ctx, wg, arch.Options.SaveTreeConcurrency, arch.blobSaver.Save, arch.Error)
}

// join returns all elements separated with a forward slash.
func join(elem ...string) string {
	return path.Join(elem...)
}

// saveTree stores a Tree in the repo, returned is the tree. snPath is the path
// within the current snapshot.
func (arch *Archiver) saveTree(ctx context.Context, snPath string, atree *Tree, previous *rest.Tree, complete CompleteFunc) (FutureNode, int, error) {

	var node *rest.Node
	if snPath != "/" {
		if atree.FileInfoPath == "" {
			return FutureNode{}, 0, errors.Errorf("FileInfoPath for %v is empty", snPath)
		}

		fi, err := arch.statDir(atree.FileInfoPath)
		if err != nil {
			return FutureNode{}, 0, err
		}

		log.Debugf("%v, dir node data loaded from %v", snPath, atree.FileInfoPath)
		// in some cases reading xattrs for directories above the backup source is not allowed
		// thus ignore errors for such folders.
		node, err = arch.nodeFromFileInfo(snPath, atree.FileInfoPath, fi, true)
		if err != nil {
			return FutureNode{}, 0, err
		}
	} else {
		// fake root node
		node = &rest.Node{}
	}

	log.Debugf("%v (%v nodes), parent %v", snPath, len(atree.Nodes), previous)
	nodeNames := atree.NodeNames()
	nodes := make([]FutureNode, 0, len(nodeNames))

	// iterate over the nodes of atree in lexicographic (=deterministic) order
	for _, name := range nodeNames {
		subatree := atree.Nodes[name]

		// test if context has been cancelled
		if ctx.Err() != nil {
			return FutureNode{}, 0, ctx.Err()
		}

		// this is a leaf node
		if subatree.Leaf() {
			fn, excluded, err := arch.save(ctx, join(snPath, name), subatree.Path, previous.Find(name))

			if err != nil {
				err = arch.error(subatree.Path, err)
				if err == nil {
					// ignore error
					continue
				}
				return FutureNode{}, 0, err
			}

			if err != nil {
				return FutureNode{}, 0, err
			}

			if !excluded {
				nodes = append(nodes, fn)
			}
			continue
		}

		snItem := join(snPath, name) + "/"
		start := time.Now()

		oldNode := previous.Find(name)
		oldSubtree, err := arch.loadSubtree(ctx, oldNode)
		if err != nil {
			err = arch.error(join(snPath, name), err)
		}
		if err != nil {
			return FutureNode{}, 0, err
		}

		// not a leaf node, archive subtree
		fn, _, err := arch.saveTree(ctx, join(snPath, name), &subatree, oldSubtree, func(n *rest.Node, is ItemStats) {
			arch.trackItem(snItem, oldNode, n, is, time.Since(start))
		})
		if err != nil {
			return FutureNode{}, 0, err
		}
		nodes = append(nodes, fn)
	}

	fn := arch.treeSaver.Save(ctx, snPath, atree.FileInfoPath, node, nodes, complete)
	return fn, len(nodes), nil
}

// Flags for the ChangeIgnoreFlags bitfield.
const (
	ChangeIgnoreCtime = 1 << iota
	ChangeIgnoreInode
)

// fileChanged tries to detect whether a file's content has changed compared
// to the contents of node, which describes the same path in the parent backup.
// It should only be run for regular files.
func fileChanged(fi os.FileInfo, node *rest.Node, ignoreFlags uint) bool {
	switch {
	case node == nil:
		return true
	case node.Type != "file":
		// We're only called for regular files, so this is a type change.
		return true
	case uint64(fi.Size()) != node.Size:
		return true
	case !fi.ModTime().Equal(node.ModTime):
		return true
	}

	checkCtime := ignoreFlags&ChangeIgnoreCtime == 0
	checkInode := ignoreFlags&ChangeIgnoreInode == 0

	extFI := fs.ExtendedStat(fi)
	switch {
	case checkCtime && !extFI.ChangeTime.Equal(node.ChangeTime):
		return true
	case checkInode && node.Inode != extFI.Inode:
		return true
	}

	return false
}

// allBlobsPresent checks if all blobs (contents) of the given node are
// present in the index.
func (arch *Archiver) allBlobsPresent(previous *rest.Node) bool {
	// check if all blobs are contained in index
	for _, id := range previous.Content {
		if _, ok := arch.Repo.LookupBlobSize(rest.DataBlob, id); !ok {
			return false
		}
	}
	return true
}

// save saves a target (file or directory) to the repo. If the item is
// excluded, this function returns a nil node and error, with excluded set to
// true.
//
// Errors and completion needs to be handled by the caller.
//
// snPath is the path within the current snapshot.
func (arch *Archiver) save(ctx context.Context, snPath, target string, previous *rest.Node) (fn FutureNode, excluded bool, err error) {
	start := time.Now()

	log.Debugf("%v target %q, previous %v", snPath, target, previous)
	abstarget, err := arch.FS.Abs(target)
	if err != nil {
		return FutureNode{}, false, err
	}

	// exclude files by path before running Lstat to reduce number of lstat calls
	if !arch.SelectByName(abstarget) {
		log.Debugf("%v is excluded by path", target)
		return FutureNode{}, true, nil
	}

	// get file info and run remaining select functions that require file information
	fi, err := arch.FS.Lstat(target)
	if err != nil {
		log.Debugf("lstat() for %v returned error: %v", target, err)
		err = arch.error(abstarget, err)
		if err != nil {
			return FutureNode{}, false, errors.WithStack(err)
		}
		return FutureNode{}, true, nil
	}
	if !arch.Select(abstarget, fi) {
		log.Debugf("%v is excluded", target)
		return FutureNode{}, true, nil
	}

	switch {
	case fs.IsRegularFile(fi):
		log.Debugf("  %v regular file", target)

		// check if the file has not changed before performing a fopen operation (more expensive, specially
		// in network filesystems)
		if previous != nil && !fileChanged(fi, previous, arch.ChangeIgnoreFlags) {
			if arch.allBlobsPresent(previous) {
				log.Debugf("%v hasn't changed, using old list of blobs", target)
				arch.trackItem(snPath, previous, previous, ItemStats{}, time.Since(start))
				arch.CompleteBlob(previous.Size)
				node, err := arch.nodeFromFileInfo(snPath, target, fi, false)
				if err != nil {
					return FutureNode{}, false, err
				}

				// copy list of blobs
				node.Content = previous.Content

				fn = newFutureNodeWithResult(futureNodeResult{
					snPath: snPath,
					target: target,
					node:   node,
				})
				return fn, false, nil
			}

			log.Debugf("%v hasn't changed, but contents are missing!", target)
			// There are contents missing - inform user!
			err := errors.Errorf("parts of %v not found in the repository index; storing the file again", target)
			err = arch.error(abstarget, err)
			if err != nil {
				return FutureNode{}, false, err
			}
		}

		// reopen file and do an fstat() on the open file to check it is still
		// a file (and has not been exchanged for e.g. a symlink)
		file, err := arch.FS.OpenFile(target, fs.O_RDONLY|fs.O_NOFOLLOW, 0)
		if err != nil {
			log.Debugf("Openfile() for %v returned error: %v", target, err)
			err = arch.error(abstarget, err)
			if err != nil {
				return FutureNode{}, false, errors.WithStack(err)
			}
			return FutureNode{}, true, nil
		}

		fi, err = file.Stat()
		if err != nil {
			log.Debugf("stat() on opened file %v returned error: %v", target, err)
			_ = file.Close()
			err = arch.error(abstarget, err)
			if err != nil {
				return FutureNode{}, false, errors.WithStack(err)
			}
			return FutureNode{}, true, nil
		}

		// make sure it's still a file
		if !fs.IsRegularFile(fi) {
			err = errors.Errorf("file %v changed type, refusing to archive", fi.Name())
			_ = file.Close()
			err = arch.error(abstarget, err)
			if err != nil {
				return FutureNode{}, false, err
			}
			return FutureNode{}, true, nil
		}

		// Save will close the file, we don't need to do that
		fn = arch.fileSaver.Save(ctx, snPath, target, file, fi, func() {
			arch.StartFile(snPath)
		}, func() {
			arch.trackItem(snPath, nil, nil, ItemStats{}, 0)
		}, func(node *rest.Node, stats ItemStats) {
			arch.trackItem(snPath, previous, node, stats, time.Since(start))
		})

	case fi.IsDir():
		log.Debugf("  %v dir", target)

		snItem := snPath + "/"
		oldSubtree, err := arch.loadSubtree(ctx, previous)
		if err != nil {
			err = arch.error(abstarget, err)
		}
		if err != nil {
			return FutureNode{}, false, err
		}

		fn, err = arch.saveDir(ctx, snPath, target, fi, oldSubtree,
			func(node *rest.Node, stats ItemStats) {
				arch.trackItem(snItem, previous, node, stats, time.Since(start))
			})
		if err != nil {
			log.Debugf("SaveDir for %v returned error: %v", snPath, err)
			return FutureNode{}, false, err
		}

	case fi.Mode()&os.ModeSocket > 0:
		log.Debugf("  %v is a socket, ignoring", target)
		return FutureNode{}, true, nil

	default:
		log.Debugf("  %v other", target)

		node, err := arch.nodeFromFileInfo(snPath, target, fi, false)
		if err != nil {
			return FutureNode{}, false, err
		}
		fn = newFutureNodeWithResult(futureNodeResult{
			snPath: snPath,
			target: target,
			node:   node,
		})
	}

	log.Debugf("return after %.3f", time.Since(start).Seconds())

	return fn, false, nil
}

// loadSubtree tries to load the subtree referenced by node. In case of an error, nil is returned.
// If there is no node to load, then nil is returned without an error.
func (arch *Archiver) loadSubtree(ctx context.Context, node *rest.Node) (*rest.Tree, error) {
	if node == nil || node.Type != "dir" || node.Subtree == nil {
		return nil, nil
	}

	tree, err := rest.LoadTree(ctx, arch.Repo, *node.Subtree)
	if err != nil {
		log.Debugf("unable to load tree %v: %v", node.Subtree.Str(), err)
		// a tree in the repository is not readable -> warn the user
		return nil, arch.wrapLoadTreeError(*node.Subtree, err)
	}

	return tree, nil
}

// loadParentTree loads a tree referenced by snapshot id. If id is null, nil is returned.
func (arch *Archiver) loadParentTree(ctx context.Context, sn *rest.Snapshot) *rest.Tree {
	if sn == nil {
		return nil
	}

	if sn.Tree == nil {
		log.Debugf("snapshot %v has empty tree %v", *sn.ID())
		return nil
	}

	log.Debugf("load parent tree %v", *sn.Tree)
	tree, err := rest.LoadTree(ctx, arch.Repo, *sn.Tree)
	if err != nil {
		log.Debugf("unable to load tree %v: %v", *sn.Tree, err)
		_ = arch.error("/", arch.wrapLoadTreeError(*sn.Tree, err))
		return nil
	}
	return tree
}

func (arch *Archiver) trackItem(item string, previous, current *rest.Node, s ItemStats, d time.Duration) {
	arch.CompleteItem(item, previous, current, s, d)

	arch.mu.Lock()
	defer arch.mu.Unlock()

	arch.summary.ItemStats.Add(s)

	if current != nil {
		arch.summary.ProcessedBytes += current.Size
	} else {
		// last item or an error occurred
		return
	}

	switch current.Type {
	case "dir":
		switch {
		case previous == nil:
			arch.summary.Dirs.New++
		case previous.Equals(*current):
			arch.summary.Dirs.Unchanged++
		default:
			arch.summary.Dirs.Changed++
		}

	case "file":
		switch {
		case previous == nil:
			arch.summary.Files.New++
		case previous.Equals(*current):
			arch.summary.Files.Unchanged++
		default:
			arch.summary.Files.Changed++
		}
	}
}

func (fn *FutureNode) take(ctx context.Context) futureNodeResult {
	if fn.res != nil {
		res := fn.res
		// free result
		fn.res = nil
		return *res
	}
	select {
	case res, ok := <-fn.ch:
		if ok {
			// free channel
			fn.ch = nil
			return res
		}
	case <-ctx.Done():
		return futureNodeResult{err: ctx.Err()}
	}
	return futureNodeResult{err: errors.Errorf("no result")}
}

func (arch *Archiver) stopWorkers() {
	arch.blobSaver.TriggerShutdown()
	arch.fileSaver.TriggerShutdown()
	arch.treeSaver.TriggerShutdown()
	arch.blobSaver = nil
	arch.fileSaver = nil
	arch.treeSaver = nil
}

// nodeFromFileInfo returns the restic node from an os.FileInfo.
func (arch *Archiver) nodeFromFileInfo(snPath, filename string, fi os.FileInfo, ignoreXattrListError bool) (*rest.Node, error) {
	node, err := rest.NodeFromFileInfo(filename, fi, ignoreXattrListError)
	if !arch.WithAtime {
		node.AccessTime = node.ModTime
	}
	// if feature.Flag.Enabled(feature.DeviceIDForHardlinks) {
	// 	if node.Links == 1 || node.Type == "dir" {
	// 		// the DeviceID is only necessary for hardlinked files
	// 		// when using subvolumes or snapshots their deviceIDs tend to change which causes
	// 		// restic to upload new tree blobs
	// 		node.DeviceID = 0
	// 	}
	// }
	// overwrite name to match that within the snapshot
	node.Name = path.Base(snPath)
	if err != nil {
		err = fmt.Errorf("incomplete metadata for %v: %w", filename, err)
		return node, arch.error(filename, err)
	}
	return node, err
}

// statDir returns the file info for the directory. Symbolic links are
// resolved. If the target directory is not a directory, an error is returned.
func (arch *Archiver) statDir(dir string) (os.FileInfo, error) {
	fi, err := arch.FS.Stat(dir)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	tpe := fi.Mode() & (os.ModeType | os.ModeCharDevice)
	if tpe != os.ModeDir {
		return fi, errors.Errorf("path is not a directory: %v", dir)
	}

	return fi, nil
}

// save saves a target (file or directory) to the repo. If the item is
// excluded, this function returns a nil node and error, with excluded set to
// true.
//
// Errors and completion needs to be handled by the caller.
//
// snPath is the path within the current snapshot.
// func (arch *Archiver) save(ctx context.Context, snPath, target string, previous *rest.Node) (fn FutureNode, excluded bool, err error) {
// 	start := time.Now()

// 	log.Debugf("%v target %q, previous %v", snPath, target, previous)
// 	abstarget, err := arch.FS.Abs(target)
// 	if err != nil {
// 		return FutureNode{}, false, err
// 	}

// 	// exclude files by path before running Lstat to reduce number of lstat calls
// 	if !arch.SelectByName(abstarget) {
// 		log.Debugf("%v is excluded by path", target)
// 		return FutureNode{}, true, nil
// 	}

// 	// get file info and run remaining select functions that require file information
// 	fi, err := arch.FS.Lstat(target)
// 	if err != nil {
// 		log.Debugf("lstat() for %v returned error: %v", target, err)
// 		err = arch.error(abstarget, err)
// 		if err != nil {
// 			return FutureNode{}, false, errors.WithStack(err)
// 		}
// 		return FutureNode{}, true, nil
// 	}
// 	if !arch.Select(abstarget, fi) {
// 		log.Debugf("%v is excluded", target)
// 		return FutureNode{}, true, nil
// 	}

// 	switch {
// 	case fs.IsRegularFile(fi):
// 		log.Debugf("  %v regular file", target)

// 		// check if the file has not changed before performing a fopen operation (more expensive, specially
// 		// in network filesystems)
// 		if previous != nil && !fileChanged(fi, previous, arch.ChangeIgnoreFlags) {
// 			if arch.allBlobsPresent(previous) {
// 				log.Debugf("%v hasn't changed, using old list of blobs", target)
// 				arch.trackItem(snPath, previous, previous, ItemStats{}, time.Since(start))
// 				arch.CompleteBlob(previous.Size)
// 				node, err := arch.nodeFromFileInfo(snPath, target, fi, false)
// 				if err != nil {
// 					return FutureNode{}, false, err
// 				}

// 				// copy list of blobs
// 				node.Content = previous.Content

// 				fn = newFutureNodeWithResult(futureNodeResult{
// 					snPath: snPath,
// 					target: target,
// 					node:   node,
// 				})
// 				return fn, false, nil
// 			}

// 			log.Debugf("%v hasn't changed, but contents are missing!", target)
// 			// There are contents missing - inform user!
// 			err := errors.Errorf("parts of %v not found in the repository index; storing the file again", target)
// 			err = arch.error(abstarget, err)
// 			if err != nil {
// 				return FutureNode{}, false, err
// 			}
// 		}

// 		// reopen file and do an fstat() on the open file to check it is still
// 		// a file (and has not been exchanged for e.g. a symlink)
// 		file, err := arch.FS.OpenFile(target, fs.O_RDONLY|fs.O_NOFOLLOW, 0)
// 		if err != nil {
// 			log.Debugf("Openfile() for %v returned error: %v", target, err)
// 			err = arch.error(abstarget, err)
// 			if err != nil {
// 				return FutureNode{}, false, errors.WithStack(err)
// 			}
// 			return FutureNode{}, true, nil
// 		}

// 		fi, err = file.Stat()
// 		if err != nil {
// 			log.Debugf("stat() on opened file %v returned error: %v", target, err)
// 			_ = file.Close()
// 			err = arch.error(abstarget, err)
// 			if err != nil {
// 				return FutureNode{}, false, errors.WithStack(err)
// 			}
// 			return FutureNode{}, true, nil
// 		}

// 		// make sure it's still a file
// 		if !fs.IsRegularFile(fi) {
// 			err = errors.Errorf("file %v changed type, refusing to archive", fi.Name())
// 			_ = file.Close()
// 			err = arch.error(abstarget, err)
// 			if err != nil {
// 				return FutureNode{}, false, err
// 			}
// 			return FutureNode{}, true, nil
// 		}

// 		// Save will close the file, we don't need to do that
// 		fn = arch.fileSaver.Save(ctx, snPath, target, file, fi, func() {
// 			arch.StartFile(snPath)
// 		}, func() {
// 			arch.trackItem(snPath, nil, nil, ItemStats{}, 0)
// 		}, func(node *rest.Node, stats ItemStats) {
// 			arch.trackItem(snPath, previous, node, stats, time.Since(start))
// 		})

// 	case fi.IsDir():
// 		log.Debugf("  %v dir", target)

// 		snItem := snPath + "/"
// 		oldSubtree, err := arch.loadSubtree(ctx, previous)
// 		if err != nil {
// 			err = arch.error(abstarget, err)
// 		}
// 		if err != nil {
// 			return FutureNode{}, false, err
// 		}

// 		fn, err = arch.saveDir(ctx, snPath, target, fi, oldSubtree,
// 			func(node *rest.Node, stats ItemStats) {
// 				arch.trackItem(snItem, previous, node, stats, time.Since(start))
// 			})
// 		if err != nil {
// 			log.Debugf("SaveDir for %v returned error: %v", snPath, err)
// 			return FutureNode{}, false, err
// 		}

// 	case fi.Mode()&os.ModeSocket > 0:
// 		log.Debugf("  %v is a socket, ignoring", target)
// 		return FutureNode{}, true, nil

// 	default:
// 		log.Debugf("  %v other", target)

// 		node, err := arch.nodeFromFileInfo(snPath, target, fi, false)
// 		if err != nil {
// 			return FutureNode{}, false, err
// 		}
// 		fn = newFutureNodeWithResult(futureNodeResult{
// 			snPath: snPath,
// 			target: target,
// 			node:   node,
// 		})
// 	}

// 	log.Debugf("return after %.3f", time.Since(start).Seconds())

// 	return fn, false, nil
// }

// error calls arch.Error if it is set and the error is different from context.Canceled.
func (arch *Archiver) error(item string, err error) error {
	if arch.Error == nil || err == nil {
		return err
	}

	if err == context.Canceled {
		return err
	}

	// not all errors include the filepath, thus add it if it is missing
	if !strings.Contains(err.Error(), item) {
		err = fmt.Errorf("%v: %w", item, err)
	}

	errf := arch.Error(item, err)
	if err != errf {
		log.Debugf("item %v: error was filtered by handler, before: %q, after: %v", item, err, errf)
	}
	return errf
}

func newFutureNodeWithResult(res futureNodeResult) FutureNode {
	return FutureNode{
		res: &res,
	}
}

// saveDir stores a directory in the repo and returns the node. snPath is the
// path within the current snapshot.
func (arch *Archiver) saveDir(ctx context.Context, snPath string, dir string, fi os.FileInfo, previous *rest.Tree, complete CompleteFunc) (d FutureNode, err error) {
	log.Debugf("%v %v", snPath, dir)

	treeNode, err := arch.nodeFromFileInfo(snPath, dir, fi, false)
	if err != nil {
		return FutureNode{}, err
	}

	names, err := fs.Readdirnames(arch.FS, dir, fs.O_NOFOLLOW)
	if err != nil {
		return FutureNode{}, err
	}
	sort.Strings(names)

	nodes := make([]FutureNode, 0, len(names))

	for _, name := range names {
		// test if context has been cancelled
		if ctx.Err() != nil {
			log.Debugf("context has been cancelled, aborting")
			return FutureNode{}, ctx.Err()
		}

		pathname := arch.FS.Join(dir, name)
		oldNode := previous.Find(name)
		snItem := join(snPath, name)
		fn, excluded, err := arch.save(ctx, snItem, pathname, oldNode)

		// return error early if possible
		if err != nil {
			err = arch.error(pathname, err)
			if err == nil {
				// ignore error
				continue
			}

			return FutureNode{}, err
		}

		if excluded {
			continue
		}

		nodes = append(nodes, fn)
	}

	fn := arch.treeSaver.Save(ctx, snPath, dir, treeNode, nodes, complete)

	return fn, nil
}

func (arch *Archiver) wrapLoadTreeError(id rest.ID, err error) error {
	if _, ok := arch.Repo.LookupBlobSize(rest.TreeBlob, id); ok {
		err = errors.Errorf("tree %v could not be loaded; the repository could be damaged: %v", id, err)
	} else {
		err = errors.Errorf("tree %v is not known; the repository could be damaged, run `repair index` to try to repair it", id)
	}
	return err
}

// Add adds other to the current ItemStats.
func (s *ItemStats) Add(other ItemStats) {
	s.DataBlobs += other.DataBlobs
	s.DataSize += other.DataSize
	s.DataSizeInRepo += other.DataSizeInRepo
	s.TreeBlobs += other.TreeBlobs
	s.TreeSize += other.TreeSize
	s.TreeSizeInRepo += other.TreeSizeInRepo
}
