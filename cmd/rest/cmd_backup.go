package main

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/skyline93/rest/internal/archiver"
	"github.com/skyline93/rest/internal/backend/local"
	"github.com/skyline93/rest/internal/fs"
	"github.com/skyline93/rest/internal/repository"
	"github.com/skyline93/rest/internal/rest"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

var cmdBackup = &cobra.Command{
	Use:   "backup [flags] [FILE/DIR] ...",
	Short: "Create a new backup of files and/or directories",
	Long: `
The "backup" command creates a new snapshot and saves the files and directories
given as the arguments.

EXIT STATUS
===========

Exit status is 0 if the command was successful.
Exit status is 1 if there was a fatal error (no snapshot created).
Exit status is 3 if some source data could not be read (incomplete snapshot created).
`,
	Run: func(cmd *cobra.Command, args []string) {
		if err := RunBackup(cmd.Context(), backupOptions.Uri, backupOptions.Password, backupOptions.Path); err != nil {
			panic(err)
		}
	},
}

// BackupOptions bundles all options for the init command.
type BackupOptions struct {
	Uri             string
	Password        string
	Path            string
	ReadConcurrency uint
}

var backupOptions BackupOptions

func init() {
	cmdRoot.AddCommand(cmdBackup)

	f := cmdBackup.Flags()
	f.StringVar(&backupOptions.Uri, "uri", "", "repository uri, example: 'local:/repo'")
	f.StringVar(&backupOptions.Password, "password", "", "repository password")
	f.StringVar(&backupOptions.Path, "path", "", "source path")
	f.UintVar(&backupOptions.ReadConcurrency, "read-concurrency", 0, "read `n` files concurrently (default: $RESTIC_READ_CONCURRENCY or 2)")
}

// parent returns the ID of the parent snapshot. If there is none, nil is
// returned.
func findParentSnapshot(ctx context.Context, repo rest.ListerLoaderUnpacked, targets []string, timeStampLimit time.Time) (*rest.Snapshot, error) {
	snName := ""
	if snName == "" {
		snName = "latest"
	}
	f := rest.SnapshotFilter{TimestampLimit: timeStampLimit}
	// if opts.GroupBy.Host {
	// 	f.Hosts = []string{opts.Host}
	// }
	// if opts.GroupBy.Path {
	// 	f.Paths = targets
	// }
	// if opts.GroupBy.Tag {
	// 	f.Tags = []rest.TagList{opts.Tags.Flatten()}
	// }

	sn, _, err := f.FindLatest(ctx, repo, repo, snName)
	// Snapshot not found is ok if no explicit parent was set
	if errors.Is(err, rest.ErrNoSnapshotFound) {
		err = nil
	}
	return sn, err
}

func RunBackup(ctx context.Context, uri string, password string, path string) error {
	var targets []string = []string{
		path,
	}

	localCfg, err := local.ParseConfig(uri)
	if err != nil {
		return err
	}

	be, err := local.Open(ctx, *localCfg)
	if err != nil {
		return err
	}

	repo, err := repository.New(be, repository.Options{})
	if err != nil {
		return err
	}

	const maxKeys = 20
	keyHint := ""
	err = repo.SearchKey(ctx, password, maxKeys, keyHint)
	if err != nil {
		return err
	}

	var targetFS fs.FS = fs.Local{}

	wg, wgCtx := errgroup.WithContext(ctx)
	cancelCtx, cancel := context.WithCancel(wgCtx)
	defer cancel()

	sc := archiver.NewScanner(targetFS)
	sc.Error = HandleScanError
	sc.Result = HandleScanResult

	wg.Go(func() error { return sc.Scan(cancelCtx, targets) })

	arch := archiver.New(repo, targetFS, archiver.Options{ReadConcurrency: backupOptions.ReadConcurrency})

	// snapshotOpts := archiver.SnapshotOptions{}

	timeStamp := time.Now()
	backupStart := timeStamp

	var parentSnapshot *rest.Snapshot

	parentSnapshot, err = findParentSnapshot(ctx, repo, targets, timeStamp)
	if err != nil {
		return err
	}

	snapshotOpts := archiver.SnapshotOptions{
		// Excludes:        opts.Excludes,
		// Tags:            opts.Tags.Flatten(),
		BackupStart: backupStart,
		Time:        timeStamp,
		// Hostname:        opts.Host,
		ParentSnapshot:  parentSnapshot,
		ProgramVersion:  "restic " + version,
		SkipIfUnchanged: true,
	}

	_, id, _, err := arch.Snapshot(ctx, targets, snapshotOpts)
	if err != nil {
		return err
	}

	log.Printf("snapshot_id: %s", id)

	// cleanly shutdown all running goroutines
	cancel()

	// let's see if one returned an error
	werr := wg.Wait()

	return werr
}

func HandleScanError(file string, err error) error {
	if err != nil {
		log.Printf("scan error, file: %s, msg: %s", file, err)
		return err
	}
	return nil
}

func HandleScanResult(item string, s archiver.ScanStats) {
	log.Printf("scan completed, item: %s, bytes: %d", item, s)
}
