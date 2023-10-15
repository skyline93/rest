package main

import (
	"context"
	"log"

	"github.com/skyline93/rest/internal/archiver"
	"github.com/skyline93/rest/internal/backend/local"
	"github.com/skyline93/rest/internal/fs"
	"github.com/skyline93/rest/internal/repository"
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

	sc := archiver.NewScanner(targetFS)
	sc.Error = HandleScanError
	sc.Result = HandleScanResult

	wg, wgCtx := errgroup.WithContext(ctx)
	cancelCtx, cancel := context.WithCancel(wgCtx)
	defer cancel()

	wg.Go(func() error { return sc.Scan(cancelCtx, targets) })

	arch := archiver.New(repo, targetFS, archiver.Options{ReadConcurrency: backupOptions.ReadConcurrency})

	snapshotOpts := archiver.SnapshotOptions{}

	_, id, err := arch.Snapshot(ctx, targets, snapshotOpts)
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
