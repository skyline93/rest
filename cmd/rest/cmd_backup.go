package main

import (
	"context"
	"fmt"
	"time"

	"github.com/skyline93/rest/internal/backend/local"
	"github.com/skyline93/rest/internal/repository"
	"github.com/skyline93/rest/internal/rest"
	"github.com/spf13/cobra"
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
		if err := backup(cmd.Context(), backupOptions.Uri, backupOptions.Password, backupOptions.Path); err != nil {
			panic(err)
		}
	},
}

// BackupOptions bundles all options for the init command.
type BackupOptions struct {
	Uri      string
	Password string
	Path     string
}

var backupOptions BackupOptions

func init() {
	cmdRoot.AddCommand(cmdBackup)

	f := cmdBackup.Flags()
	f.StringVar(&backupOptions.Uri, "uri", "", "repository uri, example: 'local:/repo'")
	f.StringVar(&backupOptions.Password, "password", "", "repository password")
	f.StringVar(&backupOptions.Path, "path", "", "source path")

}

func backup(ctx context.Context, uri string, password string, path string) error {
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

	snap, err := rest.NewSnapshot([]string{path}, []string{"tag1"}, "e7bd5837531a", time.Now())
	if err != nil {
		return err
	}
	fmt.Printf("snap: %v\n", snap)

	id, err := rest.SaveSnapshot(ctx, repo, snap)
	if err != nil {
		return err
	}

	fmt.Printf("snapshot id: %v\n", id)
	return nil
}
