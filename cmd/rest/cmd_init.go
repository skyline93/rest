package main

import (
	"context"

	"github.com/skyline93/rest/internal/backend/local"
	"github.com/skyline93/rest/internal/repository"
	"github.com/spf13/cobra"
)

var cmdInit = &cobra.Command{
	Use:   "init",
	Short: "Initialize a new repository",
	Long: `
The "init" command initializes a new repository.

EXIT STATUS
===========

Exit status is 0 if the command was successful, and non-zero if there was any error.
`,
	DisableAutoGenTag: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runInit(cmd.Context(), initOptions.Uri, initOptions.Password)
	},
}

// InitOptions bundles all options for the init command.
type InitOptions struct {
	Uri      string
	Password string
}

var initOptions InitOptions

func init() {
	cmdRoot.AddCommand(cmdInit)

	f := cmdInit.Flags()
	f.StringVar(&initOptions.Uri, "uri", "", "repository uri, example: 'local:/repo'")
	f.StringVar(&initOptions.Password, "password", "", "repository password")
}

func runInit(ctx context.Context, uri string, password string) error {
	localCfg, err := local.ParseConfig(uri)
	if err != nil {
		return err
	}

	be, err := local.Create(ctx, *localCfg)
	if err != nil {
		return err
	}

	s, err := repository.New(be, repository.Options{})
	if err != nil {
		return err
	}

	version := uint(1)
	err = s.Init(ctx, version, password, nil)
	if err != nil {
		return err
	}

	return nil
}

