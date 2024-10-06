package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var version = "0.17.1"

// cmdRoot is the base command when no other command has been specified.
var cmdRoot = &cobra.Command{
	Use:   "rest",
	Short: "Backup and restore files",
	Long: `
rest is a backup program which allows saving multiple revisions of files and
directories in an encrypted repository stored on different backends.

The full documentation can be found at https://restic.readthedocs.io/ .
`,
	SilenceErrors:     true,
	SilenceUsage:      true,
	DisableAutoGenTag: true,

	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
		os.Exit(0)
	},
}

func main() {
	if err := cmdRoot.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
