package cmd

import (
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/jamesmcdonald/zfsbackup/zfs"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "zfsbackup [flags] <source> [<source>...]",
	Short: "Back up ZFS filesystems",
	Long:  `Back up ZFS filesystems incrementally to target ZFS filesystems.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return fmt.Errorf("no source filesystems provided")
		}
		targetfs, _ := cmd.Flags().GetString("target-fs")
		dryrun, _ := cmd.Flags().GetBool("dry-run")
		debug, _ := cmd.Flags().GetBool("debug")
		sourceCmdStr, _ := cmd.Flags().GetString("source-command")
		targetCmdStr, _ := cmd.Flags().GetString("target-command")
		sourceCmd := strings.Fields(sourceCmdStr)
		targetCmd := strings.Fields(targetCmdStr)

		if debug {
			slog.SetLogLoggerLevel(slog.LevelDebug)
		}

		var sources []zfs.Source
		for _, arg := range args {
			src, err := zfs.ParseSource(arg)
			if err != nil {
				return fmt.Errorf("invalid source %q: %w", arg, err)
			}
			sources = append(sources, src)
		}

		fmt.Printf("Backing up to %s:\n", targetfs)
		for _, src := range sources {
			fmt.Printf("  %s\n", src)
		}

		var opts []zfs.BackupOption
		if dryrun {
			opts = append(opts, zfs.WithDryRunOption())
		}
		if len(sourceCmd) > 0 {
			opts = append(opts, zfs.WithSourceCommandOption(sourceCmd))
		}
		if len(targetCmd) > 0 {
			opts = append(opts, zfs.WithTargetCommandOption(targetCmd))
		}

		b, err := zfs.NewBackup(targetfs, opts...)
		if err != nil {
			return err
		}
		return b.RunBackup(sources)
	},
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.Flags().StringP("target-fs", "t", "backup", "Target filesystem")
	rootCmd.Flags().BoolP("dry-run", "n", false, "Perform a trial run with no changes made")
	rootCmd.Flags().BoolP("debug", "d", false, "Enable debug output")
	rootCmd.Flags().StringP("source-command", "S", "zfs", "Source ZFS command")
	rootCmd.Flags().StringP("target-command", "T", "zfs", "Target ZFS command")
}
