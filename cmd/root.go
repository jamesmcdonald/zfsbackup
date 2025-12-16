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
	Use:   "zfsbackup <source-fs>",
	Short: "Back up ZFS filesystems",
	Long:  `Back up ZFS filesystems incrementally to target ZFS filesystems.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return fmt.Errorf("no source filesystems provided")
		}
		sourcefs := args[0]
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
		fmt.Printf("Backing up %s to %s\n", sourcefs, targetfs)
		var opts []zfs.BackupOption
		if debug {
			opts = append(opts, zfs.WithDebugOption())
		}
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
		err = b.IncrementalBackup(sourcefs)
		return err
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
