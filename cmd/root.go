package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"strings"

	"golang.org/x/term"

	"github.com/jamesmcdonald/zfsbackup/progress"
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
		workers, _ := cmd.Flags().GetInt("workers")
		sourceCmdStr, _ := cmd.Flags().GetString("source-command")
		targetCmdStr, _ := cmd.Flags().GetString("target-command")
		sourceCmd := strings.Fields(sourceCmdStr)
		targetCmd := strings.Fields(targetCmdStr)

		level := slog.LevelInfo
		if debug {
			level = slog.LevelDebug
		}

		var opts []zfs.BackupOption
		var cancelRenderer context.CancelFunc

		if term.IsTerminal(int(os.Stderr.Fd())) {
			r := progress.NewRenderer(level)
			ctx, cancel := context.WithCancel(context.Background())
			cancelRenderer = cancel
			go r.Run(ctx)
			opts = append(opts, zfs.WithLogger(slog.New(r.LogHandler())))
			opts = append(opts, zfs.WithProgressFactory(func(label string, size int64) zfs.Progresser {
				bar := progress.NewBar(label, size)
				r.AddBar(bar)
				return bar
			}))
		} else {
			handler := slog.NewTextHandler(cmd.ErrOrStderr(), &slog.HandlerOptions{Level: level})
			opts = append(opts, zfs.WithLogger(slog.New(handler)))
		}

		if dryrun {
			opts = append(opts, zfs.WithDryRunOption())
		}
		if workers > 0 {
			opts = append(opts, zfs.WithWorkers(workers))
		}
		if len(sourceCmd) > 0 {
			opts = append(opts, zfs.WithSourceCommandOption(sourceCmd))
		}
		if len(targetCmd) > 0 {
			opts = append(opts, zfs.WithTargetCommandOption(targetCmd))
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

		b, err := zfs.NewBackup(targetfs, opts...)
		if err != nil {
			return err
		}
		runErr := b.RunBackup(sources)
		if cancelRenderer != nil {
			cancelRenderer()
		}
		return runErr
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
	rootCmd.Flags().IntP("workers", "w", 0, fmt.Sprintf("Number of parallel backup workers (default: 2 * CPU cores = %d)", runtime.NumCPU()*2))
	rootCmd.Flags().StringP("source-command", "S", "zfs", "Source ZFS command")
	rootCmd.Flags().StringP("target-command", "T", "zfs", "Target ZFS command")
}
