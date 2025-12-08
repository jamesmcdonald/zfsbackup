package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/jamesmcdonald/zfsbackup/zfs"
)

func main() {
	slog.SetLogLoggerLevel(slog.LevelDebug)
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "usage: %s <volume>\n", os.Args[0])
		os.Exit(1)
	}
	vol := os.Args[1]
	b := zfs.NewBackup("backup")
	err := b.IncrementalBackup(vol)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error during backup:", err)
	}
}
