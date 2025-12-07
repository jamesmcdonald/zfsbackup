package main

import (
	"fmt"
	"os"

	"github.com/jamesmcdonald/zfsbackup/zfs"
)

func main() {
	b := zfs.NewBackup("backup")
	err := b.IncrementalBackup("chonk")
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error during backup:", err)
	}
}
