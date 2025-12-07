package main

import (
	"github.com/jamesmcdonald/zfsbackup/zfs"
)

func main() {
	b := zfs.NewBackup("backup")
	err := b.IncrementalBackup("chonk")
	if err != nil {
		panic(err)
	}
}
