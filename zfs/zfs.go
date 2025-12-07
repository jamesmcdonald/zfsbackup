package zfs

import (
	"bytes"
	"fmt"
	"log/slog"
	"os/exec"
	"strings"
	"time"
)

type Backup struct {
	dryrun bool
	target string
}

func NewBackup(target string) *Backup {
	return &Backup{
		target: target,
	}
}

func (b *Backup) runCommand(allowdryrun bool, name string, args ...string) ([]string, string, error) {
	if b.dryrun && !allowdryrun {
		slog.Info("Skipping command due to dry run", "command", name, "args", args)
		return []string{}, "", nil
	}
	c := exec.Command(name, args...)

	var stdoutBuf, stderrBuf bytes.Buffer
	c.Stdout = &stdoutBuf
	c.Stderr = &stderrBuf

	err := c.Run()

	stdoutLines := []string{}
	if stdoutBuf.Len() > 0 {
		stdoutLines = strings.Split(strings.TrimRight(stdoutBuf.String(), "\n"), "\n")
	}

	if err == nil {
		return stdoutLines, "", nil
	}

	stderrStr := strings.TrimSpace(stderrBuf.String())

	if stderrStr == "" {
		stderrStr = err.Error()
	}

	return stdoutLines, stderrStr, err
}

func (b *Backup) listSnapshots(vol string) ([]string, error) {
	snaps, stderr, err := b.runCommand(true, "zfs", "list", "-H", "-o", "name", "-t", "snapshot", "-s", "creation", vol)
	if err != nil {
		return nil, fmt.Errorf("error listing snapshots: %s: %v", stderr, err)
	}
	return snaps, nil
}

func (b *Backup) getLatestSnapshot(vol string) (string, error) {
	snaps, err := b.listSnapshots(vol)
	if err != nil {
		return "", err
	}
	if len(snaps) < 1 {
		return "", fmt.Errorf("no snapshots found")
	}
	return snaps[len(snaps)-1], nil
}

func (b *Backup) snapshotExists(vol string, snapshot string) bool {
	snapshotName := fmt.Sprintf("%s@%s", vol, snapshot)
	_, _, err := b.runCommand(true, "zfs", "list", "-H", "-t", "snapshot", snapshotName)
	if err != nil {
		return false
	}
	return true
}

func (b *Backup) createSnapshot(vol string) (string, error) {
	now := time.Now()
	snap := now.Format("2006-01-02T15:04:05")
	fmt.Printf("new snapshot %s\n", snap)
	return snap, nil
}

func (b *Backup) IncrementalBackup(vol string) error {
	snap, err := b.getLatestSnapshot(vol)
	if err != nil {
		return err
	}
	parts := strings.Split(snap, "@")
	snapName := parts[1]
	targetVolume := fmt.Sprintf("%s/%s", b.target, vol)
	if !b.snapshotExists(targetVolume, snapName) {
		return fmt.Errorf("snapshot %s of volume %s not found", snapName, targetVolume)
	}
	fmt.Printf("Incremental backup starting from snapshot %s\n", snap)

	// Create new snapshot
	b.createSnapshot(vol)
	// Run backup
	// Clean up old snapshots on source
	// Clean up old snapshots on target
	return nil
}
