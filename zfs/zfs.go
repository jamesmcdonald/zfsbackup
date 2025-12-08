package zfs

import (
	"bytes"
	"fmt"
	"log/slog"
	"os/exec"
	"slices"
	"strings"
	"time"
)

type Backup struct {
	dryrun bool
	target string
	debug  bool
}

func NewBackup(target string) *Backup {
	return &Backup{
		target: target,
		debug:  true,
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

func (b *Backup) getLatestMatchingSnapshot(source, target string) (string, error) {
	sourceSnaps, err := b.listSnapshots(source)
	if err != nil {
		return "", err
	}
	targetSnaps, err := b.listSnapshots(target)
	if err != nil {
		return "", err
	}

	for i := len(sourceSnaps) - 1; i >= 0; i-- {
		at := strings.Index(sourceSnaps[i], "@")
		if at < 0 {
			continue
		}
		snappart := sourceSnaps[i][at:]
		targetseek := fmt.Sprintf("%s%s", target, snappart)
		if slices.Contains(targetSnaps, targetseek) {
			return sourceSnaps[i], nil
		}
	}
	return "", fmt.Errorf("no matching snapshot found")
}

func (b *Backup) snapshotExists(vol string, snapshot string) bool {
	snapshotName := fmt.Sprintf("%s@%s", vol, snapshot)
	_, _, err := b.runCommand(true, "zfs", "list", "-H", "-t", "snapshot", snapshotName)
	if err != nil {
		return false
	}
	return true
}

func (b *Backup) createSnapshot(vol string, recurse bool) (string, error) {
	now := time.Now()
	snapName := now.Format("2006-01-02T15:04:05")
	slog.Info("creating snapshot", "vol", vol, "snapshot", snapName, "recurse", recurse)
	snap := fmt.Sprintf("%s@%s", vol, snapName)
	args := []string{"zfs", "snapshot"}
	if recurse {
		args = append(args, "-r")
	}
	args = append(args, snap)
	_, stderr, err := b.runCommand(false, args[0], args[1:]...)
	if err != nil {
		return "", fmt.Errorf("error creating snapshot: %s: %v", stderr, err)
	}

	return snap, nil
}

func (b *Backup) runBackup(vol, startSnap, endSnap string) error {
	slog.Info("backup starting", "vol", vol, "start", startSnap, "end", endSnap)
	sendArgs := []string{"zfs", "send", "-R", "-i", startSnap, endSnap}
	receiveArgs := []string{"zfs", "receive", "-F", fmt.Sprintf("%s/%s", b.target, vol)}
	cmdSend := exec.Command(sendArgs[0], sendArgs[1:]...)
	cmdReceive := exec.Command(receiveArgs[0], receiveArgs[1:]...)
	sendOut, err := cmdSend.StdoutPipe()
	if err != nil {
		return fmt.Errorf("error setting up pipe: %w", err)
	}
	cmdReceive.Stdin = sendOut

	if err := cmdSend.Start(); err != nil {
		return fmt.Errorf("error starting zfs send: %w", err)
	}

	output, err := cmdReceive.CombinedOutput()
	if err != nil {
		return fmt.Errorf("error during zfs receive: %s: %w", string(output), err)
	}

	slog.Info("backup complete", "vol", vol, "start", startSnap, "end", endSnap)
	return nil

}

func (b *Backup) deleteSnapshot(snap string, recurse bool) error {
	args := []string{"zfs", "destroy", snap}
	if recurse {
		args = append(args, "-r")
	}
	slog.Info("deleting snapshot", "snap", snap)
	_, stderr, err := b.runCommand(false, args[0], args[1:]...)
	if err != nil {
		return fmt.Errorf("error deleting snapshot: %w: %s", err, stderr)
	}
	return nil
}

func isBackupSnapshot(snapshotName string) bool {
	parts := strings.Split(snapshotName, "@")
	if len(parts) != 2 {
		return false
	}

	timestampPart := parts[1]

	const layout = "2006-01-02T15:04:05"
	_, err := time.Parse(layout, timestampPart)

	return err == nil
}

func (b *Backup) cleanSnapshots(vol string, retain int) error {
	snaps, err := b.listSnapshots(vol)
	if err != nil {
		return err
	}
	slog.Info("cleaning snapshots", "vol", vol, "retain", retain, "snaps", len(snaps))
	if retain < 1 {
		slog.Warn("retain too low, retaining 1 snap", "retain", retain)
		retain = 1
	}
	if len(snaps) <= retain {
		slog.Debug("not cleaning snaps", "snaps", len(snaps), "retain", retain)
		return nil
	}
	saved := 0
	for i := len(snaps) - 1; i >= 0; i-- {
		snap := snaps[i]
		if !isBackupSnapshot(snap) {
			slog.Debug("skipping non-backup snapshot", "snap", snap)
			continue
		}
		if saved < retain {
			slog.Debug("retaining snapshot", "snap", snap)
			saved++
			continue
		}
		err := b.deleteSnapshot(snap, true)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *Backup) IncrementalBackup(vol string) error {
	snap, err := b.getLatestMatchingSnapshot(vol, fmt.Sprintf("%s/%s", b.target, vol))
	if err != nil {
		return err
	}
	parts := strings.Split(snap, "@")
	snapName := parts[1]
	targetVolume := fmt.Sprintf("%s/%s", b.target, vol)
	if !b.snapshotExists(targetVolume, snapName) {
		return fmt.Errorf("snapshot %s of volume %s not found", snapName, targetVolume)
	}
	slog.Info("incremental backup start", "snap", snap)

	// Create new snapshot
	newsnap, err := b.createSnapshot(vol, true)
	if err != nil {
		return err
	}
	// Run backup
	err = b.runBackup(vol, snap, newsnap)
	if err != nil {
		return err
	}
	// Clean up old snapshots on source
	err = b.cleanSnapshots(vol, 2)
	// Clean up old snapshots on target
	// err = b.cleanSnapshots(targetVolume, 2)
	return nil
}
