package zfs

import (
	"bytes"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jamesmcdonald/zfsbackup/util"
)

type Backup struct {
	target    string
	dryrun    bool
	debug     bool
	sourceCmd []string
	targetCmd []string
}

type BackupOption func(*Backup) error

func WithDryRunOption() BackupOption {
	return func(b *Backup) error {
		b.dryrun = true
		return nil
	}
}

func WithDebugOption() BackupOption {
	return func(b *Backup) error {
		b.debug = true
		return nil
	}
}

func WithSourceCommandOption(cmd []string) BackupOption {
	return func(b *Backup) error {
		b.sourceCmd = cmd
		return nil
	}
}

func WithTargetCommandOption(cmd []string) BackupOption {
	return func(b *Backup) error {
		b.targetCmd = cmd
		return nil
	}
}

func NewBackup(target string, opts ...BackupOption) (*Backup, error) {
	if target == "" {
		return nil, fmt.Errorf("target filesystem cannot be empty")
	}
	b := &Backup{
		target:    target,
		sourceCmd: []string{"zfs"},
		targetCmd: []string{"zfs"},
	}
	for _, opt := range opts {
		err := opt(b)
		if err != nil {
			return nil, fmt.Errorf("error applying option: %w", err)
		}
	}

	// Validate command configurations
	if len(b.sourceCmd) == 0 {
		return nil, fmt.Errorf("source command cannot be empty")
	}
	if len(b.targetCmd) == 0 {
		return nil, fmt.Errorf("target command cannot be empty")
	}

	return b, nil
}

func (b *Backup) isTargetVolume(vol string) bool {
	target := strings.TrimSuffix(b.target, "/")
	return strings.HasPrefix(vol, target+"/")
}

func (b *Backup) buildCommand(isTarget bool, args ...string) []string {
	var base []string
	if isTarget {
		base = slices.Clone(b.targetCmd)
	} else {
		base = slices.Clone(b.sourceCmd)
	}
	return append(base, args...)
}

func (b *Backup) wrapCmdError(operation string, stderr string, err error) error {
	if stderr != "" {
		return fmt.Errorf("error %s: %s: %w", operation, stderr, err)
	}
	return fmt.Errorf("error %s: %w", operation, err)
}

func splitSnapshot(fullName string) (vol, snap string) {
	parts := strings.Split(fullName, "@")
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return fullName, ""
}

func (b *Backup) runCommand(args ...string) ([]string, string, error) {
	if b.dryrun {
		slog.Info("Skipping command due to dry run", "args", args)
		return []string{}, "", nil
	}
	return b.runPipeline([][]string{args})
}

func (b *Backup) runPipeline(allCmds [][]string) ([]string, string, error) {
	if b.dryrun {
		slog.Info("Skipping pipeline due to dry run", "cmds", allCmds)
		return []string{}, "", nil
	}

	// If only one command, use simple execution
	if len(allCmds) == 1 {
		c := exec.Command(allCmds[0][0], allCmds[0][1:]...)
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

	// Pipeline execution with arbitrary number of commands
	if len(allCmds) < 2 {
		return nil, "", fmt.Errorf("pipeline needs at least 2 commands")
	}

	// Build command chain
	var cmds []*exec.Cmd
	for _, cmdArgs := range allCmds {
		if len(cmdArgs) == 0 {
			return nil, "", fmt.Errorf("empty command in pipeline")
		}
		cmds = append(cmds, exec.Command(cmdArgs[0], cmdArgs[1:]...))
	}

	// Connect pipes
	for i := 0; i < len(cmds)-1; i++ {
		stdout, err := cmds[i].StdoutPipe()
		if err != nil {
			return nil, "", fmt.Errorf("error setting up pipe: %w", err)
		}
		cmds[i+1].Stdin = stdout
	}

	// Set up stderr for pv if present (keeping the magic for now)
	for i, cmd := range cmds {
		if i > 0 && i < len(cmds)-1 && len(cmd.Args) > 0 && strings.HasSuffix(cmd.Args[0], "pv") {
			cmd.Stderr = os.Stderr
		}
	}

	// Capture final output
	var stdoutBuf, stderrBuf bytes.Buffer
	cmds[len(cmds)-1].Stdout = &stdoutBuf
	cmds[len(cmds)-1].Stderr = &stderrBuf

	// Start all commands
	for i, cmd := range cmds {
		if err := cmd.Start(); err != nil {
			return nil, "", fmt.Errorf("error starting command %d: %w", i, err)
		}
	}

	// Wait for all commands and collect errors
	var errs []error
	for i, cmd := range cmds {
		if err := cmd.Wait(); err != nil {
			errs = append(errs, fmt.Errorf("command %d failed: %w", i, err))
		}
	}

	stdoutLines := []string{}
	if stdoutBuf.Len() > 0 {
		stdoutLines = strings.Split(strings.TrimRight(stdoutBuf.String(), "\n"), "\n")
	}

	stderrStr := strings.TrimSpace(stderrBuf.String())

	if len(errs) == 0 {
		return stdoutLines, "", nil
	}

	// Return the first error with stderr
	err := errs[0]
	if stderrStr == "" {
		stderrStr = err.Error()
	}

	return stdoutLines, stderrStr, err
}

func (b *Backup) listSnapshots(vol string) ([]string, error) {
	args := b.buildCommand(b.isTargetVolume(vol), "list", "-H", "-o", "name", "-t", "snapshot", "-s", "creation", vol)
	snaps, stderr, err := b.runCommand(args...)
	if err != nil {
		return nil, b.wrapCmdError("listing snapshots", stderr, err)
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
		_, snapPart := splitSnapshot(sourceSnaps[i])
		if snapPart == "" {
			continue
		}
		targetseek := fmt.Sprintf("%s@%s", target, snapPart)
		if slices.Contains(targetSnaps, targetseek) {
			return sourceSnaps[i], nil
		}
	}
	return "", fmt.Errorf("no matching snapshot found")
}

func (b *Backup) snapshotExists(vol string, snapshot string) bool {
	snapshotName := fmt.Sprintf("%s@%s", vol, snapshot)
	args := b.buildCommand(b.isTargetVolume(vol), "list", "-H", "-t", "snapshot", snapshotName)
	_, _, err := b.runCommand(args...)
	return err == nil
}

func (b *Backup) createSnapshot(vol string, recurse bool) (string, error) {
	now := time.Now()
	snapName := now.Format("2006-01-02T15:04:05")
	slog.Info("creating snapshot", "vol", vol, "snapshot", snapName, "recurse", recurse)
	snap := fmt.Sprintf("%s@%s", vol, snapName)

	args := []string{"snapshot"}
	if recurse {
		args = append(args, "-r")
	}
	args = append(args, snap)

	cmdArgs := b.buildCommand(false, args...)
	_, stderr, err := b.runCommand(cmdArgs...)
	if err != nil {
		return "", b.wrapCmdError("creating snapshot", stderr, err)
	}

	return snap, nil
}

func pv(size int64) (*exec.Cmd, error) {
	if size <= 0 {
		return nil, fmt.Errorf("invalid size for pv: %d", size)
	}
	pv, err := exec.LookPath("pv")
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(pv, "-s", strconv.FormatInt(size, 10))
	slog.Debug("using pv for progress", "size", size)
	return cmd, nil
}

func (b *Backup) runBackup(vol, startSnap, endSnap string, size int64) error {
	slog.Info("backup starting", "vol", vol, "start", startSnap, "end", endSnap)

	sendArgs := b.buildCommand(false, "send", "-R", "-i", startSnap, endSnap)
	receiveArgs := b.buildCommand(true, "receive", "-F", fmt.Sprintf("%s/%s", b.target, vol))

	// Build middle commands (pv if available)
	var middleCmds [][]string
	if _, err := pv(size); err == nil {
		pvPath, _ := exec.LookPath("pv")
		middleCmds = append(middleCmds, []string{pvPath, "-s", strconv.FormatInt(size, 10)})
		slog.Debug("pv started for backup")
	}

	// Build pipeline: send -> middle commands -> receive
	allCmds := [][]string{sendArgs}
	allCmds = append(allCmds, middleCmds...)
	allCmds = append(allCmds, receiveArgs)

	_, stderr, err := b.runPipeline(allCmds)
	if err != nil {
		return b.wrapCmdError("during backup", stderr, err)
	}

	slog.Info("backup complete", "vol", vol, "start", startSnap, "end", endSnap)
	return nil
}

func (b *Backup) dryrunBackup(vol, startSnap, endSnap string) (int64, error) {
	slog.Info("performing dry run", "vol", vol, "start", startSnap, "end", endSnap)
	sendArgs := b.buildCommand(false, "send", "-n", "-P", "-R", "-i", startSnap, endSnap)
	lines, stderr, err := b.runCommand(sendArgs...)
	if err != nil {
		return 0, b.wrapCmdError("with dry run", stderr, err)
	}
	var size int64
	for _, l := range lines {
		parts := strings.Split(strings.TrimSpace(l), "\t")
		if parts[0] == "size" {
			size, err = strconv.ParseInt(parts[1], 10, 64)
			if err != nil {
				return 0, fmt.Errorf("size parse error: %w", err)
			}
			break
		}
	}
	if size == 0 {
		return 0, fmt.Errorf("backup size 0")
	}
	return size, nil
}

func (b *Backup) deleteSnapshot(snap string, recurse bool) error {
	args := []string{"destroy", snap}
	if recurse {
		args = append(args, "-r")
	}

	cmdArgs := b.buildCommand(b.isTargetVolume(snap), args...)
	slog.Info("deleting snapshot", "snap", snap)
	_, stderr, err := b.runCommand(cmdArgs...)
	if err != nil {
		return b.wrapCmdError("deleting snapshot", stderr, err)
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
	_, snapName := splitSnapshot(snap)
	if snapName == "" {
		return fmt.Errorf("invalid snapshot format: %s", snap)
	}
	targetVolume := fmt.Sprintf("%s/%s", b.target, vol)
	if !b.snapshotExists(targetVolume, snapName) {
		return fmt.Errorf("snapshot %s of volume %s not found", snapName, targetVolume)
	}
	slog.Info("incremental backup start", "snap", snap)

	if b.dryrun {
		slog.Info("dry run: would create new snapshot and run incremental backup", "vol", vol, "from", snap)
		return nil
	}

	// Create new snapshot
	newsnap, err := b.createSnapshot(vol, true)
	if err != nil {
		return err
	}
	size, err := b.dryrunBackup(vol, snap, newsnap)
	if err != nil {
		return err
	}
	slog.Info("estimated backup size", "size", size, "human_size", util.HumanBytes(size))
	// Run backup
	err = b.runBackup(vol, snap, newsnap, size)
	if err != nil {
		return err
	}
	// Clean up old snapshots on source
	err = b.cleanSnapshots(vol, 2)
	return err
}
