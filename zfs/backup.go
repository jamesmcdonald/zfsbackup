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

type Source struct {
	vol     string
	recurse bool
}

func (s Source) String() string {
	if s.recurse {
		return s.vol + "/..."
	}
	return s.vol
}

// ParseSource parses a source specification.
// "pool/data/..." → {vol: "pool/data", recurse: true}
// "pool/data"     → {vol: "pool/data", recurse: false}
func ParseSource(s string) (Source, error) {
	recurse := strings.HasSuffix(s, "/...")
	vol := strings.TrimSuffix(s, "/...")
	if vol == "" {
		return Source{}, fmt.Errorf("source volume cannot be empty")
	}
	return Source{vol: vol, recurse: recurse}, nil
}

type Backup struct {
	target    string
	dryrun    bool
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
		if err := opt(b); err != nil {
			return nil, fmt.Errorf("error applying option: %w", err)
		}
	}
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

// execCmd always executes a single command, regardless of dry-run mode.
func (b *Backup) execCmd(args []string) ([]string, string, error) {
	c := exec.Command(args[0], args[1:]...)
	var stdoutBuf, stderrBuf bytes.Buffer
	c.Stdout = &stdoutBuf
	c.Stderr = &stderrBuf

	err := c.Run()

	var stdoutLines []string
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

// execPipeline always executes a pipeline of commands, regardless of dry-run mode.
func (b *Backup) execPipeline(allCmds [][]string) ([]string, string, error) {
	if len(allCmds) < 2 {
		return nil, "", fmt.Errorf("pipeline needs at least 2 commands")
	}

	var cmds []*exec.Cmd
	for _, cmdArgs := range allCmds {
		if len(cmdArgs) == 0 {
			return nil, "", fmt.Errorf("empty command in pipeline")
		}
		cmds = append(cmds, exec.Command(cmdArgs[0], cmdArgs[1:]...))
	}

	for i := 0; i < len(cmds)-1; i++ {
		stdout, err := cmds[i].StdoutPipe()
		if err != nil {
			return nil, "", fmt.Errorf("error setting up pipe: %w", err)
		}
		cmds[i+1].Stdin = stdout
	}

	// Route pv stderr to the terminal so progress is visible.
	for i, cmd := range cmds {
		if i > 0 && i < len(cmds)-1 && len(cmd.Args) > 0 && strings.HasSuffix(cmd.Args[0], "pv") {
			cmd.Stderr = os.Stderr
		}
	}

	var stdoutBuf, stderrBuf bytes.Buffer
	cmds[len(cmds)-1].Stdout = &stdoutBuf
	cmds[len(cmds)-1].Stderr = &stderrBuf

	for i, cmd := range cmds {
		if err := cmd.Start(); err != nil {
			return nil, "", fmt.Errorf("error starting command %d: %w", i, err)
		}
	}

	var errs []error
	for i, cmd := range cmds {
		if err := cmd.Wait(); err != nil {
			errs = append(errs, fmt.Errorf("command %d failed: %w", i, err))
		}
	}

	var stdoutLines []string
	if stdoutBuf.Len() > 0 {
		stdoutLines = strings.Split(strings.TrimRight(stdoutBuf.String(), "\n"), "\n")
	}
	stderrStr := strings.TrimSpace(stderrBuf.String())

	if len(errs) == 0 {
		return stdoutLines, "", nil
	}

	err := errs[0]
	if stderrStr == "" {
		stderrStr = err.Error()
	}
	return stdoutLines, stderrStr, err
}

// query runs a read-only command. Always executes, even in dry-run mode.
func (b *Backup) query(args ...string) ([]string, string, error) {
	return b.execCmd(args)
}

// run executes a write command. Skipped in dry-run mode.
func (b *Backup) run(args ...string) ([]string, string, error) {
	if b.dryrun {
		slog.Info("dry run: skip", "args", args)
		return nil, "", nil
	}
	return b.execCmd(args)
}

// pipeline executes a write pipeline. Skipped in dry-run mode.
func (b *Backup) pipeline(cmds [][]string) ([]string, string, error) {
	if b.dryrun {
		slog.Info("dry run: skip", "cmds", cmds)
		return nil, "", nil
	}
	return b.execPipeline(cmds)
}

func (b *Backup) listSnapshots(vol string) ([]string, error) {
	args := b.buildCommand(b.isTargetVolume(vol), "list", "-H", "-o", "name", "-t", "snapshot", "-s", "creation", vol)
	snaps, stderr, err := b.query(args...)
	if err != nil {
		return nil, b.wrapCmdError("listing snapshots", stderr, err)
	}
	return snaps, nil
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

func (b *Backup) listFilesystems(vol string) ([]string, error) {
	args := b.buildCommand(false, "list", "-H", "-o", "name", "-r", "-t", "filesystem,volume", vol)
	lines, stderr, err := b.query(args...)
	if err != nil {
		return nil, b.wrapCmdError("listing filesystems", stderr, err)
	}
	return lines, nil
}

func (b *Backup) datasetExists(vol string) bool {
	args := b.buildCommand(b.isTargetVolume(vol), "list", "-H", "-t", "filesystem,volume", vol)
	_, _, err := b.query(args...)
	return err == nil
}

// createSnapshot creates a snapshot on vol and returns just the snapshot name (timestamp).
func (b *Backup) createSnapshot(vol string, recurse bool) (string, error) {
	snapName := time.Now().Format("2006-01-02T15:04:05")
	if b.dryrun {
		slog.Info("dry run: would create snapshot", "snapshot", snapName, "vol", vol, "recurse", recurse)
		return snapName, nil
	}

	slog.Info("creating snapshot", "vol", vol, "snapshot", snapName, "recurse", recurse)
	snap := fmt.Sprintf("%s@%s", vol, snapName)
	args := []string{"snapshot"}
	if recurse {
		args = append(args, "-r")
	}
	args = append(args, snap)

	cmdArgs := b.buildCommand(false, args...)
	_, stderr, err := b.run(cmdArgs...)
	if err != nil {
		return "", b.wrapCmdError("creating snapshot", stderr, err)
	}
	return snapName, nil
}

// dryrunSingleBackup estimates the send size using zfs send -n -P. Always runs via query.
func (b *Backup) dryrunSingleBackup(startSnap, endSnap string) (int64, error) {
	var sendArgs []string
	if startSnap != "" {
		sendArgs = b.buildCommand(false, "send", "-n", "-P", "-i", startSnap, endSnap)
	} else {
		sendArgs = b.buildCommand(false, "send", "-n", "-P", endSnap)
	}
	lines, stderr, err := b.query(sendArgs...)
	if err != nil {
		return 0, b.wrapCmdError("estimating backup size", stderr, err)
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

func (b *Backup) runSingleBackup(fs, startSnap, endSnap string, size int64) error {
	slog.Info("backup starting", "fs", fs, "start", startSnap, "end", endSnap)

	var sendArgs []string
	if startSnap != "" {
		sendArgs = b.buildCommand(false, "send", "-i", startSnap, endSnap)
	} else {
		sendArgs = b.buildCommand(false, "send", endSnap)
	}
	receiveArgs := b.buildCommand(true, "receive", "-F", fmt.Sprintf("%s/%s", b.target, fs))

	allCmds := [][]string{sendArgs}
	pvPath, pvErr := exec.LookPath("pv")
	if pvErr == nil && size > 0 {
		allCmds = append(allCmds, []string{pvPath, "-s", strconv.FormatInt(size, 10)})
		slog.Debug("using pv for progress", "size", size)
	}
	allCmds = append(allCmds, receiveArgs)

	_, stderr, err := b.pipeline(allCmds)
	if err != nil {
		return b.wrapCmdError("during backup", stderr, err)
	}

	slog.Info("backup complete", "fs", fs, "start", startSnap, "end", endSnap)
	return nil
}

func (b *Backup) deleteSnapshot(snap string, recurse bool) error {
	args := []string{"destroy"}
	if recurse {
		args = append(args, "-r")
	}
	args = append(args, snap)

	cmdArgs := b.buildCommand(b.isTargetVolume(snap), args...)
	slog.Info("deleting snapshot", "snap", snap)
	_, stderr, err := b.run(cmdArgs...)
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
	const layout = "2006-01-02T15:04:05"
	_, err := time.Parse(layout, parts[1])
	return err == nil
}

func (b *Backup) cleanSnapshots(vol string, retain int, recurse bool) error {
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
		if err := b.deleteSnapshot(snap, recurse); err != nil {
			return err
		}
	}
	return nil
}

func (b *Backup) backupFilesystem(fs, snapName string) error {
	fsSnap := fmt.Sprintf("%s@%s", fs, snapName)
	targetVol := fmt.Sprintf("%s/%s", b.target, fs)

	var startSnap string
	if b.datasetExists(targetVol) {
		var err error
		startSnap, err = b.getLatestMatchingSnapshot(fs, targetVol)
		if err != nil {
			slog.Warn("no matching snapshot found, performing full backup", "fs", fs, "err", err)
		}
	} else {
		slog.Info("target does not exist, performing full backup", "fs", fs)
	}

	size, err := b.dryrunSingleBackup(startSnap, fsSnap)
	if err != nil {
		if b.dryrun {
			// The new snapshot doesn't exist yet in dry-run, so estimation may fail.
			// Log intent without size.
			if startSnap != "" {
				slog.Info("dry run: would send incremental", "fs", fs, "from", startSnap, "to", fsSnap)
			} else {
				slog.Info("dry run: would send full", "fs", fs, "to", targetVol)
			}
			return nil
		}
		return err
	}

	if b.dryrun {
		if startSnap != "" {
			slog.Info("dry run: would send incremental", "fs", fs, "from", startSnap, "to", fsSnap, "size", util.HumanBytes(size))
		} else {
			slog.Info("dry run: would send full", "fs", fs, "to", targetVol, "size", util.HumanBytes(size))
		}
		return nil
	}

	slog.Info("estimated backup size", "fs", fs, "size", size, "human_size", util.HumanBytes(size))
	return b.runSingleBackup(fs, startSnap, fsSnap, size)
}

func (b *Backup) backupSource(src Source) error {
	snapName, err := b.createSnapshot(src.vol, src.recurse)
	if err != nil {
		return err
	}

	var filesystems []string
	if src.recurse {
		filesystems, err = b.listFilesystems(src.vol)
		if err != nil {
			return err
		}
	} else {
		filesystems = []string{src.vol}
	}

	for _, fs := range filesystems {
		if err := b.backupFilesystem(fs, snapName); err != nil {
			return err
		}
	}

	return b.cleanSnapshots(src.vol, 2, src.recurse)
}

// RunBackup backs up each source in order, failing fast on any error.
func (b *Backup) RunBackup(sources []Source) error {
	for _, src := range sources {
		if err := b.backupSource(src); err != nil {
			return err
		}
	}
	return nil
}
