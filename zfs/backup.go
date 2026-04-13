package zfs

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
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

// Progresser reports transfer progress for a single backup stream.
type Progresser interface {
	Add(int64)
	Done()
}

// ProgressFactory creates a Progresser for a given filesystem transfer.
// label is the filesystem name; size is the estimated byte count.
type ProgressFactory func(label string, size int64) Progresser

type Backup struct {
	target          string
	dryrun          bool
	sourceCmd       []string
	targetCmd       []string
	logger          *slog.Logger
	progressFactory ProgressFactory
	workers         int
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

func WithLogger(logger *slog.Logger) BackupOption {
	return func(b *Backup) error {
		b.logger = logger
		return nil
	}
}

func WithProgressFactory(f ProgressFactory) BackupOption {
	return func(b *Backup) error {
		b.progressFactory = f
		return nil
	}
}

func WithWorkers(n int) BackupOption {
	return func(b *Backup) error {
		if n > 0 {
			b.workers = n
		}
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
		logger:    slog.Default(),
		workers:   runtime.NumCPU() * 2,
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

type countingReader struct {
	r io.Reader
	p Progresser
}

func (cr *countingReader) Read(p []byte) (int, error) {
	n, err := cr.r.Read(p)
	if n > 0 {
		cr.p.Add(int64(n))
	}
	return n, err
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
		b.logger.Info("dry run: skip", "args", args)
		return nil, "", nil
	}
	return b.execCmd(args)
}

// pipeline executes a write pipeline. Skipped in dry-run mode.
func (b *Backup) pipeline(cmds [][]string) ([]string, string, error) {
	if b.dryrun {
		b.logger.Info("dry run: skip", "cmds", cmds)
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
		b.logger.Info("dry run: would create snapshot", "snapshot", snapName, "vol", vol, "recurse", recurse)
		return snapName, nil
	}

	b.logger.Info("creating snapshot", "vol", vol, "snapshot", snapName, "recurse", recurse)
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

func (b *Backup) runPipelineWithProgress(sendArgs, receiveArgs []string, p Progresser) error {
	sendCmd := exec.Command(sendArgs[0], sendArgs[1:]...)
	recvCmd := exec.Command(receiveArgs[0], receiveArgs[1:]...)

	sendOut, err := sendCmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("error setting up pipe: %w", err)
	}
	recvCmd.Stdin = &countingReader{r: sendOut, p: p}

	var stderrBuf bytes.Buffer
	recvCmd.Stderr = &stderrBuf

	if err := sendCmd.Start(); err != nil {
		return fmt.Errorf("error starting send: %w", err)
	}
	if err := recvCmd.Start(); err != nil {
		return fmt.Errorf("error starting receive: %w", err)
	}

	// Wait for both processes concurrently. Whichever fails first is the root
	// cause; we kill the other side so it doesn't hang (send blocked writing to
	// a full pipe, or recv stuck on disk I/O). sync.Once ensures the consequent
	// "signal: killed" error from the side we terminate is not reported.
	var (
		firstErr  error
		firstOnce sync.Once
	)
	setFirst := func(label string, err error) {
		firstOnce.Do(func() { firstErr = fmt.Errorf("%s failed: %w", label, err) })
	}

	sendErrCh := make(chan error, 1)
	go func() {
		err := sendCmd.Wait()
		if err != nil {
			setFirst("send", err)
			recvCmd.Process.Kill()
		}
		sendErrCh <- err
	}()

	if err := recvCmd.Wait(); err != nil {
		setFirst("receive", err)
		sendCmd.Process.Kill()
	}
	<-sendErrCh
	p.Done()

	if firstErr != nil {
		return b.wrapCmdError("during backup", strings.TrimSpace(stderrBuf.String()), firstErr)
	}
	return nil
}

func (b *Backup) runSingleBackup(fs, startSnap, endSnap string, size int64) error {
	b.logger.Info("backup starting", "fs", fs, "start", startSnap, "end", endSnap)

	var sendArgs []string
	if startSnap != "" {
		sendArgs = b.buildCommand(false, "send", "-i", startSnap, endSnap)
	} else {
		sendArgs = b.buildCommand(false, "send", endSnap)
	}
	receiveArgs := b.buildCommand(true, "receive", "-F", fmt.Sprintf("%s/%s", b.target, fs))

	if b.progressFactory != nil && size > 0 {
		p := b.progressFactory(fs, size)
		if err := b.runPipelineWithProgress(sendArgs, receiveArgs, p); err != nil {
			return err
		}
	} else {
		_, stderr, err := b.pipeline([][]string{sendArgs, receiveArgs})
		if err != nil {
			return b.wrapCmdError("during backup", stderr, err)
		}
	}

	b.logger.Info("backup complete", "fs", fs, "start", startSnap, "end", endSnap)
	return nil
}

func (b *Backup) deleteSnapshot(snap string, recurse bool) error {
	args := []string{"destroy"}
	if recurse {
		args = append(args, "-r")
	}
	args = append(args, snap)

	cmdArgs := b.buildCommand(b.isTargetVolume(snap), args...)
	b.logger.Info("deleting snapshot", "snap", snap)
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
	b.logger.Info("cleaning snapshots", "vol", vol, "retain", retain, "snaps", len(snaps))
	if retain < 1 {
		b.logger.Warn("retain too low, retaining 1 snap", "retain", retain)
		retain = 1
	}
	if len(snaps) <= retain {
		b.logger.Debug("not cleaning snaps", "snaps", len(snaps), "retain", retain)
		return nil
	}
	saved := 0
	for i := len(snaps) - 1; i >= 0; i-- {
		snap := snaps[i]
		if !isBackupSnapshot(snap) {
			b.logger.Debug("skipping non-backup snapshot", "snap", snap)
			continue
		}
		if saved < retain {
			b.logger.Debug("retaining snapshot", "snap", snap)
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
			b.logger.Warn("no matching snapshot found, performing full backup", "fs", fs, "err", err)
		}
	} else {
		b.logger.Info("target does not exist, performing full backup", "fs", fs)
	}

	size, err := b.dryrunSingleBackup(startSnap, fsSnap)
	if err != nil {
		if b.dryrun {
			// The new snapshot doesn't exist yet in dry-run, so estimation may fail.
			// Log intent without size.
			if startSnap != "" {
				b.logger.Info("dry run: would send incremental", "fs", fs, "from", startSnap, "to", fsSnap)
			} else {
				b.logger.Info("dry run: would send full", "fs", fs, "to", targetVol)
			}
			return nil
		}
		return err
	}

	if b.dryrun {
		if startSnap != "" {
			b.logger.Info("dry run: would send incremental", "fs", fs, "from", startSnap, "to", fsSnap, "size", HumanBytes(size))
		} else {
			b.logger.Info("dry run: would send full", "fs", fs, "to", targetVol, "size", HumanBytes(size))
		}
		return nil
	}

	b.logger.Info("estimated backup size", "fs", fs, "size", size, "human_size", HumanBytes(size))
	return b.runSingleBackup(fs, startSnap, fsSnap, size)
}

// RunBackup backs up all sources using a worker pool for parallel filesystem transfers.
// Phase 1 (sequential): create snapshots and enumerate filesystems for all sources.
// Phase 2 (parallel):   back up all individual filesystems via b.workers goroutines.
// Phase 3 (sequential): clean old snapshots for all sources.
func (b *Backup) RunBackup(sources []Source) error {
	type sourceState struct {
		src         Source
		snapName    string
		filesystems []string
	}

	// Phase 1: snapshots + filesystem enumeration
	states := make([]sourceState, 0, len(sources))
	for _, src := range sources {
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
		states = append(states, sourceState{src, snapName, filesystems})
	}

	// Phase 2: parallel backup
	type task struct{ fs, snapName string }
	var tasks []task
	for _, s := range states {
		for _, fs := range s.filesystems {
			tasks = append(tasks, task{fs, s.snapName})
		}
	}

	taskCh := make(chan task, len(tasks))
	for _, t := range tasks {
		taskCh <- t
	}
	close(taskCh)

	var (
		mu       sync.Mutex
		firstErr error
	)
	var wg sync.WaitGroup
	for range b.workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for t := range taskCh {
				mu.Lock()
				stopped := firstErr != nil
				mu.Unlock()
				if stopped {
					return
				}
				if err := b.backupFilesystem(t.fs, t.snapName); err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()

	if firstErr != nil {
		return firstErr
	}

	// Phase 3: clean snapshots on source and destination.
	// Both sides are trimmed to the same retain count so they always share the
	// most-recently-backed-up snapshot, keeping incremental transfers possible.
	for _, s := range states {
		if err := b.cleanSnapshots(s.src.vol, 2, s.src.recurse); err != nil {
			return err
		}
		targetVol := fmt.Sprintf("%s/%s", b.target, s.src.vol)
		if b.datasetExists(targetVol) {
			if err := b.cleanSnapshots(targetVol, 2, s.src.recurse); err != nil {
				return err
			}
		}
	}
	return nil
}
