# zfsbackup

A Go-based tool for performing incremental backups of ZFS filesystems.

## Description

`zfsbackup` is a command-line utility that performs incremental backups of ZFS filesystems to target ZFS filesystems. It creates snapshots, calculates backup sizes, and transfers data using `zfs send` and `zfs receive` commands.

Note that full backups aren't supported yet, and a matching
snapshot is already expected on source and destination filesystems
for incremental backups.

## Features

- Incremental ZFS backups
- Automatic snapshot creation with timestamp naming
- Backup size estimation using dry run
- Progress display via `pv` (pipe viewer) when available
- Debug logging support
- Configurable source and target ZFS commands
- Snapshot cleanup with retention policies

## Installation

```bash
go install github.com/jamesmcdonald/zfsbackup@latest
```

## Usage

```bash
zfsbackup <source-fs> [flags]
```

### Flags

- `-t, --target-fs string`: Target filesystem (default: "backup")
- `-n, --dry-run`: This *does not disable anything* yet. Be warned. Once implemented it will just check that matching snapshots exist.
- `-d, --debug`: Enable debug output
- `-S, --source-command string`: Source ZFS command (default: "zfs")
- `-T, --target-command string`: Target ZFS command (default: "zfs")

  You can use this to back up over ssh, for example `-T 'ssh backuphost zfs'`.

### Examples

Backup `tank/data` to `backup/tank/data`:
```bash
zfsbackup tank/data
```

Backup with custom target filesystem:
```bash
zfsbackup tank/data --target-fs backups
```

Enable debug output:
```bash
zfsbackup tank/data --debug
```

## Important Notes

**⚠️ Dry Run Limitation**: The `--dry-run` flag currently does not prevent actual backup operations from running. I have added the flag, but not wired it up yet.

## How It Works

1. Finds the latest matching snapshot between source and target
2. Creates a new snapshot on the source filesystem
3. Estimates backup size using `zfs send -n`
4. Performs the incremental backup using `zfs send` and `zfs receive`
5. Cleans up old snapshots (retains 2 snapshots by default)

## Requirements

- Go 1.24.4 or later
- ZFS utilities installed
- Optional: `pv` (pipe viewer) for progress display

## License

This project is licensed under the MIT License.
