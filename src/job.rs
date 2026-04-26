use crate::command;
use crate::progress::BackupEvent;
use chrono::{Local, NaiveDateTime};
use std::collections::HashSet;
use std::sync::mpsc::Sender;

pub struct JobBuilder {
    sources: Vec<String>,
    target: String,
    source_zfs_command: Vec<String>,
    target_zfs_command: Vec<String>,
    dryrun: bool,
    retain: usize,
    sender: Option<Sender<BackupEvent>>,
}

fn parse_command(commandstr: &str) -> Vec<String> {
    commandstr
        .split_whitespace()
        .map(|s| s.to_string())
        .collect()
}

impl JobBuilder {
    pub fn new(sources: Vec<String>, target: String) -> Self {
        JobBuilder {
            sources,
            target,
            source_zfs_command: vec!["zfs".to_string()],
            target_zfs_command: vec!["zfs".to_string()],
            dryrun: false,
            retain: 2,
            sender: None,
        }
    }

    pub fn source_zfs_command(mut self, commandstr: &str) -> Self {
        let command = parse_command(commandstr);
        self.source_zfs_command = command;
        self
    }

    pub fn target_zfs_command(mut self, commandstr: &str) -> Self {
        let command = parse_command(commandstr);
        self.target_zfs_command = command;
        self
    }

    pub fn zfs_command(mut self, commandstr: &str) -> Self {
        let command = parse_command(commandstr);
        self.source_zfs_command = command.clone();
        self.target_zfs_command = command;
        self
    }

    pub fn dryrun(mut self) -> Self {
        self.dryrun = true;
        self
    }

    pub fn retain(mut self, retain: usize) -> Self {
        self.retain = retain;
        self
    }

    pub fn sender(mut self, sender: Sender<BackupEvent>) -> Self {
        self.sender = Some(sender);
        self
    }

    pub fn build(self) -> Result<Job, String> {
        let mut job = Job {
            datasets: vec![],
            target: self.target,
            source_zfs_command: self.source_zfs_command,
            target_zfs_command: self.target_zfs_command,
            dryrun: self.dryrun,
            retain: self.retain,
            sender: self.sender,
        };
        let mut datasets: Vec<String> = vec![];
        for source in &self.sources {
            let recurse = source.ends_with("/...");
            let source = source.trim_end_matches("/...");

            let mut args = vec!["list", "-H", "-o", "name"];
            if recurse {
                args.push("-r");
            }
            args.push(source);

            let mut cmd = job.get_side_command(JobSide::Source);
            cmd.extend(args);
            let output = command::exec_command(&cmd)?;
            datasets.extend(output.lines().map(str::to_string));
        }
        if datasets.is_empty() {
            return Err(String::from("no matching source datasets found"));
        }
        job.datasets = datasets;
        Ok(job)
    }
}

pub struct Job {
    datasets: Vec<String>,
    target: String,
    source_zfs_command: Vec<String>,
    target_zfs_command: Vec<String>,
    dryrun: bool,
    retain: usize,
    sender: Option<Sender<BackupEvent>>,
}

#[derive(Debug)]
enum JobSide {
    Source,
    Destination,
}

#[derive(Debug)]
struct Snapshot {
    snapshot: String,
    side: JobSide,
}

impl Job {
    pub fn dump(&self) {
        println!("Datasets: {:?}", self.datasets);
        println!("Target: {}", self.target);
        println!("Source ZFS Command: {:?}", self.source_zfs_command);
        println!("Target ZFS Command: {:?}", self.target_zfs_command);
        println!("Dryrun: {}", self.dryrun);
    }

    fn send_event(&self, event: BackupEvent) {
        if let Some(sender) = &self.sender {
            sender.send(event).ok();
        }
    }

    fn get_side_command(&self, side: JobSide) -> Vec<&str> {
        match side {
            JobSide::Source => self.source_zfs_command.iter().map(|s| s.as_str()).collect(),
            JobSide::Destination => self.target_zfs_command.iter().map(|s| s.as_str()).collect(),
        }
    }

    fn create_snapshot(&self, dataset: &str, side: JobSide) -> Result<String, String> {
        let snapshot = format!("{}@{}", dataset, Local::now().format("%Y-%m-%dT%H:%M:%S"));
        let mut cmd = self.get_side_command(side);
        cmd.extend(["snapshot", &snapshot]);
        command::exec_command(&cmd)?;
        self.send_event(BackupEvent::SnapshotCreated(snapshot.clone()));
        Ok(snapshot)
    }

    fn estimate(&self, source: &str, inc_snapshot: Option<&str>) -> Result<u64, String> {
        let mut cmd = self.get_side_command(JobSide::Source);
        cmd.push("send");
        if let Some(inc) = inc_snapshot {
            cmd.extend(["-i", inc]);
        }
        cmd.extend(["-n", "-P"]);
        cmd.push(source);
        let output = command::exec_command(&cmd)?;
        let size = output
            .lines()
            .last()
            .ok_or("estimate parse error")?
            .split_whitespace()
            .last()
            .ok_or("estimate parse error")?
            .parse::<u64>()
            .map_err(|e| format!("estimate parse error: {}", e))?;
        self.send_event(BackupEvent::Estimate(size));
        Ok(size)
    }
    fn send_receive(
        &self,
        source: &str,
        dest: &str,
        inc_snapshot: Option<&str>,
        total: Option<u64>,
    ) -> Result<(), String> {
        let mut send_cmd = self.get_side_command(JobSide::Source);
        send_cmd.push("send");
        if let Some(inc) = inc_snapshot {
            send_cmd.extend(["-i", inc]);
        }
        send_cmd.push(source);

        let mut receive_cmd = self.get_side_command(JobSide::Destination);
        receive_cmd.extend(["receive", "-F", dest]);
        command::exec_piped_commands(&send_cmd, &receive_cmd, self.sender.clone(), total)?;
        self.send_event(BackupEvent::DatasetCompleted(source.to_string()));
        Ok(())
    }

    fn list_snapshots(&self, source: &str, side: JobSide) -> Result<Vec<String>, String> {
        let mut cmd = self.get_side_command(side);
        cmd.extend(["list", "-H", "-o", "name", "-t", "snapshot", source]);
        let output = command::exec_command(&cmd)?;
        let snapshots: Vec<&str> = output
            .split_whitespace()
            .map(|s| {
                let parts: Vec<&str> = s.split("@").collect();
                if parts.len() == 2 {
                    Ok(parts[1])
                } else {
                    Err(format!("invalid snapshot name: {}", s))
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        let result: Vec<String> = snapshots
            .iter()
            .filter(|s| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S").is_ok())
            .map(|s| s.to_string())
            .collect();
        Ok(result)
    }

    fn find_latest_matching_snapshot(&self, source: &str, dest: &str) -> Result<String, String> {
        self.find_matching_snapshots(source, dest)?
            .into_iter()
            .next()
            .ok_or(String::from("no matching snapshots"))
    }

    fn find_matching_snapshots(&self, source: &str, dest: &str) -> Result<Vec<String>, String> {
        let source_snapshots = self.list_snapshots(source, JobSide::Source)?;
        let mut dest_snapshots = self.list_snapshots(dest, JobSide::Destination)?;
        dest_snapshots.sort_by(|a, b| b.cmp(a));

        let source_set: HashSet<String> = source_snapshots.into_iter().collect();

        let matching_snapshots: Vec<String> = dest_snapshots
            .into_iter()
            .filter(|s| source_set.contains(s))
            .collect();

        if matching_snapshots.is_empty() {
            Err(String::from("no matching snapshots"))
        } else {
            Ok(matching_snapshots)
        }
    }

    fn find_old_snapshots(&self, source: &str, dest: &str) -> Result<Vec<Snapshot>, String> {
        let source_snapshots = self.list_snapshots(source, JobSide::Source)?;
        let dest_snapshots = self.list_snapshots(dest, JobSide::Destination)?;
        let matching_snapshots = self.find_matching_snapshots(source, dest)?;
        if matching_snapshots.is_empty() {
            return Err(String::from("no matching snapshots found"));
        }
        let retain: HashSet<String> = matching_snapshots.into_iter().take(self.retain).collect();
        let result = source_snapshots
            .into_iter()
            .filter(|s| !retain.contains(s))
            .map(|s| Snapshot {
                snapshot: format!("{}@{}", source, s),
                side: JobSide::Source,
            })
            .chain(
                dest_snapshots
                    .into_iter()
                    .filter(|s| !retain.contains(s))
                    .map(|s| Snapshot {
                        snapshot: format!("{}@{}", dest, s),
                        side: JobSide::Destination,
                    }),
            )
            .collect();
        Ok(result)
    }

    fn delete_snapshot(&self, snapshot: Snapshot) -> Result<String, String> {
        let mut cmd = self.get_side_command(snapshot.side);
        cmd.extend(["destroy", &snapshot.snapshot]);
        let res = command::exec_command(&cmd);
        if res.is_ok() {
            self.send_event(BackupEvent::SnapshotDeleted(snapshot.snapshot.clone()));
        }
        res
    }

    pub fn run(&self) -> Result<(), String> {
        for (index, source) in self.datasets.iter().enumerate() {
            // Check the source exists
            let mut cmd = self.get_side_command(JobSide::Source);
            cmd.extend(["list", "-H", "-o", "name", source]);
            let _ = command::exec_command(&cmd)?;

            // Check whether the destination exists
            // TODO: This will assume the destination doesn't exist if the
            // command fails for any reason.
            let dest = format!("{}/{}", self.target, source);
            cmd = self.get_side_command(JobSide::Destination);
            cmd.extend(["list", "-H", "-o", "name", &dest]);
            let dest_exists = command::exec_command(&cmd).is_ok();

            // Run backup
            if dest_exists {
                self.send_event(BackupEvent::StartingIncrementalBackup {
                    source: source.clone(),
                    dest: dest.clone(),
                    index: index + 1,
                    total: self.datasets.len(),
                });
                let inc_snapshot = format!(
                    "{}@{}",
                    source,
                    self.find_latest_matching_snapshot(source, &dest)?
                );

                let snapshot = self.create_snapshot(source, JobSide::Source)?;
                let total = self.estimate(&snapshot, Some(&inc_snapshot)).ok();
                if self.dryrun {
                    self.send_event(BackupEvent::DryrunCompleted(source.clone()));
                    self.delete_snapshot(Snapshot {
                        snapshot: snapshot.clone(),
                        side: JobSide::Source,
                    })?;
                    return Ok(());
                }
                self.send_receive(&snapshot, &dest, Some(&inc_snapshot), total)?;
            } else {
                self.send_event(BackupEvent::StartingFullBackup {
                    source: source.clone(),
                    dest: dest.clone(),
                    index: index + 1,
                    total: self.datasets.len(),
                });
                let snapshot = self.create_snapshot(source, JobSide::Source)?;
                let total = self.estimate(&snapshot, None).ok();
                if self.dryrun {
                    self.send_event(BackupEvent::DryrunCompleted(source.clone()));
                    self.delete_snapshot(Snapshot {
                        snapshot: snapshot.clone(),
                        side: JobSide::Source,
                    })?;
                    return Ok(());
                }
                self.send_receive(&snapshot, &dest, None, total)?;
            }

            // Clean up snapshots
            self.find_old_snapshots(source, &dest)?
                .into_iter()
                .map(|s| self.delete_snapshot(s))
                .collect::<Result<Vec<_>, _>>()?;
        }
        Ok(())
    }
}
