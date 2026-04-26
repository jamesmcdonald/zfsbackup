use super::{BackupEvent, human_bytes};
use std::{io::Write, sync::mpsc::Receiver};

pub struct Progressor {
    receiver: Receiver<BackupEvent>,
    estimated_size: u64,
}

impl super::Progressor for Progressor {
    fn run(&mut self) {
        while let Ok(event) = self.receiver.recv() {
            match event {
                BackupEvent::Estimate(size) => {
                    println!("Estimated total backup size: {}", human_bytes(size));
                    self.estimated_size = size;
                }
                BackupEvent::StartingFullBackup {
                    source,
                    dest,
                    index,
                    total,
                } => {
                    println!(
                        "\nStarting full backup of {} to {} ({} of {})",
                        source, dest, index, total
                    );
                }
                BackupEvent::StartingIncrementalBackup {
                    source,
                    dest,
                    index,
                    total,
                } => {
                    println!(
                        "\nStarting incremental backup of {} to {} ({} of {})",
                        source, dest, index, total
                    );
                }
                BackupEvent::SnapshotCreated(name) => {
                    println!("Created snapshot: {}", name);
                }
                BackupEvent::SnapshotDeleted(name) => {
                    println!("Deleted snapshot: {}", name);
                }
                BackupEvent::BytesTransferred {
                    bytes,
                    estimated_total,
                } => {
                    match estimated_total {
                        Some(total) => {
                            let percent: f64 = if total > 0 {
                                bytes as f64 / total as f64
                            } else {
                                0.0
                            };
                            print!(
                                "{:>3.0}% {}/{} transferred\r",
                                percent * 100.0,
                                human_bytes(bytes),
                                human_bytes(total)
                            );
                        }
                        None => {
                            print!("{} transferred\r", human_bytes(bytes));
                        }
                    }
                    std::io::stdout().flush().ok();
                }
                BackupEvent::DatasetCompleted(name) => {
                    println!("Completed backup of dataset: {}", name);
                }
                BackupEvent::DryrunCompleted(name) => {
                    println!("Completed dry run backup of dataset: {}", name);
                }
            }
        }
    }
}

impl Progressor {
    pub fn new(receiver: Receiver<BackupEvent>) -> Self {
        Self {
            receiver,
            estimated_size: 0,
        }
    }
}
