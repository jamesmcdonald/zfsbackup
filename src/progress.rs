use std::{io::Write, sync::mpsc::Receiver};

pub enum BackupEvent {
    Estimate(u64),
    StartingFullBackup {
        source: String,
        dest: String,
        index: usize,
        total: usize,
    },
    StartingIncrementalBackup {
        source: String,
        dest: String,
        index: usize,
        total: usize,
    },
    SnapshotCreated(String),
    SnapshotDeleted(String),
    BytesTransferred {
        dataset: String,
        bytes: u64,
    },
    DatasetCompleted(String),
}

pub struct ProgressReporter {
    receiver: Receiver<BackupEvent>,
    estimated_size: u64,
}

impl ProgressReporter {
    pub fn new(receiver: Receiver<BackupEvent>) -> Self {
        Self {
            receiver,
            estimated_size: 0,
        }
    }

    pub fn run(&mut self) {
        while let Ok(event) = self.receiver.recv() {
            match event {
                BackupEvent::Estimate(size) => {
                    println!("Estimated total backup size: {} bytes", size);
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
                BackupEvent::BytesTransferred { dataset, bytes } => {
                    let percent: f64 = if self.estimated_size > 0 {
                        bytes as f64 / self.estimated_size as f64
                    } else {
                        0.0
                    };
                    print!(
                        "{:>3.0}% {}/{} bytes transferred\r",
                        percent * 100.0,
                        bytes,
                        self.estimated_size
                    );
                    std::io::stdout().flush().ok();
                }
                BackupEvent::DatasetCompleted(name) => {
                    println!("Completed backup of dataset: {}", name);
                    self.estimated_size = 0;
                }
            }
        }
    }
}
