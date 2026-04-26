use super::BackupEvent;
use std::sync::mpsc::Receiver;

pub struct Progressor {
    receiver: Receiver<BackupEvent>,
    estimated_size: u64,
}

impl super::Progressor for Progressor {
    fn run(&mut self) {
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
                        "Starting full backup of {} to {} ({} of {})",
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
                        "Starting incremental backup of {} to {} ({} of {})",
                        source, dest, index, total
                    );
                }
                BackupEvent::SnapshotCreated(name) => {
                    println!("Created snapshot: {}", name);
                }
                BackupEvent::SnapshotDeleted(name) => {
                    println!("Deleted snapshot: {}", name);
                }
                BackupEvent::BytesTransferred { .. } => {}
                BackupEvent::DatasetCompleted(name) => {
                    println!("Completed backup of dataset: {}", name);
                    self.estimated_size = 0;
                }
                BackupEvent::DryrunCompleted(name) => {
                    println!("Completed dry run backup of dataset: {}", name);
                    self.estimated_size = 0;
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
