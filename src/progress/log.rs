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
                BackupEvent::FullBackupStart {
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
                BackupEvent::IncrementalBackupStart {
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
                BackupEvent::SnapshotCreate(name) => {
                    println!("Created snapshot: {}", name);
                }
                BackupEvent::SnapshotDelete(name) => {
                    println!("Deleted snapshot: {}", name);
                }
                BackupEvent::BytesTransferred { .. } => {}
                BackupEvent::SendComplete(bytes) => {
                    println!("Send completed: {} bytes", bytes);
                }
                BackupEvent::DatasetComplete(name) => {
                    println!("Completed backup of dataset: {}", name);
                    self.estimated_size = 0;
                }
                BackupEvent::DryrunComplete(name) => {
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
