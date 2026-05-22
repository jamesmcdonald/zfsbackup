use crate::progress::format_eta;

use super::rate::RateTracker;
use super::{BackupEvent, human_bytes};
use std::{io::Write, sync::mpsc::Receiver};

pub struct Progressor {
    receiver: Receiver<BackupEvent>,
    estimated_size: u64,
    rate_tracker: RateTracker,
}

impl super::Progressor for Progressor {
    fn run(&mut self) {
        while let Ok(event) = self.receiver.recv() {
            match event {
                BackupEvent::Estimate(size) => {
                    println!("Estimated total backup size: {}", human_bytes(size as f64));
                    self.estimated_size = size;
                }
                BackupEvent::FullBackupStart {
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
                BackupEvent::IncrementalBackupStart {
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
                BackupEvent::SnapshotCreate(name) => {
                    println!("Created snapshot: {}", name);
                }
                BackupEvent::SnapshotDelete(name) => {
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
                            self.rate_tracker.push(bytes);
                            let rate = self.rate_tracker.rate();
                            let rate_display = match rate {
                                None => String::from("[unknown]"),
                                Some(0.0) => {
                                    String::from("[stalled] ETA heat death of the universe")
                                }
                                Some(rate) => {
                                    let eta = if bytes > total {
                                        String::from("any moment now")
                                    } else {
                                        let eta = (total - bytes) as f64 / rate;
                                        format_eta(eta)
                                    };
                                    format!("{}/s ETA {}", human_bytes(rate), eta)
                                }
                            };

                            print!(
                                "\x1b[2K{:>3.0}% {}/{} @ {}\r",
                                percent * 100.0,
                                human_bytes(bytes as f64),
                                human_bytes(total as f64),
                                rate_display,
                            );
                        }
                        None => {
                            print!("\x1b[2K{} transferred\r", human_bytes(bytes as f64));
                        }
                    }
                    std::io::stdout().flush().ok();
                }
                BackupEvent::SendComplete(bytes) => {
                    println!(
                        "\x1b[2KSend completed at {}, awaiting receive completion",
                        human_bytes(bytes as f64)
                    );
                }
                BackupEvent::DatasetComplete(name) => {
                    println!("\x1b[2KCompleted backup of dataset: {}", name);
                }
                BackupEvent::DryrunComplete(name) => {
                    println!("\x1b[2KCompleted dry run backup of dataset: {}", name);
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
            rate_tracker: RateTracker::new(100),
        }
    }
}
