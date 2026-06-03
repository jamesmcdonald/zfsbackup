use super::{BackupEvent, human_bytes};
use std::io::Write;
use std::sync::mpsc::Receiver;

pub struct Progressor {
    receiver: Receiver<BackupEvent>,
    source: String,
}

impl super::Progressor for Progressor {
    fn run(&mut self) {
        while let Ok(event) = self.receiver.recv() {
            match event {
                BackupEvent::Estimate(_) => {}
                BackupEvent::FullBackupStart {
                    source,
                    dest: _,
                    index: _,
                    total: _,
                } => {
                    self.source = source;
                    print!("{}\r", self.source);
                }
                BackupEvent::IncrementalBackupStart {
                    source,
                    dest: _,
                    index: _,
                    total: _,
                } => {
                    self.source = source;
                    print!("{}\r", self.source);
                }
                BackupEvent::SnapshotCreate(_) => {}
                BackupEvent::SnapshotDelete(_) => {}
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

                            print!("\x1b[2K{}: {:>3.0}%\r", self.source, percent * 100.0,);
                        }
                        None => {
                            print!(
                                "\x1b[2K{}: {} transferred\r",
                                self.source,
                                human_bytes(bytes as f64)
                            );
                        }
                    }
                    std::io::stdout().flush().ok();
                }
                BackupEvent::SendComplete(bytes) => {
                    print!(
                        "\x1b[2K{}: send complete ({})\r",
                        self.source,
                        human_bytes(bytes as f64)
                    );
                }
                BackupEvent::DatasetComplete(_) => {
                    println!("\x1b[2K{}: complete", self.source);
                }
                BackupEvent::DryrunComplete(_) => {
                    println!("\x1b[2K{}: complete", self.source);
                }
            }
        }
    }
}

impl Progressor {
    pub fn new(receiver: Receiver<BackupEvent>) -> Self {
        Self {
            receiver,
            source: String::new(),
        }
    }
}
