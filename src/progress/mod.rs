pub mod filter;
pub mod log;
pub mod terminal;

pub trait Progressor: Send {
    fn run(&mut self);
}

fn human_bytes(bytes: u64) -> String {
    let units = ["B", "kB", "MB", "GB", "TB"];

    let mut value = bytes as f64;
    let mut index = 0;
    while value >= 1024.0 && index < units.len() - 1 {
        value /= 1024.0;
        index += 1;
    }
    format!("{:.2} {:2}", value, units[index])
}

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
        bytes: u64,
        estimated_total: Option<u64>,
    },
    DatasetCompleted(String),
    DryrunCompleted(String),
}
