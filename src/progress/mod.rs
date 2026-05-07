pub mod filter;
pub mod log;
mod rate;
pub mod terminal;

pub trait Progressor: Send {
    fn run(&mut self);
}

fn human_bytes(bytes: f64) -> String {
    let units = ["B", "kB", "MB", "GB", "TB"];

    let mut a = bytes;
    let mut i = 0;
    while a >= 1024.0 && i < units.len() - 1 {
        a /= 1024.0;
        i += 1;
    }
    format!("{:.2} {}", a, units[i])
}

fn format_eta(seconds: f64) -> String {
    let secs = seconds as u64;
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    format!("{:02}:{:02}:{:02}", h, m, s)
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
    SendCompleted(u64),
    DatasetCompleted(String),
    DryrunCompleted(String),
}
