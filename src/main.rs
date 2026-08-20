use clap::Parser;

use std::error::Error;
use std::io::IsTerminal;
use std::sync::mpsc::channel;
use std::thread;
use zfsbackup::job::JobBuilder;
use zfsbackup::progress::{Progressor, curt, log, terminal};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    /// Target dataset for the backup
    #[arg(short, default_value = "backup")]
    target: String,

    #[arg(short = 'T', long)]
    target_zfs_command: Option<String>,

    #[arg(short, long)]
    source_zfs_command: Option<String>,

    #[arg(short, long)]
    zfs_command: Option<String>,

    #[arg(short, long)]
    dry_run: bool,

    #[arg(short, long)]
    retain: Option<usize>,

    #[arg(short, long)]
    progressor: Option<String>,

    #[arg(short, long)]
    exclude: Vec<String>,

    datasets: Vec<String>,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    let mut builder = JobBuilder::new(args.datasets, args.target);
    if let Some(cmd) = args.zfs_command {
        builder = builder.zfs_command(&cmd);
    }
    if let Some(cmd) = args.source_zfs_command {
        builder = builder.source_zfs_command(&cmd);
    }
    if let Some(cmd) = args.target_zfs_command {
        builder = builder.target_zfs_command(&cmd);
    }
    if let Some(retain) = args.retain {
        builder = builder.retain(retain);
    }
    builder = builder.exclude(args.exclude);

    let (tx, rx) = channel();
    let mut pr: Box<dyn Progressor> = match args.progressor {
        Some(name) => match name.as_str() {
            "curt" => Box::new(curt::Progressor::new(rx)),
            "terminal" => Box::new(terminal::Progressor::new(rx)),
            "log" => Box::new(log::Progressor::new(rx)),
            _ => {
                eprintln!("progressor {} not found, defaulting to 'log'", name);
                Box::new(log::Progressor::new(rx))
            }
        },
        None => {
            if std::io::stdout().is_terminal() {
                Box::new(terminal::Progressor::new(rx))
            } else {
                Box::new(log::Progressor::new(rx))
            }
        }
    };
    let handle = thread::spawn(move || pr.run());
    builder = builder.sender(tx);

    // Create the job in a block so the sender is dropped before
    // joining the progress thread, allowing it to exit cleanly.
    {
        let job = builder.build()?;
        job.run(!args.dry_run)?;
    }
    handle.join().unwrap();
    Ok(())
}
