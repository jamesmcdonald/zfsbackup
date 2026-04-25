use clap::Parser;
use std::env::args;

use zfsbackup::JobBuilder;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
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

    datasets: Vec<String>,
}

fn main() {
    let args = Args::parse();
    let mut builder = JobBuilder::new(args.datasets, args.target);
    if args.dry_run {
        builder = builder.dryrun();
    }
    if let Some(cmd) = args.zfs_command {
        builder = builder.zfs_command(&cmd);
    }
    if let Some(cmd) = args.source_zfs_command {
        builder = builder.source_zfs_command(&cmd);
    }
    if let Some(cmd) = args.target_zfs_command {
        builder = builder.target_zfs_command(&cmd);
    }
    let job = builder.build().expect("asplode");
    job.run().expect("boom");
}
