use std::env::args;

use zfsbackup::JobBuilder;

fn main() {
    let job = JobBuilder::new(args().skip(1).collect(), "backup".to_string())
        .build()
        .expect("asplode");
    job.dump()
}
