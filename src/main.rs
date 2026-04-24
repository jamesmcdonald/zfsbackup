use zfsbackup::JobBuilder;

fn main() {
    let job = JobBuilder::new(vec!["chonk".to_string()], "backup".to_string()).build();
}
