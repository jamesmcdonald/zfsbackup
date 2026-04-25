mod command;

pub struct JobBuilder {
    sources: Vec<String>,
    target: String,
    source_zfs_command: Vec<String>,
    target_zfs_command: Vec<String>,
    dryrun: bool,
}

fn parse_command(commandstr: &str) -> Vec<String> {
    commandstr
        .split_whitespace()
        .map(|s| s.to_string())
        .collect()
}

impl JobBuilder {
    pub fn new(sources: Vec<String>, target: String) -> Self {
        JobBuilder {
            sources,
            target,
            source_zfs_command: vec!["zfs".to_string()],
            target_zfs_command: vec!["zfs".to_string()],
            dryrun: false,
        }
    }

    pub fn source_zfs_command(mut self, commandstr: &str) -> Self {
        let command: Vec<String>;
        command = parse_command(commandstr);
        self.source_zfs_command = command;
        self
    }

    pub fn target_zfs_command(mut self, commandstr: &str) -> Self {
        let command: Vec<String>;
        command = parse_command(commandstr);
        self.target_zfs_command = command;
        self
    }

    pub fn zfs_command(mut self, commandstr: &str) -> Self {
        let command: Vec<String>;
        command = parse_command(commandstr);
        self.source_zfs_command = command.clone();
        self.target_zfs_command = command;
        self
    }

    pub fn dryrun(mut self) -> Self {
        self.dryrun = true;
        self
    }

    pub fn build(self) -> Result<Job, String> {
        let mut job = Job {
            datasets: vec![],
            target: self.target,
            source_zfs_command: self.source_zfs_command,
            target_zfs_command: self.target_zfs_command,
            dryrun: self.dryrun,
        };
        let mut datasets: Vec<String> = vec![];
        for source in &self.sources {
            let recurse = source.ends_with("/...");
            let source = source.trim_end_matches("/...");

            let mut args = vec!["list", "-H", "-o", "name"];
            if recurse {
                args.push("-r");
            }
            args.push(source);

            let mut cmd = job.get_side_command(JobSide::Source);
            cmd.extend(args);
            let output = command::exec_command(&cmd)?;
            datasets = [datasets, output.lines().map(&str::to_string).collect()].concat();
        }
        job.datasets = datasets;
        Ok(job)
    }
}

pub struct Job {
    datasets: Vec<String>,
    target: String,
    source_zfs_command: Vec<String>,
    target_zfs_command: Vec<String>,
    dryrun: bool,
}

enum JobSide {
    Source,
    Destination,
}

impl Job {
    pub fn dump(&self) {
        println!("Datasets: {:?}", self.datasets);
        println!("Target: {}", self.target);
        println!("Source ZFS Command: {:?}", self.source_zfs_command);
        println!("Target ZFS Command: {:?}", self.target_zfs_command);
        println!("Dryrun: {}", self.dryrun);
    }

    fn get_side_command(&self, side: JobSide) -> Vec<&str> {
        match side {
            JobSide::Source => self.source_zfs_command.iter().map(|s| s.as_str()).collect(),
            JobSide::Destination => self.target_zfs_command.iter().map(|s| s.as_str()).collect(),
        }
    }
}
