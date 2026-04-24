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

    pub fn build(self) -> Job {
        for source in &self.sources {
            let mut cmd = self.source_zfs_command.clone();
            let mut args = vec!["list".to_string(), source.clone()];
            cmd.append(&mut args);
            match command::exec_command(&cmd) {
                Ok(_) => (),
                Err(e) => panic!("Cannot list source dataset {}: {}", source, e),
            }
            println!("Source: {}", source);
        }
        Job {
            datasets: vec!["dataset1".to_string(), "dataset2".to_string()],
            target: self.target,
            source_zfs_command: self.source_zfs_command,
            target_zfs_command: self.target_zfs_command,
            dryrun: self.dryrun,
        }
    }
}

pub struct Job {
    datasets: Vec<String>,
    target: String,
    source_zfs_command: Vec<String>,
    target_zfs_command: Vec<String>,
    dryrun: bool,
}
