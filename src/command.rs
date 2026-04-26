use crate::progress::BackupEvent;
use std::io;
use std::io::Read;
use std::sync::mpsc::Sender;
use std::time::{Duration, Instant};

pub fn exec_command(command: &Vec<&str>) -> Result<String, String> {
    if command.is_empty() {
        return Err("Command is empty".to_string());
    }
    let mut cmd = std::process::Command::new(command[0]);
    if command.len() > 1 {
        cmd.args(&command[1..]);
    }
    let output = cmd.output().map_err(|e| e.to_string())?;
    if !output.status.success() {
        return Err(format!(
            "Command {:?} failed with status {}: {}",
            command,
            output.status,
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let output_str = String::from_utf8_lossy(&output.stdout).to_string();
    Ok(output_str)
}

struct CountingReader<R: Read> {
    inner: R,
    sender: Sender<BackupEvent>,
    bytes: u64,
    last_send: Instant,
    total: Option<u64>,
}

impl<R: Read> Read for CountingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.bytes += n as u64;
        if self.last_send.elapsed().as_millis() >= 100 {
            self.sender
                .send(BackupEvent::BytesTransferred {
                    bytes: self.bytes,
                    estimated_total: self.total,
                })
                .ok();
            self.last_send = Instant::now();
        }
        Ok(n)
    }
}

impl<R: Read> CountingReader<R> {
    fn new(inner: R, sender: Sender<BackupEvent>, total: Option<u64>) -> Self {
        Self {
            inner,
            sender,
            total,
            bytes: 0,
            last_send: Instant::now() - Duration::from_secs(1),
        }
    }
}

pub fn exec_piped_commands(
    source: &Vec<&str>,
    dest: &Vec<&str>,
    sender: Option<Sender<BackupEvent>>,
    total: Option<u64>,
) -> Result<(), String> {
    if source.is_empty() || dest.is_empty() {
        return Err("Source or destination command is empty".to_string());
    }
    let mut send_cmd = std::process::Command::new(source[0]);
    if source.len() > 1 {
        send_cmd.args(&source[1..]);
    }
    let mut receive_cmd = std::process::Command::new(dest[0]);
    if dest.len() > 1 {
        receive_cmd.args(&dest[1..]);
    }

    let mut send_process = send_cmd
        .stdout(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| e.to_string())?;

    let mut receive_process = receive_cmd
        .stdin(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| e.to_string())?;

    let send_stdout = send_process.stdout.take().unwrap();
    let mut receive_stdin = receive_process.stdin.take().unwrap();

    let mut reader: Box<dyn Read> = match sender {
        Some(s) => Box::new(CountingReader::new(send_stdout, s, total)),
        None => Box::new(send_stdout),
    };
    std::io::copy(&mut reader, &mut receive_stdin).map_err(|e| e.to_string())?;

    let receive_status = receive_process.wait().map_err(|e| e.to_string())?;
    let send_status = send_process.wait().map_err(|e| e.to_string())?;
    if !receive_status.success() {
        send_process.kill().ok();
        return Err(format!(
            "Receive {:?} failed with status {}",
            dest, receive_status
        ));
    }
    if !send_status.success() {
        return Err(format!(
            "Send command {:?} failed with status {}",
            source, send_status
        ));
    }
    Ok(())
}
