use std::io::Read;

/// A trait for filters that can be applied to the output of a command before
/// it is piped to another command.
pub trait Filter {
    fn filter(&self, reader: Box<dyn Read>) -> Box<dyn Read>;
}

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

pub fn exec_piped_commands(
    source: &Vec<&str>,
    dest: &Vec<&str>,
    filters: Vec<Box<dyn Filter>>,
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

    let mut reader: Box<dyn Read> = Box::new(send_stdout);
    for filter in filters {
        reader = filter.filter(reader);
    }

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
