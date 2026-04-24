pub fn exec_command(command: &Vec<String>) -> Result<(), String> {
    if command.is_empty() {
        return Err("Command is empty".to_string());
    }
    let mut cmd = std::process::Command::new(&command[0]);
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
    Ok(())
}
