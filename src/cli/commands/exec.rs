// Container execution commands (exec and shell)
// Production implementation with output capture support

use crate::quilt::quilt_service_client::QuiltServiceClient;
use crate::quilt::{ExecContainerRequest, ExecContainerResponse};
use crate::utils::logger::Logger;
use super::common::resolve_container_id;
use tonic::transport::Channel;
use std::collections::HashMap;

/// Execute a command in a container
pub async fn handle_exec(
    client: &mut QuiltServiceClient<Channel>,
    container: Option<String>,
    by_name: bool,
    command: Vec<String>,
    workdir: Option<String>,
    capture: bool,
) -> Result<i32, Box<dyn std::error::Error>> {
    let container_id = resolve_container_id(client, &container, by_name).await?;

    if command.is_empty() {
        return Err("Command is required".into());
    }

    Logger::info(&format!("Executing command in container: {}", container_id));
    Logger::detail("command", &command.join(" "));

    let request = tonic::Request::new(ExecContainerRequest {
        container_id: container_id.clone(),
        command: command.clone(),
        working_directory: workdir.unwrap_or_default(),
        environment: HashMap::new(),
        capture_output: capture,
        container_name: String::new(),
        copy_script: false,
    });

    match client.exec_container(request).await {
        Ok(response) => {
            let res: ExecContainerResponse = response.into_inner();

            if res.success {
                // Display captured output if any
                if capture {
                    if !res.stdout.is_empty() {
                        Logger::blank();
                        print!("{}", res.stdout);
                        Logger::blank();
                    }
                    if !res.stderr.is_empty() {
                        Logger::warning("Standard Error:");
                        eprint!("{}", res.stderr);
                    }
                }

                if res.exit_code == 0 {
                    Logger::success(&format!("Command executed successfully (exit code: {})", res.exit_code));
                } else {
                    Logger::warning(&format!("Command exited with code: {}", res.exit_code));
                }

                Ok(res.exit_code)
            } else {
                Logger::error(&format!("Failed to execute command: {}", res.error_message));
                Err(res.error_message.into())
            }
        }
        Err(e) => {
            Logger::error(&format!("Failed to execute command: {}", e));
            Err(e.into())
        }
    }
}

/// Enter interactive shell in a container
///
/// **DEPRECATED**: This non-interactive implementation is superseded by InteractiveShell.
/// This function does NOT provide a proper interactive shell - it just executes the shell
/// command once and exits. Use `cli::shell::InteractiveShell` instead for proper PTY support.
#[deprecated(note = "Use cli::shell::InteractiveShell for proper interactive shell support")]
#[allow(dead_code)]
pub async fn handle_shell(
    client: &mut QuiltServiceClient<Channel>,
    container: Option<String>,
    by_name: bool,
    shell: String,
) -> Result<i32, Box<dyn std::error::Error>> {
    // For now, exec the shell with capture disabled (live output)
    // TODO: Future enhancement - use ExecContainerInteractive for full PTY support
    Logger::info("Starting interactive shell (use Ctrl+D to exit)");

    let shell_command = vec![shell];
    handle_exec(client, container, by_name, shell_command, None, false).await
}
