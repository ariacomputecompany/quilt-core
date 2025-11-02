// Allow dead code temporarily until client command integration is complete
#![allow(dead_code)]

use std::io::{self, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use termion::raw::{IntoRawMode, RawTerminal};
use termion::terminal_size;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Streaming;

use crate::quilt::quilt_service_client::QuiltServiceClient;
use crate::quilt::{
    ExecInteractiveRequest, ExecInteractiveResponse, ExecStartRequest, TerminalResize,
};

/// RAII guard that restores terminal to normal mode on drop
struct RawTerminalGuard {
    _stdout: RawTerminal<io::Stdout>,
}

impl RawTerminalGuard {
    fn new() -> Result<Self, io::Error> {
        let stdout = io::stdout().into_raw_mode()?;
        Ok(Self { _stdout: stdout })
    }
}

impl Drop for RawTerminalGuard {
    fn drop(&mut self) {
        // Terminal is automatically restored by RawTerminal's drop
        // Ensure we write a newline before exiting
        let _ = io::stdout().write_all(b"\r\n");
        let _ = io::stdout().flush();
    }
}

/// Interactive shell session manager
pub struct InteractiveShell {
    container_id: String,
    command: Vec<String>,
    terminal_rows: u16,
    terminal_cols: u16,
    resize_flag: Arc<AtomicBool>,
}

impl InteractiveShell {
    /// Create a new interactive shell session
    pub fn new(container_id: String, command: Vec<String>) -> Result<Self, String> {
        // Get terminal size, use default if not a TTY
        let (cols, rows) = terminal_size().unwrap_or((80, 24));

        let resize_flag = Arc::new(AtomicBool::new(false));

        // Setup SIGWINCH handler for terminal resize events
        let resize_flag_clone = resize_flag.clone();
        if let Err(e) = signal_hook::flag::register(signal_hook::consts::SIGWINCH, resize_flag_clone) {
            eprintln!("Warning: Failed to register SIGWINCH handler: {}", e);
        }

        Ok(Self {
            container_id,
            command,
            terminal_rows: rows,
            terminal_cols: cols,
            resize_flag,
        })
    }

    /// Run the interactive shell session
    pub async fn run(
        mut self,
        mut client: QuiltServiceClient<tonic::transport::Channel>,
    ) -> Result<i32, String> {
        // Put terminal in raw mode (RAII cleanup on drop)
        // Skip raw mode if stdin is not a TTY (e.g., when piping input for testing)
        let _terminal_guard = match RawTerminalGuard::new() {
            Ok(guard) => Some(guard),
            Err(_e) => None
        };

        // Create channel for sending requests to server
        let (tx, rx) = mpsc::channel::<ExecInteractiveRequest>(32);

        // Send initial start request
        let start_request = ExecInteractiveRequest {
            message: Some(crate::quilt::exec_interactive_request::Message::Start(
                ExecStartRequest {
                    container_id: self.container_id.clone(),
                    command: self.command.clone(),
                    working_directory: String::new(),
                    environment: Default::default(),
                    container_name: String::new(),
                    terminal_rows: self.terminal_rows as u32,
                    terminal_cols: self.terminal_cols as u32,
                },
            )),
        };

        tx.send(start_request)
            .await
            .map_err(|e| format!("Failed to send start request: {}", e))?;

        // Create streaming request
        let request_stream = ReceiverStream::new(rx);

        let response = client
            .exec_container_interactive(request_stream)
            .await
            .map_err(|e| format!("Failed to start interactive session: {}", e))?;

        let mut response_stream = response.into_inner();

        // Spawn task to forward stdin to container
        let tx_clone = tx.clone();
        let resize_flag = self.resize_flag.clone();
        let stdin_task = tokio::spawn(async move {
            Self::forward_stdin(tx_clone, resize_flag).await
        });

        // Spawn task to receive and display output from container
        let exit_code = self.handle_output(&mut response_stream, tx.clone()).await?;

        // Abort stdin task - no need to wait since shell has exited
        stdin_task.abort();

        Ok(exit_code)
    }

    /// Forward stdin to the container
    async fn forward_stdin(
        tx: mpsc::Sender<ExecInteractiveRequest>,
        resize_flag: Arc<AtomicBool>,
    ) -> Result<(), String> {
        let mut stdin = tokio::io::stdin();
        let mut buffer = [0u8; 4096];
        let mut last_rows = 0u16;
        let mut last_cols = 0u16;

        loop {
            // Check if terminal was resized
            if resize_flag.swap(false, Ordering::Relaxed) {
                if let Ok((cols, rows)) = terminal_size() {
                    if rows != last_rows || cols != last_cols {
                        last_rows = rows;
                        last_cols = cols;

                        let resize_request = ExecInteractiveRequest {
                            message: Some(
                                crate::quilt::exec_interactive_request::Message::Resize(
                                    TerminalResize {
                                        rows: rows as u32,
                                        cols: cols as u32,
                                    },
                                ),
                            ),
                        };

                        if tx.send(resize_request).await.is_err() {
                            break; // Channel closed, exit
                        }
                    }
                }
            }

            // Try to read from stdin with timeout
            tokio::select! {
                result = stdin.read(&mut buffer) => {
                    match result {
                        Ok(0) => break, // EOF
                        Ok(n) => {
                            let stdin_data = ExecInteractiveRequest {
                                message: Some(
                                    crate::quilt::exec_interactive_request::Message::StdinData(
                                        buffer[..n].to_vec(),
                                    ),
                                ),
                            };

                            if tx.send(stdin_data).await.is_err() {
                                break; // Channel closed, exit
                            }
                        }
                        Err(_) => break,
                    }
                }
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {
                    // Timeout - check resize flag again
                    continue;
                }
            }
        }

        Ok(())
    }

    /// Handle output from the container and display to stdout
    async fn handle_output(
        &mut self,
        response_stream: &mut Streaming<ExecInteractiveResponse>,
        _tx: mpsc::Sender<ExecInteractiveRequest>,
    ) -> Result<i32, String> {
        let mut stdout = io::stdout();
        let mut exit_code = 0;

        // Event-driven loop - no timeouts, messages arrive when ready
        loop {
            let response = match response_stream.message().await {
                Ok(Some(r)) => r,
                Ok(None) => break,
                Err(e) => return Err(format!("Stream error: {}", e)),
            };

            if let Some(message) = response.message {
                match message {
                    crate::quilt::exec_interactive_response::Message::StdoutData(data) => {
                        // Write stdout data to terminal
                        if let Err(e) = stdout.write_all(&data) {
                            eprintln!("Failed to write to stdout: {}", e);
                            break;
                        }
                        if let Err(e) = stdout.flush() {
                            eprintln!("Failed to flush stdout: {}", e);
                            break;
                        }
                    }
                    crate::quilt::exec_interactive_response::Message::StderrData(data) => {
                        // Write stderr data to terminal (PTY combines stdout/stderr)
                        if let Err(e) = stdout.write_all(&data) {
                            eprintln!("Failed to write stderr to stdout: {}", e);
                            break;
                        }
                        if let Err(e) = stdout.flush() {
                            eprintln!("Failed to flush stdout: {}", e);
                            break;
                        }
                    }
                    crate::quilt::exec_interactive_response::Message::ExitCode(code) => {
                        exit_code = code;
                        break;
                    }
                    crate::quilt::exec_interactive_response::Message::Error(error) => {
                        // Write error in red if terminal supports it
                        let _ = stdout.write_all(b"\r\n");
                        let _ = stdout.write_all(format!("Error: {}\r\n", error).as_bytes());
                        let _ = stdout.flush();
                        return Err(error);
                    }
                }
            }
        }

        Ok(exit_code)
    }
}
