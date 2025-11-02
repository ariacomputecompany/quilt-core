use nix::pty::{openpty, Winsize};
use nix::sys::termios::{tcgetattr, tcsetattr, SetArg};
use nix::unistd::{close, dup2, read, setsid, write};
use std::os::unix::io::RawFd;

/// PTY master/slave file descriptor pair
pub struct PtyPair {
    pub master: RawFd,
    pub slave: RawFd,
}

impl Drop for PtyPair {
    fn drop(&mut self) {
        // Clean up file descriptors
        let _ = close(self.master);
        let _ = close(self.slave);
    }
}

/// Creates a new PTY pair with specified terminal size
pub fn create_pty_with_size(rows: u16, cols: u16) -> Result<PtyPair, String> {
    let winsize = Winsize {
        ws_row: rows,
        ws_col: cols,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };

    let pty_result = openpty(Some(&winsize), None)
        .map_err(|e| format!("Failed to create PTY: {}", e))?;

    Ok(PtyPair {
        master: pty_result.master,
        slave: pty_result.slave,
    })
}

/// Creates a new PTY pair with default size (24x80)
#[allow(dead_code)]
pub fn create_pty() -> Result<PtyPair, String> {
    create_pty_with_size(24, 80)
}

/// Resizes an existing PTY
pub fn resize_pty(master: RawFd, rows: u16, cols: u16) -> Result<(), String> {
    use nix::libc::{ioctl, TIOCSWINSZ};

    let winsize = Winsize {
        ws_row: rows,
        ws_col: cols,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };

    let result = unsafe {
        ioctl(master, TIOCSWINSZ, &winsize as *const Winsize)
    };

    if result == -1 {
        Err("Failed to resize PTY".to_string())
    } else {
        Ok(())
    }
}

/// Setup slave PTY for use in child process
/// This should be called after fork() in the child process
#[allow(dead_code)]
pub fn setup_slave_pty(slave: RawFd) -> Result<(), String> {
    use std::os::unix::io::IntoRawFd;
    use std::fs::File;

    // Create a new session and become process group leader
    setsid().map_err(|e| format!("Failed to create new session: {}", e))?;

    // Duplicate slave to stdin, stdout, stderr
    dup2(slave, 0).map_err(|e| format!("Failed to dup2 stdin: {}", e))?;
    dup2(slave, 1).map_err(|e| format!("Failed to dup2 stdout: {}", e))?;
    dup2(slave, 2).map_err(|e| format!("Failed to dup2 stderr: {}", e))?;

    // Close the original slave fd if it's not stdin/stdout/stderr
    if slave > 2 {
        close(slave).map_err(|e| format!("Failed to close slave fd: {}", e))?;
    }

    // Open /dev/tty to make this the controlling terminal
    let tty = File::open("/dev/tty")
        .map_err(|e| format!("Failed to open /dev/tty: {}", e))?;
    let tty_fd = tty.into_raw_fd();

    // Set terminal attributes if available
    if let Ok(termios) = tcgetattr(tty_fd) {
        let _ = tcsetattr(tty_fd, SetArg::TCSANOW, &termios);
    }

    Ok(())
}

/// Read from PTY master (non-blocking)
#[allow(dead_code)]
pub fn read_from_pty(fd: RawFd, buf: &mut [u8]) -> Result<usize, String> {
    match read(fd, buf) {
        Ok(n) => Ok(n),
        Err(nix::errno::Errno::EAGAIN) => Ok(0),
        Err(e) => Err(format!("Failed to read from PTY: {}", e)),
    }
}

/// Write to PTY master
#[allow(dead_code)]
pub fn write_to_pty(fd: RawFd, buf: &[u8]) -> Result<usize, String> {
    match write(fd, buf) {
        Ok(n) => Ok(n),
        Err(nix::errno::Errno::EAGAIN) => Ok(0),
        Err(e) => Err(format!("Failed to write to PTY: {}", e)),
    }
}

/// Make PTY master non-blocking
pub fn make_pty_nonblocking(fd: RawFd) -> Result<(), String> {
    use nix::fcntl::{fcntl, FcntlArg, OFlag};

    // Get current flags
    let flags = fcntl(fd, FcntlArg::F_GETFL)
        .map_err(|e| format!("Failed to get fd flags: {}", e))?;

    // Add O_NONBLOCK flag
    let mut flags = OFlag::from_bits_truncate(flags);
    flags.insert(OFlag::O_NONBLOCK);

    // Set new flags
    fcntl(fd, FcntlArg::F_SETFL(flags))
        .map_err(|e| format!("Failed to set fd flags: {}", e))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_pty() {
        let pty = create_pty();
        assert!(pty.is_ok());
        let pty = pty.unwrap();
        assert!(pty.master > 0);
        assert!(pty.slave > 0);
    }

    #[test]
    fn test_create_pty_with_custom_size() {
        let pty = create_pty_with_size(40, 120);
        assert!(pty.is_ok());
    }

    #[test]
    fn test_resize_pty() {
        let pty = create_pty().unwrap();
        let result = resize_pty(pty.master, 50, 100);
        assert!(result.is_ok());
    }
}
