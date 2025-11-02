// Allow dead code temporarily until client command integration is complete
#![allow(dead_code)]

use std::env;
use std::io::{self, Write as _};

/// ANSI color codes matching the specified color scheme
pub struct Colors;

impl Colors {
    /// Primary actions/success - #5FADEB
    pub const MAIN: &'static str = "\x1b[38;2;95;173;235m";
    /// Info/progress - #4A9BD9
    pub const SECONDARY: &'static str = "\x1b[38;2;74;155;217m";
    /// Technical details - #808080
    pub const DIM_GRAY: &'static str = "\x1b[38;2;128;128;128m";
    /// Errors - #FF3B30
    pub const ERROR: &'static str = "\x1b[38;2;255;59;48m";
    /// Warnings - #FF9500
    pub const WARNING: &'static str = "\x1b[38;2;255;149;0m";
    /// Reset all formatting
    pub const RESET: &'static str = "\x1b[0m";
    /// Bold text
    pub const BOLD: &'static str = "\x1b[1m";
}

/// Unicode symbols for different message types
pub struct Symbols;

impl Symbols {
    pub const SUCCESS: &'static str = "✓";
    pub const ERROR: &'static str = "✗";
    pub const WARNING: &'static str = "!";
    pub const INFO: &'static str = "ℹ";
    pub const ARROW: &'static str = "→";
    pub const BULLET: &'static str = "•";
}

/// Production-grade logger with Bun-inspired clean output
pub struct Logger;

impl Logger {
    /// Log a success message (green checkmark)
    pub fn success(message: &str) {
        println!("{}{} {}{}",
            Colors::MAIN, Symbols::SUCCESS, message, Colors::RESET);
    }

    /// Log an error message (red X) to stderr
    pub fn error(message: &str) {
        eprintln!("{}{} {}{}",
            Colors::ERROR, Symbols::ERROR, message, Colors::RESET);
    }

    /// Log a warning message (orange exclamation)
    pub fn warning(message: &str) {
        println!("{}{} {}{}",
            Colors::WARNING, Symbols::WARNING, message, Colors::RESET);
    }

    /// Log an info message (blue info icon)
    pub fn info(message: &str) {
        println!("{}{} {}{}",
            Colors::SECONDARY, Symbols::INFO, message, Colors::RESET);
    }

    /// Log a debug message (only shown if QUILT_DEBUG=1)
    pub fn debug(message: &str) {
        if env::var("QUILT_DEBUG").is_ok() {
            println!("{}{} {}{}",
                Colors::DIM_GRAY, Symbols::BULLET, message, Colors::RESET);
        }
    }

    /// Display a key-value detail line (indented, dimmed label with colored value)
    /// Example: "  id → abc123..."
    pub fn detail(label: &str, value: &str) {
        println!("  {}{}{} {} {}{}",
            Colors::DIM_GRAY, label, Colors::RESET,
            Symbols::ARROW,
            Colors::SECONDARY, value);
    }

    /// Display a section header (bold, main color)
    /// Example: "Container Status"
    pub fn section(title: &str) {
        println!("\n{}{}{}{}",
            Colors::BOLD, Colors::MAIN, title, Colors::RESET);
    }

    /// Display an indented bullet point
    /// Example: "  • Network configured"
    pub fn item(message: &str) {
        println!("  {}{} {}{}",
            Colors::DIM_GRAY, Symbols::BULLET, message, Colors::RESET);
    }

    /// Display a progress message (stays on same line, use with caution)
    pub fn progress(message: &str) {
        print!("{}{} {}{}\r",
            Colors::SECONDARY, Symbols::INFO, message, Colors::RESET);
        io::stdout().flush().ok();
    }

    /// Clear the current line (useful after progress messages)
    pub fn clear_line() {
        print!("\r\x1b[K");
        io::stdout().flush().ok();
    }

    /// Display an error with a suggestion
    pub fn error_with_hint(error: &str, hint: &str) {
        eprintln!("{}{} {}{}",
            Colors::ERROR, Symbols::ERROR, error, Colors::RESET);
        eprintln!("  {}{} {}{}",
            Colors::DIM_GRAY, Symbols::ARROW, hint, Colors::RESET);
    }

    /// Display a blank line (for spacing)
    pub fn blank() {
        println!();
    }
}

/// Check if we should suppress output (for --quiet mode)
pub fn is_quiet_mode() -> bool {
    env::var("QUILT_QUIET").is_ok()
}

/// Check if we should show verbose output
pub fn is_verbose_mode() -> bool {
    env::var("QUILT_VERBOSE").is_ok() || env::var("QUILT_DEBUG").is_ok()
}

/// Check if colors should be disabled
pub fn should_use_colors() -> bool {
    // Disable colors if NO_COLOR is set or if not a TTY
    env::var("NO_COLOR").is_err() &&
    console::Term::stdout().is_term()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_logger_methods_dont_panic() {
        Logger::success("test");
        Logger::error("test");
        Logger::warning("test");
        Logger::info("test");
        Logger::detail("key", "value");
        Logger::section("Test Section");
        Logger::item("test item");
    }
}
