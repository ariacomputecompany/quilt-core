// Command handler modules for the unified Quilt binary
// Production-grade implementation with proper error handling

pub mod common;
pub mod lifecycle;
pub mod logs;
pub mod create;
pub mod exec;
pub mod volume;

// Re-export main command handlers for convenience
pub use lifecycle::{handle_status, handle_start, handle_stop, handle_kill, handle_remove};
pub use logs::handle_logs;
pub use create::handle_create;
pub use exec::handle_exec; // handle_shell is deprecated - use cli::shell::InteractiveShell
pub use volume::{handle_volume_create, handle_volume_list, handle_volume_inspect, handle_volume_remove};
pub use common::resolve_container_id;
