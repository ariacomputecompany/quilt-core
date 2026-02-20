// src/icc/messaging.rs
// Inter-container message broker for container-to-container communication

use crate::utils::console::ConsoleLogger;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub struct MessageBroker {
    channels: Arc<Mutex<HashMap<String, Vec<String>>>>,
}

#[allow(dead_code)] // Inter-container messaging methods for future features
impl MessageBroker {
    pub fn new() -> Self {
        Self {
            channels: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn start(&self) {
        ConsoleLogger::info("🔗 Message broker started for inter-container communication");
    }

    /// Send a message from one container to another
    pub fn send_message(
        &self,
        from_container: &str,
        to_container: &str,
        message: &str,
    ) -> Result<(), String> {
        let full_message = format!("[{}]: {}", from_container, message);

        if let Ok(mut channels) = self.channels.lock() {
            channels
                .entry(to_container.to_string())
                .or_insert_with(Vec::new)
                .push(full_message);

            ConsoleLogger::debug(&format!(
                "📧 Message sent from {} to {}: {}",
                from_container, to_container, message
            ));
            Ok(())
        } else {
            Err("Failed to acquire message channels lock".to_string())
        }
    }

    /// Get messages for a container
    pub fn get_messages(&self, container_id: &str) -> Result<Vec<String>, String> {
        if let Ok(mut channels) = self.channels.lock() {
            Ok(channels.remove(container_id).unwrap_or_default())
        } else {
            Err("Failed to acquire message channels lock".to_string())
        }
    }

    /// Clear all messages for a container (cleanup)
    pub fn cleanup_container(&self, container_id: &str) {
        if let Ok(mut channels) = self.channels.lock() {
            channels.remove(container_id);
            ConsoleLogger::debug(&format!(
                "🧹 Cleaned up messages for container {}",
                container_id
            ));
        }
    }
}
