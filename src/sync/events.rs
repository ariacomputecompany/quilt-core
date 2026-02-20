use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const DEFAULT_BUFFER_SIZE: usize = 1000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContainerEvent {
    pub event_type: EventType,
    pub container_id: String,
    pub timestamp: u64,
    pub attributes: std::collections::HashMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventType {
    Created,
    Started,
    Stopped,
    Died,
    Removed,
    ExecStarted,
    ExecDied,
    ResourceLimit,
    HealthStatus,
    NetworkConnect,
    NetworkDisconnect,
    VolumeMount,
    VolumeUnmount,
}

impl EventType {
    pub fn as_str(&self) -> &'static str {
        match self {
            EventType::Created => "created",
            EventType::Started => "started",
            EventType::Stopped => "stopped",
            EventType::Died => "died",
            EventType::Removed => "removed",
            EventType::ExecStarted => "exec_started",
            EventType::ExecDied => "exec_died",
            EventType::ResourceLimit => "resource_limit",
            EventType::HealthStatus => "health_status",
            EventType::NetworkConnect => "network_connect",
            EventType::NetworkDisconnect => "network_disconnect",
            EventType::VolumeMount => "volume_mount",
            EventType::VolumeUnmount => "volume_unmount",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "created" => Some(EventType::Created),
            "started" => Some(EventType::Started),
            "stopped" => Some(EventType::Stopped),
            "died" => Some(EventType::Died),
            "removed" => Some(EventType::Removed),
            "exec_started" => Some(EventType::ExecStarted),
            "exec_died" => Some(EventType::ExecDied),
            "resource_limit" => Some(EventType::ResourceLimit),
            "health_status" => Some(EventType::HealthStatus),
            "network_connect" => Some(EventType::NetworkConnect),
            "network_disconnect" => Some(EventType::NetworkDisconnect),
            "volume_mount" => Some(EventType::VolumeMount),
            "volume_unmount" => Some(EventType::VolumeUnmount),
            _ => None,
        }
    }
}

/// Ring buffer for container events
pub struct EventRingBuffer {
    buffer: Arc<RwLock<VecDeque<ContainerEvent>>>,
    max_size: usize,
}

impl EventRingBuffer {
    pub fn new(max_size: Option<usize>) -> Self {
        Self {
            buffer: Arc::new(RwLock::new(VecDeque::new())),
            max_size: max_size.unwrap_or(DEFAULT_BUFFER_SIZE),
        }
    }

    /// Add an event to the ring buffer
    pub fn push(&self, event: ContainerEvent) {
        let mut buffer = self.buffer.write();

        // Remove oldest events if at capacity
        while buffer.len() >= self.max_size {
            buffer.pop_front();
        }

        buffer.push_back(event);
    }

    /// Create and push an event
    pub fn emit(
        &self,
        event_type: EventType,
        container_id: &str,
        attributes: Option<std::collections::HashMap<String, String>>,
    ) {
        let event = ContainerEvent {
            event_type,
            container_id: container_id.to_string(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            attributes: attributes.unwrap_or_default(),
        };

        self.push(event);
    }

    // get_all() method removed - not used in core functionality

    /// Get events filtered by criteria
    pub fn get_filtered(
        &self,
        container_ids: Option<&[String]>,
        event_types: Option<&[EventType]>,
        since_timestamp: Option<u64>,
    ) -> Vec<ContainerEvent> {
        let buffer = self.buffer.read();

        buffer
            .iter()
            .rev()
            .filter(|event| {
                // Filter by container IDs
                if let Some(ids) = container_ids {
                    if !ids.is_empty() && !ids.contains(&event.container_id) {
                        return false;
                    }
                }

                // Filter by event types
                if let Some(types) = event_types {
                    if !types.is_empty() && !types.contains(&event.event_type) {
                        return false;
                    }
                }

                // Filter by timestamp
                if let Some(since) = since_timestamp {
                    if event.timestamp < since {
                        return false;
                    }
                }

                true
            })
            .cloned()
            .collect()
    }

    // Utility methods clear(), len(), is_empty() removed - not used in core functionality
}

/// Global event buffer instance
static EVENT_BUFFER: once_cell::sync::OnceCell<EventRingBuffer> = once_cell::sync::OnceCell::new();

pub fn global_event_buffer() -> &'static EventRingBuffer {
    EVENT_BUFFER.get_or_init(|| EventRingBuffer::new(None))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ring_buffer_capacity() {
        let buffer = EventRingBuffer::new(Some(3));

        // Add 4 events to a buffer with capacity 3
        for i in 0..4 {
            buffer.emit(EventType::Created, &format!("container-{}", i), None);
        }

        // Should only have 3 events (oldest was removed)
        assert_eq!(buffer.len(), 3);

        let events = buffer.get_all();
        assert_eq!(events[0].container_id, "container-3");
        assert_eq!(events[1].container_id, "container-2");
        assert_eq!(events[2].container_id, "container-1");
    }

    #[test]
    fn test_event_filtering() {
        let buffer = EventRingBuffer::new(None);

        buffer.emit(EventType::Created, "container-1", None);
        buffer.emit(EventType::Started, "container-1", None);
        buffer.emit(EventType::Created, "container-2", None);
        buffer.emit(EventType::Died, "container-1", None);

        // Filter by container ID
        let events = buffer.get_filtered(Some(&["container-1".to_string()]), None, None);
        assert_eq!(events.len(), 3);

        // Filter by event type
        let events = buffer.get_filtered(None, Some(&[EventType::Created]), None);
        assert_eq!(events.len(), 2);
    }
}
