// src/icc/dns.rs
// DNS server for container name resolution

// Warnings handled at crate level

use crate::utils::console::ConsoleLogger;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, RwLock};
use tokio::net::UdpSocket;
use trust_dns_proto::op::{Message, MessageType, OpCode, ResponseCode};
use trust_dns_proto::rr::{DNSClass, RData, Record, RecordType};

#[derive(Debug, Clone)]
pub struct DnsEntry {
    pub container_id: String,
    pub container_name: String,
    pub ip_address: IpAddr,
    pub ttl: u32,
}

pub struct DnsServer {
    entries: Arc<RwLock<HashMap<String, DnsEntry>>>,
    bind_address: SocketAddr,
    domain_suffix: String,
}

impl DnsServer {
    pub fn new(bind_address: SocketAddr) -> Self {
        Self {
            entries: Arc::new(RwLock::new(HashMap::new())),
            bind_address,
            domain_suffix: "quilt.local".to_string(),
        }
    }

    /// Register a container with DNS
    pub fn register_container(
        &self,
        container_id: &str,
        container_name: &str,
        ip_address: &str,
    ) -> Result<(), String> {
        let ip = ip_address
            .parse::<IpAddr>()
            .map_err(|e| format!("Invalid IP address {}: {}", ip_address, e))?;

        ConsoleLogger::debug(&format!(
            "🔧 [DNS-REG] Registering container DNS: id={}, name={}, ip={}",
            container_id, container_name, ip_address
        ));

        let entry = DnsEntry {
            container_id: container_id.to_string(),
            container_name: container_name.to_string(),
            ip_address: ip,
            ttl: 300, // 5 minutes
        };

        // Register both by name and by ID
        let mut entries = self
            .entries
            .write()
            .map_err(|e| format!("Failed to acquire write lock: {}", e))?;

        entries.insert(container_name.to_string(), entry.clone());
        entries.insert(container_id.to_string(), entry.clone());

        // Also register with domain suffix
        let fqdn = format!("{}.{}", container_name, self.domain_suffix);
        entries.insert(fqdn, entry.clone());

        ConsoleLogger::success(&format!(
            "✅ [DNS-REG] DNS registration complete: {} entries now registered",
            entries.len()
        ));

        // Debug: List all registered entries
        for (name, entry) in entries.iter() {
            ConsoleLogger::debug(&format!(
                "📋 [DNS-REG] Entry: {} -> {} ({})",
                name, entry.ip_address, entry.container_id
            ));
        }

        ConsoleLogger::info(&format!(
            "DNS: Registered {} ({}) -> {}",
            container_name, container_id, ip_address
        ));

        Ok(())
    }

    /// Unregister a container from DNS
    pub fn unregister_container(&self, container_id: &str) -> Result<(), String> {
        let mut entries = self
            .entries
            .write()
            .map_err(|e| format!("Failed to acquire write lock: {}", e))?;

        // Find and remove all entries for this container
        let to_remove: Vec<String> = entries
            .iter()
            .filter(|(_, entry)| entry.container_id == container_id)
            .map(|(key, _)| key.clone())
            .collect();

        for key in to_remove {
            entries.remove(&key);
            ConsoleLogger::debug(&format!("DNS: Unregistered {}", key));
        }

        Ok(())
    }

    /// Start the DNS server
    pub async fn start(&self) -> Result<(), String> {
        let socket = UdpSocket::bind(&self.bind_address)
            .await
            .map_err(|e| format!("Failed to bind DNS server to {}: {}", self.bind_address, e))?;

        ConsoleLogger::info(&format!("DNS server listening on {}", self.bind_address));

        let entries = self.entries.clone();
        let domain_suffix = self.domain_suffix.clone();

        tokio::spawn(async move {
            let mut buf = vec![0u8; 512];

            loop {
                match socket.recv_from(&mut buf).await {
                    Ok((len, src)) => match Message::from_vec(&buf[..len]) {
                        Ok(query) => {
                            ConsoleLogger::debug(&format!(
                                "🔍 [DNS-QUERY] Received DNS query from {}: {} queries",
                                src,
                                query.query_count()
                            ));
                            for q in query.queries() {
                                ConsoleLogger::debug(&format!(
                                    "🔍 [DNS-QUERY] Query: {} (type: {:?})",
                                    q.name(),
                                    q.query_type()
                                ));
                            }

                            if let Ok(response) =
                                Self::handle_query(query, &entries, &domain_suffix)
                            {
                                ConsoleLogger::debug(&format!(
                                    "📤 [DNS-RESPONSE] Sending response with {} answers",
                                    response.answer_count()
                                ));
                                if let Ok(response_bytes) = response.to_vec() {
                                    let _ = socket.send_to(&response_bytes, src).await;
                                }
                            } else {
                                ConsoleLogger::warning(&format!(
                                    "❌ [DNS-QUERY] Failed to handle query from {}",
                                    src
                                ));
                            }
                        }
                        Err(e) => {
                            ConsoleLogger::debug(&format!(
                                "❌ [DNS-PARSE] Failed to parse query: {}",
                                e
                            ));
                        }
                    },
                    Err(e) => {
                        ConsoleLogger::warning(&format!("DNS: Failed to receive packet: {}", e));
                    }
                }
            }
        });

        Ok(())
    }

    /// Handle a DNS query
    fn handle_query(
        query: Message,
        entries: &Arc<RwLock<HashMap<String, DnsEntry>>>,
        domain_suffix: &str,
    ) -> Result<Message, String> {
        let mut response = Message::new();
        response.set_id(query.id());
        response.set_message_type(MessageType::Response);
        response.set_op_code(OpCode::Query);
        response.set_authoritative(true);
        response.set_recursion_desired(query.recursion_desired());
        response.set_recursion_available(false);

        // Copy the query to the response
        for q in query.queries() {
            response.add_query(q.clone());
        }

        // Only handle standard queries
        if query.op_code() != OpCode::Query {
            response.set_response_code(ResponseCode::NotImp);
            return Ok(response);
        }

        // Process each query
        for query in query.queries() {
            if query.query_type() == RecordType::A && query.query_class() == DNSClass::IN {
                let name = query.name().to_string().trim_end_matches('.').to_string();

                // Try to find the entry
                let entries = entries
                    .read()
                    .map_err(|e| format!("Failed to read entries: {}", e))?;

                if let Some(entry) = entries.get(&name) {
                    if let IpAddr::V4(ipv4) = entry.ip_address {
                        ConsoleLogger::debug(&format!(
                            "🔍 [DNS-MATCH] Found entry for {}: {} (ttl: {})",
                            name, ipv4, entry.ttl
                        ));

                        // CRITICAL FIX: Use proper builder pattern to avoid temporary value issues
                        let record = Record::new()
                            .set_name(query.name().clone())
                            .set_ttl(entry.ttl)
                            .set_rr_type(RecordType::A)
                            .set_dns_class(DNSClass::IN)
                            .set_data(Some(RData::A(trust_dns_proto::rr::rdata::A::from(ipv4))))
                            .clone();

                        ConsoleLogger::debug(&format!(
                            "🔧 [DNS-RECORD] Created A record for {}: name={}, ttl={}, data={:?}",
                            name,
                            record.name(),
                            record.ttl(),
                            record.data()
                        ));

                        ConsoleLogger::debug(&format!(
                            "🔧 [DNS-RECORD] Pre-add: response has {} answers",
                            response.answer_count()
                        ));
                        response.add_answer(record.clone());
                        ConsoleLogger::debug(&format!(
                            "🔧 [DNS-RECORD] Post-add: response has {} answers",
                            response.answer_count()
                        ));
                        ConsoleLogger::debug(&format!(
                            "✅ [DNS-ADD] Added answer to response. Total answers: {}",
                            response.answer_count()
                        ));
                        ConsoleLogger::debug(&format!("DNS: Resolved {} -> {}", name, ipv4));
                    }
                } else {
                    // Try without domain suffix if it was included
                    ConsoleLogger::debug(&format!(
                        "🔍 [DNS-FALLBACK] No direct match for '{}', trying fallback patterns",
                        name
                    ));
                    let short_name = name
                        .trim_end_matches(&format!(".{}", domain_suffix))
                        .trim_end_matches(domain_suffix);

                    ConsoleLogger::debug(&format!(
                        "🔍 [DNS-FALLBACK] Trying short name: '{}'",
                        short_name
                    ));

                    if let Some(entry) = entries.get(short_name) {
                        if let IpAddr::V4(ipv4) = entry.ip_address {
                            ConsoleLogger::debug(&format!("🔍 [DNS-MATCH-FALLBACK] Found entry for short name {}: {} (ttl: {})", short_name, ipv4, entry.ttl));

                            // CRITICAL FIX: Use proper builder pattern to avoid temporary value issues
                            let record = Record::new()
                                .set_name(query.name().clone())
                                .set_ttl(entry.ttl)
                                .set_rr_type(RecordType::A)
                                .set_dns_class(DNSClass::IN)
                                .set_data(Some(RData::A(trust_dns_proto::rr::rdata::A::from(ipv4))))
                                .clone();

                            ConsoleLogger::debug(&format!("🔧 [DNS-RECORD-FALLBACK] Created A record for {}: name={}, ttl={}, data={:?}", 
                                short_name, record.name(), record.ttl(), record.data()));

                            ConsoleLogger::debug(&format!(
                                "🔧 [DNS-RECORD-FALLBACK] Pre-add: response has {} answers",
                                response.answer_count()
                            ));
                            response.add_answer(record.clone());
                            ConsoleLogger::debug(&format!(
                                "🔧 [DNS-RECORD-FALLBACK] Post-add: response has {} answers",
                                response.answer_count()
                            ));
                            ConsoleLogger::debug(&format!(
                                "✅ [DNS-ADD-FALLBACK] Added answer to response. Total answers: {}",
                                response.answer_count()
                            ));
                            ConsoleLogger::debug(&format!(
                                "DNS: Resolved {} -> {}",
                                short_name, ipv4
                            ));
                        }
                    } else {
                        ConsoleLogger::debug(&format!(
                            "❌ [DNS-NOTFOUND] Name not found: '{}' (short: '{}')",
                            name, short_name
                        ));

                        // Debug: List all available entries
                        ConsoleLogger::debug(&format!(
                            "🔧 [DNS-DEBUG] Available entries: {:?}",
                            entries.keys().collect::<Vec<_>>()
                        ));
                    }
                }
            }
        }

        // Set response code based on whether we found any answers
        if response.answer_count() == 0 {
            response.set_response_code(ResponseCode::NXDomain);
        } else {
            response.set_response_code(ResponseCode::NoError);
        }

        Ok(response)
    }

    /// Get all registered containers
    pub fn list_entries(&self) -> Result<Vec<DnsEntry>, String> {
        let entries = self
            .entries
            .read()
            .map_err(|e| format!("Failed to read entries: {}", e))?;

        // Deduplicate by container_id
        let mut unique_entries = HashMap::new();
        for entry in entries.values() {
            unique_entries.insert(entry.container_id.clone(), entry.clone());
        }

        Ok(unique_entries.into_values().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_dns_registration() {
        let dns = DnsServer::new("10.42.0.1:1053".parse().unwrap());

        // Register a container
        dns.register_container("container-123", "web-server", "10.42.0.5")
            .unwrap();

        // Check entries
        let entries = dns.list_entries().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].container_name, "web-server");
        assert_eq!(
            entries[0].ip_address,
            IpAddr::from_str("10.42.0.5").unwrap()
        );

        // Unregister
        dns.unregister_container("container-123").unwrap();
        let entries = dns.list_entries().unwrap();
        assert_eq!(entries.len(), 0);
    }
}
