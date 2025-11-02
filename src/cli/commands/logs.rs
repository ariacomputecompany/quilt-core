// Container logs command handler

use crate::quilt::quilt_service_client::QuiltServiceClient;
use crate::quilt::{GetContainerLogsRequest, GetContainerLogsResponse};
use crate::utils::logger::Logger;
use super::common::resolve_container_id;
use tonic::transport::Channel;

/// Get and display container logs
pub async fn handle_logs(
    client: &mut QuiltServiceClient<Channel>,
    container: Option<String>,
    by_name: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let container_id = resolve_container_id(client, &container, by_name).await?;

    Logger::info(&format!("Fetching logs for container: {}", container_id));

    let request = tonic::Request::new(GetContainerLogsRequest {
        container_id: container_id.clone(),
        container_name: String::new(),
    });

    match client.get_container_logs(request).await {
        Ok(response) => {
            let res: GetContainerLogsResponse = response.into_inner();

            if res.logs.is_empty() {
                Logger::info("No logs available for this container");
            } else {
                Logger::blank();
                // Print each log entry with timestamp
                for log_entry in res.logs {
                    let formatted_time = super::common::format_timestamp(log_entry.timestamp);
                    println!("[{}] {}", formatted_time, log_entry.message);
                }
                Logger::blank();
                Logger::success(&format!("Showing logs for container: {}", container_id));
            }

            Ok(())
        }
        Err(e) => {
            Logger::error(&format!("Failed to fetch logs: {}", e));
            Err(e.into())
        }
    }
}
