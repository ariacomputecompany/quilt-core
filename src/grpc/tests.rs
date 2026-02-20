#[cfg(test)]
mod tests {
    use crate::*;
    use std::sync::Arc;
    use tokio::sync::Mutex as TokioMutex;

    // Mock sync engine for testing
    struct MockSyncEngine {
        containers: Arc<TokioMutex<HashMap<String, ContainerState>>>,
        names: Arc<TokioMutex<HashMap<String, String>>>, // name -> id
    }

    impl MockSyncEngine {
        fn new() -> Self {
            Self {
                containers: Arc::new(TokioMutex::new(HashMap::new())),
                names: Arc::new(TokioMutex::new(HashMap::new())),
            }
        }

        async fn create_container(
            &self,
            config: sync::containers::ContainerConfig,
        ) -> Result<(), String> {
            let mut containers = self.containers.lock().await;
            let mut names = self.names.lock().await;

            // Check name uniqueness
            if let Some(ref name) = config.name {
                if !name.is_empty() && names.contains_key(name) {
                    return Err(format!("Container with name '{}' already exists", name));
                }
            }

            containers.insert(config.id.clone(), ContainerState::Created);

            if let Some(ref name) = config.name {
                if !name.is_empty() {
                    names.insert(name.clone(), config.id.clone());
                }
            }

            Ok(())
        }

        async fn get_container_by_name(&self, name: &str) -> Result<String, String> {
            let names = self.names.lock().await;
            names
                .get(name)
                .cloned()
                .ok_or_else(|| format!("Container with name '{}' not found", name))
        }

        async fn get_container_status(&self, id: &str) -> Result<ContainerState, String> {
            let containers = self.containers.lock().await;
            containers
                .get(id)
                .cloned()
                .ok_or_else(|| format!("Container {} not found", id))
        }

        async fn update_container_state(
            &self,
            id: &str,
            state: ContainerState,
        ) -> Result<(), String> {
            let mut containers = self.containers.lock().await;
            if let Some(container_state) = containers.get_mut(id) {
                *container_state = state;
                Ok(())
            } else {
                Err(format!("Container {} not found", id))
            }
        }
    }

    #[tokio::test]
    async fn test_create_container_with_name() {
        let sync_engine = Arc::new(SyncEngine::new(":memory:").await.unwrap());
        let service = QuiltServiceImpl { sync_engine };

        let request = tonic::Request::new(CreateContainerRequest {
            image_path: "test.tar.gz".to_string(),
            command: vec!["echo".to_string(), "test".to_string()],
            environment: HashMap::new(),
            working_directory: String::new(),
            setup_commands: vec![],
            memory_limit_mb: 0,
            cpu_limit_percent: 0.0,
            enable_pid_namespace: true,
            enable_mount_namespace: true,
            enable_uts_namespace: true,
            enable_ipc_namespace: true,
            enable_network_namespace: true,
            name: "test-container".to_string(),
            async_mode: false,
        });

        let response = service.create_container(request).await;
        assert!(response.is_ok());

        let res = response.unwrap().into_inner();
        assert!(res.success);
        assert!(!res.container_id.is_empty());
    }

    #[tokio::test]
    async fn test_async_container_without_command() {
        let sync_engine = Arc::new(SyncEngine::new(":memory:").await.unwrap());
        let service = QuiltServiceImpl { sync_engine };

        let request = tonic::Request::new(CreateContainerRequest {
            image_path: "test.tar.gz".to_string(),
            command: vec![], // Empty command
            environment: HashMap::new(),
            working_directory: String::new(),
            setup_commands: vec![],
            memory_limit_mb: 0,
            cpu_limit_percent: 0.0,
            enable_pid_namespace: true,
            enable_mount_namespace: true,
            enable_uts_namespace: true,
            enable_ipc_namespace: true,
            enable_network_namespace: true,
            name: "async-test".to_string(),
            async_mode: true, // Async mode
        });

        let response = service.create_container(request).await;
        assert!(response.is_ok());

        let res = response.unwrap().into_inner();
        assert!(res.success);
        // Verify it used default command
    }

    #[tokio::test]
    async fn test_non_async_without_command_fails() {
        let sync_engine = Arc::new(SyncEngine::new(":memory:").await.unwrap());
        let service = QuiltServiceImpl { sync_engine };

        let request = tonic::Request::new(CreateContainerRequest {
            image_path: "test.tar.gz".to_string(),
            command: vec![], // Empty command
            environment: HashMap::new(),
            working_directory: String::new(),
            setup_commands: vec![],
            memory_limit_mb: 0,
            cpu_limit_percent: 0.0,
            enable_pid_namespace: true,
            enable_mount_namespace: true,
            enable_uts_namespace: true,
            enable_ipc_namespace: true,
            enable_network_namespace: true,
            name: "fail-test".to_string(),
            async_mode: false, // Not async
        });

        let response = service.create_container(request).await;
        assert!(response.is_err());

        let err = response.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("Command required"));
    }

    #[tokio::test]
    async fn test_get_container_by_name_rpc() {
        let sync_engine = Arc::new(SyncEngine::new(":memory:").await.unwrap());
        let service = QuiltServiceImpl {
            sync_engine: sync_engine.clone(),
        };

        // Create a container with name first
        let config = sync::containers::ContainerConfig {
            id: "test-id-123".to_string(),
            name: Some("lookup-test".to_string()),
            image_path: "test.tar.gz".to_string(),
            command: "echo test".to_string(),
            environment: HashMap::new(),
            memory_limit_mb: None,
            cpu_limit_percent: None,
            enable_network_namespace: true,
            enable_pid_namespace: true,
            enable_mount_namespace: true,
            enable_uts_namespace: true,
            enable_ipc_namespace: true,
        };

        sync_engine.create_container(config).await.unwrap();

        // Test the RPC
        let request = tonic::Request::new(GetContainerByNameRequest {
            name: "lookup-test".to_string(),
        });

        let response = service.get_container_by_name(request).await;
        assert!(response.is_ok());

        let res = response.unwrap().into_inner();
        assert!(res.found);
        assert_eq!(res.container_id, "test-id-123");
        assert!(res.error_message.is_empty());
    }

    #[tokio::test]
    async fn test_get_container_by_name_not_found() {
        // Use new_for_testing with custom IP range for this test
        let start_ip = std::net::Ipv4Addr::new(10, 0, 0, 2);
        let end_ip = std::net::Ipv4Addr::new(10, 0, 0, 3);
        let sync_engine = Arc::new(
            SyncEngine::new_for_testing(":memory:", start_ip, end_ip)
                .await
                .unwrap(),
        );
        let service = QuiltServiceImpl { sync_engine };

        let request = tonic::Request::new(GetContainerByNameRequest {
            name: "non-existent".to_string(),
        });

        let response = service.get_container_by_name(request).await;
        assert!(response.is_ok());

        let res = response.unwrap().into_inner();
        assert!(!res.found);
        assert!(res.container_id.is_empty());
        assert!(res.error_message.contains("not found"));
    }
}
