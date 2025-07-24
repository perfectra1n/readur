use anyhow::{anyhow, Result};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{debug, error, info};

use crate::models::{
    FileIngestionInfo, WebDAVConnectionResult, WebDAVCrawlEstimate, WebDAVTestConnection,
};

use super::config::{WebDAVConfig, RetryConfig, ConcurrencyConfig};
use super::connection::WebDAVConnection;
use super::discovery::{WebDAVDiscovery, WebDAVDiscoveryResult};
use super::validation::{WebDAVValidator, ValidationReport};

/// Main WebDAV service that coordinates all WebDAV operations
pub struct WebDAVService {
    connection: Arc<WebDAVConnection>,
    discovery: Arc<WebDAVDiscovery>,
    validator: Arc<WebDAVValidator>,
    config: WebDAVConfig,
    retry_config: RetryConfig,
    concurrency_config: ConcurrencyConfig,
    scan_semaphore: Arc<Semaphore>,
    download_semaphore: Arc<Semaphore>,
}

impl WebDAVService {
    /// Creates a new WebDAV service with default configurations
    pub fn new(config: WebDAVConfig) -> Result<Self> {
        Self::new_with_configs(config, RetryConfig::default(), ConcurrencyConfig::default())
    }

    /// Creates a new WebDAV service with custom retry configuration
    pub fn new_with_retry(config: WebDAVConfig, retry_config: RetryConfig) -> Result<Self> {
        Self::new_with_configs(config, retry_config, ConcurrencyConfig::default())
    }

    /// Creates a new WebDAV service with all custom configurations
    pub fn new_with_configs(
        config: WebDAVConfig, 
        retry_config: RetryConfig, 
        concurrency_config: ConcurrencyConfig
    ) -> Result<Self> {
        // Validate configuration
        config.validate()?;

        // Create connection handler
        let connection = Arc::new(WebDAVConnection::new(config.clone(), retry_config.clone())?);
        
        // Create discovery handler
        let discovery = Arc::new(WebDAVDiscovery::new(
            connection.as_ref().clone(),
            config.clone(),
            concurrency_config.clone(),
        ));

        // Create validator
        let validator = Arc::new(WebDAVValidator::new(
            connection.as_ref().clone(),
            config.clone(),
        ));

        // Create semaphores for concurrency control
        let scan_semaphore = Arc::new(Semaphore::new(concurrency_config.max_concurrent_scans));
        let download_semaphore = Arc::new(Semaphore::new(concurrency_config.max_concurrent_downloads));

        Ok(Self {
            connection,
            discovery,
            validator,
            config,
            retry_config,
            concurrency_config,
            scan_semaphore,
            download_semaphore,
        })
    }

    /// Tests the WebDAV connection
    pub async fn test_connection(&self) -> Result<WebDAVConnectionResult> {
        info!("🔍 Testing WebDAV connection for service");
        self.connection.test_connection().await
    }

    /// Tests WebDAV connection with provided configuration (static method)
    pub async fn test_connection_with_config(test_config: &WebDAVTestConnection) -> Result<WebDAVConnectionResult> {
        WebDAVConnection::test_connection_with_config(test_config).await
    }
}

/// Tests WebDAV connection with provided configuration (standalone function for backward compatibility)
pub async fn test_webdav_connection(test_config: &WebDAVTestConnection) -> Result<WebDAVConnectionResult> {
    WebDAVConnection::test_connection_with_config(test_config).await
}

impl WebDAVService {
    /// Performs a comprehensive system validation
    pub async fn validate_system(&self) -> Result<ValidationReport> {
        info!("🔍 Performing comprehensive WebDAV system validation");
        self.validator.validate_system().await
    }

    /// Estimates crawl time and resource requirements
    pub async fn estimate_crawl(&self) -> Result<WebDAVCrawlEstimate> {
        info!("📊 Estimating WebDAV crawl requirements");
        self.discovery.estimate_crawl().await
    }

    /// Discovers all files in watch folders
    pub async fn discover_all_files(&self) -> Result<Vec<FileIngestionInfo>> {
        info!("🔍 Discovering all files in watch folders");
        let mut all_files = Vec::new();

        for watch_folder in &self.config.watch_folders {
            info!("📁 Scanning watch folder: {}", watch_folder);
            
            match self.discovery.discover_files(watch_folder, true).await {
                Ok(files) => {
                    info!("✅ Found {} files in {}", files.len(), watch_folder);
                    all_files.extend(files);
                }
                Err(e) => {
                    error!("❌ Failed to scan watch folder '{}': {}", watch_folder, e);
                    return Err(anyhow!("Failed to scan watch folder '{}': {}", watch_folder, e));
                }
            }
        }

        // Deduplicate files across folders
        let deduplicated_files = self.discovery.deduplicate_files(all_files);
        
        info!("🎯 Total unique files discovered: {}", deduplicated_files.len());
        Ok(deduplicated_files)
    }

    /// Discovers files changed since a specific date (for incremental syncs)
    pub async fn discover_changed_files(&self, since: chrono::DateTime<chrono::Utc>) -> Result<Vec<FileIngestionInfo>> {
        info!("🔍 Discovering files changed since: {}", since);
        
        let all_files = self.discover_all_files().await?;
        let changed_files = self.discovery.filter_files_by_date(all_files, since);
        
        info!("📈 Found {} files changed since {}", changed_files.len(), since);
        Ok(changed_files)
    }

    /// Discovers files in a specific directory
    pub async fn discover_files_in_directory(&self, directory_path: &str, recursive: bool) -> Result<Vec<FileIngestionInfo>> {
        info!("🔍 Discovering files in directory: {} (recursive: {})", directory_path, recursive);
        self.discovery.discover_files(directory_path, recursive).await
    }

    /// Discovers both files and directories with their ETags for smart sync
    pub async fn discover_files_and_directories(&self, directory_path: &str, recursive: bool) -> Result<WebDAVDiscoveryResult> {
        info!("🔍 Discovering files and directories: {} (recursive: {})", directory_path, recursive);
        self.discovery.discover_files_and_directories(directory_path, recursive).await
    }

    /// Downloads a file from WebDAV server by path
    pub async fn download_file(&self, file_path: &str) -> Result<Vec<u8>> {
        let _permit = self.download_semaphore.acquire().await?;
        
        debug!("⬇️ Downloading file: {}", file_path);
        
        // Convert full WebDAV paths to relative paths to prevent double path construction
        let relative_path = self.convert_to_relative_path(file_path);
        let url = self.connection.get_url_for_path(&relative_path);
        
        let response = self.connection
            .authenticated_request(
                reqwest::Method::GET,
                &url,
                None,
                None,
            )
            .await?;

        if !response.status().is_success() {
            return Err(anyhow!(
                "Failed to download file '{}': HTTP {}",
                file_path,
                response.status()
            ));
        }

        let content = response.bytes().await?;
        debug!("✅ Downloaded {} bytes for file: {}", content.len(), file_path);
        
        Ok(content.to_vec())
    }

    /// Downloads a file from WebDAV server using FileIngestionInfo
    pub async fn download_file_info(&self, file_info: &FileIngestionInfo) -> Result<Vec<u8>> {
        let _permit = self.download_semaphore.acquire().await?;
        
        debug!("⬇️ Downloading file: {}", file_info.path);
        
        // Convert full WebDAV paths to relative paths to prevent double path construction
        let relative_path = self.convert_to_relative_path(&file_info.path);
        let url = self.connection.get_url_for_path(&relative_path);
        
        let response = self.connection
            .authenticated_request(
                reqwest::Method::GET,
                &url,
                None,
                None,
            )
            .await?;

        if !response.status().is_success() {
            return Err(anyhow!(
                "Failed to download file '{}': HTTP {}",
                file_info.path,
                response.status()
            ));
        }

        let content = response.bytes().await?;
        debug!("✅ Downloaded {} bytes for file: {}", content.len(), file_info.path);
        
        Ok(content.to_vec())
    }

    /// Downloads multiple files concurrently
    pub async fn download_files(&self, files: &[FileIngestionInfo]) -> Result<Vec<(FileIngestionInfo, Result<Vec<u8>>)>> {
        info!("⬇️ Downloading {} files concurrently", files.len());
        
        let tasks = files.iter().map(|file| {
            let file_clone = file.clone();
            let service_clone = self.clone();
            
            async move {
                let result = service_clone.download_file_info(&file_clone).await;
                (file_clone, result)
            }
        });

        let results = futures_util::future::join_all(tasks).await;
        
        let success_count = results.iter().filter(|(_, result)| result.is_ok()).count();
        let failure_count = results.len() - success_count;
        
        info!("📊 Download completed: {} successful, {} failed", success_count, failure_count);
        
        Ok(results)
    }

    /// Gets file metadata without downloading content
    pub async fn get_file_metadata(&self, file_path: &str) -> Result<FileIngestionInfo> {
        debug!("📋 Getting metadata for file: {}", file_path);
        
        // Convert full WebDAV paths to relative paths to prevent double path construction
        let relative_path = self.convert_to_relative_path(file_path);
        let url = self.connection.get_url_for_path(&relative_path);
        
        let propfind_body = r#"<?xml version="1.0" encoding="utf-8"?>
            <D:propfind xmlns:D="DAV:">
                <D:prop>
                    <D:displayname/>
                    <D:getcontentlength/>
                    <D:getlastmodified/>
                    <D:getetag/>
                    <D:resourcetype/>
                    <D:creationdate/>
                </D:prop>
            </D:propfind>"#;

        let response = self.connection
            .authenticated_request(
                reqwest::Method::from_bytes(b"PROPFIND")?,
                &url,
                Some(propfind_body.to_string()),
                Some(vec![
                    ("Depth", "0"),
                    ("Content-Type", "application/xml"),
                ]),
            )
            .await?;

        let body = response.text().await?;
        let files = crate::webdav_xml_parser::parse_propfind_response(&body)?;
        
        files.into_iter()
            .find(|f| f.path == file_path)
            .ok_or_else(|| anyhow!("File metadata not found: {}", file_path))
    }

    /// Checks if a file exists on the WebDAV server
    pub async fn file_exists(&self, file_path: &str) -> Result<bool> {
        match self.get_file_metadata(file_path).await {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    /// Gets the server capabilities and features
    pub async fn get_server_capabilities(&self) -> Result<ServerCapabilities> {
        debug!("🔍 Checking server capabilities");
        
        let options_response = self.connection
            .authenticated_request(
                reqwest::Method::OPTIONS,
                &self.config.webdav_url(),
                None,
                None,
            )
            .await?;

        let dav_header = options_response
            .headers()
            .get("dav")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();

        let allow_header = options_response
            .headers()
            .get("allow")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();

        let server_header = options_response
            .headers()
            .get("server")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        Ok(ServerCapabilities {
            dav_compliance: dav_header.clone(),
            allowed_methods: allow_header,
            server_software: server_header,
            supports_etag: dav_header.contains("1") || dav_header.contains("2"),
            supports_depth_infinity: dav_header.contains("1"),
        })
    }

    /// Performs a health check on the WebDAV service
    pub async fn health_check(&self) -> Result<HealthStatus> {
        info!("🏥 Performing WebDAV service health check");
        
        let start_time = std::time::Instant::now();
        
        // Test basic connectivity
        let connection_result = self.test_connection().await?;
        if !connection_result.success {
            return Ok(HealthStatus {
                healthy: false,
                message: format!("Connection failed: {}", connection_result.message),
                response_time_ms: start_time.elapsed().as_millis() as u64,
                details: None,
            });
        }

        // Test each watch folder
        for folder in &self.config.watch_folders {
            if let Err(e) = self.connection.test_propfind(folder).await {
                return Ok(HealthStatus {
                    healthy: false,
                    message: format!("Watch folder '{}' is inaccessible: {}", folder, e),
                    response_time_ms: start_time.elapsed().as_millis() as u64,
                    details: Some(serde_json::json!({
                        "failed_folder": folder,
                        "error": e.to_string()
                    })),
                });
            }
        }

        let response_time = start_time.elapsed().as_millis() as u64;
        
        Ok(HealthStatus {
            healthy: true,
            message: "All systems operational".to_string(),
            response_time_ms: response_time,
            details: Some(serde_json::json!({
                "tested_folders": self.config.watch_folders,
                "server_type": connection_result.server_type,
                "server_version": connection_result.server_version
            })),
        })
    }

    /// Gets configuration information
    pub fn get_config(&self) -> &WebDAVConfig {
        &self.config
    }

    /// Gets retry configuration
    pub fn get_retry_config(&self) -> &RetryConfig {
        &self.retry_config
    }

    /// Gets concurrency configuration
    pub fn get_concurrency_config(&self) -> &ConcurrencyConfig {
        &self.concurrency_config
    }

    /// Tests if the server supports recursive ETag scanning
    pub async fn test_recursive_etag_support(&self) -> Result<bool> {
        debug!("🔍 Testing recursive ETag support");
        
        // Get server capabilities to check ETag support
        let capabilities = self.get_server_capabilities().await?;
        
        // Check if server supports ETags at all
        if !capabilities.supports_etag {
            debug!("❌ Server does not support ETags");
            return Ok(false);
        }

        // Check server type for known recursive ETag support
        if let Some(server_software) = &capabilities.server_software {
            let server_lower = server_software.to_lowercase();
            
            // Nextcloud and ownCloud support recursive ETags
            if server_lower.contains("nextcloud") || server_lower.contains("owncloud") {
                debug!("✅ Server supports recursive ETags (Nextcloud/ownCloud)");
                return Ok(true);
            }
            
            // Apache mod_dav typically supports recursive ETags
            if server_lower.contains("apache") && capabilities.dav_compliance.contains("1") {
                debug!("✅ Server likely supports recursive ETags (Apache WebDAV)");
                return Ok(true);
            }
        }

        // For unknown servers, assume basic ETag support but not recursive
        debug!("⚠️ Unknown server type, assuming no recursive ETag support");
        Ok(false)
    }

    /// Checks if a path is a direct child of a parent directory
    pub fn is_direct_child(&self, file_path: &str, parent_path: &str) -> bool {
        // Normalize paths by removing trailing slashes
        let normalized_parent = parent_path.trim_end_matches('/');
        let normalized_file = file_path.trim_end_matches('/');
        
        // Handle root case
        if normalized_parent.is_empty() || normalized_parent == "/" {
            return !normalized_file.is_empty() && normalized_file.matches('/').count() == 1;
        }
        
        // Check if file path starts with parent path
        if !normalized_file.starts_with(normalized_parent) {
            return false;
        }
        
        // Get the remainder after the parent path
        let remainder = &normalized_file[normalized_parent.len()..];
        
        // Should start with '/' and contain no additional '/' characters
        remainder.starts_with('/') && remainder[1..].find('/').is_none()
    }

    /// Converts a full WebDAV path to a relative path by removing server-specific prefixes
    pub fn convert_to_relative_path(&self, full_webdav_path: &str) -> String {
        // For Nextcloud/ownCloud, remove the server-specific prefixes
        if let Some(server_type) = &self.config.server_type {
            if server_type == "nextcloud" {
                let username = &self.config.username;
                let prefix = format!("/remote.php/dav/files/{}", username);
                
                if full_webdav_path.starts_with(&prefix) {
                    let relative = &full_webdav_path[prefix.len()..];
                    return if relative.is_empty() { "/" } else { relative }.to_string();
                }
            } else if server_type == "owncloud" {
                // ownCloud uses /remote.php/webdav prefix
                if full_webdav_path.starts_with("/remote.php/webdav") {
                    let relative = &full_webdav_path[18..]; // Remove "/remote.php/webdav"
                    return if relative.is_empty() { "/" } else { relative }.to_string();
                }
            } else if server_type == "generic" {
                // For generic servers, remove the /webdav prefix if present
                if full_webdav_path.starts_with("/webdav") {
                    let relative = &full_webdav_path[7..]; // Remove "/webdav"
                    return if relative.is_empty() { "/" } else { relative }.to_string();
                }
            }
        }
        
        // For other servers, return as-is
        full_webdav_path.to_string()
    }
}

// Implement Clone to allow sharing the service
impl Clone for WebDAVService {
    fn clone(&self) -> Self {
        Self {
            connection: Arc::clone(&self.connection),
            discovery: Arc::clone(&self.discovery),
            validator: Arc::clone(&self.validator),
            config: self.config.clone(),
            retry_config: self.retry_config.clone(),
            concurrency_config: self.concurrency_config.clone(),
            scan_semaphore: Arc::clone(&self.scan_semaphore),
            download_semaphore: Arc::clone(&self.download_semaphore),
        }
    }
}

/// Server capabilities information
#[derive(Debug, Clone)]
pub struct ServerCapabilities {
    pub dav_compliance: String,
    pub allowed_methods: String,
    pub server_software: Option<String>,
    pub supports_etag: bool,
    pub supports_depth_infinity: bool,
}

/// Health status information
#[derive(Debug, Clone)]
pub struct HealthStatus {
    pub healthy: bool,
    pub message: String,
    pub response_time_ms: u64,
    pub details: Option<serde_json::Value>,
}