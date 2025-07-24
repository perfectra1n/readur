use anyhow::Result;
use reqwest::Method;
use std::collections::HashSet;
use tokio::sync::Semaphore;
use futures_util::stream::{self, StreamExt};
use tracing::{debug, info, warn};

use crate::models::{FileIngestionInfo, WebDAVCrawlEstimate, WebDAVFolderInfo};
use crate::webdav_xml_parser::{parse_propfind_response, parse_propfind_response_with_directories};
use super::config::{WebDAVConfig, ConcurrencyConfig};
use super::connection::WebDAVConnection;

/// Results from WebDAV discovery including both files and directories
#[derive(Debug, Clone)]
pub struct WebDAVDiscoveryResult {
    pub files: Vec<FileIngestionInfo>,
    pub directories: Vec<FileIngestionInfo>,
}

pub struct WebDAVDiscovery {
    connection: WebDAVConnection,
    config: WebDAVConfig,
    concurrency_config: ConcurrencyConfig,
}

impl WebDAVDiscovery {
    pub fn new(
        connection: WebDAVConnection, 
        config: WebDAVConfig, 
        concurrency_config: ConcurrencyConfig
    ) -> Self {
        Self { 
            connection, 
            config, 
            concurrency_config 
        }
    }

    /// Discovers files in a directory with support for pagination and filtering
    pub async fn discover_files(&self, directory_path: &str, recursive: bool) -> Result<Vec<FileIngestionInfo>> {
        info!("🔍 Discovering files in directory: {}", directory_path);
        
        if recursive {
            self.discover_files_recursive(directory_path).await
        } else {
            self.discover_files_single_directory(directory_path).await
        }
    }

    /// Discovers both files and directories with their ETags for directory tracking
    pub async fn discover_files_and_directories(&self, directory_path: &str, recursive: bool) -> Result<WebDAVDiscoveryResult> {
        info!("🔍 Discovering files and directories in: {}", directory_path);
        
        if recursive {
            self.discover_files_and_directories_recursive(directory_path).await
        } else {
            self.discover_files_and_directories_single(directory_path).await
        }
    }

    /// Discovers files in a single directory (non-recursive)
    async fn discover_files_single_directory(&self, directory_path: &str) -> Result<Vec<FileIngestionInfo>> {
        let url = self.connection.get_url_for_path(directory_path);
        
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
                Method::from_bytes(b"PROPFIND")?,
                &url,
                Some(propfind_body.to_string()),
                Some(vec![
                    ("Depth", "1"),
                    ("Content-Type", "application/xml"),
                ]),
            )
            .await?;

        let body = response.text().await?;
        let files = parse_propfind_response(&body)?;
        
        // Filter files based on supported extensions
        let filtered_files: Vec<FileIngestionInfo> = files
            .into_iter()
            .filter(|file| {
                !file.is_directory && self.config.is_supported_extension(&file.name)
            })
            .collect();

        debug!("Found {} supported files in directory: {}", filtered_files.len(), directory_path);
        Ok(filtered_files)
    }

    /// Discovers both files and directories in a single directory (non-recursive)
    async fn discover_files_and_directories_single(&self, directory_path: &str) -> Result<WebDAVDiscoveryResult> {
        let url = self.connection.get_url_for_path(directory_path);
        
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
                Method::from_bytes(b"PROPFIND")?,
                &url,
                Some(propfind_body.to_string()),
                Some(vec![
                    ("Depth", "1"),
                    ("Content-Type", "application/xml"),
                ]),
            )
            .await?;

        let body = response.text().await?;
        let all_items = parse_propfind_response_with_directories(&body)?;
        
        // Separate files and directories
        let mut files = Vec::new();
        let mut directories = Vec::new();
        
        for item in all_items {
            if item.is_directory {
                directories.push(item);
            } else if self.config.is_supported_extension(&item.name) {
                files.push(item);
            }
        }

        debug!("Single directory '{}': {} files, {} directories", 
            directory_path, files.len(), directories.len());
            
        Ok(WebDAVDiscoveryResult { files, directories })
    }

    /// Discovers files recursively in directory tree
    async fn discover_files_recursive(&self, root_directory: &str) -> Result<Vec<FileIngestionInfo>> {
        let mut all_files = Vec::new();
        let mut directories_to_scan = vec![root_directory.to_string()];
        let semaphore = Semaphore::new(self.concurrency_config.max_concurrent_scans);

        while !directories_to_scan.is_empty() {
            let current_batch: Vec<String> = directories_to_scan
                .drain(..)
                .take(self.concurrency_config.max_concurrent_scans)
                .collect();

            let tasks = current_batch.into_iter().map(|dir| {
                let semaphore = &semaphore;
                async move {
                    let _permit = semaphore.acquire().await.unwrap();
                    self.scan_directory_with_subdirs(&dir).await
                }
            });

            let results = stream::iter(tasks)
                .buffer_unordered(self.concurrency_config.max_concurrent_scans)
                .collect::<Vec<_>>()
                .await;

            for result in results {
                match result {
                    Ok((files, subdirs)) => {
                        all_files.extend(files);
                        directories_to_scan.extend(subdirs);
                    }
                    Err(e) => {
                        warn!("Failed to scan directory: {}", e);
                    }
                }
            }
        }

        info!("Recursive discovery found {} total files", all_files.len());
        Ok(all_files)
    }

    /// Discovers both files and directories recursively in directory tree
    async fn discover_files_and_directories_recursive(&self, root_directory: &str) -> Result<WebDAVDiscoveryResult> {
        let mut all_files = Vec::new();
        let mut all_directories = Vec::new();
        let mut directories_to_scan = vec![root_directory.to_string()];
        let semaphore = Semaphore::new(self.concurrency_config.max_concurrent_scans);

        while !directories_to_scan.is_empty() {
            let current_batch: Vec<String> = directories_to_scan
                .drain(..)
                .take(self.concurrency_config.max_concurrent_scans)
                .collect();

            let tasks = current_batch.into_iter().map(|dir| {
                let semaphore = &semaphore;
                async move {
                    let _permit = semaphore.acquire().await.unwrap();
                    self.scan_directory_with_all_info(&dir).await
                }
            });

            let results = stream::iter(tasks)
                .buffer_unordered(self.concurrency_config.max_concurrent_scans)
                .collect::<Vec<_>>()
                .await;

            for result in results {
                match result {
                    Ok((files, directories, subdirs_to_scan)) => {
                        all_files.extend(files);
                        all_directories.extend(directories);
                        directories_to_scan.extend(subdirs_to_scan);
                    }
                    Err(e) => {
                        warn!("Failed to scan directory: {}", e);
                    }
                }
            }
        }

        info!("Recursive discovery found {} total files and {} directories", 
              all_files.len(), all_directories.len());
        Ok(WebDAVDiscoveryResult { 
            files: all_files, 
            directories: all_directories 
        })
    }

    /// Scans a directory and returns both files and subdirectories
    async fn scan_directory_with_subdirs(&self, directory_path: &str) -> Result<(Vec<FileIngestionInfo>, Vec<String>)> {
        let url = self.connection.get_url_for_path(directory_path);
        
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
                Method::from_bytes(b"PROPFIND")?,
                &url,
                Some(propfind_body.to_string()),
                Some(vec![
                    ("Depth", "1"),
                    ("Content-Type", "application/xml"),
                ]),
            )
            .await?;

        let body = response.text().await?;
        let all_items = parse_propfind_response_with_directories(&body)?;
        
        // Separate files and directories
        let mut filtered_files = Vec::new();
        let mut subdirectory_paths = Vec::new();
        
        for item in all_items {
            if item.is_directory {
                // Convert directory path to full path
                let full_path = if directory_path == "/" {
                    format!("/{}", item.path.trim_start_matches('/'))
                } else {
                    format!("{}/{}", directory_path.trim_end_matches('/'), item.path.trim_start_matches('/'))
                };
                subdirectory_paths.push(full_path);
            } else if self.config.is_supported_extension(&item.name) {
                filtered_files.push(item);
            }
        }
        
        let full_dir_paths = subdirectory_paths;

        debug!("Directory '{}': {} files, {} subdirectories", 
            directory_path, filtered_files.len(), full_dir_paths.len());
            
        Ok((filtered_files, full_dir_paths))
    }

    /// Scans a directory and returns files, directories, and subdirectory paths for queue
    async fn scan_directory_with_all_info(&self, directory_path: &str) -> Result<(Vec<FileIngestionInfo>, Vec<FileIngestionInfo>, Vec<String>)> {
        let url = self.connection.get_url_for_path(directory_path);
        
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
                Method::from_bytes(b"PROPFIND")?,
                &url,
                Some(propfind_body.to_string()),
                Some(vec![
                    ("Depth", "1"),
                    ("Content-Type", "application/xml"),
                ]),
            )
            .await?;

        let body = response.text().await?;
        let all_items = parse_propfind_response_with_directories(&body)?;
        
        // Separate files and directories
        let mut filtered_files = Vec::new();
        let mut directories = Vec::new();
        let mut subdirectory_paths = Vec::new();
        
        for item in all_items {
            if item.is_directory {
                // Fix the directory path to be absolute
                let full_path = if directory_path == "/" {
                    format!("/{}", item.path.trim_start_matches('/'))
                } else {
                    format!("{}/{}", directory_path.trim_end_matches('/'), item.path.trim_start_matches('/'))
                };
                
                // Create a directory info with the corrected path
                let mut directory_info = item.clone();
                directory_info.path = full_path.clone();
                directories.push(directory_info);
                
                // Add to paths for further scanning
                subdirectory_paths.push(full_path);
            } else if self.config.is_supported_extension(&item.name) {
                filtered_files.push(item);
            }
        }

        debug!("Directory '{}': {} files, {} directories, {} paths to scan", 
            directory_path, filtered_files.len(), directories.len(), subdirectory_paths.len());
            
        Ok((filtered_files, directories, subdirectory_paths))
    }

    /// Estimates crawl time and file counts for watch folders
    pub async fn estimate_crawl(&self) -> Result<WebDAVCrawlEstimate> {
        info!("📊 Estimating crawl for WebDAV watch folders");
        
        let mut folders = Vec::new();
        let mut total_files = 0;
        let mut total_supported_files = 0;
        let mut total_size_mb = 0.0;

        for watch_folder in &self.config.watch_folders {
            match self.estimate_folder(watch_folder).await {
                Ok(folder_info) => {
                    total_files += folder_info.total_files;
                    total_supported_files += folder_info.supported_files;
                    total_size_mb += folder_info.total_size_mb;
                    folders.push(folder_info);
                }
                Err(e) => {
                    warn!("Failed to estimate folder '{}': {}", watch_folder, e);
                    // Add empty folder info for failed estimates
                    folders.push(WebDAVFolderInfo {
                        path: watch_folder.clone(),
                        total_files: 0,
                        supported_files: 0,
                        estimated_time_hours: 0.0,
                        total_size_mb: 0.0,
                    });
                }
            }
        }

        // Estimate total time based on file count and average processing time
        let avg_time_per_file_seconds = 2.0; // Conservative estimate
        let total_estimated_time_hours = (total_supported_files as f32 * avg_time_per_file_seconds) / 3600.0;

        Ok(WebDAVCrawlEstimate {
            folders,
            total_files,
            total_supported_files,
            total_estimated_time_hours,
            total_size_mb,
        })
    }

    /// Estimates file count and size for a specific folder
    async fn estimate_folder(&self, folder_path: &str) -> Result<WebDAVFolderInfo> {
        debug!("Estimating folder: {}", folder_path);
        
        // Sample a few subdirectories to estimate the total
        let sample_files = self.discover_files_single_directory(folder_path).await?;
        
        // Get subdirectories for deeper estimation
        let subdirs = self.get_subdirectories(folder_path).await?;
        
        let mut total_files = sample_files.len() as i64;
        let mut total_size: i64 = sample_files.iter().map(|f| f.size).sum();
        
        // Sample a few subdirectories to extrapolate
        let sample_size = std::cmp::min(5, subdirs.len());
        if sample_size > 0 {
            let mut sample_total = 0i64;
            
            for subdir in subdirs.iter().take(sample_size) {
                if let Ok(subdir_files) = self.discover_files_single_directory(subdir).await {
                    sample_total += subdir_files.len() as i64;
                }
            }
            
            // Extrapolate based on sample
            if sample_total > 0 {
                let avg_files_per_subdir = sample_total as f64 / sample_size as f64;
                total_files += (avg_files_per_subdir * subdirs.len() as f64) as i64;
            }
        }

        // Filter for supported files
        let supported_files = (total_files as f64 * self.calculate_support_ratio(&sample_files)) as i64;
        
        let total_size_mb = total_size as f64 / (1024.0 * 1024.0);
        let estimated_time_hours = (supported_files as f32 * 2.0) / 3600.0; // 2 seconds per file

        Ok(WebDAVFolderInfo {
            path: folder_path.to_string(),
            total_files,
            supported_files,
            estimated_time_hours,
            total_size_mb,
        })
    }

    /// Gets subdirectories for a given path
    async fn get_subdirectories(&self, directory_path: &str) -> Result<Vec<String>> {
        let url = self.connection.get_url_for_path(directory_path);
        
        let propfind_body = r#"<?xml version="1.0" encoding="utf-8"?>
            <D:propfind xmlns:D="DAV:">
                <D:prop>
                    <D:resourcetype/>
                </D:prop>
            </D:propfind>"#;

        let response = self.connection
            .authenticated_request(
                Method::from_bytes(b"PROPFIND")?,
                &url,
                Some(propfind_body.to_string()),
                Some(vec![
                    ("Depth", "1"),
                    ("Content-Type", "application/xml"),
                ]),
            )
            .await?;

        let body = response.text().await?;
        let all_items = parse_propfind_response_with_directories(&body)?;
        
        // Filter out only directories and extract their paths
        let directory_paths: Vec<String> = all_items
            .into_iter()
            .filter(|item| item.is_directory)
            .map(|item| item.path)
            .collect();
        
        Ok(directory_paths)
    }

    /// Calculates the ratio of supported files in a sample
    fn calculate_support_ratio(&self, sample_files: &[FileIngestionInfo]) -> f64 {
        if sample_files.is_empty() {
            return 1.0; // Assume all files are supported if no sample
        }

        let supported_count = sample_files
            .iter()
            .filter(|file| self.config.is_supported_extension(&file.name))
            .count();

        supported_count as f64 / sample_files.len() as f64
    }

    /// Filters files by last modified date (for incremental syncs)
    pub fn filter_files_by_date(&self, files: Vec<FileIngestionInfo>, since: chrono::DateTime<chrono::Utc>) -> Vec<FileIngestionInfo> {
        files
            .into_iter()
            .filter(|file| {
                file.last_modified
                    .map(|modified| modified > since)
                    .unwrap_or(true) // Include files without modification date
            })
            .collect()
    }

    /// Deduplicates files by ETag or path
    pub fn deduplicate_files(&self, files: Vec<FileIngestionInfo>) -> Vec<FileIngestionInfo> {
        let mut seen_etags = HashSet::new();
        let mut seen_paths = HashSet::new();
        let mut deduplicated = Vec::new();

        for file in files {
            let is_duplicate = if !file.etag.is_empty() {
                !seen_etags.insert(file.etag.clone())
            } else {
                !seen_paths.insert(file.path.clone())
            };

            if !is_duplicate {
                deduplicated.push(file);
            }
        }

        debug!("Deduplicated {} files", deduplicated.len());
        deduplicated
    }
}