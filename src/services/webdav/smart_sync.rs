use std::sync::Arc;
use std::collections::HashMap;
use anyhow::Result;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::{AppState, models::{CreateWebDAVDirectory, FileIngestionInfo}};
use super::WebDAVService;

/// Smart sync service that provides intelligent WebDAV synchronization
/// by comparing directory ETags to avoid unnecessary scans
pub struct SmartSyncService {
    state: Arc<AppState>,
}

/// Result of smart sync evaluation
#[derive(Debug, Clone)]
pub enum SmartSyncDecision {
    /// No changes detected, sync can be skipped entirely
    SkipSync,
    /// Smart sync detected changes, need to perform discovery
    RequiresSync(SmartSyncStrategy),
}

/// Strategy for performing sync after smart evaluation
#[derive(Debug, Clone)]
pub enum SmartSyncStrategy {
    /// Full deep scan needed (first time, too many changes, or fallback)
    FullDeepScan,
    /// Targeted scan of specific changed directories
    TargetedScan(Vec<String>),
}

/// Complete result from smart sync operation
#[derive(Debug, Clone)]
pub struct SmartSyncResult {
    pub files: Vec<FileIngestionInfo>,
    pub directories: Vec<FileIngestionInfo>,
    pub strategy_used: SmartSyncStrategy,
    pub directories_scanned: usize,
    pub directories_skipped: usize,
}

impl SmartSyncService {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }

    /// Evaluates whether sync is needed and determines the best strategy
    pub async fn evaluate_sync_need(
        &self,
        user_id: Uuid,
        webdav_service: &WebDAVService,
        folder_path: &str,
    ) -> Result<SmartSyncDecision> {
        info!("🧠 Evaluating smart sync for folder: {}", folder_path);
        
        // Get all known directory ETags from database in bulk
        let known_directories = self.state.db.list_webdav_directories(user_id).await
            .map_err(|e| anyhow::anyhow!("Failed to fetch known directories: {}", e))?;
        
        // Filter to only directories under the current folder path
        let relevant_dirs: HashMap<String, String> = known_directories
            .into_iter()
            .filter(|dir| dir.directory_path.starts_with(folder_path))
            .map(|dir| (dir.directory_path, dir.directory_etag))
            .collect();
        
        if relevant_dirs.is_empty() {
            info!("No known directories for {}, requires full deep scan", folder_path);
            return Ok(SmartSyncDecision::RequiresSync(SmartSyncStrategy::FullDeepScan));
        }
        
        info!("Found {} known directories for smart sync comparison", relevant_dirs.len());
        
        // Do a shallow discovery of the root folder to check immediate changes
        match webdav_service.discover_files_and_directories(folder_path, false).await {
            Ok(root_discovery) => {
                let mut changed_directories = Vec::new();
                let mut new_directories = Vec::new();
                
                // Check if any immediate subdirectories have changed ETags
                for directory in &root_discovery.directories {
                    match relevant_dirs.get(&directory.path) {
                        Some(known_etag) => {
                            if known_etag != &directory.etag {
                                info!("Directory changed: {} (old: {}, new: {})", 
                                      directory.path, known_etag, directory.etag);
                                changed_directories.push(directory.path.clone());
                            }
                        }
                        None => {
                            info!("New directory discovered: {}", directory.path);
                            new_directories.push(directory.path.clone());
                        }
                    }
                }
                
                // If no changes detected in immediate subdirectories, we can skip
                if changed_directories.is_empty() && new_directories.is_empty() {
                    info!("✅ Smart sync: No directory changes detected, sync can be skipped");
                    return Ok(SmartSyncDecision::SkipSync);
                }
                
                // Determine strategy based on scope of changes
                let total_changes = changed_directories.len() + new_directories.len();
                let total_known = relevant_dirs.len();
                let change_ratio = total_changes as f64 / total_known.max(1) as f64;
                
                if change_ratio > 0.3 || new_directories.len() > 5 {
                    // Too many changes, do full deep scan for efficiency
                    info!("📁 Smart sync: Large changes detected ({} changed, {} new), using full deep scan", 
                          changed_directories.len(), new_directories.len());
                    return Ok(SmartSyncDecision::RequiresSync(SmartSyncStrategy::FullDeepScan));
                } else {
                    // Targeted scan of changed directories
                    let mut targets = changed_directories;
                    targets.extend(new_directories);
                    info!("🎯 Smart sync: Targeted changes detected, scanning {} directories", targets.len());
                    return Ok(SmartSyncDecision::RequiresSync(SmartSyncStrategy::TargetedScan(targets)));
                }
            }
            Err(e) => {
                warn!("Smart sync evaluation failed, falling back to deep scan: {}", e);
                return Ok(SmartSyncDecision::RequiresSync(SmartSyncStrategy::FullDeepScan));
            }
        }
    }

    /// Performs smart sync based on the strategy determined by evaluation
    pub async fn perform_smart_sync(
        &self,
        user_id: Uuid,
        webdav_service: &WebDAVService,
        folder_path: &str,
        strategy: SmartSyncStrategy,
    ) -> Result<SmartSyncResult> {
        match strategy {
            SmartSyncStrategy::FullDeepScan => {
                info!("🔍 Performing full deep scan for: {}", folder_path);
                self.perform_full_deep_scan(user_id, webdav_service, folder_path).await
            }
            SmartSyncStrategy::TargetedScan(target_dirs) => {
                info!("🎯 Performing targeted scan of {} directories", target_dirs.len());
                self.perform_targeted_scan(user_id, webdav_service, target_dirs).await
            }
        }
    }

    /// Combined evaluation and execution for convenience
    pub async fn evaluate_and_sync(
        &self,
        user_id: Uuid,
        webdav_service: &WebDAVService,
        folder_path: &str,
    ) -> Result<Option<SmartSyncResult>> {
        match self.evaluate_sync_need(user_id, webdav_service, folder_path).await? {
            SmartSyncDecision::SkipSync => {
                info!("✅ Smart sync: Skipping sync for {} - no changes detected", folder_path);
                Ok(None)
            }
            SmartSyncDecision::RequiresSync(strategy) => {
                let result = self.perform_smart_sync(user_id, webdav_service, folder_path, strategy).await?;
                Ok(Some(result))
            }
        }
    }

    /// Performs a full deep scan and saves all directory ETags
    async fn perform_full_deep_scan(
        &self,
        user_id: Uuid,
        webdav_service: &WebDAVService,
        folder_path: &str,
    ) -> Result<SmartSyncResult> {
        let discovery_result = webdav_service.discover_files_and_directories(folder_path, true).await?;
        
        info!("Deep scan found {} files and {} directories in folder {}", 
              discovery_result.files.len(), discovery_result.directories.len(), folder_path);
        
        // Save all discovered directories to database for ETag tracking
        let mut directories_saved = 0;
        for directory_info in &discovery_result.directories {
            let webdav_directory = CreateWebDAVDirectory {
                user_id,
                directory_path: directory_info.path.clone(),
                directory_etag: directory_info.etag.clone(),
                file_count: 0, // Will be updated by stats
                total_size_bytes: 0, // Will be updated by stats
            };
            
            match self.state.db.create_or_update_webdav_directory(&webdav_directory).await {
                Ok(_) => {
                    debug!("Saved directory ETag: {} -> {}", directory_info.path, directory_info.etag);
                    directories_saved += 1;
                }
                Err(e) => {
                    warn!("Failed to save directory ETag for {}: {}", directory_info.path, e);
                }
            }
        }
        
        info!("Saved ETags for {}/{} directories", directories_saved, discovery_result.directories.len());
        
        Ok(SmartSyncResult {
            files: discovery_result.files,
            directories: discovery_result.directories.clone(),
            strategy_used: SmartSyncStrategy::FullDeepScan,
            directories_scanned: discovery_result.directories.len(),
            directories_skipped: 0,
        })
    }

    /// Performs targeted scans of specific directories
    async fn perform_targeted_scan(
        &self,
        user_id: Uuid,
        webdav_service: &WebDAVService,
        target_directories: Vec<String>,
    ) -> Result<SmartSyncResult> {
        let mut all_files = Vec::new();
        let mut all_directories = Vec::new();
        let mut directories_scanned = 0;

        // Scan each target directory recursively
        for target_dir in &target_directories {
            match webdav_service.discover_files_and_directories(target_dir, true).await {
                Ok(discovery_result) => {
                    all_files.extend(discovery_result.files);
                    
                    // Save directory ETags for this scan
                    for directory_info in &discovery_result.directories {
                        let webdav_directory = CreateWebDAVDirectory {
                            user_id,
                            directory_path: directory_info.path.clone(),
                            directory_etag: directory_info.etag.clone(),
                            file_count: 0,
                            total_size_bytes: 0,
                        };
                        
                        if let Err(e) = self.state.db.create_or_update_webdav_directory(&webdav_directory).await {
                            warn!("Failed to save directory ETag for {}: {}", directory_info.path, e);
                        } else {
                            debug!("Updated directory ETag: {} -> {}", directory_info.path, directory_info.etag);
                        }
                    }
                    
                    all_directories.extend(discovery_result.directories);
                    directories_scanned += 1;
                }
                Err(e) => {
                    warn!("Failed to scan target directory {}: {}", target_dir, e);
                }
            }
        }

        info!("Targeted scan completed: {} directories scanned, {} files found", 
              directories_scanned, all_files.len());

        Ok(SmartSyncResult {
            files: all_files,
            directories: all_directories,
            strategy_used: SmartSyncStrategy::TargetedScan(target_directories),
            directories_scanned,
            directories_skipped: 0, // TODO: Could track this if needed
        })
    }
}