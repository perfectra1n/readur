use std::sync::Arc;
use std::time::Duration;
use std::collections::HashMap;
use tokio::time::interval;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use chrono::Utc;
use uuid::Uuid;
use sqlx::Row;

use crate::{
    AppState,
    models::{SourceType, LocalFolderSourceConfig, S3SourceConfig, WebDAVSourceConfig},
};
use super::source_sync::SourceSyncService;

struct SyncHealthAnalysis {
    score_penalty: i32,
    issues: Vec<serde_json::Value>,
}

struct ErrorAnalysis {
    score_penalty: i32,
    issues: Vec<serde_json::Value>,
}

pub struct SourceScheduler {
    state: Arc<AppState>,
    sync_service: SourceSyncService,
    check_interval: Duration,
    // Track running sync tasks and their cancellation tokens
    running_syncs: Arc<RwLock<HashMap<Uuid, CancellationToken>>>,
}

impl SourceScheduler {
    pub fn new(state: Arc<AppState>) -> Self {
        let sync_service = SourceSyncService::new(state.clone());
        
        Self {
            state,
            sync_service,
            check_interval: Duration::from_secs(60), // Check every minute for due syncs
            running_syncs: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn start(&self) {
        info!("Starting universal source sync scheduler");
        
        // First, check for any interrupted syncs that need to be resumed
        if let Err(e) = self.resume_interrupted_syncs().await {
            error!("Error resuming interrupted syncs: {}", e);
        }
        
        let mut interval_timer = interval(self.check_interval);
        
        loop {
            interval_timer.tick().await;
            
            if let Err(e) = self.check_and_sync_sources().await {
                error!("Error in source sync scheduler: {}", e);
            }
            
            // Run periodic validation checks for all sources
            if let Err(e) = self.run_periodic_validations().await {
                error!("Error in periodic validation checks: {}", e);
            }
        }
    }

    async fn resume_interrupted_syncs(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Checking for interrupted source syncs to resume");
        
        // Get all enabled sources that might have been interrupted
        let sources = match self.state.db.get_sources_for_sync().await {
            Ok(sources) => {
                info!("Successfully loaded {} sources from database for sync check", sources.len());
                sources
            }
            Err(e) => {
                error!("Failed to load sources from database during startup: {}", e);
                return Err(e.into());
            }
        };
        
        for source in sources {
            info!("Processing source during startup check: ID={}, Name='{}', Type={}, Status={}", 
                  source.id, source.name, source.source_type.to_string(), source.status.to_string());
            
            // Validate source configuration before attempting any operations
            if let Err(e) = self.validate_source_config(&source) {
                error!("❌ CONFIGURATION ERROR for source '{}' (ID: {}): {}", 
                       source.name, source.id, e);
                error!("Source config JSON: {}", serde_json::to_string_pretty(&source.config).unwrap_or_else(|_| "Invalid JSON".to_string()));
                continue;
            }
            
            // Check if this source was likely interrupted during sync
            // This is a simplified check - you might want to add specific interrupted tracking
            if source.status.to_string() == "syncing" {
                info!("Found potentially interrupted sync for source {}, will resume", source.name);
                
                // Reset status and trigger new sync
                if let Err(e) = sqlx::query(
                    r#"UPDATE sources SET status = 'idle', updated_at = NOW() WHERE id = $1"#
                )
                .bind(source.id)
                .execute(self.state.db.get_pool())
                .await {
                    error!("Failed to reset interrupted source status: {}", e);
                    continue;
                }
                
                // Always resume interrupted syncs regardless of auto_sync setting
                // This ensures that manually triggered syncs that were interrupted by server restart
                // will continue downloading files instead of just starting OCR on existing files
                let should_resume = true;
                
                if should_resume {
                    info!("Resuming interrupted sync for source {}", source.name);
                    
                    let sync_service = self.sync_service.clone();
                    let source_clone = source.clone();
                    let state_clone = self.state.clone();
                    
                    tokio::spawn(async move {
                        // Get user's OCR setting - simplified, you might want to store this in source config
                        let enable_background_ocr = true; // Default to true, could be made configurable per source
                        
                        match sync_service.sync_source(&source_clone, enable_background_ocr).await {
                            Ok(files_processed) => {
                                info!("Resumed sync completed for source {}: {} files processed", 
                                      source_clone.name, files_processed);
                                
                                // Create notification for successful resume
                                let notification = crate::models::CreateNotification {
                                    notification_type: "success".to_string(),
                                    title: "Source Sync Resumed".to_string(),
                                    message: format!("Resumed sync for {} after server restart. Processed {} files", 
                                                   source_clone.name, files_processed),
                                    action_url: Some("/sources".to_string()),
                                    metadata: Some(serde_json::json!({
                                        "source_type": source_clone.source_type.to_string(),
                                        "source_id": source_clone.id,
                                        "files_processed": files_processed
                                    })),
                                };
                                
                                if let Err(e) = state_clone.db.create_notification(source_clone.user_id, &notification).await {
                                    error!("Failed to create resume notification: {}", e);
                                }
                            }
                            Err(e) => {
                                error!("Resumed sync failed for source {}: {}", source_clone.name, e);
                            }
                        }
                    });
                }
            }
        }
        
        Ok(())
    }

    async fn check_and_sync_sources(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Get all sources that might need syncing
        let sources = self.state.db.get_sources_for_sync().await?;
        
        for source in sources {
            // Skip sources that are already in error status due to configuration issues
            if source.status == crate::models::SourceStatus::Error &&
               source.last_error.as_ref().map(|e| e.contains("Configuration error")).unwrap_or(false) {
                // Only log this once every hour to reduce spam
                if source.last_error_at.map(|t| chrono::Utc::now() - t > chrono::Duration::hours(1)).unwrap_or(true) {
                    warn!("⚠️ Skipping source '{}' (ID: {}) due to persistent configuration error: {}", 
                          source.name, source.id, source.last_error.as_ref().unwrap_or(&"Unknown error".to_string()));
                }
                continue;
            }
            
            // Validate source configuration before checking if sync is due
            if let Err(e) = self.validate_source_config(&source) {
                error!("❌ CONFIGURATION ERROR during background sync check for source '{}' (ID: {}): {}", 
                       source.name, source.id, e);
                error!("Source config JSON: {}", serde_json::to_string_pretty(&source.config).unwrap_or_else(|_| "Invalid JSON".to_string()));
                
                // Update source with error status
                if let Err(update_err) = sqlx::query(
                    r#"UPDATE sources SET status = 'error', last_error = $1, last_error_at = NOW(), updated_at = NOW() WHERE id = $2"#
                )
                .bind(format!("Configuration error: {}", e))
                .bind(source.id)
                .execute(self.state.db.get_pool())
                .await {
                    error!("Failed to update source error status: {}", update_err);
                }
                
                continue;
            }
            
            // Check if sync is due for this source
            if self.is_sync_due(&source).await? {
                info!("Starting background sync for source: {} ({})", source.name, source.source_type);
                
                let sync_service = self.sync_service.clone();
                let source_clone = source.clone();
                let state_clone = self.state.clone();
                let running_syncs_clone = self.running_syncs.clone();
                
                // Create cancellation token for this sync
                let cancellation_token = CancellationToken::new();
                
                // Register the sync task
                {
                    let mut running_syncs = running_syncs_clone.write().await;
                    running_syncs.insert(source.id, cancellation_token.clone());
                }
                
                // Start sync in background task
                let sync_handle = tokio::spawn(async move {
                    // Get user's OCR setting - simplified, you might want to store this in source config  
                    let enable_background_ocr = true; // Default to true, could be made configurable per source
                    
                    // Pass cancellation token to sync service
                    match sync_service.sync_source_with_cancellation(&source_clone, enable_background_ocr, cancellation_token.clone()).await {
                        Ok(files_processed) => {
                            info!("Background sync completed for source {}: {} files processed", 
                                  source_clone.name, files_processed);
                            
                            // Perform automatic validation check after sync completion
                            if let Err(e) = Self::validate_source_health(&source_clone, &state_clone).await {
                                error!("Failed to perform validation check: {}", e);
                            }
                            
                            // Update last sync time
                            if let Err(e) = sqlx::query(
                                r#"UPDATE sources 
                                   SET last_sync_at = NOW(), 
                                       total_files_synced = total_files_synced + $2,
                                       updated_at = NOW()
                                   WHERE id = $1"#
                            )
                            .bind(source_clone.id)
                            .bind(files_processed as i64)
                            .execute(state_clone.db.get_pool())
                            .await {
                                error!("Failed to update source sync time: {}", e);
                            }
                            
                            // Send notification if files were processed
                            if files_processed > 0 {
                                let notification = crate::models::CreateNotification {
                                    notification_type: "success".to_string(),
                                    title: "Source Sync Completed".to_string(),
                                    message: format!("Successfully processed {} files from {}", 
                                                   files_processed, source_clone.name),
                                    action_url: Some("/documents".to_string()),
                                    metadata: Some(serde_json::json!({
                                        "source_type": source_clone.source_type.to_string(),
                                        "source_id": source_clone.id,
                                        "files_processed": files_processed
                                    })),
                                };
                                
                                if let Err(e) = state_clone.db.create_notification(source_clone.user_id, &notification).await {
                                    error!("Failed to create success notification: {}", e);
                                }
                            }
                        }
                        Err(e) => {
                            error!("Background sync failed for source {}: {}", source_clone.name, e);
                            
                            // Send error notification
                            let notification = crate::models::CreateNotification {
                                notification_type: "error".to_string(),
                                title: "Source Sync Failed".to_string(),
                                message: format!("Sync failed for {}: {}", source_clone.name, e),
                                action_url: Some("/sources".to_string()),
                                metadata: Some(serde_json::json!({
                                    "source_type": source_clone.source_type.to_string(),
                                    "source_id": source_clone.id,
                                    "error": e.to_string()
                                })),
                            };
                            
                            if let Err(e) = state_clone.db.create_notification(source_clone.user_id, &notification).await {
                                error!("Failed to create error notification: {}", e);
                            }
                        }
                    }
                    
                    // Cleanup: Remove the sync from running list
                    {
                        let mut running_syncs = running_syncs_clone.write().await;
                        running_syncs.remove(&source_clone.id);
                    }
                });
            }
        }

        Ok(())
    }

    async fn is_sync_due(&self, source: &crate::models::Source) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        // Get sync interval from source config
        let sync_interval_minutes = match source.source_type {
            SourceType::WebDAV => {
                let config: WebDAVSourceConfig = serde_json::from_value(source.config.clone())?;
                if !config.auto_sync { return Ok(false); }
                config.sync_interval_minutes
            }
            SourceType::LocalFolder => {
                let config: LocalFolderSourceConfig = serde_json::from_value(source.config.clone())?;
                if !config.auto_sync { return Ok(false); }
                config.sync_interval_minutes
            }
            SourceType::S3 => {
                let config: S3SourceConfig = serde_json::from_value(source.config.clone())?;
                if !config.auto_sync { return Ok(false); }
                config.sync_interval_minutes
            }
        };
        
        if sync_interval_minutes <= 0 {
            warn!("Invalid sync interval for source {}: {} minutes", source.name, sync_interval_minutes);
            return Ok(false);
        }

        // Check if a sync is already running
        if source.status.to_string() == "syncing" {
            info!("Sync already running for source {}", source.name);
            return Ok(false);
        }

        // Check last sync time
        if let Some(last_sync) = source.last_sync_at {
            let elapsed = Utc::now() - last_sync;
            let elapsed_minutes = elapsed.num_minutes();
            
            if elapsed_minutes < sync_interval_minutes as i64 {
                // Only log this occasionally to avoid spam
                if elapsed_minutes % 10 == 0 {
                    crate::debug_log!("SOURCE_SCHEDULER", "Sync not due for source {} (last sync {} minutes ago, interval {} minutes)", 
                        source.name, elapsed_minutes, sync_interval_minutes);
                }
                return Ok(false);
            }
            
            info!("Sync is due for source {} (last sync {} minutes ago, interval {} minutes)", 
                source.name, elapsed_minutes, sync_interval_minutes);
        } else {
            info!("No previous sync found for source {}, sync is due", source.name);
        }

        // Sync is due
        Ok(true)
    }

    pub async fn trigger_sync(&self, source_id: uuid::Uuid) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Triggering manual sync for source {}", source_id);
        
        if let Some(source) = self.state.db.get_source_by_id(source_id).await? {
            let sync_service = self.sync_service.clone();
            let state_clone = self.state.clone();
            let running_syncs_clone = self.running_syncs.clone();
            
            // Create cancellation token for this sync
            let cancellation_token = CancellationToken::new();
            
            // Register the sync task
            {
                let mut running_syncs = running_syncs_clone.write().await;
                running_syncs.insert(source_id, cancellation_token.clone());
            }
            
            tokio::spawn(async move {
                let enable_background_ocr = true; // Could be made configurable
                
                match sync_service.sync_source_with_cancellation(&source, enable_background_ocr, cancellation_token).await {
                    Ok(files_processed) => {
                        info!("Manual sync completed for source {}: {} files processed", 
                              source.name, files_processed);
                        
                        // Update sync stats
                        if let Err(e) = sqlx::query(
                            r#"UPDATE sources 
                               SET last_sync_at = NOW(), 
                                   total_files_synced = total_files_synced + $2,
                                   updated_at = NOW()
                               WHERE id = $1"#
                        )
                        .bind(source.id)
                        .bind(files_processed as i64)
                        .execute(state_clone.db.get_pool())
                        .await {
                            error!("Failed to update source sync stats: {}", e);
                        }
                    }
                    Err(e) => {
                        error!("Manual sync failed for source {}: {}", source.name, e);
                    }
                }
                
                // Cleanup: Remove the sync from running list
                {
                    let mut running_syncs = running_syncs_clone.write().await;
                    running_syncs.remove(&source.id);
                }
            });
            
            Ok(())
        } else {
            Err("Source not found".into())
        }
    }

    pub async fn stop_sync(&self, source_id: Uuid) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Stopping sync for source {}", source_id);
        
        // Get the cancellation token for this sync
        let cancellation_token = {
            let running_syncs = self.running_syncs.read().await;
            running_syncs.get(&source_id).cloned()
        };
        
        if let Some(token) = cancellation_token {
            // Cancel the sync operation
            token.cancel();
            info!("Cancellation signal sent for source {}", source_id);
            
            // Update source status to indicate cancellation
            if let Err(e) = sqlx::query(
                r#"UPDATE sources SET status = 'idle', last_error = 'Sync cancelled by user', last_error_at = NOW(), updated_at = NOW() WHERE id = $1"#
            )
            .bind(source_id)
            .execute(self.state.db.get_pool())
            .await {
                error!("Failed to update source status after cancellation: {}", e);
            }
            
            // Remove from running syncs list
            {
                let mut running_syncs = self.running_syncs.write().await;
                running_syncs.remove(&source_id);
            }
            
            Ok(())
        } else {
            Err("No running sync found for this source".into())
        }
    }

    /// Validates a source configuration and provides detailed error messages for debugging
    fn validate_source_config(&self, source: &crate::models::Source) -> Result<(), String> {
        use crate::models::{SourceType, WebDAVSourceConfig, S3SourceConfig, LocalFolderSourceConfig};
        
        match source.source_type {
            SourceType::WebDAV => {
                // Attempt to deserialize WebDAV config
                let config: WebDAVSourceConfig = serde_json::from_value(source.config.clone())
                    .map_err(|e| format!("Failed to parse WebDAV configuration JSON: {}", e))?;
                
                // Validate server URL format
                self.validate_webdav_url(&config.server_url, &source.name)?;
                
                // Additional WebDAV validations
                if config.username.trim().is_empty() {
                    return Err(format!("WebDAV username cannot be empty"));
                }
                
                if config.password.trim().is_empty() {
                    return Err(format!("WebDAV password cannot be empty"));
                }
                
                if config.watch_folders.is_empty() {
                    return Err(format!("WebDAV watch_folders cannot be empty"));
                }
                
                Ok(())
            }
            SourceType::S3 => {
                let _config: S3SourceConfig = serde_json::from_value(source.config.clone())
                    .map_err(|e| format!("Failed to parse S3 configuration JSON: {}", e))?;
                Ok(())
            }
            SourceType::LocalFolder => {
                let _config: LocalFolderSourceConfig = serde_json::from_value(source.config.clone())
                    .map_err(|e| format!("Failed to parse Local Folder configuration JSON: {}", e))?;
                Ok(())
            }
        }
    }

    /// Validates WebDAV server URL and provides specific error messages
    fn validate_webdav_url(&self, server_url: &str, source_name: &str) -> Result<(), String> {
        if server_url.trim().is_empty() {
            return Err(format!("WebDAV server_url is empty"));
        }
        
        // Check if URL starts with a valid scheme
        if !server_url.starts_with("http://") && !server_url.starts_with("https://") {
            return Err(format!(
                "WebDAV server_url must start with 'http://' or 'https://'. \
                 Current value: '{}'. \
                 Examples of valid URLs: \
                 - https://cloud.example.com \
                 - http://192.168.1.100:8080 \
                 - https://nextcloud.mydomain.com:443", 
                server_url
            ));
        }
        
        // Try to parse as URL to catch other issues
        match reqwest::Url::parse(server_url) {
            Ok(url) => {
                if url.scheme() != "http" && url.scheme() != "https" {
                    return Err(format!(
                        "WebDAV server_url has invalid scheme '{}'. Only 'http' and 'https' are supported. \
                         Current URL: '{}'", 
                        url.scheme(), server_url
                    ));
                }
                
                if url.host_str().is_none() {
                    return Err(format!(
                        "WebDAV server_url is missing hostname. \
                         Current URL: '{}'. \
                         Example: https://cloud.example.com", 
                        server_url
                    ));
                }
                
                crate::debug_log!("SOURCE_SCHEDULER", "✅ WebDAV URL validation passed for source '{}': {}", source_name, server_url);
                Ok(())
            }
            Err(e) => {
                Err(format!(
                    "WebDAV server_url is not a valid URL: {}. \
                     Current value: '{}'. \
                     The URL must be absolute and include the full domain. \
                     Examples: \
                     - https://cloud.example.com \
                     - http://192.168.1.100:8080/webdav \
                     - https://nextcloud.mydomain.com", 
                    e, server_url
                ))
            }
        }
    }

    /// Check if a deep scan should be triggered based on sync results
    async fn check_and_trigger_deep_scan(
        source: &crate::models::Source,
        files_processed: usize,
        state: &Arc<AppState>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Get sync history for intelligent decision making
        let recent_syncs = sqlx::query(
            r#"
            SELECT 
                COUNT(*) as sync_count,
                SUM(CASE WHEN total_files_synced = 0 THEN 1 ELSE 0 END) as empty_sync_count,
                MAX(last_sync_at) as last_sync,
                MIN(last_sync_at) as first_sync
            FROM (
                SELECT total_files_synced, last_sync_at 
                FROM sources 
                WHERE id = $1 
                ORDER BY last_sync_at DESC 
                LIMIT 10
            ) recent_syncs
            "#
        )
        .bind(source.id)
        .fetch_one(state.db.get_pool())
        .await?;

        // Get last deep scan time
        let last_deep_scan = sqlx::query(
            r#"
            SELECT MAX(created_at) as last_deep_scan
            FROM notifications
            WHERE user_id = $1 
            AND metadata->>'source_id' = $2
            AND metadata->>'scan_type' = 'deep_scan'
            AND notification_type = 'success'
            "#
        )
        .bind(source.user_id)
        .bind(source.id.to_string())
        .fetch_one(state.db.get_pool())
        .await?;

        let mut should_trigger_deep_scan = false;
        let mut reason = String::new();

        // Trigger conditions:
        
        // 1. If the last 5+ syncs found no files, something might be wrong
        let empty_sync_count: i64 = recent_syncs.try_get("empty_sync_count").unwrap_or(0);
        if empty_sync_count >= 5 {
            should_trigger_deep_scan = true;
            reason = "Multiple consecutive syncs found no files - deep scan needed to verify directory structure".to_string();
        }
        
        // 2. If we haven't done a deep scan in over 7 days
        let last_deep_time: Option<chrono::DateTime<chrono::Utc>> = last_deep_scan.try_get("last_deep_scan").ok();
        if let Some(last_deep) = last_deep_time {
            let days_since_deep_scan = (chrono::Utc::now() - last_deep).num_days();
            if days_since_deep_scan > 7 {
                should_trigger_deep_scan = true;
                reason = format!("No deep scan in {} days - periodic verification needed", days_since_deep_scan);
            }
        }
        
        // 3. If this is the first sync ever (no deep scan history)
        let sync_count: i64 = recent_syncs.try_get("sync_count").unwrap_or(0);
        if last_deep_time.is_none() && sync_count <= 1 {
            should_trigger_deep_scan = true;
            reason = "First sync completed - deep scan recommended for initial directory discovery".to_string();
        }
        
        // 4. If sync found files but we've been getting inconsistent results
        else if files_processed > 0 {
            // Check for erratic sync patterns (alternating between finding files and not)
            let erratic_check = sqlx::query(
                r#"
                SELECT 
                    COUNT(DISTINCT CASE WHEN total_files_synced > 0 THEN 1 ELSE 0 END) as distinct_states
                FROM (
                    SELECT total_files_synced 
                    FROM sources 
                    WHERE id = $1 
                    ORDER BY last_sync_at DESC 
                    LIMIT 5
                ) recent
                "#
            )
            .bind(source.id)
            .fetch_one(state.db.get_pool())
            .await?;
            
            let distinct_states: i64 = erratic_check.try_get("distinct_states").unwrap_or(0);
            if distinct_states > 1 {
                should_trigger_deep_scan = true;
                reason = "Inconsistent sync results detected - deep scan needed for stability".to_string();
            }
        }

        if should_trigger_deep_scan {
            info!("🎯 Intelligent deep scan trigger activated for source {}: {}", source.name, reason);
            
            // Create notification about automatic deep scan
            let notification = crate::models::CreateNotification {
                notification_type: "info".to_string(),
                title: "Automatic Deep Scan Triggered".to_string(),
                message: format!("Starting deep scan for {}: {}", source.name, reason),
                action_url: Some("/sources".to_string()),
                metadata: Some(serde_json::json!({
                    "source_type": source.source_type.to_string(),
                    "source_id": source.id,
                    "scan_type": "deep_scan",
                    "trigger_reason": reason,
                    "automatic": true
                })),
            };
            
            if let Err(e) = state.db.create_notification(source.user_id, &notification).await {
                error!("Failed to create deep scan notification: {}", e);
            }
            
            // Trigger the deep scan via the API endpoint
            // We'll reuse the existing deep scan logic from the sources route
            let webdav_config: WebDAVSourceConfig = serde_json::from_value(source.config.clone())?;
            let webdav_service = crate::services::webdav::WebDAVService::new(
                crate::services::webdav::WebDAVConfig {
                    server_url: webdav_config.server_url.clone(),
                    username: webdav_config.username.clone(),
                    password: webdav_config.password.clone(),
                    watch_folders: webdav_config.watch_folders.clone(),
                    file_extensions: webdav_config.file_extensions.clone(),
                    timeout_seconds: 600, // 10 minutes for deep scan
                    server_type: webdav_config.server_type.clone(),
                }
            )?;
            
            // Run smart deep scan in background
            let source_clone = source.clone();
            let state_clone = state.clone();
            tokio::spawn(async move {
                // Use smart sync service for automatic deep scans
                let smart_sync_service = crate::services::webdav::SmartSyncService::new(state_clone.clone());
                let mut all_files_to_process = Vec::new();
                let mut total_directories_tracked = 0;
                
                // Process all watch folders using smart deep scan
                for watch_folder in &webdav_config.watch_folders {
                    match smart_sync_service.perform_smart_sync(
                        source_clone.user_id, 
                        &webdav_service, 
                        watch_folder, 
                        crate::services::webdav::SmartSyncStrategy::FullDeepScan // Force deep scan for automatic triggers
                    ).await {
                        Ok(sync_result) => {
                            all_files_to_process.extend(sync_result.files);
                            total_directories_tracked += sync_result.directories.len();
                        }
                        Err(e) => {
                            error!("Automatic smart deep scan failed for watch folder {}: {}", watch_folder, e);
                        }
                    }
                }
                
                info!("🎉 Automatic smart deep scan completed for {}: {} files found, {} directories tracked", 
                      source_clone.name, all_files_to_process.len(), total_directories_tracked);
                
                // Process the files if any were found
                let files_processed = if !all_files_to_process.is_empty() {
                            let total_files = all_files_to_process.len();
                            // Filter and process files as in the manual deep scan
                            let files_to_process: Vec<_> = all_files_to_process.into_iter()
                                .filter(|file_info| {
                                    if file_info.is_directory {
                                        return false;
                                    }
                                    let file_extension = std::path::Path::new(&file_info.name)
                                        .extension()
                                        .and_then(|ext| ext.to_str())
                                        .unwrap_or("")
                                        .to_lowercase();
                                    webdav_config.file_extensions.contains(&file_extension)
                                })
                                .collect();
                            
                            let processed_count = files_to_process.len();
                            
                            if let Err(e) = crate::routes::webdav::webdav_sync::process_files_for_deep_scan(
                                state_clone.clone(),
                                source_clone.user_id,
                                &webdav_service,
                                &files_to_process,
                                true, // enable background OCR
                                Some(source_clone.id)
                            ).await {
                                error!("Failed to process files from automatic deep scan: {}", e);
                            }
                            
                            processed_count
                        } else {
                            0
                        };
                        
                        // Success notification
                        let notification = crate::models::CreateNotification {
                            notification_type: "success".to_string(),
                            title: "Automatic Deep Scan Completed".to_string(),
                            message: format!("Deep scan of {} completed successfully", source_clone.name),
                            action_url: Some("/documents".to_string()),
                            metadata: Some(serde_json::json!({
                                "source_type": source_clone.source_type.to_string(),
                                "source_id": source_clone.id,
                                "scan_type": "deep_scan",
                                "automatic": true,
                                "files_found": files_processed
                            })),
                        };
                        
                        if let Err(e) = state_clone.db.create_notification(source_clone.user_id, &notification).await {
                            error!("Failed to create success notification: {}", e);
                        }
            });
        }
        
        Ok(())
    }

    /// Perform automatic validation of source health and connectivity
    pub async fn validate_source_health(
        source: &crate::models::Source,
        state: &Arc<AppState>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("🔍 Starting validation check for source: {}", source.name);

        let mut validation_score = 100;
        let mut validation_issues = Vec::<serde_json::Value>::new();
        let mut validation_status = "healthy";

        // 1. Configuration validation
        if let Err(config_error) = Self::validate_source_config_detailed(source) {
            validation_score -= 30;
            validation_status = "critical";
            validation_issues.push(serde_json::json!({
                "type": "configuration",
                "severity": "critical",
                "message": format!("Configuration error: {}", config_error),
                "recommendation": "Check and fix source configuration in settings"
            }));
        }

        // 2. Connectivity validation
        match source.source_type {
            crate::models::SourceType::WebDAV => {
                if let Err(e) = Self::validate_webdav_connectivity(source).await {
                    validation_score -= 25;
                    if validation_status == "healthy" { validation_status = "warning"; }
                    validation_issues.push(serde_json::json!({
                        "type": "connectivity",
                        "severity": "warning",
                        "message": format!("WebDAV connectivity issue: {}", e),
                        "recommendation": "Check server URL, credentials, and network connectivity"
                    }));
                }
            }
            crate::models::SourceType::LocalFolder => {
                if let Err(e) = Self::validate_local_folder_access(source).await {
                    validation_score -= 25;
                    if validation_status == "healthy" { validation_status = "warning"; }
                    validation_issues.push(serde_json::json!({
                        "type": "connectivity",
                        "severity": "warning", 
                        "message": format!("Local folder access issue: {}", e),
                        "recommendation": "Check folder permissions and path accessibility"
                    }));
                }
            }
            crate::models::SourceType::S3 => {
                if let Err(e) = Self::validate_s3_connectivity(source).await {
                    validation_score -= 25;
                    if validation_status == "healthy" { validation_status = "warning"; }
                    validation_issues.push(serde_json::json!({
                        "type": "connectivity",
                        "severity": "warning",
                        "message": format!("S3 connectivity issue: {}", e),
                        "recommendation": "Check AWS credentials, bucket access, and permissions"
                    }));
                }
            }
        }

        // 3. Sync pattern analysis
        if let Ok(sync_health) = Self::analyze_sync_patterns(source, state).await {
            validation_score -= sync_health.score_penalty;
            if sync_health.score_penalty > 15 && validation_status == "healthy" {
                validation_status = "warning";
            }
            for issue in sync_health.issues {
                validation_issues.push(issue);
            }
        }

        // 4. Error rate analysis
        if let Ok(error_analysis) = Self::analyze_error_patterns(source, state).await {
            validation_score -= error_analysis.score_penalty;
            if error_analysis.score_penalty > 20 {
                validation_status = "warning";
            }
            for issue in error_analysis.issues {
                validation_issues.push(issue);
            }
        }

        // Cap the minimum score at 0
        validation_score = validation_score.max(0);

        // Update validation status in database
        let validation_issues_json = serde_json::to_string(&validation_issues)
            .unwrap_or_else(|_| "[]".to_string());

        if let Err(e) = sqlx::query(
            r#"
            UPDATE sources 
            SET validation_status = $1, 
                last_validation_at = NOW(), 
                validation_score = $2,
                validation_issues = $3,
                updated_at = NOW()
            WHERE id = $4
            "#
        )
        .bind(validation_status)
        .bind(validation_score)
        .bind(validation_issues_json)
        .bind(source.id)
        .execute(state.db.get_pool())
        .await {
            error!("Failed to update validation status: {}", e);
        }

        // Send notification if there are critical issues
        if validation_status == "critical" || validation_score < 50 {
            let notification = crate::models::CreateNotification {
                notification_type: if validation_status == "critical" { "error" } else { "warning" }.to_string(),
                title: format!("Source Validation {}", if validation_status == "critical" { "Failed" } else { "Warning" }),
                message: format!("Source {} has validation issues (score: {})", source.name, validation_score),
                action_url: Some("/sources".to_string()),
                metadata: Some(serde_json::json!({
                    "source_type": source.source_type.to_string(),
                    "source_id": source.id,
                    "validation_type": "health_check",
                    "validation_score": validation_score,
                    "validation_status": validation_status,
                    "issue_count": validation_issues.len()
                })),
            };

            if let Err(e) = state.db.create_notification(source.user_id, &notification).await {
                error!("Failed to create validation notification: {}", e);
            }
        }

        info!("✅ Validation completed for {}: {} (score: {})", source.name, validation_status, validation_score);
        Ok(())
    }

    fn validate_source_config_detailed(source: &crate::models::Source) -> Result<(), String> {
        // Reuse existing validation logic but return more detailed errors
        Self::validate_source_config_static(source)
    }

    fn validate_source_config_static(source: &crate::models::Source) -> Result<(), String> {
        use crate::models::{SourceType, WebDAVSourceConfig, S3SourceConfig, LocalFolderSourceConfig};
        
        match source.source_type {
            SourceType::WebDAV => {
                let config: WebDAVSourceConfig = serde_json::from_value(source.config.clone())
                    .map_err(|e| format!("Failed to parse WebDAV configuration: {}", e))?;
                
                if config.server_url.trim().is_empty() {
                    return Err("WebDAV server URL is empty".to_string());
                }
                if config.username.trim().is_empty() {
                    return Err("WebDAV username is empty".to_string());
                }
                if config.password.trim().is_empty() {
                    return Err("WebDAV password is empty".to_string());
                }
                if config.watch_folders.is_empty() {
                    return Err("WebDAV watch folders list is empty".to_string());
                }
                Ok(())
            }
            SourceType::S3 => {
                let _config: S3SourceConfig = serde_json::from_value(source.config.clone())
                    .map_err(|e| format!("Failed to parse S3 configuration: {}", e))?;
                Ok(())
            }
            SourceType::LocalFolder => {
                let _config: LocalFolderSourceConfig = serde_json::from_value(source.config.clone())
                    .map_err(|e| format!("Failed to parse Local Folder configuration: {}", e))?;
                Ok(())
            }
        }
    }

    async fn validate_webdav_connectivity(source: &crate::models::Source) -> Result<(), String> {
        use crate::models::WebDAVSourceConfig;
        
        let config: WebDAVSourceConfig = serde_json::from_value(source.config.clone())
            .map_err(|e| format!("Config parse error: {}", e))?;

        let webdav_config = crate::services::webdav::WebDAVConfig {
            server_url: config.server_url.clone(),
            username: config.username.clone(),
            password: config.password.clone(),
            watch_folders: config.watch_folders.clone(),
            file_extensions: config.file_extensions.clone(),
            timeout_seconds: 30, // Quick connectivity test
            server_type: config.server_type.clone(),
        };

        let webdav_service = crate::services::webdav::WebDAVService::new(webdav_config)
            .map_err(|e| format!("Service creation failed: {}", e))?;

        let test_config = crate::models::WebDAVTestConnection {
            server_url: config.server_url,
            username: config.username,
            password: config.password,
            server_type: config.server_type,
        };
        
        crate::services::webdav::WebDAVService::test_connection_with_config(&test_config).await
            .map_err(|e| format!("Connection test failed: {}", e))?;

        Ok(())
    }

    async fn validate_local_folder_access(_source: &crate::models::Source) -> Result<(), String> {
        // Simplified local folder validation - could be enhanced
        // For now, just return OK as local folders are validated differently
        Ok(())
    }

    async fn validate_s3_connectivity(_source: &crate::models::Source) -> Result<(), String> {
        // Simplified S3 validation - could be enhanced with actual AWS SDK calls
        // For now, just return OK as S3 validation requires more complex setup
        Ok(())
    }


    async fn analyze_sync_patterns(
        source: &crate::models::Source,
        state: &Arc<AppState>
    ) -> Result<SyncHealthAnalysis, Box<dyn std::error::Error + Send + Sync>> {
        let mut score_penalty = 0;
        let mut issues = Vec::new();

        // Check recent sync history
        let sync_stats = sqlx::query(
            r#"
            SELECT 
                COUNT(*) as total_syncs,
                SUM(CASE WHEN total_files_synced = 0 THEN 1 ELSE 0 END) as empty_syncs,
                MAX(last_sync_at) as last_sync,
                AVG(total_files_synced) as avg_files_per_sync
            FROM sources 
            WHERE id = $1 AND last_sync_at >= NOW() - INTERVAL '7 days'
            "#
        )
        .bind(source.id)
        .fetch_one(state.db.get_pool())
        .await?;

        let total_syncs: i64 = sync_stats.try_get("total_syncs").unwrap_or(0);
        let empty_syncs: i64 = sync_stats.try_get("empty_syncs").unwrap_or(0);

        if total_syncs > 0 {
            let empty_sync_ratio = (empty_syncs as f64) / (total_syncs as f64);
            
            if empty_sync_ratio > 0.8 {
                score_penalty += 20;
                issues.push(serde_json::json!({
                    "type": "sync_pattern",
                    "severity": "warning",
                    "message": format!("High empty sync ratio: {:.1}% of recent syncs found no files", empty_sync_ratio * 100.0),
                    "recommendation": "This may indicate connectivity issues or that the source has no new content"
                }));
            }

            if total_syncs < 2 && chrono::Utc::now().signed_duration_since(source.created_at).num_days() > 1 {
                score_penalty += 10;
                issues.push(serde_json::json!({
                    "type": "sync_pattern",
                    "severity": "info",
                    "message": "Very few syncs performed since source creation",
                    "recommendation": "Consider enabling auto-sync or manually triggering sync to ensure content is up to date"
                }));
            }
        }

        Ok(SyncHealthAnalysis { score_penalty, issues })
    }


    async fn analyze_error_patterns(
        source: &crate::models::Source,
        _state: &Arc<AppState>
    ) -> Result<ErrorAnalysis, Box<dyn std::error::Error + Send + Sync>> {
        let mut score_penalty = 0;
        let mut issues = Vec::new();

        // Check if source has recent errors
        if let Some(last_error_at) = source.last_error_at {
            let hours_since_error = chrono::Utc::now().signed_duration_since(last_error_at).num_hours();
            
            if hours_since_error < 24 {
                score_penalty += 15;
                issues.push(serde_json::json!({
                    "type": "error_pattern", 
                    "severity": "warning",
                    "message": format!("Recent error occurred {} hours ago", hours_since_error),
                    "recommendation": format!("Last error: {}", source.last_error.as_deref().unwrap_or("Unknown error"))
                }));
            }
        }

        // Check if source is in error state
        if source.status == crate::models::SourceStatus::Error {
            score_penalty += 25;
            issues.push(serde_json::json!({
                "type": "error_pattern",
                "severity": "critical", 
                "message": "Source is currently in error state",
                "recommendation": "Review and fix the configuration or connectivity issues"
            }));
        }

        Ok(ErrorAnalysis { score_penalty, issues })
    }

    async fn run_periodic_validations(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Get all enabled sources
        let sources = self.state.db.get_sources_for_sync().await?;
        
        for source in sources {
            // Only validate if it's been more than 30 minutes since last validation
            let should_validate = if let Some(last_validation) = source.last_validation_at {
                chrono::Utc::now().signed_duration_since(last_validation).num_minutes() > 30
            } else {
                true // Never validated before
            };
            
            if should_validate && source.enabled {
                info!("Running periodic validation for source: {}", source.name);
                
                // Run validation in background to avoid blocking
                let source_clone = source.clone();
                let state_clone = self.state.clone();
                tokio::spawn(async move {
                    if let Err(e) = Self::validate_source_health(&source_clone, &state_clone).await {
                        error!("Periodic validation failed for source {}: {}", source_clone.name, e);
                    }
                });
            }
        }
        
        Ok(())
    }
}