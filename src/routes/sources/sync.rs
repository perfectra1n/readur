use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::Json,
};
use std::sync::Arc;
use uuid::Uuid;
use tracing::{error, info};

use crate::{
    auth::AuthUser,
    models::SourceStatus,
    AppState,
};

/// Trigger a sync for a source
#[utoipa::path(
    post,
    path = "/api/sources/{id}/sync",
    tag = "sources",
    security(
        ("bearer_auth" = [])
    ),
    params(
        ("id" = Uuid, Path, description = "Source ID")
    ),
    responses(
        (status = 200, description = "Sync triggered successfully"),
        (status = 401, description = "Unauthorized"),
        (status = 404, description = "Source not found"),
        (status = 409, description = "Source is already syncing"),
        (status = 500, description = "Internal server error"),
        (status = 501, description = "Not implemented - Source type not supported")
    )
)]
pub async fn trigger_sync(
    auth_user: AuthUser,
    Path(source_id): Path<Uuid>,
    State(state): State<Arc<AppState>>,
) -> Result<StatusCode, StatusCode> {
    let source = state
        .db
        .get_source(auth_user.user.id, source_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    // Check if already syncing
    if matches!(source.status, SourceStatus::Syncing) {
        return Err(StatusCode::CONFLICT);
    }

    // Update status to syncing
    state
        .db
        .update_source_status(source_id, SourceStatus::Syncing, None)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Trigger sync using the universal source scheduler
    if let Some(scheduler) = &state.source_scheduler {
        if let Err(e) = scheduler.trigger_sync(source_id).await {
            error!("Failed to trigger sync for source {}: {}", source_id, e);
            state
                .db
                .update_source_status(
                    source_id,
                    SourceStatus::Error,
                    Some(format!("Failed to trigger sync: {}", e)),
                )
                .await
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    } else {
        // Fallback to WebDAV scheduler for backward compatibility
        match source.source_type {
            crate::models::SourceType::WebDAV => {
                if let Some(webdav_scheduler) = &state.webdav_scheduler {
                    webdav_scheduler.trigger_sync(source_id).await;
                } else {
                    state
                        .db
                        .update_source_status(
                            source_id,
                            SourceStatus::Error,
                            Some("No scheduler available".to_string()),
                        )
                        .await
                        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
                    return Err(StatusCode::INTERNAL_SERVER_ERROR);
                }
            }
            _ => {
                state
                    .db
                    .update_source_status(
                        source_id,
                        SourceStatus::Error,
                        Some("Source type not supported".to_string()),
                    )
                    .await
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
                return Err(StatusCode::NOT_IMPLEMENTED);
            }
        }
    }

    Ok(StatusCode::OK)
}

/// Stop sync for a source
#[utoipa::path(
    post,
    path = "/api/sources/{id}/sync/stop",
    tag = "sources",
    security(
        ("bearer_auth" = [])
    ),
    params(
        ("id" = Uuid, Path, description = "Source ID")
    ),
    responses(
        (status = 200, description = "Sync stopped successfully"),
        (status = 401, description = "Unauthorized"),
        (status = 404, description = "Source not found"),
        (status = 409, description = "Source is not currently syncing"),
        (status = 500, description = "Internal server error")
    )
)]
pub async fn stop_sync(
    auth_user: AuthUser,
    Path(source_id): Path<Uuid>,
    State(state): State<Arc<AppState>>,
) -> Result<StatusCode, StatusCode> {
    let source = state
        .db
        .get_source(auth_user.user.id, source_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    // Allow stopping sync regardless of current status to handle edge cases
    // where the database status might be out of sync with actual running tasks

    // Stop sync using the universal source scheduler
    if let Some(scheduler) = &state.source_scheduler {
        if let Err(e) = scheduler.stop_sync(source_id).await {
            let error_msg = e.to_string();
            // If no sync is running, treat it as success since the desired state is achieved
            if error_msg.contains("No running sync found") {
                info!("No sync was running for source {}, updating status to idle", source_id);
                // Update status to idle since no sync is running
                state
                    .db
                    .update_source_status(
                        source_id,
                        SourceStatus::Idle,
                        None,
                    )
                    .await
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            } else {
                error!("Failed to stop sync for source {}: {}", source_id, e);
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }
        }
    } else {
        // Update status directly if no scheduler available (fallback)
        state
            .db
            .update_source_status(
                source_id,
                SourceStatus::Idle,
                Some("Sync cancelled by user".to_string()),
            )
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    }

    Ok(StatusCode::OK)
}

/// Trigger a deep scan for a source
#[utoipa::path(
    post,
    path = "/api/sources/{id}/deep-scan",
    tag = "sources",
    security(
        ("bearer_auth" = [])
    ),
    params(
        ("id" = Uuid, Path, description = "Source ID")
    ),
    responses(
        (status = 200, description = "Deep scan started successfully"),
        (status = 401, description = "Unauthorized"),
        (status = 404, description = "Source not found"),
        (status = 409, description = "Source is already syncing"),
        (status = 500, description = "Internal server error")
    )
)]
pub async fn trigger_deep_scan(
    auth_user: AuthUser,
    Path(source_id): Path<Uuid>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    info!("Starting deep scan for source {} by user {}", source_id, auth_user.user.username);
    
    let source = state
        .db
        .get_source(auth_user.user.id, source_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    // Check if source is already syncing
    if matches!(source.status, SourceStatus::Syncing) {
        return Ok(Json(serde_json::json!({
            "success": false,
            "error": "source_already_syncing",
            "message": "Source is already syncing. Please wait for the current sync to complete before starting a deep scan."
        })));
    }

    match source.source_type {
        crate::models::SourceType::WebDAV => {
            // Handle WebDAV deep scan
            let config: crate::models::WebDAVSourceConfig = serde_json::from_value(source.config)
                .map_err(|e| {
                    error!("Failed to parse WebDAV config for source {}: {}", source_id, e);
                    StatusCode::INTERNAL_SERVER_ERROR
                })?;

            // Create WebDAV service
            let webdav_config = crate::services::webdav::WebDAVConfig {
                server_url: config.server_url.clone(),
                username: config.username.clone(),
                password: config.password.clone(),
                watch_folders: config.watch_folders.clone(),
                file_extensions: config.file_extensions.clone(),
                timeout_seconds: 600, // 10 minutes for deep scan
                server_type: config.server_type.clone(),
            };

            let webdav_service = crate::services::webdav::WebDAVService::new(webdav_config.clone())
                .map_err(|e| {
                    error!("Failed to create WebDAV service for deep scan: {}", e);
                    StatusCode::INTERNAL_SERVER_ERROR
                })?;

            // Update source status to syncing
            state
                .db
                .update_source_status(
                    source_id,
                    SourceStatus::Syncing,
                    Some("Deep scan in progress".to_string()),
                )
                .await
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

            // Start deep scan in background
            let state_clone = state.clone();
            let user_id = auth_user.user.id;
            let source_name = source.name.clone();
            let source_id_clone = source_id;
            let config_clone = config.clone();
            
            tokio::spawn(async move {
                let start_time = chrono::Utc::now();
                
                // Use smart sync service for deep scans - this will properly reset directory ETags
                let smart_sync_service = crate::services::webdav::SmartSyncService::new(state_clone.clone());
                let mut all_files_to_process = Vec::new();
                let mut total_directories_tracked = 0;
                
                // Process each watch folder using smart sync
                for watch_folder in &webdav_config.watch_folders {
                    info!("🔍 Deep scan processing watch folder: {}", watch_folder);
                    
                    match smart_sync_service.perform_smart_sync(
                        user_id, 
                        &webdav_service, 
                        watch_folder, 
                        crate::services::webdav::SmartSyncStrategy::FullDeepScan // Force deep scan for directory reset
                    ).await {
                        Ok(sync_result) => {
                            info!("Deep scan found {} files and {} directories in {}", 
                                  sync_result.files.len(), sync_result.directories.len(), watch_folder);
                            
                            // Filter files by extensions 
                            let filtered_files: Vec<_> = sync_result.files.into_iter()
                                .filter(|file_info| {
                                    let file_extension = std::path::Path::new(&file_info.name)
                                        .extension()
                                        .and_then(|ext| ext.to_str())
                                        .unwrap_or("")
                                        .to_lowercase();
                                    config_clone.file_extensions.contains(&file_extension)
                                })
                                .collect();
                                
                            all_files_to_process.extend(filtered_files);
                            total_directories_tracked += sync_result.directories.len();
                        }
                        Err(e) => {
                            error!("Deep scan failed for watch folder {}: {}", watch_folder, e);
                            // Continue with other folders rather than failing completely
                        }
                    }
                }
                
                if !all_files_to_process.is_empty() {
                    info!("Deep scan will process {} files from {} directories for source {}", 
                          all_files_to_process.len(), total_directories_tracked, source_id_clone);
                        
                            // Process files using the existing sync mechanism
                            match crate::routes::webdav::webdav_sync::process_files_for_deep_scan(
                                state_clone.clone(),
                                user_id,
                                &webdav_service,
                                &all_files_to_process,
                                true, // enable background OCR
                                Some(source_id_clone)
                            ).await {
                                Ok(files_processed) => {
                                    let duration = chrono::Utc::now() - start_time;
                                    info!("Deep scan completed for source {}: {} files processed in {:?}", 
                                        source_id_clone, files_processed, duration);
                                    
                                    // Update source status to idle
                                    if let Err(e) = state_clone.db.update_source_status(
                                        source_id_clone,
                                        SourceStatus::Idle,
                                        Some(format!("Deep scan completed: {} files processed", files_processed)),
                                    ).await {
                                        error!("Failed to update source status after deep scan: {}", e);
                                    }
                                    
                                    // Send success notification
                                    let notification = crate::models::CreateNotification {
                                        notification_type: "success".to_string(),
                                        title: "Deep Scan Completed".to_string(),
                                        message: format!(
                                            "Smart deep scan of {} completed successfully. {} files processed, {} directories tracked in {:.1} minutes.",
                                            source_name,
                                            files_processed,
                                            total_directories_tracked,
                                            duration.num_seconds() as f64 / 60.0
                                        ),
                                        action_url: Some("/documents".to_string()),
                                        metadata: Some(serde_json::json!({
                                            "source_id": source_id_clone,
                                            "scan_type": "deep_scan",
                                            "files_processed": files_processed,
                                            "duration_seconds": duration.num_seconds()
                                        })),
                                    };
                                    
                                    if let Err(e) = state_clone.db.create_notification(user_id, &notification).await {
                                        error!("Failed to create deep scan success notification: {}", e);
                                    }
                                }
                                Err(e) => {
                                    error!("Deep scan file processing failed for source {}: {}", source_id_clone, e);
                                    
                                    // Update source status to error
                                    if let Err(e2) = state_clone.db.update_source_status(
                                        source_id_clone,
                                        SourceStatus::Error,
                                        Some(format!("Deep scan failed: {}", e)),
                                    ).await {
                                        error!("Failed to update source status after deep scan error: {}", e2);
                                    }
                                    
                                    // Send error notification
                                    let notification = crate::models::CreateNotification {
                                        notification_type: "error".to_string(),
                                        title: "Deep Scan Failed".to_string(),
                                        message: format!("Deep scan of {} failed: {}", source_name, e),
                                        action_url: Some("/sources".to_string()),
                                        metadata: Some(serde_json::json!({
                                            "source_id": source_id_clone,
                                            "scan_type": "deep_scan",
                                            "error": e.to_string()
                                        })),
                                    };
                                    
                                    if let Err(e) = state_clone.db.create_notification(user_id, &notification).await {
                                        error!("Failed to create deep scan error notification: {}", e);
                                    }
                                }
                            }

                        } else {
                            info!("Deep scan found no files but tracked {} directories for source {}", 
                                  total_directories_tracked, source_id_clone);
                            
                            // Update source status to idle even if no files found
                            if let Err(e) = state_clone.db.update_source_status(
                                source_id_clone,
                                SourceStatus::Idle,
                                Some(format!("Smart deep scan completed: {} directories tracked, no files found", total_directories_tracked)),
                            ).await {
                                error!("Failed to update source status after empty deep scan: {}", e);
                            }
                        }
            });

            Ok(Json(serde_json::json!({
                "success": true,
                "message": format!("Deep scan started for source '{}'. This will perform a complete rescan of all configured folders.", source.name)
            })))
        }
        _ => {
            error!("Deep scan not supported for source type: {:?}", source.source_type);
            Ok(Json(serde_json::json!({
                "success": false,
                "error": "unsupported_source_type",
                "message": "Deep scan is currently only supported for WebDAV sources"
            })))
        }
    }
}