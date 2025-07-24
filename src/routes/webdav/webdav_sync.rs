use std::sync::Arc;
use std::path::Path;
use tracing::{debug, error, info, warn};
use chrono::Utc;
use tokio::sync::Semaphore;
use futures::stream::{FuturesUnordered, StreamExt};

use crate::{
    AppState,
    models::{CreateWebDAVFile, UpdateWebDAVSyncState},
    services::file_service::FileService,
    ingestion::document_ingestion::{DocumentIngestionService, IngestionResult},
    services::webdav::{WebDAVConfig, WebDAVService, SmartSyncService},
};

pub async fn perform_webdav_sync_with_tracking(
    state: Arc<AppState>,
    user_id: uuid::Uuid,
    webdav_service: WebDAVService,
    config: WebDAVConfig,
    enable_background_ocr: bool,
    webdav_source_id: Option<uuid::Uuid>,
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    info!("Performing WebDAV sync for user {} on {} folders", user_id, config.watch_folders.len());
    
    // Update sync state to running
    let sync_state_update = UpdateWebDAVSyncState {
        last_sync_at: Some(Utc::now()),
        sync_cursor: None,
        is_running: true,
        files_processed: 0,
        files_remaining: 0,
        current_folder: None,
        errors: Vec::new(),
    };
    
    if let Err(e) = state.db.update_webdav_sync_state(user_id, &sync_state_update).await {
        error!("Failed to update sync state: {}", e);
    }

    // Ensure sync state is cleared on any exit path
    let cleanup_sync_state = |errors: Vec<String>, files_processed: usize| {
        let state_clone = state.clone();
        let user_id_clone = user_id;
        tokio::spawn(async move {
            let final_state = UpdateWebDAVSyncState {
                last_sync_at: Some(Utc::now()),
                sync_cursor: None,
                is_running: false,
                files_processed: files_processed as i64,
                files_remaining: 0,
                current_folder: None,
                errors,
            };
            
            if let Err(e) = state_clone.db.update_webdav_sync_state(user_id_clone, &final_state).await {
                error!("Failed to cleanup sync state: {}", e);
            }
        });
    };

    // Perform sync with proper cleanup
    let sync_result = perform_sync_internal(state.clone(), user_id, webdav_service, config, enable_background_ocr, webdav_source_id).await;
    
    match &sync_result {
        Ok(files_processed) => {
            cleanup_sync_state(Vec::new(), *files_processed);
        }
        Err(e) => {
            let error_msg = format!("Sync failed: {}", e);
            cleanup_sync_state(vec![error_msg], 0);
        }
    }
    
    sync_result
}

async fn perform_sync_internal(
    state: Arc<AppState>,
    user_id: uuid::Uuid,
    webdav_service: WebDAVService,
    config: WebDAVConfig,
    enable_background_ocr: bool,
    webdav_source_id: Option<uuid::Uuid>,
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    
    let mut total_files_processed = 0;
    let mut sync_errors = Vec::new();
    
    // Process each watch folder
    for folder_path in &config.watch_folders {
        // Check if sync has been cancelled before processing each folder
        if let Ok(Some(sync_state)) = state.db.get_webdav_sync_state(user_id).await {
            if !sync_state.is_running {
                info!("WebDAV sync cancelled, stopping folder processing");
                return Err("Sync cancelled by user".into());
            }
        }
        
        info!("Syncing folder: {}", folder_path);
        
        // Update current folder in sync state
        let folder_update = UpdateWebDAVSyncState {
            last_sync_at: Some(Utc::now()),
            sync_cursor: None,
            is_running: true,
            files_processed: total_files_processed as i64,
            files_remaining: 0,
            current_folder: Some(folder_path.clone()),
            errors: sync_errors.clone(),
        };
        
        if let Err(e) = state.db.update_webdav_sync_state(user_id, &folder_update).await {
            warn!("Failed to update sync folder state: {}", e);
        }
        
        // Use smart sync service for intelligent scanning
        let smart_sync_service = SmartSyncService::new(state.clone());
        
        match smart_sync_service.evaluate_and_sync(user_id, &webdav_service, folder_path).await {
            Ok(Some(sync_result)) => {
                info!("🧠 Smart sync completed for {}: {} files found using {:?}", 
                      folder_path, sync_result.files.len(), sync_result.strategy_used);
                
                // Filter files for processing (directories already handled by smart sync service)
                let files_to_process: Vec<_> = sync_result.files.into_iter()
                    .filter(|file_info| {
                        // Check if file extension is supported
                        let file_extension = Path::new(&file_info.name)
                            .extension()
                            .and_then(|ext| ext.to_str())
                            .unwrap_or("")
                            .to_lowercase();
                        
                        config.file_extensions.contains(&file_extension)
                    })
                    .collect();
                
                info!("Processing {} files from folder {}", files_to_process.len(), folder_path);
                
                // Process files concurrently with a limit to avoid overwhelming the system
                let concurrent_limit = 5; // Max 5 concurrent downloads
                let semaphore = Arc::new(Semaphore::new(concurrent_limit));
                let mut folder_files_processed = 0;
                
                // Create futures for processing each file concurrently
                let mut file_futures = FuturesUnordered::new();
                
                for file_info in files_to_process.iter() {
                    let state_clone = state.clone();
                    let webdav_service_clone = webdav_service.clone();
                    let file_info_clone = file_info.clone();
                    let semaphore_clone = semaphore.clone();
                    
                    // Create a future for processing this file
                    let future = async move {
                        process_single_file(
                            state_clone,
                            user_id,
                            &webdav_service_clone,
                            &file_info_clone,
                            enable_background_ocr,
                            semaphore_clone,
                            webdav_source_id,
                        ).await
                    };
                    
                    file_futures.push(future);
                }
                
                // Process files concurrently and collect results
                while let Some(result) = file_futures.next().await {
                    // Check if sync has been cancelled
                    if let Ok(Some(sync_state)) = state.db.get_webdav_sync_state(user_id).await {
                        if !sync_state.is_running {
                            info!("WebDAV sync cancelled during file processing, stopping");
                            // Cancel remaining futures
                            file_futures.clear();
                            sync_errors.push("Sync cancelled by user during file processing".to_string());
                            break;
                        }
                    }
                    
                    match result {
                        Ok(processed) => {
                            if processed {
                                folder_files_processed += 1;
                                debug!("Successfully processed file ({} completed in this folder)", folder_files_processed);
                            }
                        }
                        Err(error) => {
                            error!("File processing error: {}", error);
                            sync_errors.push(error);
                        }
                    }
                    
                    // Update progress periodically
                    let progress_update = UpdateWebDAVSyncState {
                        last_sync_at: Some(Utc::now()),
                        sync_cursor: None,
                        is_running: true,
                        files_processed: (total_files_processed + folder_files_processed) as i64,
                        files_remaining: (files_to_process.len() - folder_files_processed) as i64,
                        current_folder: Some(folder_path.clone()),
                        errors: sync_errors.clone(),
                    };
                    
                    if let Err(e) = state.db.update_webdav_sync_state(user_id, &progress_update).await {
                        warn!("Failed to update sync progress: {}", e);
                    }
                }
                
                total_files_processed += folder_files_processed;
            }
            Ok(None) => {
                info!("✅ Smart sync: No changes detected for {}, skipping folder", folder_path);
                // No files to process, continue to next folder
            }
            Err(e) => {
                error!("Smart sync failed for folder {}: {}", folder_path, e);
                sync_errors.push(format!("Smart sync failed for folder {}: {}", folder_path, e));
            }
        }
    }
    
    info!("WebDAV sync completed for user {}: {} files processed", user_id, total_files_processed);
    Ok(total_files_processed)
}

// Helper function to process a single file asynchronously
async fn process_single_file(
    state: Arc<AppState>,
    user_id: uuid::Uuid,
    webdav_service: &WebDAVService,
    file_info: &crate::models::FileIngestionInfo,
    enable_background_ocr: bool,
    semaphore: Arc<Semaphore>,
    webdav_source_id: Option<uuid::Uuid>,
) -> Result<bool, String> {
    // Acquire semaphore permit to limit concurrent downloads
    let _permit = semaphore.acquire().await.map_err(|e| format!("Semaphore error: {}", e))?;
    
    // Check if sync has been cancelled before processing this file
    if let Ok(Some(sync_state)) = state.db.get_webdav_sync_state(user_id).await {
        if !sync_state.is_running {
            info!("Sync cancelled, skipping file: {}", file_info.path);
            return Err("Sync cancelled by user".to_string());
        }
    }
    
    debug!("Processing file: {}", file_info.path);
    
    // Check if we've already processed this file
    debug!("Checking WebDAV tracking for: {}", file_info.path);
    match state.db.get_webdav_file_by_path(user_id, &file_info.path).await {
        Ok(Some(existing_file)) => {
            debug!("Found existing WebDAV file record: {} (current ETag: {}, remote ETag: {})", 
                file_info.path, existing_file.etag, file_info.etag);
            
            // Check if file has changed (compare ETags)
            if existing_file.etag == file_info.etag {
                debug!("Skipping unchanged WebDAV file: {} (ETag: {})", file_info.path, file_info.etag);
                return Ok(false); // Not processed (no change)
            }
            debug!("WebDAV file has changed: {} (old ETag: {}, new ETag: {})", 
                file_info.path, existing_file.etag, file_info.etag);
        }
        Ok(None) => {
            debug!("New WebDAV file detected: {}", file_info.path);
        }
        Err(e) => {
            warn!("Error checking existing WebDAV file {}: {}", file_info.path, e);
        }
    }
    
    // Download the file
    let file_data = webdav_service.download_file(&file_info.path).await
        .map_err(|e| format!("Failed to download {}: {}", file_info.path, e))?;
    
    debug!("Downloaded file: {} ({} bytes)", file_info.name, file_data.len());
    
    // Use the unified ingestion service for consistent deduplication
    let file_service = FileService::new(state.config.upload_path.clone());
    let ingestion_service = DocumentIngestionService::new(state.db.clone(), file_service);
    
    let result = if let Some(source_id) = webdav_source_id {
        ingestion_service
            .ingest_from_file_info(
                &file_info,
                file_data,
                user_id,
                crate::ingestion::document_ingestion::DeduplicationPolicy::TrackAsDuplicate,
                "webdav_sync",
                Some(source_id),
            )
            .await
    } else {
        // Fallback for backward compatibility - treat as generic WebDAV sync
        ingestion_service
            .ingest_from_file_info(
                &file_info,
                file_data,
                user_id,
                crate::ingestion::document_ingestion::DeduplicationPolicy::Skip,
                "webdav_sync",
                Some(uuid::Uuid::new_v4()), // Generate a temporary ID for tracking
            )
            .await
    };

    let result = result.map_err(|e| format!("Document ingestion failed for {}: {}", file_info.name, e))?;

    let (document, should_queue_ocr, webdav_sync_status) = match result {
        IngestionResult::Created(doc) => {
            debug!("Created new document for {}: {}", file_info.name, doc.id);
            (doc, true, "synced") // New document - queue for OCR
        }
        IngestionResult::ExistingDocument(doc) => {
            debug!("Found existing document for {}: {}", file_info.name, doc.id);
            (doc, false, "duplicate_content") // Existing document - don't re-queue OCR
        }
        IngestionResult::TrackedAsDuplicate { existing_document_id } => {
            debug!("Tracked {} as duplicate of existing document: {}", file_info.name, existing_document_id);
            
            // For duplicates, we still need to get the document info for WebDAV tracking
            let existing_doc = state.db.get_document_by_id(existing_document_id, user_id, crate::models::UserRole::User).await
                .map_err(|e| format!("Failed to get existing document: {}", e))?
                .ok_or_else(|| "Document not found".to_string())?;
            
            (existing_doc, false, "duplicate_content") // Track as duplicate
        }
        IngestionResult::Skipped { existing_document_id, reason: _ } => {
            debug!("Skipped duplicate file {}: existing document {}", file_info.name, existing_document_id);
            
            // For skipped files, we still need to get the document info for WebDAV tracking
            let existing_doc = state.db.get_document_by_id(existing_document_id, user_id, crate::models::UserRole::User).await
                .map_err(|e| format!("Failed to get existing document: {}", e))?
                .ok_or_else(|| "Document not found".to_string())?;
            
            (existing_doc, false, "duplicate_content") // Track as duplicate
        }
    };

    // Record WebDAV file in tracking table
    let webdav_file = CreateWebDAVFile {
        user_id,
        webdav_path: file_info.path.clone(),
        etag: file_info.etag.clone(),
        last_modified: file_info.last_modified,
        file_size: file_info.size,
        mime_type: file_info.mime_type.clone(),
        document_id: Some(document.id),
        sync_status: webdav_sync_status.to_string(),
        sync_error: None,
    };
    
    if let Err(e) = state.db.create_or_update_webdav_file(&webdav_file).await {
        error!("Failed to record WebDAV file: {}", e);
    }
    
    // Queue for OCR processing if enabled and this is a new document
    if enable_background_ocr && should_queue_ocr {
        debug!("Background OCR is enabled, queueing document {} for processing", document.id);
        
        // Determine priority based on file size
        let priority = if file_info.size <= 1024 * 1024 { 10 } // ≤ 1MB: High priority
        else if file_info.size <= 5 * 1024 * 1024 { 8 } // ≤ 5MB: Medium priority  
        else if file_info.size <= 10 * 1024 * 1024 { 6 } // ≤ 10MB: Normal priority
        else if file_info.size <= 50 * 1024 * 1024 { 4 } // ≤ 50MB: Low priority
        else { 2 }; // > 50MB: Lowest priority
        
        if let Err(e) = state.queue_service.enqueue_document(document.id, priority, file_info.size).await {
            error!("Failed to enqueue document for OCR: {}", e);
        } else {
            debug!("Enqueued document {} for OCR processing", document.id);
        }
    } else {
        debug!("Background OCR is disabled or document already processed, skipping OCR queue for document {}", document.id);
    }
    
    Ok(true) // Successfully processed
}

/// Process files for deep scan - similar to regular sync but forces processing
pub async fn process_files_for_deep_scan(
    state: Arc<AppState>,
    user_id: uuid::Uuid,
    webdav_service: &WebDAVService,
    files_to_process: &[crate::models::FileIngestionInfo],
    enable_background_ocr: bool,
    webdav_source_id: Option<uuid::Uuid>,
) -> Result<usize, anyhow::Error> {
    info!("Processing {} files for deep scan", files_to_process.len());
    
    let concurrent_limit = 5; // Max 5 concurrent downloads
    let semaphore = Arc::new(Semaphore::new(concurrent_limit));
    let mut files_processed = 0;
    let mut sync_errors = Vec::new();
    
    // Create futures for processing each file concurrently
    let mut file_futures = FuturesUnordered::new();
    
    for file_info in files_to_process.iter() {
        let state_clone = state.clone();
        let webdav_service_clone = webdav_service.clone();
        let file_info_clone = file_info.clone();
        let semaphore_clone = semaphore.clone();
        
        // Create a future for processing this file
        let future = async move {
            process_single_file(
                state_clone,
                user_id,
                &webdav_service_clone,
                &file_info_clone,
                enable_background_ocr,
                semaphore_clone,
                webdav_source_id,
            ).await
        };
        
        file_futures.push(future);
    }
    
    // Process files concurrently and collect results
    while let Some(result) = file_futures.next().await {
        match result {
            Ok(processed) => {
                if processed {
                    files_processed += 1;
                    debug!("Deep scan: Successfully processed file ({} completed)", files_processed);
                }
            }
            Err(error) => {
                error!("Deep scan file processing error: {}", error);
                sync_errors.push(error);
            }
        }
    }
    
    if !sync_errors.is_empty() {
        warn!("Deep scan completed with {} errors: {:?}", sync_errors.len(), sync_errors);
    }
    
    info!("Deep scan file processing completed: {} files processed successfully", files_processed);
    Ok(files_processed)
}


