use std::sync::Arc;
use std::path::Path;
use anyhow::{anyhow, Result};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use futures::stream::{FuturesUnordered, StreamExt};
use tracing::{debug, error, info};
use uuid::Uuid;

use crate::{
    AppState,
    models::{FileIngestionInfo, Source, SourceType, SourceStatus, LocalFolderSourceConfig, S3SourceConfig, WebDAVSourceConfig},
    services::file_service::FileService,
    ingestion::document_ingestion::{DocumentIngestionService, IngestionResult},
    services::local_folder_service::LocalFolderService,
    services::s3_service::S3Service,
    services::webdav::{WebDAVService, WebDAVConfig},
};

#[derive(Clone)]
pub struct SourceSyncService {
    state: Arc<AppState>,
}

impl SourceSyncService {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }

    /// Perform sync for any source type
    pub async fn sync_source(&self, source: &Source, enable_background_ocr: bool) -> Result<usize> {
        // Call the cancellable version with no cancellation token
        self.sync_source_with_cancellation(source, enable_background_ocr, CancellationToken::new()).await
    }

    /// Perform sync for any source type with cancellation support
    pub async fn sync_source_with_cancellation(&self, source: &Source, enable_background_ocr: bool, cancellation_token: CancellationToken) -> Result<usize> {
        info!("Starting sync for source {} ({})", source.name, source.source_type);

        // Check for cancellation before starting
        if cancellation_token.is_cancelled() {
            info!("Sync for source {} was cancelled before starting", source.name);
            return Err(anyhow!("Sync cancelled"));
        }

        // Update source status to syncing
        if let Err(e) = self.update_source_status(source.id, SourceStatus::Syncing, None).await {
            error!("Failed to update source status: {}", e);
        }

        let sync_result = match source.source_type {
            SourceType::WebDAV => self.sync_webdav_source_with_cancellation(source, enable_background_ocr, cancellation_token.clone()).await,
            SourceType::LocalFolder => self.sync_local_folder_source_with_cancellation(source, enable_background_ocr, cancellation_token.clone()).await,
            SourceType::S3 => self.sync_s3_source_with_cancellation(source, enable_background_ocr, cancellation_token.clone()).await,
        };

        match &sync_result {
            Ok(files_processed) => {
                if cancellation_token.is_cancelled() {
                    info!("Sync for source {} was cancelled during execution", source.name);
                    if let Err(e) = self.update_source_status(source.id, SourceStatus::Idle, Some("Sync cancelled by user")).await {
                        error!("Failed to update source status after cancellation: {}", e);
                    }
                } else {
                    info!("Sync completed for source {}: {} files processed", source.name, files_processed);
                    if let Err(e) = self.update_source_status(source.id, SourceStatus::Idle, None).await {
                        error!("Failed to update source status after successful sync: {}", e);
                    }
                }
            }
            Err(e) => {
                if cancellation_token.is_cancelled() {
                    info!("Sync for source {} was cancelled: {}", source.name, e);
                    if let Err(e) = self.update_source_status(source.id, SourceStatus::Idle, Some("Sync cancelled by user")).await {
                        error!("Failed to update source status after cancellation: {}", e);
                    }
                } else {
                    error!("Sync failed for source {}: {}", source.name, e);
                    let error_msg = format!("Sync failed: {}", e);
                    if let Err(e) = self.update_source_status(source.id, SourceStatus::Error, Some(&error_msg)).await {
                        error!("Failed to update source status after error: {}", e);
                    }
                }
            }
        }

        sync_result
    }

    async fn sync_webdav_source(&self, source: &Source, enable_background_ocr: bool) -> Result<usize> {
        self.sync_webdav_source_with_cancellation(source, enable_background_ocr, CancellationToken::new()).await
    }

    async fn sync_webdav_source_with_cancellation(&self, source: &Source, enable_background_ocr: bool, cancellation_token: CancellationToken) -> Result<usize> {
        let config: WebDAVSourceConfig = serde_json::from_value(source.config.clone())
            .map_err(|e| anyhow!("Invalid WebDAV config: {}", e))?;

        info!("WebDAV source sync config: server_url={}, username={}, watch_folders={:?}, file_extensions={:?}, server_type={:?}", 
            config.server_url, config.username, config.watch_folders, config.file_extensions, config.server_type);

        // Requests to list files in a Nextcloud folder might take > 2 minutes
        // Set timeout to 3 minutes to accommodate large folder structures
        let webdav_config = WebDAVConfig {
            server_url: config.server_url,
            username: config.username,
            password: config.password,
            watch_folders: config.watch_folders,
            file_extensions: config.file_extensions,
            timeout_seconds: 180, // 3 minutes for discover_files_in_folder operations
            server_type: config.server_type,
        };

        let webdav_service = WebDAVService::new(webdav_config.clone())
            .map_err(|e| anyhow!("Failed to create WebDAV service: {}", e))?;

        info!("WebDAV service created successfully, starting sync with {} folders", webdav_config.watch_folders.len());

        self.perform_sync_internal_with_cancellation(
            source.user_id,
            source.id,
            &webdav_config.watch_folders,
            &webdav_config.file_extensions,
            enable_background_ocr,
            cancellation_token,
            |folder_path| {
                let service = webdav_service.clone();
                let state_clone = self.state.clone();
                let user_id = source.user_id;  // Capture user_id from source
                async move { 
                    info!("🧠 Using smart sync for scheduled sync: {}", folder_path);
                    
                    // Use smart sync service for intelligent discovery
                    let smart_sync_service = crate::services::webdav::SmartSyncService::new(state_clone);
                    
                    match smart_sync_service.evaluate_and_sync(user_id, &service, &folder_path).await {
                        Ok(Some(sync_result)) => {
                            info!("✅ Smart sync completed for {}: {} files found using {:?}", 
                                  folder_path, sync_result.files.len(), sync_result.strategy_used);
                            Ok(sync_result.files)
                        },
                        Ok(None) => {
                            info!("🔍 Smart sync: No changes detected for {}, skipping", folder_path);
                            Ok(Vec::new()) // No files to process
                        },
                        Err(e) => {
                            error!("Smart sync failed for scheduled sync {}: {}", folder_path, e);
                            Err(e)
                        }
                    }
                }
            },
            |file_path| {
                let service = webdav_service.clone();
                async move { 
                    debug!("WebDAV download_file called for: {}", file_path);
                    let result = service.download_file(&file_path).await;
                    match &result {
                        Ok(data) => debug!("WebDAV downloaded {} bytes for file: {}", data.len(), file_path),
                        Err(e) => error!("WebDAV download failed for file {}: {}", file_path, e),
                    }
                    result
                }
            }
        ).await
    }

    async fn sync_local_folder_source(&self, source: &Source, enable_background_ocr: bool) -> Result<usize> {
        self.sync_local_folder_source_with_cancellation(source, enable_background_ocr, CancellationToken::new()).await
    }

    async fn sync_local_folder_source_with_cancellation(&self, source: &Source, enable_background_ocr: bool, cancellation_token: CancellationToken) -> Result<usize> {
        let config: LocalFolderSourceConfig = serde_json::from_value(source.config.clone())
            .map_err(|e| anyhow!("Invalid LocalFolder config: {}", e))?;

        let local_service = LocalFolderService::new(config.clone())
            .map_err(|e| anyhow!("Failed to create LocalFolder service: {}", e))?;

        self.perform_sync_internal_with_cancellation(
            source.user_id,
            source.id,
            &config.watch_folders,
            &config.file_extensions,
            enable_background_ocr,
            cancellation_token,
            |folder_path| {
                let service = local_service.clone();
                async move { service.discover_files_in_folder(&folder_path).await }
            },
            |file_path| {
                let service = local_service.clone();
                async move { service.read_file(&file_path).await }
            }
        ).await
    }

    async fn sync_s3_source(&self, source: &Source, enable_background_ocr: bool) -> Result<usize> {
        self.sync_s3_source_with_cancellation(source, enable_background_ocr, CancellationToken::new()).await
    }

    async fn sync_s3_source_with_cancellation(&self, source: &Source, enable_background_ocr: bool, cancellation_token: CancellationToken) -> Result<usize> {
        let config: S3SourceConfig = serde_json::from_value(source.config.clone())
            .map_err(|e| anyhow!("Invalid S3 config: {}", e))?;

        let s3_service = S3Service::new(config.clone()).await
            .map_err(|e| anyhow!("Failed to create S3 service: {}", e))?;

        self.perform_sync_internal_with_cancellation(
            source.user_id,
            source.id,
            &config.watch_folders,
            &config.file_extensions,
            enable_background_ocr,
            cancellation_token,
            |folder_path| {
                let service = s3_service.clone();
                async move { service.discover_files_in_folder(&folder_path).await }
            },
            |file_path| {
                let service = s3_service.clone();
                async move { service.download_file(&file_path).await }
            }
        ).await
    }

    async fn perform_sync_internal<F, D, Fut1, Fut2>(
        &self,
        user_id: Uuid,
        source_id: Uuid,
        watch_folders: &[String],
        file_extensions: &[String],
        enable_background_ocr: bool,
        discover_files: F,
        download_file: D,
    ) -> Result<usize>
    where
        F: Fn(String) -> Fut1,
        D: Fn(String) -> Fut2 + Clone,
        Fut1: std::future::Future<Output = Result<Vec<FileIngestionInfo>>>,
        Fut2: std::future::Future<Output = Result<Vec<u8>>>,
    {
        let mut total_files_processed = 0;

        for folder_path in watch_folders {
            info!("Syncing folder: {}", folder_path);

            // Discover files in the folder
            match discover_files(folder_path.clone()).await {
                Ok(files) => {
                    info!("Found {} files in folder {}", files.len(), folder_path);

                    // Filter files for processing
                    let files_to_process: Vec<_> = files.into_iter()
                        .filter(|file_info| {
                            if file_info.is_directory {
                                return false;
                            }

                            let file_extension = Path::new(&file_info.name)
                                .extension()
                                .and_then(|ext| ext.to_str())
                                .unwrap_or("")
                                .to_lowercase();

                            file_extensions.contains(&file_extension)
                        })
                        .collect();

                    info!("Processing {} files from folder {}", files_to_process.len(), folder_path);

                    // Process files concurrently with a limit
                    let concurrent_limit = 5;
                    let semaphore = Arc::new(Semaphore::new(concurrent_limit));
                    let mut folder_files_processed = 0;

                    let mut file_futures = FuturesUnordered::new();

                    for file_info in files_to_process.iter() {
                        let state_clone = self.state.clone();
                        let file_info_clone = file_info.clone();
                        let semaphore_clone = semaphore.clone();
                        let download_file_clone = download_file.clone();

                        let future = async move {
                            Self::process_single_file(
                                state_clone,
                                user_id,
                                source_id,
                                &file_info_clone,
                                enable_background_ocr,
                                semaphore_clone,
                                download_file_clone,
                            ).await
                        };

                        file_futures.push(future);
                    }

                    // Process files concurrently
                    while let Some(result) = file_futures.next().await {
                        match result {
                            Ok(processed) => {
                                if processed {
                                    folder_files_processed += 1;
                                    debug!("Successfully processed file ({} completed in this folder)", folder_files_processed);
                                }
                            }
                            Err(error) => {
                                error!("File processing error: {}", error);
                            }
                        }
                    }

                    total_files_processed += folder_files_processed;
                }
                Err(e) => {
                    error!("Failed to discover files in folder {}: {}", folder_path, e);
                }
            }
        }

        info!("Source sync completed: {} files processed", total_files_processed);
        Ok(total_files_processed)
    }

    async fn perform_sync_internal_with_cancellation<F, D, Fut1, Fut2>(
        &self,
        user_id: Uuid,
        source_id: Uuid,
        watch_folders: &[String],
        file_extensions: &[String],
        enable_background_ocr: bool,
        cancellation_token: CancellationToken,
        discover_files: F,
        download_file: D,
    ) -> Result<usize>
    where
        F: Fn(String) -> Fut1,
        D: Fn(String) -> Fut2 + Clone,
        Fut1: std::future::Future<Output = Result<Vec<FileIngestionInfo>>>,
        Fut2: std::future::Future<Output = Result<Vec<u8>>>,
    {
        let mut total_files_processed = 0;
        let mut total_files_discovered = 0;
        let mut total_size_bytes = 0i64;

        // First pass: discover all files and calculate totals
        for folder_path in watch_folders {
            if cancellation_token.is_cancelled() {
                info!("Sync cancelled during folder discovery");
                return Err(anyhow!("Sync cancelled"));
            }

            match discover_files(folder_path.clone()).await {
                Ok(files) => {
                    let files_to_process: Vec<_> = files.into_iter()
                        .filter(|file_info| {
                            if file_info.is_directory {
                                return false;
                            }

                            let file_extension = Path::new(&file_info.name)
                                .extension()
                                .and_then(|ext| ext.to_str())
                                .unwrap_or("")
                                .to_lowercase();

                            file_extensions.contains(&file_extension)
                        })
                        .collect();

                    total_files_discovered += files_to_process.len();
                    total_size_bytes += files_to_process.iter().map(|f| f.size).sum::<i64>();
                }
                Err(e) => {
                    error!("Failed to discover files in folder {}: {}", folder_path, e);
                }
            }
        }

        // Update initial statistics with discovered files
        if let Err(e) = self.state.db.update_source_sync_stats(
            source_id,
            0, // files_synced starts at 0
            total_files_discovered as i64,
            total_size_bytes,
        ).await {
            error!("Failed to update initial sync stats: {}", e);
        }

        // Second pass: process files and update stats progressively
        for folder_path in watch_folders {
            // Check for cancellation before processing each folder
            if cancellation_token.is_cancelled() {
                info!("Sync cancelled during folder processing");
                return Err(anyhow!("Sync cancelled"));
            }

            info!("Syncing folder: {}", folder_path);

            // Discover files in the folder
            match discover_files(folder_path.clone()).await {
                Ok(files) => {
                    if cancellation_token.is_cancelled() {
                        info!("Sync cancelled after discovering files");
                        return Err(anyhow!("Sync cancelled"));
                    }

                    info!("Found {} files in folder {}", files.len(), folder_path);

                    // Filter files for processing
                    let files_to_process: Vec<_> = files.into_iter()
                        .filter(|file_info| {
                            if file_info.is_directory {
                                return false;
                            }

                            let file_extension = Path::new(&file_info.name)
                                .extension()
                                .and_then(|ext| ext.to_str())
                                .unwrap_or("")
                                .to_lowercase();

                            file_extensions.contains(&file_extension)
                        })
                        .collect();

                    info!("Processing {} files from folder {}", files_to_process.len(), folder_path);

                    // Process files concurrently with a limit
                    let concurrent_limit = 5;
                    let semaphore = Arc::new(Semaphore::new(concurrent_limit));
                    let mut folder_files_processed = 0;

                    let mut file_futures = FuturesUnordered::new();

                    for file_info in files_to_process.iter() {
                        // Check for cancellation before processing each file
                        if cancellation_token.is_cancelled() {
                            info!("Sync cancelled during file processing");
                            return Err(anyhow!("Sync cancelled"));
                        }

                        let state_clone = self.state.clone();
                        let file_info_clone = file_info.clone();
                        let semaphore_clone = semaphore.clone();
                        let download_file_clone = download_file.clone();
                        let cancellation_token_clone = cancellation_token.clone();

                        let future = async move {
                            Self::process_single_file_with_cancellation(
                                state_clone,
                                user_id,
                                source_id,
                                &file_info_clone,
                                enable_background_ocr,
                                semaphore_clone,
                                download_file_clone,
                                cancellation_token_clone,
                            ).await
                        };

                        file_futures.push(future);
                    }

                    // Process files concurrently and update stats periodically
                    while let Some(result) = file_futures.next().await {
                        // Check for cancellation during processing
                        if cancellation_token.is_cancelled() {
                            info!("Sync cancelled during concurrent file processing");
                            return Err(anyhow!("Sync cancelled"));
                        }

                        match result {
                            Ok(processed) => {
                                if processed {
                                    folder_files_processed += 1;
                                    total_files_processed += 1;
                                    
                                    // Update statistics every 10 files processed or every file if under 10 total
                                    if total_files_processed % 10 == 0 || total_files_discovered <= 10 {
                                        let files_pending = total_files_discovered as i64 - total_files_processed as i64;
                                        if let Err(e) = self.state.db.update_source_sync_stats(
                                            source_id,
                                            total_files_processed as i64,
                                            files_pending.max(0),
                                            total_size_bytes,
                                        ).await {
                                            error!("Failed to update sync stats: {}", e);
                                        }
                                    }
                                    
                                    debug!("Successfully processed file ({} completed in this folder, {} total)", folder_files_processed, total_files_processed);
                                }
                            }
                            Err(error) => {
                                error!("File processing error: {}", error);
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to discover files in folder {}: {}", folder_path, e);
                }
            }
        }

        // Final statistics update
        if let Err(e) = self.state.db.update_source_sync_stats(
            source_id,
            total_files_processed as i64,
            0, // All files are now processed
            total_size_bytes,
        ).await {
            error!("Failed to update final sync stats: {}", e);
        }

        info!("Source sync completed: {} files processed", total_files_processed);
        Ok(total_files_processed)
    }

    async fn process_single_file<D, Fut>(
        state: Arc<AppState>,
        user_id: Uuid,
        source_id: Uuid,
        file_info: &FileIngestionInfo,
        enable_background_ocr: bool,
        semaphore: Arc<Semaphore>,
        download_file: D,
    ) -> Result<bool>
    where
        D: Fn(String) -> Fut,
        Fut: std::future::Future<Output = Result<Vec<u8>>>,
    {
        let _permit = semaphore.acquire().await
            .map_err(|e| anyhow!("Semaphore error: {}", e))?;

        debug!("Processing file: {}", file_info.path);
        
        // Download the file
        let file_data = download_file(file_info.path.clone()).await
            .map_err(|e| anyhow!("Failed to download {}: {}", file_info.path, e))?;

        debug!("Downloaded file: {} ({} bytes)", file_info.name, file_data.len());

        // Use the unified ingestion service for consistent deduplication
        let file_service = FileService::new(state.config.upload_path.clone());
        let ingestion_service = DocumentIngestionService::new(state.db.clone(), file_service);
        
        let result = ingestion_service
            .ingest_from_file_info(
                file_info,
                file_data,
                user_id,
                crate::ingestion::document_ingestion::DeduplicationPolicy::Skip,
                "source_sync",
                Some(source_id),
            )
            .await
            .map_err(|e| anyhow!("Document ingestion failed for {}: {}", file_info.name, e))?;

        let (document, should_queue_ocr) = match result {
            IngestionResult::Created(doc) => {
                debug!("Created new document for {}: {}", file_info.name, doc.id);
                (doc, true) // New document - queue for OCR
            }
            IngestionResult::Skipped { existing_document_id, reason } => {
                info!("Skipped duplicate file {}: {} (existing: {})", file_info.name, reason, existing_document_id);
                return Ok(false); // File was skipped due to deduplication
            }
            IngestionResult::ExistingDocument(doc) => {
                debug!("Found existing document for {}: {}", file_info.name, doc.id);
                (doc, false) // Existing document - don't re-queue OCR
            }
            IngestionResult::TrackedAsDuplicate { existing_document_id } => {
                info!("Tracked {} as duplicate of existing document: {}", file_info.name, existing_document_id);
                return Ok(false); // File was tracked as duplicate
            }
        };

        // Queue for OCR if enabled and this is a new document
        if enable_background_ocr && should_queue_ocr {
            debug!("Background OCR enabled, queueing document {} for processing", document.id);

            let priority = if file_info.size <= 1024 * 1024 { 10 }
            else if file_info.size <= 5 * 1024 * 1024 { 8 }
            else if file_info.size <= 10 * 1024 * 1024 { 6 }
            else if file_info.size <= 50 * 1024 * 1024 { 4 }
            else { 2 };

            if let Err(e) = state.queue_service.enqueue_document(document.id, priority, file_info.size).await {
                error!("Failed to enqueue document for OCR: {}", e);
            } else {
                debug!("Enqueued document {} for OCR processing", document.id);
            }
        }

        Ok(true)
    }

    async fn process_single_file_with_cancellation<D, Fut>(
        state: Arc<AppState>,
        user_id: Uuid,
        source_id: Uuid,
        file_info: &FileIngestionInfo,
        enable_background_ocr: bool,
        semaphore: Arc<Semaphore>,
        download_file: D,
        cancellation_token: CancellationToken,
    ) -> Result<bool>
    where
        D: Fn(String) -> Fut,
        Fut: std::future::Future<Output = Result<Vec<u8>>>,
    {
        // Check for cancellation before starting file processing
        if cancellation_token.is_cancelled() {
            info!("File processing cancelled before starting: {}", file_info.path);
            return Err(anyhow!("Processing cancelled"));
        }

        let _permit = semaphore.acquire().await
            .map_err(|e| anyhow!("Semaphore error: {}", e))?;

        debug!("Processing file: {}", file_info.path);

        // Check for cancellation again after acquiring semaphore
        if cancellation_token.is_cancelled() {
            info!("File processing cancelled after acquiring semaphore: {}", file_info.path);
            return Err(anyhow!("Processing cancelled"));
        }

        // Download the file
        let file_data = download_file(file_info.path.clone()).await
            .map_err(|e| anyhow!("Failed to download {}: {}", file_info.path, e))?;

        // Check for cancellation after download
        if cancellation_token.is_cancelled() {
            info!("File processing cancelled after download: {}", file_info.path);
            return Err(anyhow!("Processing cancelled"));
        }

        debug!("Downloaded file: {} ({} bytes)", file_info.name, file_data.len());

        // Check for cancellation before processing
        if cancellation_token.is_cancelled() {
            info!("File processing cancelled before ingestion: {}", file_info.path);
            return Err(anyhow!("Processing cancelled"));
        }

        // Use the unified ingestion service for consistent deduplication
        let file_service = FileService::new(state.config.upload_path.clone());
        let ingestion_service = DocumentIngestionService::new(state.db.clone(), file_service);
        
        let result = ingestion_service
            .ingest_from_file_info(
                file_info,
                file_data,
                user_id,
                crate::ingestion::document_ingestion::DeduplicationPolicy::Skip,
                "source_sync",
                Some(source_id),
            )
            .await
            .map_err(|e| anyhow!("Document ingestion failed for {}: {}", file_info.name, e))?;

        let (document, should_queue_ocr) = match result {
            IngestionResult::Created(doc) => {
                debug!("Created new document for {}: {}", file_info.name, doc.id);
                (doc, true) // New document - queue for OCR
            }
            IngestionResult::Skipped { existing_document_id, reason } => {
                info!("Skipped duplicate file {}: {} (existing: {})", file_info.name, reason, existing_document_id);
                return Ok(false); // File was skipped due to deduplication
            }
            IngestionResult::ExistingDocument(doc) => {
                debug!("Found existing document for {}: {}", file_info.name, doc.id);
                (doc, false) // Existing document - don't re-queue OCR
            }
            IngestionResult::TrackedAsDuplicate { existing_document_id } => {
                info!("Tracked {} as duplicate of existing document: {}", file_info.name, existing_document_id);
                return Ok(false); // File was tracked as duplicate
            }
        };

        // Queue for OCR if enabled and this is a new document (OCR continues even if sync is cancelled)
        if enable_background_ocr && should_queue_ocr {
            debug!("Background OCR enabled, queueing document {} for processing", document.id);

            let priority = if file_info.size <= 1024 * 1024 { 10 }
            else if file_info.size <= 5 * 1024 * 1024 { 8 }
            else if file_info.size <= 10 * 1024 * 1024 { 6 }
            else if file_info.size <= 50 * 1024 * 1024 { 4 }
            else { 2 };

            if let Err(e) = state.queue_service.enqueue_document(document.id, priority, file_info.size).await {
                error!("Failed to enqueue document for OCR: {}", e);
            } else {
                debug!("Enqueued document {} for OCR processing", document.id);
            }
        }

        Ok(true)
    }

    async fn update_source_status(&self, source_id: Uuid, status: SourceStatus, error_message: Option<&str>) -> Result<()> {
        let query = if let Some(error) = error_message {
            sqlx::query(
                r#"UPDATE sources 
                   SET status = $2, last_error = $3, last_error_at = NOW(), updated_at = NOW()
                   WHERE id = $1"#
            )
            .bind(source_id)
            .bind(status.to_string())
            .bind(error)
        } else {
            sqlx::query(
                r#"UPDATE sources 
                   SET status = $2, last_error = NULL, last_error_at = NULL, updated_at = NOW()
                   WHERE id = $1"#
            )
            .bind(source_id)
            .bind(status.to_string())
        };

        query.execute(self.state.db.get_pool()).await
            .map_err(|e| anyhow!("Failed to update source status: {}", e))?;

        Ok(())
    }

}