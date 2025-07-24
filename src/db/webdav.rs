use anyhow::Result;
use sqlx::Row;
use uuid::Uuid;

use super::Database;

impl Database {
    pub async fn get_webdav_sync_state(&self, user_id: Uuid) -> Result<Option<crate::models::WebDAVSyncState>> {
        self.with_retry(|| async {
            let row = sqlx::query(
                r#"SELECT id, user_id, last_sync_at, sync_cursor, is_running, files_processed, 
                   files_remaining, current_folder, errors, created_at, updated_at
                   FROM webdav_sync_state WHERE user_id = $1"#
            )
            .bind(user_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| anyhow::anyhow!("Database query failed: {}", e))?;

        match row {
            Some(row) => Ok(Some(crate::models::WebDAVSyncState {
                id: row.get("id"),
                user_id: row.get("user_id"),
                last_sync_at: row.get("last_sync_at"),
                sync_cursor: row.get("sync_cursor"),
                is_running: row.get("is_running"),
                files_processed: row.get("files_processed"),
                files_remaining: row.get("files_remaining"),
                current_folder: row.get("current_folder"),
                errors: row.get("errors"),
                created_at: row.get("created_at"),
                updated_at: row.get("updated_at"),
            })),
            None => Ok(None),
        }
        }).await
    }

    pub async fn update_webdav_sync_state(&self, user_id: Uuid, state: &crate::models::UpdateWebDAVSyncState) -> Result<()> {
        self.with_retry(|| async {
            sqlx::query(
                r#"INSERT INTO webdav_sync_state (user_id, last_sync_at, sync_cursor, is_running, 
                   files_processed, files_remaining, current_folder, errors, updated_at)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
                   ON CONFLICT (user_id) DO UPDATE SET
                   last_sync_at = EXCLUDED.last_sync_at,
                   sync_cursor = EXCLUDED.sync_cursor,
                   is_running = EXCLUDED.is_running,
                   files_processed = EXCLUDED.files_processed,
                   files_remaining = EXCLUDED.files_remaining,
                   current_folder = EXCLUDED.current_folder,
                   errors = EXCLUDED.errors,
                   updated_at = NOW()"#
            )
            .bind(user_id)
            .bind(state.last_sync_at)
            .bind(&state.sync_cursor)
            .bind(state.is_running)
            .bind(state.files_processed)
            .bind(state.files_remaining)
            .bind(&state.current_folder)
            .bind(&state.errors)
            .execute(&self.pool)
            .await
            .map_err(|e| anyhow::anyhow!("Database update failed: {}", e))?;

            Ok(())
        }).await
    }

    // Reset any running WebDAV syncs on startup (handles server restart during sync)
    pub async fn reset_running_webdav_syncs(&self) -> Result<i64> {
        let result = sqlx::query(
            r#"UPDATE webdav_sync_state 
               SET is_running = false, 
                   current_folder = NULL,
                   errors = CASE 
                       WHEN array_length(errors, 1) IS NULL OR array_length(errors, 1) = 0 
                       THEN ARRAY['Sync interrupted by server restart']
                       ELSE array_append(errors, 'Sync interrupted by server restart')
                   END,
                   updated_at = NOW()
               WHERE is_running = true"#
        )
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() as i64)
    }

    // Reset any running source syncs on startup (handles server restart during sync)
    pub async fn reset_running_source_syncs(&self) -> Result<i64> {
        let result = sqlx::query(
            r#"UPDATE sources 
               SET status = 'idle',
                   last_error = CASE 
                       WHEN last_error IS NULL OR last_error = ''
                       THEN 'Sync interrupted by server restart'
                       ELSE last_error || '; Sync interrupted by server restart'
                   END,
                   last_error_at = NOW(),
                   updated_at = NOW()
               WHERE status = 'syncing'"#
        )
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() as i64)
    }

    // WebDAV file tracking operations
    pub async fn get_webdav_file_by_path(&self, user_id: Uuid, webdav_path: &str) -> Result<Option<crate::models::WebDAVFile>> {
        let row = sqlx::query(
            r#"SELECT id, user_id, webdav_path, etag, last_modified, file_size, 
               mime_type, document_id, sync_status, sync_error, created_at, updated_at
               FROM webdav_files WHERE user_id = $1 AND webdav_path = $2"#
        )
        .bind(user_id)
        .bind(webdav_path)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some(row) => Ok(Some(crate::models::WebDAVFile {
                id: row.get("id"),
                user_id: row.get("user_id"),
                webdav_path: row.get("webdav_path"),
                etag: row.get("etag"),
                last_modified: row.get("last_modified"),
                file_size: row.get("file_size"),
                mime_type: row.get("mime_type"),
                document_id: row.get("document_id"),
                sync_status: row.get("sync_status"),
                sync_error: row.get("sync_error"),
                created_at: row.get("created_at"),
                updated_at: row.get("updated_at"),
            })),
            None => Ok(None),
        }
    }

    pub async fn create_or_update_webdav_file(&self, file: &crate::models::CreateWebDAVFile) -> Result<crate::models::WebDAVFile> {
        let row = sqlx::query(
            r#"INSERT INTO webdav_files (user_id, webdav_path, etag, last_modified, file_size, 
               mime_type, document_id, sync_status, sync_error)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
               ON CONFLICT (user_id, webdav_path) DO UPDATE SET
               etag = EXCLUDED.etag,
               last_modified = EXCLUDED.last_modified,
               file_size = EXCLUDED.file_size,
               mime_type = EXCLUDED.mime_type,
               document_id = EXCLUDED.document_id,
               sync_status = EXCLUDED.sync_status,
               sync_error = EXCLUDED.sync_error,
               updated_at = NOW()
               RETURNING id, user_id, webdav_path, etag, last_modified, file_size, 
               mime_type, document_id, sync_status, sync_error, created_at, updated_at"#
        )
        .bind(file.user_id)
        .bind(&file.webdav_path)
        .bind(&file.etag)
        .bind(file.last_modified)
        .bind(file.file_size)
        .bind(&file.mime_type)
        .bind(file.document_id)
        .bind(&file.sync_status)
        .bind(&file.sync_error)
        .fetch_one(&self.pool)
        .await?;

        Ok(crate::models::WebDAVFile {
            id: row.get("id"),
            user_id: row.get("user_id"),
            webdav_path: row.get("webdav_path"),
            etag: row.get("etag"),
            last_modified: row.get("last_modified"),
            file_size: row.get("file_size"),
            mime_type: row.get("mime_type"),
            document_id: row.get("document_id"),
            sync_status: row.get("sync_status"),
            sync_error: row.get("sync_error"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        })
    }

    pub async fn get_pending_webdav_files(&self, user_id: Uuid, limit: i64) -> Result<Vec<crate::models::WebDAVFile>> {
        let rows = sqlx::query(
            r#"SELECT id, user_id, webdav_path, etag, last_modified, file_size, 
               mime_type, document_id, sync_status, sync_error, created_at, updated_at
               FROM webdav_files 
               WHERE user_id = $1 AND sync_status = 'pending'
               ORDER BY created_at ASC
               LIMIT $2"#
        )
        .bind(user_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        let mut files = Vec::new();
        for row in rows {
            files.push(crate::models::WebDAVFile {
                id: row.get("id"),
                user_id: row.get("user_id"),
                webdav_path: row.get("webdav_path"),
                etag: row.get("etag"),
                last_modified: row.get("last_modified"),
                file_size: row.get("file_size"),
                mime_type: row.get("mime_type"),
                document_id: row.get("document_id"),
                sync_status: row.get("sync_status"),
                sync_error: row.get("sync_error"),
                created_at: row.get("created_at"),
                updated_at: row.get("updated_at"),
            });
        }

        Ok(files)
    }

    // Directory tracking functions for efficient sync optimization
    pub async fn get_webdav_directory(&self, user_id: Uuid, directory_path: &str) -> Result<Option<crate::models::WebDAVDirectory>> {
        self.with_retry(|| async {
            let row = sqlx::query(
                r#"SELECT id, user_id, directory_path, directory_etag, last_scanned_at, 
                   file_count, total_size_bytes, created_at, updated_at
                   FROM webdav_directories WHERE user_id = $1 AND directory_path = $2"#
            )
            .bind(user_id)
            .bind(directory_path)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| anyhow::anyhow!("Database query failed: {}", e))?;

            match row {
                Some(row) => Ok(Some(crate::models::WebDAVDirectory {
                    id: row.get("id"),
                    user_id: row.get("user_id"),
                    directory_path: row.get("directory_path"),
                    directory_etag: row.get("directory_etag"),
                    last_scanned_at: row.get("last_scanned_at"),
                    file_count: row.get("file_count"),
                    total_size_bytes: row.get("total_size_bytes"),
                    created_at: row.get("created_at"),
                    updated_at: row.get("updated_at"),
                })),
                None => Ok(None),
            }
        }).await
    }

    pub async fn create_or_update_webdav_directory(&self, directory: &crate::models::CreateWebDAVDirectory) -> Result<crate::models::WebDAVDirectory> {
        let row = sqlx::query(
            r#"INSERT INTO webdav_directories (user_id, directory_path, directory_etag, 
               file_count, total_size_bytes, last_scanned_at, updated_at)
               VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
               ON CONFLICT (user_id, directory_path) DO UPDATE SET
               directory_etag = EXCLUDED.directory_etag,
               file_count = EXCLUDED.file_count,
               total_size_bytes = EXCLUDED.total_size_bytes,
               last_scanned_at = NOW(),
               updated_at = NOW()
               RETURNING id, user_id, directory_path, directory_etag, last_scanned_at,
               file_count, total_size_bytes, created_at, updated_at"#
        )
        .bind(directory.user_id)
        .bind(&directory.directory_path)
        .bind(&directory.directory_etag)
        .bind(directory.file_count)
        .bind(directory.total_size_bytes)
        .fetch_one(&self.pool)
        .await?;

        Ok(crate::models::WebDAVDirectory {
            id: row.get("id"),
            user_id: row.get("user_id"),
            directory_path: row.get("directory_path"),
            directory_etag: row.get("directory_etag"),
            last_scanned_at: row.get("last_scanned_at"),
            file_count: row.get("file_count"),
            total_size_bytes: row.get("total_size_bytes"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        })
    }

    pub async fn update_webdav_directory(&self, user_id: Uuid, directory_path: &str, update: &crate::models::UpdateWebDAVDirectory) -> Result<()> {
        self.with_retry(|| async {
            sqlx::query(
                r#"UPDATE webdav_directories SET 
                   directory_etag = $3,
                   last_scanned_at = $4,
                   file_count = $5,
                   total_size_bytes = $6,
                   updated_at = NOW()
                   WHERE user_id = $1 AND directory_path = $2"#
            )
            .bind(user_id)
            .bind(directory_path)
            .bind(&update.directory_etag)
            .bind(update.last_scanned_at)
            .bind(update.file_count)
            .bind(update.total_size_bytes)
            .execute(&self.pool)
            .await
            .map_err(|e| anyhow::anyhow!("Database update failed: {}", e))?;

            Ok(())
        }).await
    }

    pub async fn list_webdav_directories(&self, user_id: Uuid) -> Result<Vec<crate::models::WebDAVDirectory>> {
        let rows = sqlx::query(
            r#"SELECT id, user_id, directory_path, directory_etag, last_scanned_at,
               file_count, total_size_bytes, created_at, updated_at
               FROM webdav_directories 
               WHERE user_id = $1
               ORDER BY directory_path ASC"#
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await?;

        let mut directories = Vec::new();
        for row in rows {
            directories.push(crate::models::WebDAVDirectory {
                id: row.get("id"),
                user_id: row.get("user_id"),
                directory_path: row.get("directory_path"),
                directory_etag: row.get("directory_etag"),
                last_scanned_at: row.get("last_scanned_at"),
                file_count: row.get("file_count"),
                total_size_bytes: row.get("total_size_bytes"),
                created_at: row.get("created_at"),
                updated_at: row.get("updated_at"),
            });
        }

        Ok(directories)
    }

    /// Clear all WebDAV directory tracking for a user (used for deep scan)
    pub async fn clear_webdav_directories(&self, user_id: Uuid) -> Result<i64> {
        let result = sqlx::query(
            r#"DELETE FROM webdav_directories WHERE user_id = $1"#
        )
        .bind(user_id)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() as i64)
    }

    /// Delete a specific WebDAV directory by path
    pub async fn delete_webdav_directory(&self, user_id: Uuid, directory_path: &str) -> Result<bool> {
        let result = sqlx::query(
            r#"DELETE FROM webdav_directories WHERE user_id = $1 AND directory_path = $2"#
        )
        .bind(user_id)
        .bind(directory_path)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    /// Find directories with incomplete scans that need recovery
    pub async fn get_incomplete_webdav_scans(&self, user_id: Uuid) -> Result<Vec<String>> {
        let rows = sqlx::query(
            r#"SELECT directory_path FROM webdav_directories 
               WHERE user_id = $1 AND scan_in_progress = TRUE
               ORDER BY scan_started_at ASC"#
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|row| row.get("directory_path")).collect())
    }

    /// Find scans that have been running too long (possible crashes)
    pub async fn get_stale_webdav_scans(&self, user_id: Uuid, timeout_minutes: i64) -> Result<Vec<String>> {
        let rows = sqlx::query(
            r#"SELECT directory_path FROM webdav_directories 
               WHERE user_id = $1 AND scan_in_progress = TRUE 
               AND scan_started_at < NOW() - INTERVAL '1 minute' * $2
               ORDER BY scan_started_at ASC"#
        )
        .bind(user_id)
        .bind(timeout_minutes)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|row| row.get("directory_path")).collect())
    }

    /// Mark a directory scan as in progress
    pub async fn mark_webdav_scan_in_progress(&self, user_id: Uuid, directory_path: &str) -> Result<()> {
        sqlx::query(
            r#"INSERT INTO webdav_directories (user_id, directory_path, directory_etag, scan_in_progress, scan_started_at, last_scanned_at)
               VALUES ($1, $2, '', TRUE, NOW(), NOW())
               ON CONFLICT (user_id, directory_path)
               DO UPDATE SET scan_in_progress = TRUE, scan_started_at = NOW(), scan_error = NULL"#
        )
        .bind(user_id)
        .bind(directory_path)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Mark a directory scan as complete
    pub async fn mark_webdav_scan_complete(&self, user_id: Uuid, directory_path: &str) -> Result<()> {
        sqlx::query(
            r#"UPDATE webdav_directories 
               SET scan_in_progress = FALSE, scan_started_at = NULL, scan_error = NULL,
                   last_scanned_at = NOW(), updated_at = NOW()
               WHERE user_id = $1 AND directory_path = $2"#
        )
        .bind(user_id)
        .bind(directory_path)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Mark a directory scan as failed with error
    pub async fn mark_webdav_scan_failed(&self, user_id: Uuid, directory_path: &str, error: &str) -> Result<()> {
        sqlx::query(
            r#"UPDATE webdav_directories 
               SET scan_in_progress = FALSE, scan_error = $3, updated_at = NOW()
               WHERE user_id = $1 AND directory_path = $2"#
        )
        .bind(user_id)
        .bind(directory_path)
        .bind(error)
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}