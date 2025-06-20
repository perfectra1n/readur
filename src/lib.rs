pub mod auth;
pub mod batch_ingest;
pub mod config;
pub mod db;
pub mod db_guardrails_simple;
pub mod document_ingestion;
pub mod enhanced_ocr;
pub mod error_management;
pub mod file_service;
pub mod local_folder_service;
pub mod models;
pub mod ocr;
pub mod ocr_api;
pub mod ocr_enhanced;
pub mod ocr_error;
pub mod ocr_health;
pub mod ocr_queue;
pub mod ocr_tests;
pub mod request_throttler;
pub mod routes;
pub mod s3_service;
pub mod seed;
pub mod source_scheduler;
pub mod source_sync;
pub mod swagger;
pub mod watcher;
pub mod webdav_service;
pub mod webdav_scheduler;
pub mod webdav_xml_parser;

#[cfg(test)]
mod tests;

#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

use axum::{http::StatusCode, Json};
use config::Config;
use db::Database;

#[derive(Clone)]
pub struct AppState {
    pub db: Database,
    pub config: Config,
    pub webdav_scheduler: Option<std::sync::Arc<webdav_scheduler::WebDAVScheduler>>,
    pub source_scheduler: Option<std::sync::Arc<source_scheduler::SourceScheduler>>,
    pub queue_service: std::sync::Arc<ocr_queue::OcrQueueService>,
}

/// Health check endpoint for monitoring
pub async fn health_check() -> Result<Json<serde_json::Value>, StatusCode> {
    Ok(Json(serde_json::json!({"status": "ok"})))
}
