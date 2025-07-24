//! Test utilities for loading and working with test images and data
//! 
//! This module provides utilities for loading test images from the tests/test_images/
//! directory and working with them in unit and integration tests.

use std::path::Path;

#[cfg(any(test, feature = "test-utils"))]
use std::sync::Arc;
#[cfg(any(test, feature = "test-utils"))]
use crate::{AppState, models::UserResponse};
#[cfg(any(test, feature = "test-utils"))]
use axum::Router;
#[cfg(any(test, feature = "test-utils"))]
use serde_json::json;
#[cfg(any(test, feature = "test-utils"))]
use uuid;
#[cfg(any(test, feature = "test-utils"))]
use testcontainers::{runners::AsyncRunner, ContainerAsync, ImageExt};
#[cfg(any(test, feature = "test-utils"))]
use testcontainers_modules::postgres::Postgres;
#[cfg(any(test, feature = "test-utils"))]
use tower::util::ServiceExt;
#[cfg(any(test, feature = "test-utils"))]
use serde_json::Value;
#[cfg(any(test, feature = "test-utils"))]
use reqwest::{Response, StatusCode};

/// Test image information with expected OCR content
#[derive(Debug, Clone)]
pub struct TestImage {
    pub filename: &'static str,
    pub path: String,
    pub mime_type: &'static str,
    pub expected_content: &'static str,
}

impl TestImage {
    pub fn new(filename: &'static str, mime_type: &'static str, expected_content: &'static str) -> Self {
        Self {
            filename,
            path: format!("tests/test_images/{}", filename),
            mime_type,
            expected_content,
        }
    }
    
    pub fn exists(&self) -> bool {
        Path::new(&self.path).exists()
    }
    
    pub async fn load_data(&self) -> Result<Vec<u8>, std::io::Error> {
        tokio::fs::read(&self.path).await
    }
}

/// Get all available test images with their expected OCR content
pub fn get_test_images() -> Vec<TestImage> {
    vec![
        TestImage::new("test1.png", "image/png", "Test 1\nThis is some text from text 1"),
        TestImage::new("test2.jpg", "image/jpeg", "Test 2\nThis is some text from text 2"),
        TestImage::new("test3.jpeg", "image/jpeg", "Test 3\nThis is some text from text 3"),
        TestImage::new("test4.png", "image/png", "Test 4\nThis is some text from text 4"),
        TestImage::new("test5.jpg", "image/jpeg", "Test 5\nThis is some text from text 5"),
        TestImage::new("test6.jpeg", "image/jpeg", "Test 6\nThis is some text from text 6"),
        TestImage::new("test7.png", "image/png", "Test 7\nThis is some text from text 7"),
        TestImage::new("test8.jpeg", "image/jpeg", "Test 8\nThis is some text from text 8"),
        TestImage::new("test9.png", "image/png", "Test 9\nThis is some text from text 9"),
    ]
}

/// Get a specific test image by number (1-9)
pub fn get_test_image(number: u8) -> Option<TestImage> {
    if number < 1 || number > 9 {
        return None;
    }
    
    get_test_images().into_iter().nth((number - 1) as usize)
}

/// Load test image data by filename
pub async fn load_test_image(filename: &str) -> Result<Vec<u8>, std::io::Error> {
    let path = format!("tests/test_images/{}", filename);
    tokio::fs::read(path).await
}

/// Check if test images directory exists and is accessible
pub fn test_images_available() -> bool {
    Path::new("tests/test_images").exists()
}

/// Get available test images (only those that exist on filesystem)
pub fn get_available_test_images() -> Vec<TestImage> {
    get_test_images()
        .into_iter()
        .filter(|img| img.exists())
        .collect()
}

/// Skip test macro for conditional testing based on test image availability
#[macro_export]
macro_rules! skip_if_no_test_images {
    () => {
        if !crate::test_utils::test_images_available() {
            println!("Skipping test: test images directory not available");
            return;
        }
    };
}

/// Skip test macro for specific test image
#[macro_export]
macro_rules! skip_if_test_image_missing {
    ($image:expr) => {
        if !$image.exists() {
            println!("Skipping test: {} not found", $image.filename);
            return;
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_image_paths_are_valid() {
        let images = get_test_images();
        assert_eq!(images.len(), 9);
        
        for (i, image) in images.iter().enumerate() {
            assert_eq!(image.filename, format!("test{}.{}", i + 1, 
                if image.mime_type == "image/png" { "png" } 
                else if image.filename.ends_with(".jpg") { "jpg" }
                else { "jpeg" }
            ));
            assert!(image.expected_content.starts_with(&format!("Test {}", i + 1)));
        }
    }

    #[test]
    fn test_get_specific_image() {
        let image1 = get_test_image(1).unwrap();
        assert_eq!(image1.filename, "test1.png");
        assert_eq!(image1.mime_type, "image/png");
        assert!(image1.expected_content.contains("Test 1"));

        let image5 = get_test_image(5).unwrap();
        assert_eq!(image5.filename, "test5.jpg");
        assert_eq!(image5.mime_type, "image/jpeg");
        assert!(image5.expected_content.contains("Test 5"));

        // Invalid numbers should return None
        assert!(get_test_image(0).is_none());
        assert!(get_test_image(10).is_none());
    }
}

/// Unified test context that eliminates duplication across integration tests
#[cfg(any(test, feature = "test-utils"))]
pub struct TestContext {
    pub app: Router,
    pub container: ContainerAsync<Postgres>,
    pub state: Arc<AppState>,
}

#[cfg(any(test, feature = "test-utils"))]
impl TestContext {
    /// Create a new test context with default test configuration
    pub async fn new() -> Self {
        Self::with_config(TestConfigBuilder::default()).await
    }
    
    /// Create a test context with custom configuration
    pub async fn with_config(config_builder: TestConfigBuilder) -> Self {
        let postgres_image = Postgres::default()
            .with_tag("15")  // Use PostgreSQL 15 which has gen_random_uuid() built-in
            .with_env_var("POSTGRES_USER", "readur")
            .with_env_var("POSTGRES_PASSWORD", "readur")
            .with_env_var("POSTGRES_DB", "readur");
        
        let container = postgres_image.start().await.expect("Failed to start postgres container");
        let port = container.get_host_port_ipv4(5432).await.expect("Failed to get postgres port");
        
        let database_url = std::env::var("TEST_DATABASE_URL")
            .unwrap_or_else(|_| format!("postgresql://readur:readur@localhost:{}/readur", port));
        // Use enhanced pool configuration for testing with more connections and faster timeouts
        let db = crate::db::Database::new_with_pool_config(&database_url, 100, 10).await.unwrap();
        
        // Run proper SQLx migrations (PostgreSQL 15+ has gen_random_uuid() built-in)
        let migrations = sqlx::migrate!("./migrations");
        migrations.run(&db.pool).await.unwrap();
        
        let config = config_builder.build(database_url);
        let queue_service = Arc::new(crate::ocr::queue::OcrQueueService::new(db.clone(), db.pool.clone(), 2));
        
        let state = Arc::new(AppState { 
            db, 
            config,
            webdav_scheduler: None,
            source_scheduler: None,
            queue_service,
            oidc_client: None,
        });
        
        let app = Router::new()
            .nest("/api/auth", crate::routes::auth::router())
            .nest("/api/documents", crate::routes::documents::router())
            .nest("/api/search", crate::routes::search::router())
            .nest("/api/settings", crate::routes::settings::router())
            .nest("/api/users", crate::routes::users::router())
            .nest("/api/ignored-files", crate::routes::ignored_files::ignored_files_routes())
            .nest("/api/metrics", crate::routes::metrics::router())
            .nest("/metrics", crate::routes::prometheus_metrics::router())
            .with_state(state.clone());
        
        Self { app, container, state }
    }
    
    /// Get the app router for making requests
    pub fn app(&self) -> &Router {
        &self.app
    }
    
    /// Get the application state
    pub fn state(&self) -> &Arc<AppState> {
        &self.state
    }
}

/// Builder pattern for test configuration to eliminate config duplication
#[cfg(any(test, feature = "test-utils"))]
pub struct TestConfigBuilder {
    upload_path: String,
    watch_folder: String,
    jwt_secret: String,
    concurrent_ocr_jobs: usize,
    ocr_timeout_seconds: u64,
    max_file_size_mb: u64,
    memory_limit_mb: u64,
    oidc_enabled: bool,
}

#[cfg(any(test, feature = "test-utils"))]
impl Default for TestConfigBuilder {
    fn default() -> Self {
        Self {
            upload_path: "./test-uploads".to_string(),
            watch_folder: "./test-watch".to_string(),
            jwt_secret: "test-secret".to_string(),
            concurrent_ocr_jobs: 2,
            ocr_timeout_seconds: 60,
            max_file_size_mb: 10,
            memory_limit_mb: 256,
            oidc_enabled: false,
        }
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl TestConfigBuilder {
    pub fn with_upload_path(mut self, path: &str) -> Self {
        self.upload_path = path.to_string();
        self
    }
    
    pub fn with_watch_folder(mut self, folder: &str) -> Self {
        self.watch_folder = folder.to_string();
        self
    }
    
    pub fn with_concurrent_ocr_jobs(mut self, jobs: usize) -> Self {
        self.concurrent_ocr_jobs = jobs;
        self
    }
    
    pub fn with_oidc_enabled(mut self, enabled: bool) -> Self {
        self.oidc_enabled = enabled;
        self
    }
    
    fn build(self, database_url: String) -> crate::config::Config {
        crate::config::Config {
            database_url,
            server_address: "127.0.0.1:0".to_string(),
            jwt_secret: self.jwt_secret,
            upload_path: self.upload_path,
            watch_folder: self.watch_folder,
            allowed_file_types: vec!["pdf".to_string(), "txt".to_string(), "png".to_string()],
            watch_interval_seconds: Some(30),
            file_stability_check_ms: Some(500),
            max_file_age_hours: None,
            
            // OCR Configuration
            ocr_language: "eng".to_string(),
            concurrent_ocr_jobs: self.concurrent_ocr_jobs,
            ocr_timeout_seconds: self.ocr_timeout_seconds,
            max_file_size_mb: self.max_file_size_mb,
            
            // Performance
            memory_limit_mb: self.memory_limit_mb as usize,
            cpu_priority: "normal".to_string(),
            
            // OIDC Configuration
            oidc_enabled: self.oidc_enabled,
            oidc_client_id: None,
            oidc_client_secret: None,
            oidc_issuer_url: None,
            oidc_redirect_uri: None,
        }
    }
}

/// Create test app with provided AppState
#[cfg(any(test, feature = "test-utils"))]
pub fn create_test_app(state: Arc<AppState>) -> Router {
    Router::new()
        .nest("/api/auth", crate::routes::auth::router())
        .nest("/api/documents", crate::routes::documents::router())
        .nest("/api/search", crate::routes::search::router())
        .nest("/api/settings", crate::routes::settings::router())
        .nest("/api/users", crate::routes::users::router())
        .nest("/api/ignored-files", crate::routes::ignored_files::ignored_files_routes())
        .nest("/api/ocr", crate::routes::ocr::router())
        .nest("/api/queue", crate::routes::queue::router())
        .with_state(state)
}

/// Legacy function for backward compatibility - will be deprecated
#[cfg(any(test, feature = "test-utils"))]
pub async fn create_test_app_with_container() -> (Router, ContainerAsync<Postgres>) {
    let ctx = TestContext::new().await;
    (ctx.app, ctx.container)
}

/// Unified test authentication helper that replaces TestClient/AdminTestClient patterns
#[cfg(any(test, feature = "test-utils"))]
pub struct TestAuthHelper {
    app: Router,
}

#[cfg(any(test, feature = "test-utils"))]
impl TestAuthHelper {
    pub fn new(app: Router) -> Self {
        Self { app }
    }
    
    /// Create a regular test user with unique credentials
    pub async fn create_test_user(&self) -> TestUser {
        // Generate a more unique ID using process ID, thread ID (as debug string), and nanoseconds
        let test_id = format!("{}_{}_{}", 
            std::process::id(),
            format!("{:?}", std::thread::current().id()).replace("ThreadId(", "").replace(")", ""),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let username = format!("testuser_{}", test_id);
        let email = format!("test_{}@example.com", test_id);
        let password = "password123";
        
        let user_data = json!({
            "username": username,
            "email": email,
            "password": password
        });
        
        let response = self.make_request("POST", "/api/auth/register", Some(user_data), None).await;
        
        // Debug logging to understand CI vs local differences
        let response_str = String::from_utf8_lossy(&response);
        println!("DEBUG: Register response body: {}", response_str);
        println!("DEBUG: Register response length: {} bytes", response.len());
        
        // Try to parse as JSON first to see what we actually got
        let user_response = match serde_json::from_slice::<serde_json::Value>(&response) {
            Ok(json_value) => {
                println!("DEBUG: Parsed JSON structure: {:#}", json_value);
                
                // Check if this is an error response due to username collision
                if let Some(error_msg) = json_value.get("error").and_then(|e| e.as_str()) {
                    if error_msg.contains("Username already exists") {
                        println!("DEBUG: Username collision detected, retrying with UUID suffix");
                        // Retry with a UUID suffix for guaranteed uniqueness
                        let retry_username = format!("{}_{}",
                            username,
                            uuid::Uuid::new_v4().to_string().replace('-', "")[..8].to_string()
                        );
                        let retry_email = format!("test_{}@example.com", 
                            uuid::Uuid::new_v4().to_string().replace('-', "")[..16].to_string()
                        );
                        
                        let retry_user_data = json!({
                            "username": retry_username,
                            "email": retry_email,
                            "password": password
                        });
                        
                        let retry_response = self.make_request("POST", "/api/auth/register", Some(retry_user_data), None).await;
                        let retry_response_str = String::from_utf8_lossy(&retry_response);
                        println!("DEBUG: Retry register response body: {}", retry_response_str);
                        
                        let retry_json_value = serde_json::from_slice::<serde_json::Value>(&retry_response)
                            .expect("Retry response should be valid JSON");
                        
                        match serde_json::from_value::<UserResponse>(retry_json_value) {
                            Ok(user_response) => {
                                return TestUser {
                                    user_response,
                                    username: retry_username,
                                    password: password.to_string(),
                                    token: None,
                                };
                            },
                            Err(e) => {
                                eprintln!("ERROR: Failed to parse UserResponse from retry JSON: {}", e);
                                panic!("Failed to parse UserResponse from retry: {}", e);
                            }
                        }
                    }
                }
                
                // Try to parse as UserResponse
                match serde_json::from_value::<UserResponse>(json_value) {
                    Ok(user_response) => user_response,
                    Err(e) => {
                        eprintln!("ERROR: Failed to parse UserResponse from JSON: {}", e);
                        eprintln!("ERROR: Expected fields: id (UUID), username (String), email (String), role (UserRole)");
                        panic!("Failed to parse UserResponse: {}", e);
                    }
                }
            },
            Err(e) => {
                eprintln!("ERROR: Response is not valid JSON: {}", e);
                eprintln!("ERROR: Raw response: {:?}", response);
                panic!("Invalid JSON response from register endpoint: {}", e);
            }
        };
        
        TestUser {
            user_response,
            username,
            password: password.to_string(),
            token: None,
        }
    }
    
    /// Create an admin test user with unique credentials
    pub async fn create_admin_user(&self) -> TestUser {
        // Generate a more unique ID using process ID, thread ID (as debug string), and nanoseconds
        let test_id = format!("{}_{}_{}", 
            std::process::id(),
            format!("{:?}", std::thread::current().id()).replace("ThreadId(", "").replace(")", ""),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let username = format!("adminuser_{}", test_id);
        let email = format!("admin_{}@example.com", test_id);
        let password = "adminpass123";
        
        let admin_data = json!({
            "username": username,
            "email": email,
            "password": password,
            "role": "admin"
        });
        
        let response = self.make_request("POST", "/api/auth/register", Some(admin_data), None).await;
        
        // Debug logging to understand CI vs local differences
        let response_str = String::from_utf8_lossy(&response);
        println!("DEBUG: Admin register response body: {}", response_str);
        println!("DEBUG: Admin register response length: {} bytes", response.len());
        
        // Try to parse as JSON first to see what we actually got
        let user_response = match serde_json::from_slice::<serde_json::Value>(&response) {
            Ok(json_value) => {
                println!("DEBUG: Admin parsed JSON structure: {:#}", json_value);
                
                // Check if this is an error response due to username collision
                if let Some(error_msg) = json_value.get("error").and_then(|e| e.as_str()) {
                    if error_msg.contains("Username already exists") {
                        println!("DEBUG: Admin username collision detected, retrying with UUID suffix");
                        // Retry with a UUID suffix for guaranteed uniqueness
                        let retry_username = format!("{}_{}",
                            username,
                            uuid::Uuid::new_v4().to_string().replace('-', "")[..8].to_string()
                        );
                        let retry_email = format!("admin_{}@example.com", 
                            uuid::Uuid::new_v4().to_string().replace('-', "")[..16].to_string()
                        );
                        
                        let retry_admin_data = json!({
                            "username": retry_username,
                            "email": retry_email,
                            "password": password,
                            "role": "admin"
                        });
                        
                        let retry_response = self.make_request("POST", "/api/auth/register", Some(retry_admin_data), None).await;
                        let retry_response_str = String::from_utf8_lossy(&retry_response);
                        println!("DEBUG: Retry admin register response body: {}", retry_response_str);
                        
                        let retry_json_value = serde_json::from_slice::<serde_json::Value>(&retry_response)
                            .expect("Retry admin response should be valid JSON");
                        
                        match serde_json::from_value::<UserResponse>(retry_json_value) {
                            Ok(user_response) => {
                                return TestUser {
                                    user_response,
                                    username: retry_username,
                                    password: password.to_string(),
                                    token: None,
                                };
                            },
                            Err(e) => {
                                eprintln!("ERROR: Failed to parse UserResponse from retry admin JSON: {}", e);
                                panic!("Failed to parse UserResponse from retry admin: {}", e);
                            }
                        }
                    }
                }
                
                // Try to parse as UserResponse
                match serde_json::from_value::<UserResponse>(json_value) {
                    Ok(user_response) => user_response,
                    Err(e) => {
                        eprintln!("ERROR: Failed to parse admin UserResponse from JSON: {}", e);
                        eprintln!("ERROR: Expected fields: id (UUID), username (String), email (String), role (UserRole)");
                        panic!("Failed to parse admin UserResponse: {}", e);
                    }
                }
            },
            Err(e) => {
                eprintln!("ERROR: Admin response is not valid JSON: {}", e);
                eprintln!("ERROR: Raw admin response: {:?}", response);
                panic!("Invalid JSON response from admin register endpoint: {}", e);
            }
        };
        
        TestUser {
            user_response,
            username,
            password: password.to_string(),
            token: None,
        }
    }
    
    /// Create an admin test user (alias for create_admin_user for backward compatibility)
    pub async fn create_test_admin(&self) -> TestUser {
        self.create_admin_user().await
    }
    
    /// Login a user and return their authentication token
    pub async fn login_user(&self, username: &str, password: &str) -> String {
        let login_data = json!({
            "username": username,
            "password": password
        });
        
        let response = self.make_request("POST", "/api/auth/login", Some(login_data), None).await;
        let login_response: serde_json::Value = serde_json::from_slice(&response).unwrap();
        login_response["token"].as_str().unwrap().to_string()
    }
    
    /// Make an authenticated HTTP request
    pub async fn make_authenticated_request(&self, method: &str, uri: &str, body: Option<serde_json::Value>, token: &str) -> Vec<u8> {
        self.make_request(method, uri, body, Some(token)).await
    }
    
    /// Make an HTTP request (internal helper)
    async fn make_request(&self, method: &str, uri: &str, body: Option<serde_json::Value>, token: Option<&str>) -> Vec<u8> {
        let mut builder = axum::http::Request::builder()
            .method(method)
            .uri(uri)
            .header("Content-Type", "application/json");
        
        if let Some(token) = token {
            builder = builder.header("Authorization", format!("Bearer {}", token));
        }
        
        let request_body = if let Some(body) = body {
            axum::body::Body::from(serde_json::to_vec(&body).unwrap())
        } else {
            axum::body::Body::empty()
        };
        
        let response = self.app
            .clone()
            .oneshot(builder.body(request_body).unwrap())
            .await
            .unwrap();
        
        axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec()
    }
}

/// Test user with authentication capabilities
#[cfg(any(test, feature = "test-utils"))]
pub struct TestUser {
    pub user_response: UserResponse,
    pub username: String,
    pub password: String,
    pub token: Option<String>,
}

#[cfg(any(test, feature = "test-utils"))]
impl TestUser {
    /// Login this user and store the authentication token
    pub async fn login(&mut self, auth_helper: &TestAuthHelper) -> Result<&str, Box<dyn std::error::Error>> {
        let token = auth_helper.login_user(&self.username, &self.password).await;
        self.token = Some(token);
        Ok(self.token.as_ref().unwrap())
    }
    
    /// Make an authenticated request as this user
    pub async fn make_request(&self, auth_helper: &TestAuthHelper, method: &str, uri: &str, body: Option<serde_json::Value>) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let token = self.token.as_ref().ok_or("User not logged in")?;
        Ok(auth_helper.make_authenticated_request(method, uri, body, token).await)
    }
    
    /// Get user ID
    pub fn id(&self) -> String {
        self.user_response.id.to_string()
    }
    
    /// Check if user is admin
    pub fn is_admin(&self) -> bool {
        matches!(self.user_response.role, crate::models::UserRole::Admin)
    }
}

/// Legacy functions for backward compatibility - will be deprecated
#[cfg(any(test, feature = "test-utils"))]
pub async fn create_test_user(app: &Router) -> UserResponse {
    let auth_helper = TestAuthHelper::new(app.clone());
    let test_user = auth_helper.create_test_user().await;
    test_user.user_response
}

#[cfg(any(test, feature = "test-utils"))]
pub async fn create_admin_user(app: &Router) -> UserResponse {
    let auth_helper = TestAuthHelper::new(app.clone());
    let admin_user = auth_helper.create_admin_user().await;
    admin_user.user_response
}

#[cfg(any(test, feature = "test-utils"))]
pub async fn login_user(app: &Router, username: &str, password: &str) -> String {
    let auth_helper = TestAuthHelper::new(app.clone());
    auth_helper.login_user(username, password).await
}

/// Centralized test Document helpers to reduce duplication across test files
#[cfg(any(test, feature = "test-utils"))]
pub mod document_helpers {
    use uuid::Uuid;
    use chrono::Utc;
    use crate::models::Document;

    /// Create a basic test document with all required fields
    pub fn create_test_document(user_id: Uuid) -> Document {
        Document {
            id: Uuid::new_v4(),
            filename: "test_document.pdf".to_string(),
            original_filename: "test_document.pdf".to_string(),
            file_path: "/path/to/test_document.pdf".to_string(),
            file_size: 1024,
            mime_type: "application/pdf".to_string(),
            content: Some("Test document content".to_string()),
            ocr_text: Some("This is extracted OCR text".to_string()),
            ocr_confidence: Some(95.5),
            ocr_word_count: Some(150),
            ocr_processing_time_ms: Some(1200),
            ocr_status: Some("completed".to_string()),
            ocr_error: None,
            ocr_completed_at: Some(Utc::now()),
            tags: vec!["test".to_string()],
            created_at: Utc::now(),
            updated_at: Utc::now(),
            user_id,
            file_hash: Some("hash123".to_string()),
            original_created_at: None,
            original_modified_at: None,
            source_path: None,
            source_type: None,
            source_id: None,
            file_permissions: None,
            file_owner: None,
            file_group: None,
            source_metadata: None,
            ocr_retry_count: None,
            ocr_failure_reason: None,
        }
    }

    /// Create a test document with custom filename and hash
    pub fn create_test_document_with_hash(user_id: Uuid, filename: &str, file_hash: String) -> Document {
        Document {
            id: Uuid::new_v4(),
            filename: filename.to_string(),
            original_filename: filename.to_string(),
            file_path: format!("/tmp/{}", filename),
            file_size: 1024,
            mime_type: "application/pdf".to_string(),
            content: None,
            ocr_text: None,
            ocr_confidence: None,
            ocr_word_count: None,
            ocr_processing_time_ms: None,
            ocr_status: Some("pending".to_string()),
            ocr_error: None,
            ocr_completed_at: None,
            ocr_retry_count: None,
            ocr_failure_reason: None,
            tags: Vec::new(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            user_id,
            file_hash: Some(file_hash),
            original_created_at: None,
            original_modified_at: None,
            source_path: None,
            source_type: None,
            source_id: None,
            file_permissions: None,
            file_owner: None,
            file_group: None,
            source_metadata: None,
        }
    }

    /// Create a test document with low OCR confidence
    pub fn create_low_confidence_document(user_id: Uuid, confidence: f32) -> Document {
        Document {
            id: Uuid::new_v4(),
            filename: format!("low_conf_{}.pdf", confidence),
            original_filename: format!("low_conf_{}.pdf", confidence),
            file_path: format!("/uploads/low_conf_{}.pdf", confidence),
            file_size: 1024,
            mime_type: "application/pdf".to_string(),
            content: Some("Test document content".to_string()),
            ocr_text: Some("Low quality OCR text".to_string()),
            ocr_confidence: Some(confidence),
            ocr_word_count: Some(10),
            ocr_processing_time_ms: Some(500),
            ocr_status: Some("completed".to_string()),
            ocr_error: None,
            ocr_completed_at: Some(Utc::now()),
            tags: vec!["low-confidence".to_string()],
            created_at: Utc::now(),
            updated_at: Utc::now(),
            user_id,
            file_hash: Some("abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890".to_string()),
            original_created_at: None,
            original_modified_at: None,
            source_path: None,
            source_type: None,
            source_id: None,
            file_permissions: None,
            file_owner: None,
            file_group: None,
            source_metadata: None,
            ocr_retry_count: None,
            ocr_failure_reason: None,
        }
    }

    /// Create a document without OCR data
    pub fn create_document_without_ocr(user_id: Uuid) -> Document {
        Document {
            id: Uuid::new_v4(),
            filename: "no_ocr_document.pdf".to_string(),
            original_filename: "no_ocr_document.pdf".to_string(),
            file_path: "/path/to/no_ocr_document.pdf".to_string(),
            file_size: 2048,
            mime_type: "application/pdf".to_string(),
            content: Some("Document content without OCR".to_string()),
            ocr_text: None,
            ocr_confidence: None,
            ocr_word_count: None,
            ocr_processing_time_ms: None,
            ocr_status: Some("pending".to_string()),
            ocr_error: None,
            ocr_completed_at: None,
            tags: vec!["no-ocr".to_string()],
            created_at: Utc::now(),
            updated_at: Utc::now(),
            user_id,
            file_hash: Some("noocrhash456".to_string()),
            original_created_at: None,
            original_modified_at: None,
            source_path: None,
            source_type: None,
            source_id: None,
            file_permissions: None,
            file_owner: None,
            file_group: None,
            source_metadata: None,
            ocr_retry_count: None,
            ocr_failure_reason: None,
        }
    }

    /// Create a document with OCR error
    pub fn create_document_with_ocr_error(user_id: Uuid) -> Document {
        Document {
            id: Uuid::new_v4(),
            filename: "ocr_error_document.pdf".to_string(),
            original_filename: "ocr_error_document.pdf".to_string(),
            file_path: "/path/to/ocr_error_document.pdf".to_string(),
            file_size: 1536,
            mime_type: "application/pdf".to_string(),
            content: Some("Document that failed OCR".to_string()),
            ocr_text: None,
            ocr_confidence: None,
            ocr_word_count: None,
            ocr_processing_time_ms: Some(300),
            ocr_status: Some("failed".to_string()),
            ocr_error: Some("OCR processing failed".to_string()),
            ocr_completed_at: Some(Utc::now()),
            tags: vec!["failed-ocr".to_string()],
            created_at: Utc::now(),
            updated_at: Utc::now(),
            user_id,
            file_hash: Some("errorhash789".to_string()),
            original_created_at: None,
            original_modified_at: None,
            source_path: None,
            source_type: None,
            source_id: None,
            file_permissions: None,
            file_owner: None,
            file_group: None,
            source_metadata: None,
            ocr_retry_count: Some(3),
            ocr_failure_reason: Some("OCR engine timeout".to_string()),
        }
    }

    /// Enhanced test assertion utility for HTTP responses with detailed debug output
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn assert_response_status_with_debug(
        response: reqwest::Response,
        expected_status: reqwest::StatusCode,
        context: &str,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let actual_status = response.status();
        let url = response.url().clone();
        
        if actual_status == expected_status {
            // Success case - try to parse JSON
            let response_text = response.text().await?;
            
            if response_text.is_empty() {
                println!("✅ {} - Status {} as expected (empty response)", context, expected_status);
                return Ok(serde_json::Value::Null);
            }
            
            match serde_json::from_str::<serde_json::Value>(&response_text) {
                Ok(json_value) => {
                    println!("✅ {} - Status {} as expected", context, expected_status);
                    Ok(json_value)
                }
                Err(e) => {
                    println!("⚠️  {} - Status {} as expected but failed to parse JSON: {}", context, expected_status, e);
                    println!("Response text: {}", response_text);
                    Err(format!("JSON parse error: {}", e).into())
                }
            }
        } else {
            // Failure case - provide detailed debug info
            let response_text = response.text().await.unwrap_or_else(|_| "Unable to read response body".to_string());
            
            println!("❌ {} - Expected status {}, got {}", context, expected_status, actual_status);
            println!("🔗 Request URL: {}", url);
            println!("📄 Response headers:");
            
            println!("📝 Response body:");
            println!("{}", response_text);
            
            // Try to parse as JSON for better formatting
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(&response_text) {
                println!("📋 Formatted JSON response:");
                println!("{}", serde_json::to_string_pretty(&json_value).unwrap_or_else(|_| response_text.clone()));
            }
            
            Err(format!(
                "{} - Expected status {}, got {}. URL: {}. Response: {}", 
                context, expected_status, actual_status, url, response_text
            ).into())
        }
}

    /// Quick assertion for successful responses (2xx status codes)
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn assert_success_with_debug(
        response: reqwest::Response,
        context: &str,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let status = response.status();
        
        if status.is_success() {
            assert_response_status_with_debug(response, status, context).await
        } else {
            assert_response_status_with_debug(response, reqwest::StatusCode::OK, context).await
        }
    }

    /// Assert a specific error status with debug output
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn assert_error_with_debug(
        response: reqwest::Response,
        expected_status: reqwest::StatusCode,
        context: &str,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        assert_response_status_with_debug(response, expected_status, context).await
    }
}

/// Enhanced request assertion helper that provides comprehensive debugging information
#[cfg(any(test, feature = "test-utils"))]
pub struct AssertRequest;

#[cfg(any(test, feature = "test-utils"))]
impl AssertRequest {
    /// Assert response status with comprehensive debugging output including URL, payload, and response
    pub async fn assert_response(
        response: axum::response::Response,
        expected_status: axum::http::StatusCode,
        context: &str,
        original_url: &str,
        payload: Option<&serde_json::Value>,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let actual_status = response.status();
        let headers = response.headers().clone();
        
        // Extract response body
        let response_bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
        let response_text = String::from_utf8_lossy(&response_bytes);
        
        println!("🔍 AssertRequest Debug Info for: {}", context);
        println!("🔗 Request URL: {}", original_url);
        
        if let Some(payload) = payload {
            println!("📤 Request Payload:");
            println!("{}", serde_json::to_string_pretty(payload).unwrap_or_else(|_| "Invalid JSON payload".to_string()));
        } else {
            println!("📤 Request Payload: (empty)");
        }
        
        println!("📊 Response Status: {} (expected: {})", actual_status, expected_status);
        println!("📋 Response Headers:");
        for (name, value) in headers.iter() {
            println!("  {}: {}", name, value.to_str().unwrap_or("<invalid header>"));
        }
        
        println!("📝 Response Body ({} bytes):", response_bytes.len());
        if response_text.is_empty() {
            println!("  (empty response)");
        } else {
            // Try to format as JSON for better readability
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(&response_text) {
                println!("{}", serde_json::to_string_pretty(&json_value).unwrap_or_else(|_| response_text.to_string()));
            } else {
                println!("{}", response_text);
            }
        }
        
        if actual_status == expected_status {
            println!("✅ {} - Status {} as expected", context, expected_status);
            
            if response_text.is_empty() {
                Ok(serde_json::Value::Null)
            } else {
                match serde_json::from_str::<serde_json::Value>(&response_text) {
                    Ok(json_value) => Ok(json_value),
                    Err(e) => {
                        println!("⚠️  JSON parse error: {}", e);
                        Err(format!("JSON parse error: {}", e).into())
                    }
                }
            }
        } else {
            println!("❌ {} - Expected status {}, got {}", context, expected_status, actual_status);
            Err(format!(
                "{} - Expected status {}, got {}. URL: {}. Response: {}", 
                context, expected_status, actual_status, original_url, response_text
            ).into())
        }
    }
    
    /// Assert successful response (2xx status codes) with comprehensive debugging
    pub async fn assert_success(
        response: axum::response::Response,
        context: &str,
        original_url: &str,
        payload: Option<&serde_json::Value>,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let status = response.status();
        
        if status.is_success() {
            Self::assert_response(response, status, context, original_url, payload).await
        } else {
            Self::assert_response(response, axum::http::StatusCode::OK, context, original_url, payload).await
        }
    }
    
    /// Assert client error (4xx) with comprehensive debugging
    pub async fn assert_client_error(
        response: axum::response::Response,
        expected_status: axum::http::StatusCode,
        context: &str,
        original_url: &str,
        payload: Option<&serde_json::Value>,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        Self::assert_response(response, expected_status, context, original_url, payload).await
    }
    
    /// Assert server error (5xx) with comprehensive debugging
    pub async fn assert_server_error(
        response: axum::response::Response,
        expected_status: axum::http::StatusCode,
        context: &str,
        original_url: &str,
        payload: Option<&serde_json::Value>,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        Self::assert_response(response, expected_status, context, original_url, payload).await
    }
    
    /// Make a request and assert the response in one call
    pub async fn make_and_assert(
        app: &axum::Router,
        method: &str,
        uri: &str,
        payload: Option<serde_json::Value>,
        expected_status: axum::http::StatusCode,
        context: &str,
        token: Option<&str>,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let mut builder = axum::http::Request::builder()
            .method(method)
            .uri(uri)
            .header("Content-Type", "application/json");
        
        if let Some(token) = token {
            builder = builder.header("Authorization", format!("Bearer {}", token));
        }
        
        let request_body = if let Some(ref body) = payload {
            axum::body::Body::from(serde_json::to_vec(body)?)
        } else {
            axum::body::Body::empty()
        };
        
        let response = app
            .clone()
            .oneshot(builder.body(request_body)?)
            .await?;
        
        Self::assert_response(response, expected_status, context, uri, payload.as_ref()).await
    }
}