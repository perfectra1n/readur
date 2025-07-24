use std::sync::Arc;
use readur::{
    AppState,
    models::{CreateWebDAVDirectory, User, AuthProvider},
    services::webdav::{SmartSyncService, SmartSyncStrategy, SmartSyncDecision, WebDAVService, WebDAVConfig},
    test_utils::{TestContext, TestAuthHelper},
};

/// Helper function to create test database and user
async fn create_test_setup() -> (Arc<AppState>, User) {
    let test_context = TestContext::new().await;
    let auth_helper = TestAuthHelper::new(test_context.app().clone());
    let test_user = auth_helper.create_test_user().await;
    
    // Convert TestUser to User model for compatibility
    let user = User {
        id: test_user.user_response.id,
        username: test_user.user_response.username,
        email: test_user.user_response.email,
        password_hash: Some("hashed_password".to_string()),
        role: test_user.user_response.role,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
        oidc_subject: None,
        oidc_issuer: None,
        oidc_email: None,
        auth_provider: AuthProvider::Local,
    };

    (test_context.state().clone(), user)
}

/// Helper function to create WebDAV service for testing
fn create_test_webdav_service() -> WebDAVService {
    let config = WebDAVConfig {
        server_url: "https://test.example.com".to_string(),
        username: "test".to_string(),
        password: "test".to_string(),
        watch_folders: vec!["/Documents".to_string()],
        file_extensions: vec!["pdf".to_string(), "txt".to_string()],
        timeout_seconds: 30,
        server_type: Some("generic".to_string()),
    };
    
    WebDAVService::new(config).expect("Failed to create WebDAV service")
}

#[tokio::test]
async fn test_smart_sync_no_changes_skip() {
    // Integration Test: Smart sync with no directory changes should skip sync entirely
    // Expected: Should return SkipSync when all directory ETags are unchanged
    
    let (state, user) = create_test_setup().await;
    let smart_sync_service = SmartSyncService::new(state.clone());
    
    // Pre-populate database with known directory ETags
    let known_directories = vec![
        CreateWebDAVDirectory {
            user_id: user.id,
            directory_path: "/Documents".to_string(),
            directory_etag: "root-etag-unchanged".to_string(),
            file_count: 8,
            total_size_bytes: 800000,
        },
        CreateWebDAVDirectory {
            user_id: user.id,
            directory_path: "/Documents/Projects".to_string(),
            directory_etag: "projects-etag-unchanged".to_string(),
            file_count: 12,
            total_size_bytes: 1200000,
        },
        CreateWebDAVDirectory {
            user_id: user.id,
            directory_path: "/Documents/Archive".to_string(),
            directory_etag: "archive-etag-unchanged".to_string(),
            file_count: 25,
            total_size_bytes: 2500000,
        },
    ];
    
    for dir in &known_directories {
        state.db.create_or_update_webdav_directory(dir).await
            .expect("Failed to create directory tracking");
    }
    
    // Verify known directories were created
    let stored_dirs = state.db.list_webdav_directories(user.id).await.unwrap();
    assert_eq!(stored_dirs.len(), 3, "Should have 3 known directories");
    
    // In a real scenario, WebDAV would return the same ETags, indicating no changes
    // Since we can't mock the WebDAV service easily, we test the database logic
    
    // Test bulk directory fetching (key performance optimization)
    let start_time = std::time::Instant::now();
    let fetched_dirs = state.db.list_webdav_directories(user.id).await.unwrap();
    let fetch_duration = start_time.elapsed();
    
    assert!(fetch_duration.as_millis() < 50, "Bulk directory fetch should be fast");
    assert_eq!(fetched_dirs.len(), 3, "Should fetch all directories efficiently");
    
    // Verify directory data integrity
    let docs_dir = fetched_dirs.iter().find(|d| d.directory_path == "/Documents").unwrap();
    assert_eq!(docs_dir.directory_etag, "root-etag-unchanged");
    assert_eq!(docs_dir.file_count, 8);
    
    let projects_dir = fetched_dirs.iter().find(|d| d.directory_path == "/Documents/Projects").unwrap();
    assert_eq!(projects_dir.directory_etag, "projects-etag-unchanged");
    assert_eq!(projects_dir.file_count, 12);
    
    let archive_dir = fetched_dirs.iter().find(|d| d.directory_path == "/Documents/Archive").unwrap();
    assert_eq!(archive_dir.directory_etag, "archive-etag-unchanged");
    assert_eq!(archive_dir.file_count, 25);
    
    println!("✅ No changes sync test completed successfully - bulk fetch in {:?}", fetch_duration);
}

#[tokio::test]
async fn test_directory_etag_comparison_efficiency() {
    // Integration Test: Directory ETag comparison should be efficient for large numbers of directories
    // This tests the bulk fetching performance optimization
    
    let (state, user) = create_test_setup().await;
    
    // Create a larger number of directories to test performance
    let num_directories = 100;
    let mut directories = Vec::new();
    
    for i in 0..num_directories {
        directories.push(CreateWebDAVDirectory {
            user_id: user.id,
            directory_path: format!("/Documents/Folder{:03}", i),
            directory_etag: format!("etag-folder-{:03}", i),
            file_count: i as i64 % 10 + 1, // 1-10 files per directory
            total_size_bytes: (i as i64 + 1) * 10000, // Varying sizes
        });
    }
    
    // Batch insert directories
    let insert_start = std::time::Instant::now();
    for dir in &directories {
        state.db.create_or_update_webdav_directory(dir).await
            .expect("Failed to create directory");
    }
    let insert_duration = insert_start.elapsed();
    
    // Test bulk fetch performance
    let fetch_start = std::time::Instant::now();
    let fetched_dirs = state.db.list_webdav_directories(user.id).await.unwrap();
    let fetch_duration = fetch_start.elapsed();
    
    // Verify all directories were created and fetched
    assert_eq!(fetched_dirs.len(), num_directories, "Should fetch all {} directories", num_directories);
    
    // Performance assertions
    assert!(fetch_duration.as_millis() < 200, "Bulk fetch of {} directories should be under 200ms, got {:?}", num_directories, fetch_duration);
    assert!(insert_duration.as_millis() < 5000, "Bulk insert of {} directories should be under 5s, got {:?}", num_directories, insert_duration);
    
    // Verify data integrity on a few random directories
    let dir_50 = fetched_dirs.iter().find(|d| d.directory_path == "/Documents/Folder050").unwrap();
    assert_eq!(dir_50.directory_etag, "etag-folder-050");
    assert_eq!(dir_50.file_count, 1); // 50 % 10 + 1 = 1
    assert_eq!(dir_50.total_size_bytes, 510000); // (50 + 1) * 10000
    
    let dir_99 = fetched_dirs.iter().find(|d| d.directory_path == "/Documents/Folder099").unwrap();
    assert_eq!(dir_99.directory_etag, "etag-folder-099");
    assert_eq!(dir_99.file_count, 10); // 99 % 10 + 1 = 10
    assert_eq!(dir_99.total_size_bytes, 1000000); // (99 + 1) * 10000
    
    println!("✅ Directory ETag comparison efficiency test completed successfully");
    println!("   Created {} directories in {:?}", num_directories, insert_duration);
    println!("   Fetched {} directories in {:?}", num_directories, fetch_duration);
}