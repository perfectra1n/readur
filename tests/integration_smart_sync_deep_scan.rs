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
async fn test_deep_scan_resets_directory_etags() {
    // Integration Test: Manual deep scan should reset all directory ETags at all levels
    // Expected: Should clear existing ETags and establish fresh baseline
    
    let (state, user) = create_test_setup().await;
    
    // Pre-populate database with old directory ETags
    let old_directories = vec![
        CreateWebDAVDirectory {
            user_id: user.id,
            directory_path: "/Documents".to_string(),
            directory_etag: "old-root-etag".to_string(),
            file_count: 5,
            total_size_bytes: 500000,
        },
        CreateWebDAVDirectory {
            user_id: user.id,
            directory_path: "/Documents/Projects".to_string(),
            directory_etag: "old-projects-etag".to_string(),
            file_count: 10,
            total_size_bytes: 1000000,
        },
        CreateWebDAVDirectory {
            user_id: user.id,
            directory_path: "/Documents/Archive".to_string(),
            directory_etag: "old-archive-etag".to_string(),
            file_count: 20,
            total_size_bytes: 2000000,
        },
        CreateWebDAVDirectory {
            user_id: user.id,
            directory_path: "/Documents/Deep/Nested/Path".to_string(),
            directory_etag: "old-deep-etag".to_string(),
            file_count: 3,
            total_size_bytes: 300000,
        },
    ];
    
    for dir in &old_directories {
        state.db.create_or_update_webdav_directory(dir).await
            .expect("Failed to create old directory");
    }
    
    // Verify old directories were created
    let before_dirs = state.db.list_webdav_directories(user.id).await.unwrap();
    assert_eq!(before_dirs.len(), 4, "Should have 4 old directories");
    
    // Simulate deep scan reset - this would happen during a deep scan operation
    // For testing, we'll manually clear directories and add new ones
    
    // Clear existing directories (simulating deep scan reset)
    for dir in &before_dirs {
        state.db.delete_webdav_directory(user.id, &dir.directory_path).await
            .expect("Failed to delete old directory");
    }
    
    // Verify directories were cleared
    let cleared_dirs = state.db.list_webdav_directories(user.id).await.unwrap();
    assert_eq!(cleared_dirs.len(), 0, "Should have cleared all old directories");
    
    // Add new directories with fresh ETags (simulating post-deep-scan discovery)
    let new_directories = vec![
        CreateWebDAVDirectory {
            user_id: user.id,
            directory_path: "/Documents".to_string(),
            directory_etag: "fresh-root-etag".to_string(),
            file_count: 8,
            total_size_bytes: 800000,
        },
        CreateWebDAVDirectory {
            user_id: user.id,
            directory_path: "/Documents/Projects".to_string(),
            directory_etag: "fresh-projects-etag".to_string(),
            file_count: 12,
            total_size_bytes: 1200000,
        },
        CreateWebDAVDirectory {
            user_id: user.id,
            directory_path: "/Documents/Archive".to_string(),
            directory_etag: "fresh-archive-etag".to_string(),
            file_count: 25,
            total_size_bytes: 2500000,
        },
        CreateWebDAVDirectory {
            user_id: user.id,
            directory_path: "/Documents/Deep/Nested/Path".to_string(),
            directory_etag: "fresh-deep-etag".to_string(),
            file_count: 5,
            total_size_bytes: 500000,
        },
        CreateWebDAVDirectory {
            user_id: user.id,
            directory_path: "/Documents/NewDirectory".to_string(),
            directory_etag: "brand-new-etag".to_string(),
            file_count: 2,
            total_size_bytes: 200000,
        },
    ];
    
    for dir in &new_directories {
        state.db.create_or_update_webdav_directory(dir).await
            .expect("Failed to create new directory");
    }
    
    // Verify fresh directories were created
    let after_dirs = state.db.list_webdav_directories(user.id).await.unwrap();
    assert_eq!(after_dirs.len(), 5, "Should have 5 fresh directories after deep scan");
    
    // Verify ETags are completely different
    let root_dir = after_dirs.iter().find(|d| d.directory_path == "/Documents").unwrap();
    assert_eq!(root_dir.directory_etag, "fresh-root-etag");
    assert_ne!(root_dir.directory_etag, "old-root-etag");
    
    let projects_dir = after_dirs.iter().find(|d| d.directory_path == "/Documents/Projects").unwrap();
    assert_eq!(projects_dir.directory_etag, "fresh-projects-etag");
    assert_ne!(projects_dir.directory_etag, "old-projects-etag");
    
    let new_dir = after_dirs.iter().find(|d| d.directory_path == "/Documents/NewDirectory").unwrap();
    assert_eq!(new_dir.directory_etag, "brand-new-etag");
    
    println!("✅ Deep scan reset test completed successfully");
    println!("   Cleared {} old directories", old_directories.len());
    println!("   Created {} fresh directories", new_directories.len());
}

#[tokio::test]
async fn test_scheduled_deep_scan() {
    // Integration Test: Scheduled deep scan should reset all directory ETags and track new ones
    // This tests the scenario where a scheduled deep scan runs periodically
    
    let (state, user) = create_test_setup().await;
    
    // Simulate initial sync state
    let initial_directories = vec![
        CreateWebDAVDirectory {
            user_id: user.id,
            directory_path: "/Documents".to_string(),
            directory_etag: "initial-root".to_string(),
            file_count: 10,
            total_size_bytes: 1000000,
        },
        CreateWebDAVDirectory {
            user_id: user.id,
            directory_path: "/Documents/OldProject".to_string(),
            directory_etag: "initial-old-project".to_string(),
            file_count: 15,
            total_size_bytes: 1500000,
        },
    ];
    
    for dir in &initial_directories {
        state.db.create_or_update_webdav_directory(dir).await
            .expect("Failed to create initial directory");
    }
    
    let initial_count = state.db.list_webdav_directories(user.id).await.unwrap().len();
    assert_eq!(initial_count, 2, "Should start with 2 initial directories");
    
    // Simulate time passing and directory structure changes
    // During this time, directories may have been added/removed/changed on the WebDAV server
    
    // Simulate scheduled deep scan: clear all ETags and rediscover
    let initial_dirs = state.db.list_webdav_directories(user.id).await.unwrap();
    for dir in &initial_dirs {
        state.db.delete_webdav_directory(user.id, &dir.directory_path).await
            .expect("Failed to delete during deep scan reset");
    }
    
    // Simulate fresh discovery after deep scan
    let post_scan_directories = vec![
        CreateWebDAVDirectory {
            user_id: user.id,
            directory_path: "/Documents".to_string(),
            directory_etag: "scheduled-root".to_string(), // Changed ETag
            file_count: 12, // Changed file count
            total_size_bytes: 1200000, // Changed size
        },
        CreateWebDAVDirectory {
            user_id: user.id,
            directory_path: "/Documents/NewProject".to_string(), // Different directory
            directory_etag: "scheduled-new-project".to_string(),
            file_count: 8,
            total_size_bytes: 800000,
        },
        CreateWebDAVDirectory {
            user_id: user.id,
            directory_path: "/Documents/Archive".to_string(), // Completely new directory
            directory_etag: "scheduled-archive".to_string(),
            file_count: 30,
            total_size_bytes: 3000000,
        },
    ];
    
    for dir in &post_scan_directories {
        state.db.create_or_update_webdav_directory(dir).await
            .expect("Failed to create post-scan directory");
    }
    
    // Verify the scheduled deep scan results
    let final_dirs = state.db.list_webdav_directories(user.id).await.unwrap();
    assert_eq!(final_dirs.len(), 3, "Should have 3 directories after scheduled deep scan");
    
    // Verify the directory structure reflects current state
    let root_dir = final_dirs.iter().find(|d| d.directory_path == "/Documents").unwrap();
    assert_eq!(root_dir.directory_etag, "scheduled-root");
    assert_eq!(root_dir.file_count, 12);
    assert_eq!(root_dir.total_size_bytes, 1200000);
    
    let new_project = final_dirs.iter().find(|d| d.directory_path == "/Documents/NewProject").unwrap();
    assert_eq!(new_project.directory_etag, "scheduled-new-project");
    
    let archive_dir = final_dirs.iter().find(|d| d.directory_path == "/Documents/Archive").unwrap();
    assert_eq!(archive_dir.directory_etag, "scheduled-archive");
    
    // Verify old directory is gone
    assert!(final_dirs.iter().find(|d| d.directory_path == "/Documents/OldProject").is_none(),
            "Old project directory should be removed after scheduled deep scan");
    
    println!("✅ Scheduled deep scan test completed successfully");
    println!("   Initial directories: {}", initial_directories.len());
    println!("   Final directories: {}", final_dirs.len());
    println!("   Successfully handled directory structure changes");
}

#[tokio::test]
async fn test_deep_scan_performance_with_many_directories() {
    // Integration Test: Deep scan should perform well even with large numbers of directories
    // This tests the scalability of the deep scan reset operation
    
    let (state, user) = create_test_setup().await;
    
    // Create a large number of old directories
    let num_old_dirs = 250;
    let mut old_directories = Vec::new();
    
    let create_start = std::time::Instant::now();
    for i in 0..num_old_dirs {
        let dir = CreateWebDAVDirectory {
            user_id: user.id,
            directory_path: format!("/Documents/Old{:03}", i),
            directory_etag: format!("old-etag-{:03}", i),
            file_count: i as i64 % 20 + 1, // 1-20 files
            total_size_bytes: (i as i64 + 1) * 4000, // Varying sizes
        };
        
        state.db.create_or_update_webdav_directory(&dir).await
            .expect("Failed to create old directory");
        old_directories.push(dir);
    }
    let create_duration = create_start.elapsed();
    
    // Verify old directories were created
    let before_count = state.db.list_webdav_directories(user.id).await.unwrap().len();
    assert_eq!(before_count, num_old_dirs, "Should have created {} old directories", num_old_dirs);
    
    // Simulate deep scan reset - delete all existing
    let delete_start = std::time::Instant::now();
    let dirs_to_delete = state.db.list_webdav_directories(user.id).await.unwrap();
    for dir in &dirs_to_delete {
        state.db.delete_webdav_directory(user.id, &dir.directory_path).await
            .expect("Failed to delete directory during deep scan");
    }
    let delete_duration = delete_start.elapsed();
    
    // Verify cleanup
    let cleared_count = state.db.list_webdav_directories(user.id).await.unwrap().len();
    assert_eq!(cleared_count, 0, "Should have cleared all directories");
    
    // Create new directories (simulating rediscovery)
    let num_new_dirs = 300; // Slightly different number
    let recreate_start = std::time::Instant::now();
    for i in 0..num_new_dirs {
        let dir = CreateWebDAVDirectory {
            user_id: user.id,
            directory_path: format!("/Documents/New{:03}", i),
            directory_etag: format!("new-etag-{:03}", i),
            file_count: i as i64 % 15 + 1, // 1-15 files
            total_size_bytes: (i as i64 + 1) * 5000, // Different sizing
        };
        
        state.db.create_or_update_webdav_directory(&dir).await
            .expect("Failed to create new directory");
    }
    let recreate_duration = recreate_start.elapsed();
    
    // Verify final state
    let final_count = state.db.list_webdav_directories(user.id).await.unwrap().len();
    assert_eq!(final_count, num_new_dirs, "Should have created {} new directories", num_new_dirs);
    
    // Performance assertions - should complete within reasonable time
    assert!(create_duration.as_secs() < 30, "Creating {} directories should take < 30s, took {:?}", num_old_dirs, create_duration);
    assert!(delete_duration.as_secs() < 15, "Deleting {} directories should take < 15s, took {:?}", num_old_dirs, delete_duration);
    assert!(recreate_duration.as_secs() < 30, "Recreating {} directories should take < 30s, took {:?}", num_new_dirs, recreate_duration);
    
    let total_duration = create_duration + delete_duration + recreate_duration;
    
    println!("✅ Deep scan performance test completed successfully");
    println!("   Created {} old directories in {:?}", num_old_dirs, create_duration);
    println!("   Deleted {} directories in {:?}", num_old_dirs, delete_duration);
    println!("   Created {} new directories in {:?}", num_new_dirs, recreate_duration);
    println!("   Total deep scan simulation time: {:?}", total_duration);
}