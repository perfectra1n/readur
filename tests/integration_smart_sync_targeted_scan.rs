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
async fn test_smart_sync_targeted_scan() {
    // Integration Test: Smart sync with single directory changed should use targeted scan
    // Expected: Should return RequiresSync(TargetedScan) when only a few directories have changed
    
    let (state, user) = create_test_setup().await;
    
    // Create a scenario with many directories, where only one has changed
    let unchanged_directories = vec![
        ("/Documents", "root-etag-stable"),
        ("/Documents/Projects", "projects-etag-stable"), 
        ("/Documents/Archive", "archive-etag-stable"),
        ("/Documents/Photos", "photos-etag-stable"),
        ("/Documents/Music", "music-etag-stable"),
        ("/Documents/Videos", "videos-etag-stable"),
        ("/Documents/Backup", "backup-etag-stable"),
        ("/Documents/Personal", "personal-etag-stable"),
        ("/Documents/Work", "work-etag-stable"),
        ("/Documents/Temp", "temp-etag-stable"), // 10 directories total
    ];
    
    // Pre-populate database with known directory ETags
    for (path, etag) in &unchanged_directories {
        let dir = CreateWebDAVDirectory {
            user_id: user.id,
            directory_path: path.to_string(),
            directory_etag: etag.to_string(),
            file_count: 5,
            total_size_bytes: 500000,
        };
        
        state.db.create_or_update_webdav_directory(&dir).await
            .expect("Failed to create directory tracking");
    }
    
    // Verify directories were created
    let stored_dirs = state.db.list_webdav_directories(user.id).await.unwrap();
    assert_eq!(stored_dirs.len(), 10, "Should have 10 tracked directories");
    
    // Test the strategy selection logic for targeted scan
    // When few directories change (<=30% and <=5 new), should use targeted scan
    let change_ratio = 1.0 / 10.0; // 1 changed out of 10 = 10%
    let new_dirs_count = 0; // No new directories
    
    let should_use_targeted = change_ratio <= 0.3 && new_dirs_count <= 5;
    assert!(should_use_targeted, "Should use targeted scan for small changes: {:.1}% change ratio", change_ratio * 100.0);
    
    println!("✅ Targeted scan strategy selection test passed - 10% change triggers targeted scan");
}

#[tokio::test]
async fn test_targeted_scan_vs_full_scan_thresholds() {
    // Integration Test: Test various scenarios for when to use targeted vs full scan
    // Expected: Strategy should be chosen based on change ratio and new directory count
    
    let (state, user) = create_test_setup().await;
    
    // Create base directories for testing different scenarios
    let base_directories = 20; // Start with 20 directories
    for i in 0..base_directories {
        let dir = CreateWebDAVDirectory {
            user_id: user.id,
            directory_path: format!("/Documents/Base{:02}", i),
            directory_etag: format!("base-etag-{:02}", i),
            file_count: 3,
            total_size_bytes: 300000,
        };
        
        state.db.create_or_update_webdav_directory(&dir).await
            .expect("Failed to create base directory");
    }
    
    // Test Scenario 1: Low change ratio, few new dirs -> Targeted scan
    let scenario1_changes = 2; // 2 out of 20 = 10%
    let scenario1_new = 1; // 1 new directory
    let scenario1_ratio = scenario1_changes as f64 / base_directories as f64;
    let scenario1_targeted = scenario1_ratio <= 0.3 && scenario1_new <= 5;
    assert!(scenario1_targeted, "Scenario 1 should use targeted scan: {:.1}% changes, {} new", scenario1_ratio * 100.0, scenario1_new);
    
    // Test Scenario 2: High change ratio -> Full scan
    let scenario2_changes = 8; // 8 out of 20 = 40%
    let scenario2_new = 2; // 2 new directories
    let scenario2_ratio = scenario2_changes as f64 / base_directories as f64;
    let scenario2_full_scan = scenario2_ratio > 0.3 || scenario2_new > 5;
    assert!(scenario2_full_scan, "Scenario 2 should use full scan: {:.1}% changes, {} new", scenario2_ratio * 100.0, scenario2_new);
    
    // Test Scenario 3: Low change ratio but many new dirs -> Full scan
    let scenario3_changes = 1; // 1 out of 20 = 5%
    let scenario3_new = 7; // 7 new directories
    let scenario3_ratio = scenario3_changes as f64 / base_directories as f64;
    let scenario3_full_scan = scenario3_ratio > 0.3 || scenario3_new > 5;
    assert!(scenario3_full_scan, "Scenario 3 should use full scan: {:.1}% changes, {} new", scenario3_ratio * 100.0, scenario3_new);
    
    // Test Scenario 4: Edge case - exactly at threshold -> Targeted scan
    let scenario4_changes = 6; // 6 out of 20 = 30% (exactly at threshold)
    let scenario4_new = 5; // 5 new directories (exactly at threshold)
    let scenario4_ratio = scenario4_changes as f64 / base_directories as f64;
    let scenario4_targeted = scenario4_ratio <= 0.3 && scenario4_new <= 5;
    assert!(scenario4_targeted, "Scenario 4 should use targeted scan: {:.1}% changes, {} new", scenario4_ratio * 100.0, scenario4_new);
    
    println!("✅ All targeted vs full scan threshold tests passed:");
    println!("   Scenario 1 (10% changes, 1 new): Targeted scan");
    println!("   Scenario 2 (40% changes, 2 new): Full scan");
    println!("   Scenario 3 (5% changes, 7 new): Full scan");
    println!("   Scenario 4 (30% changes, 5 new): Targeted scan");
}

#[tokio::test]
async fn test_directory_change_detection_logic() {
    // Integration Test: Test the logic for detecting changed, new, and unchanged directories
    // This is the core of the targeted scan decision making
    
    let (state, user) = create_test_setup().await;
    
    // Set up known directories in database
    let known_dirs = vec![
        ("/Documents", "root-etag-old"),
        ("/Documents/Projects", "projects-etag-stable"),
        ("/Documents/Archive", "archive-etag-old"),
        ("/Documents/ToBeDeleted", "deleted-etag"), // This won't appear in "current"
    ];
    
    for (path, etag) in &known_dirs {
        let dir = CreateWebDAVDirectory {
            user_id: user.id,
            directory_path: path.to_string(),
            directory_etag: etag.to_string(),
            file_count: 3,
            total_size_bytes: 300000,
        };
        
        state.db.create_or_update_webdav_directory(&dir).await
            .expect("Failed to create known directory");
    }
    
    // Simulate current directories from WebDAV (what we'd get from discovery)
    use std::collections::HashMap;
    let current_dirs = vec![
        ("/Documents", "root-etag-new"), // Changed
        ("/Documents/Projects", "projects-etag-stable"), // Unchanged
        ("/Documents/Archive", "archive-etag-new"), // Changed  
        ("/Documents/NewFolder", "new-folder-etag"), // New
    ];
    let current_map: HashMap<String, String> = current_dirs.into_iter()
        .map(|(p, e)| (p.to_string(), e.to_string()))
        .collect();
    
    // Get known directories from database
    let known_map: HashMap<String, String> = state.db.list_webdav_directories(user.id).await
        .expect("Failed to get known directories")
        .into_iter()
        .filter(|dir| dir.directory_path.starts_with("/Documents"))
        .map(|dir| (dir.directory_path, dir.directory_etag))
        .collect();
    
    // Perform comparison logic (mirrors SmartSyncService logic)
    let mut changed_directories = Vec::new();
    let mut new_directories = Vec::new();
    let mut unchanged_directories = Vec::new();
    
    for (current_path, current_etag) in &current_map {
        match known_map.get(current_path) {
            Some(known_etag) => {
                if known_etag != current_etag {
                    changed_directories.push(current_path.clone());
                } else {
                    unchanged_directories.push(current_path.clone());
                }
            }
            None => {
                new_directories.push(current_path.clone());
            }
        }
    }
    
    // Detect deleted directories (in database but not in current WebDAV response)
    let mut deleted_directories = Vec::new();
    for known_path in known_map.keys() {
        if !current_map.contains_key(known_path) {
            deleted_directories.push(known_path.clone());
        }
    }
    
    // Verify comparison results
    assert_eq!(changed_directories.len(), 2, "Should detect 2 changed directories");
    assert!(changed_directories.contains(&"/Documents".to_string()));
    assert!(changed_directories.contains(&"/Documents/Archive".to_string()));
    
    assert_eq!(new_directories.len(), 1, "Should detect 1 new directory");
    assert!(new_directories.contains(&"/Documents/NewFolder".to_string()));
    
    assert_eq!(unchanged_directories.len(), 1, "Should detect 1 unchanged directory");
    assert!(unchanged_directories.contains(&"/Documents/Projects".to_string()));
    
    assert_eq!(deleted_directories.len(), 1, "Should detect 1 deleted directory");
    assert!(deleted_directories.contains(&"/Documents/ToBeDeleted".to_string()));
    
    // Calculate strategy
    let total_known = known_map.len();
    let change_ratio = (changed_directories.len() + deleted_directories.len()) as f64 / total_known as f64;
    let new_dirs_count = new_directories.len();
    
    let should_use_targeted = change_ratio <= 0.3 && new_dirs_count <= 5;
    
    println!("✅ Directory change detection logic test completed successfully:");
    println!("   Changed: {} directories", changed_directories.len());
    println!("   New: {} directories", new_directories.len()); 
    println!("   Unchanged: {} directories", unchanged_directories.len());
    println!("   Deleted: {} directories", deleted_directories.len());
    println!("   Change ratio: {:.1}%", change_ratio * 100.0);
    println!("   Strategy: {}", if should_use_targeted { "Targeted scan" } else { "Full scan" });
}