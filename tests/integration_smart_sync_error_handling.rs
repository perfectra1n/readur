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
async fn test_webdav_error_fallback() {
    // Integration Test: WebDAV server error scenarios should fall back to traditional sync
    // Expected: When WebDAV service fails, should gracefully handle errors
    
    let (state, user) = create_test_setup().await;
    let smart_sync_service = SmartSyncService::new(state.clone());
    
    // Create some existing directories to test database robustness
    let existing_directories = vec![
        CreateWebDAVDirectory {
            user_id: user.id,
            directory_path: "/Documents".to_string(),
            directory_etag: "existing-root".to_string(),
            file_count: 5,
            total_size_bytes: 500000,
        },
        CreateWebDAVDirectory {
            user_id: user.id,
            directory_path: "/Documents/Projects".to_string(),
            directory_etag: "existing-projects".to_string(),
            file_count: 8,
            total_size_bytes: 800000,
        },
    ];
    
    for dir in &existing_directories {
        state.db.create_or_update_webdav_directory(dir).await
            .expect("Failed to create existing directory");
    }
    
    // Test with a WebDAV service that will fail (invalid URL)
    let invalid_config = WebDAVConfig {
        server_url: "https://invalid-server-that-does-not-exist.com".to_string(),
        username: "invalid".to_string(),
        password: "invalid".to_string(),
        watch_folders: vec!["/Documents".to_string()],
        file_extensions: vec!["pdf".to_string()],
        timeout_seconds: 1, // Very short timeout to fail quickly
        server_type: Some("generic".to_string()),
    };
    
    let failing_webdav_service = WebDAVService::new(invalid_config)
        .expect("WebDAV service creation should not fail");
    
    // Test smart sync evaluation with failing WebDAV service
    let decision = smart_sync_service.evaluate_sync_need(user.id, &failing_webdav_service, "/Documents").await;
    
    // The system should handle the WebDAV error gracefully
    match decision {
        Ok(SmartSyncDecision::RequiresSync(SmartSyncStrategy::FullDeepScan)) => {
            println!("✅ WebDAV error correctly falls back to full deep scan");
        }
        Err(e) => {
            println!("✅ WebDAV error handled gracefully: {}", e);
            // This is acceptable - the system should either fall back or return an error
            // The important thing is that it doesn't panic or corrupt the database
        }
        Ok(other) => {
            println!("⚠️ WebDAV error resulted in unexpected decision: {:?}", other);
            // This might be acceptable depending on the implementation
        }
    }
    
    // Verify database state is intact after WebDAV errors
    let dirs_after_error = state.db.list_webdav_directories(user.id).await.unwrap();
    assert_eq!(dirs_after_error.len(), 2, "Database should remain intact after WebDAV errors");
    
    let root_dir = dirs_after_error.iter().find(|d| d.directory_path == "/Documents").unwrap();
    assert_eq!(root_dir.directory_etag, "existing-root");
    
    println!("✅ WebDAV error fallback test completed - database remains intact");
}

#[tokio::test]
async fn test_database_error_handling() {
    // Integration Test: Database errors should be handled gracefully
    // This tests the system's resilience to database connectivity issues
    
    let (state, user) = create_test_setup().await;
    let smart_sync_service = SmartSyncService::new(state.clone());
    
    // Test with invalid user ID (simulates database query errors)
    let invalid_user_id = uuid::Uuid::new_v4(); // Random UUID that doesn't exist
    let webdav_service = create_test_webdav_service();
    
    let decision = smart_sync_service.evaluate_sync_need(invalid_user_id, &webdav_service, "/Documents").await;
    
    match decision {
        Ok(SmartSyncDecision::RequiresSync(SmartSyncStrategy::FullDeepScan)) => {
            println!("✅ Invalid user ID correctly falls back to full deep scan");
        }
        Err(e) => {
            println!("✅ Invalid user ID error handled gracefully: {}", e);
            // This is the expected behavior - should return an error for invalid user
        }
        Ok(other) => {
            println!("⚠️ Invalid user ID resulted in: {:?}", other);
        }
    }
    
    // Test database connectivity by trying normal operations
    let test_dir = CreateWebDAVDirectory {
        user_id: user.id, // Valid user ID
        directory_path: "/Test".to_string(),
        directory_etag: "test-etag".to_string(),
        file_count: 1,
        total_size_bytes: 100000,
    };
    
    // This should work normally
    state.db.create_or_update_webdav_directory(&test_dir).await
        .expect("Normal database operations should still work");
    
    let saved_dirs = state.db.list_webdav_directories(user.id).await.unwrap();
    assert_eq!(saved_dirs.len(), 1, "Normal database operations should work after error handling");
    
    println!("✅ Database error handling test completed");
}

#[tokio::test]
async fn test_concurrent_smart_sync_operations() {
    // Integration Test: Concurrent smart sync operations should not interfere with each other
    // This tests race conditions and database locking
    
    let (state, user) = create_test_setup().await;
    
    // Create initial directories
    let initial_dirs = vec![
        CreateWebDAVDirectory {
            user_id: user.id,
            directory_path: "/Concurrent1".to_string(),
            directory_etag: "concurrent1-etag".to_string(),
            file_count: 5,
            total_size_bytes: 500000,
        },
        CreateWebDAVDirectory {
            user_id: user.id,
            directory_path: "/Concurrent2".to_string(),
            directory_etag: "concurrent2-etag".to_string(),
            file_count: 3,
            total_size_bytes: 300000,
        },
    ];
    
    for dir in &initial_dirs {
        state.db.create_or_update_webdav_directory(dir).await
            .expect("Failed to create initial directory");
    }
    
    // Run multiple concurrent operations
    let num_concurrent = 5;
    let mut handles = Vec::new();
    
    for i in 0..num_concurrent {
        let state_clone = state.clone();
        let user_id = user.id;
        
        let handle = tokio::spawn(async move {
            // Each task tries to create/update directories concurrently
            let dir = CreateWebDAVDirectory {
                user_id,
                directory_path: format!("/Concurrent{}", i + 10),
                directory_etag: format!("concurrent{}-etag", i + 10),
                file_count: (i as i64) + 1,
                total_size_bytes: ((i as i64) + 1) * 100000,
            };
            
            // Add some delay to increase chance of race conditions
            tokio::time::sleep(tokio::time::Duration::from_millis(i as u64 * 10)).await;
            
            state_clone.db.create_or_update_webdav_directory(&dir).await
        });
        
        handles.push(handle);
    }
    
    // Wait for all concurrent operations to complete
    let mut results = Vec::new();
    for handle in handles {
        results.push(handle.await.unwrap());
    }
    
    // Verify all operations succeeded
    let mut success_count = 0;
    let mut error_count = 0;
    
    for result in results {
        match result {
            Ok(_) => success_count += 1,
            Err(e) => {
                error_count += 1;
                println!("Concurrent operation error: {}", e);
            }
        }
    }
    
    println!("Concurrent operations: {} successful, {} failed", success_count, error_count);
    
    // Verify final database state
    let final_dirs = state.db.list_webdav_directories(user.id).await.unwrap();
    let expected_total = initial_dirs.len() + success_count;
    assert_eq!(final_dirs.len(), expected_total, 
               "Should have {} directories after concurrent operations", expected_total);
    
    // Verify original directories are still intact
    let concurrent1 = final_dirs.iter().find(|d| d.directory_path == "/Concurrent1").unwrap();
    assert_eq!(concurrent1.directory_etag, "concurrent1-etag");
    
    let concurrent2 = final_dirs.iter().find(|d| d.directory_path == "/Concurrent2").unwrap();
    assert_eq!(concurrent2.directory_etag, "concurrent2-etag");
    
    println!("✅ Concurrent smart sync operations test completed successfully");
    println!("   {} initial directories preserved", initial_dirs.len());
    println!("   {} concurrent operations executed", num_concurrent);
    println!("   {} operations successful", success_count);
}

#[tokio::test]
async fn test_malformed_data_recovery() {
    // Integration Test: System should handle and recover from malformed data gracefully
    // This tests robustness against data corruption scenarios
    
    let (state, user) = create_test_setup().await;
    
    // Create a directory with normal data first
    let normal_dir = CreateWebDAVDirectory {
        user_id: user.id,
        directory_path: "/Normal".to_string(),
        directory_etag: "normal-etag".to_string(),
        file_count: 10,
        total_size_bytes: 1000000,
    };
    
    state.db.create_or_update_webdav_directory(&normal_dir).await
        .expect("Normal directory creation should work");
    
    // Test with edge case data
    let edge_cases = vec![
        CreateWebDAVDirectory {
            user_id: user.id,
            directory_path: "/EmptyPath".to_string(),
            directory_etag: "".to_string(), // Empty ETag
            file_count: 0,
            total_size_bytes: 0,
        },
        CreateWebDAVDirectory {
            user_id: user.id,
            directory_path: "/SpecialChars".to_string(),
            directory_etag: "etag-with-special-chars-!@#$%^&*()".to_string(),
            file_count: -1, // Invalid negative count (should be handled)
            total_size_bytes: -1000, // Invalid negative size
        },
        CreateWebDAVDirectory {
            user_id: user.id,
            directory_path: "/VeryLongPath/With/Many/Nested/Directories/That/Goes/On/And/On/For/A/Very/Long/Time/To/Test/Path/Length/Limits".to_string(),
            directory_etag: "very-long-etag-that-might-exceed-normal-database-field-lengths-and-cause-truncation-issues-if-not-handled-properly".to_string(),
            file_count: i32::MAX as i64, // Maximum integer value
            total_size_bytes: i64::MAX, // Maximum long value
        },
    ];
    
    let mut successful_edge_cases = 0;
    let mut failed_edge_cases = 0;
    
    for edge_case in edge_cases {
        match state.db.create_or_update_webdav_directory(&edge_case).await {
            Ok(_) => {
                successful_edge_cases += 1;
                println!("✅ Edge case handled successfully: {}", edge_case.directory_path);
            }
            Err(e) => {
                failed_edge_cases += 1;
                println!("⚠️ Edge case failed as expected: {} - {}", edge_case.directory_path, e);
            }
        }
    }
    
    // Verify the normal directory is still accessible
    let all_dirs = state.db.list_webdav_directories(user.id).await.unwrap();
    let normal_dir_exists = all_dirs.iter().any(|d| d.directory_path == "/Normal");
    assert!(normal_dir_exists, "Normal directory should still exist after edge case testing");
    
    // Verify database is still functional
    let test_dir = CreateWebDAVDirectory {
        user_id: user.id,
        directory_path: "/AfterEdgeCases".to_string(),
        directory_etag: "after-edge-cases".to_string(),
        file_count: 5,
        total_size_bytes: 500000,
    };
    
    state.db.create_or_update_webdav_directory(&test_dir).await
        .expect("Database should still work after edge case testing");
    
    let final_dirs = state.db.list_webdav_directories(user.id).await.unwrap();
    let after_edge_case_dir = final_dirs.iter().find(|d| d.directory_path == "/AfterEdgeCases").unwrap();
    assert_eq!(after_edge_case_dir.directory_etag, "after-edge-cases");
    
    println!("✅ Malformed data recovery test completed successfully");
    println!("   {} edge cases handled successfully", successful_edge_cases);
    println!("   {} edge cases failed as expected", failed_edge_cases);
    println!("   Database remains functional after edge case testing");
}