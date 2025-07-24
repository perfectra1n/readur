// WebDAV service modules organized by functionality

pub mod config;
pub mod connection;
pub mod discovery;
pub mod validation;
pub mod service;
pub mod smart_sync;

// Re-export main types for convenience
pub use config::{WebDAVConfig, RetryConfig, ConcurrencyConfig};
pub use connection::WebDAVConnection;
pub use discovery::WebDAVDiscovery;
pub use validation::{
    WebDAVValidator, ValidationReport, ValidationIssue, ValidationIssueType, 
    ValidationSeverity, ValidationRecommendation, ValidationAction, ValidationSummary
};
pub use service::{WebDAVService, ServerCapabilities, HealthStatus, test_webdav_connection};
pub use smart_sync::{SmartSyncService, SmartSyncDecision, SmartSyncStrategy, SmartSyncResult};

// Test modules
#[cfg(test)]
mod url_construction_tests;
#[cfg(test)]
mod subdirectory_edge_cases_tests;