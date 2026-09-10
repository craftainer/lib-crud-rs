//! Generic result views for the bulk update/delete actions on the CRUD
//! router -- port of `views/bulk.py`. Resource-agnostic (just a matched
//! count and the affected ids), so one pair of types serves every
//! resource's bulk routes.

use serde::Serialize;

/// Result of a bulk update: how many records matched and which ones were
/// updated.
#[derive(Debug, Serialize)]
pub struct BulkUpdateResult {
    pub matched: usize,
    pub ids: Vec<i32>,
}

/// Result of a bulk delete: how many records matched and which ones were
/// deleted.
#[derive(Debug, Serialize)]
pub struct BulkDeleteResult {
    pub matched: usize,
    pub ids: Vec<i32>,
}
