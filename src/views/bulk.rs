//! Generic result views for the bulk update/delete actions on the CRUD
//! router. Resource-agnostic (just a matched
//! count and the affected ids), so one pair of types serves every
//! resource's bulk routes.

use serde::Serialize;

/// Result of a bulk update: how many records matched and which ones were
/// updated.
#[derive(Debug, Serialize)]
pub struct BulkUpdateResult {
    /// How many records matched.
    pub matched: usize,
    /// The ids of the affected records.
    pub ids: Vec<i32>,
}

/// Result of a bulk delete: how many records matched and which ones were
/// deleted.
#[derive(Debug, Serialize)]
pub struct BulkDeleteResult {
    /// How many records matched.
    pub matched: usize,
    /// The ids of the affected records.
    pub ids: Vec<i32>,
}
