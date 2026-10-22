//! Generic result view for the bulk update/delete actions on the CRUD
//! router. Resource-agnostic (just a matched count and the affected ids),
//! so one type serves every resource's bulk routes.

use serde::Serialize;

/// Result of a bulk action: how many records matched and which ones were
/// affected. The JSON shape is `{"matched": n, "ids": [...]}` for both an
/// update and a delete.
#[derive(Debug, Serialize)]
pub struct BulkResult {
    /// How many records matched.
    pub matched: usize,
    /// The ids of the affected records.
    pub ids: Vec<i32>,
}

/// Result of a bulk update. The same type as [`BulkResult`]; the alias keeps
/// the name `lib-crud-py` gives the update result.
pub type BulkUpdateResult = BulkResult;

/// Result of a bulk delete. The same type as [`BulkResult`]; the alias keeps
/// the name `lib-crud-py` gives the delete result.
pub type BulkDeleteResult = BulkResult;
