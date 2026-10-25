//! Shared id/filter/bulk decision logic for a resource's CRUD routes: the
//! `?id=` versus filter-based branching, the bulk-action size cap and the
//! error mapping. Generic over any `R: Repository`, so every resource's
//! router (JSON, XML, HTML, ...) reuses the same decisions instead of
//! duplicating them.
//!
//! # `?id=` excludes filters
//!
//! A request addresses one record (`id`) or a set of them (filters), never
//! both: with an `id` and at least one filter every `resolve_*` answers
//! [`CrudError::UnprocessableEntity`], on a read as well as on a write.
//! `lib-crud-py` does not refuse: it returns before it parses the filters, so
//! `DELETE ?id=5&status=draft` there deletes record 5 whatever its status. For
//! a mutation that turns a guard the caller wrote into a no-op, so this crate
//! refuses (a deliberate difference, see ADR 0003). The reserved keys
//! (`skip`, `limit`, `sort`, `include_archived`, `only_archived`, ...) are not
//! filters and may accompany an `id`.
//!
//! # `only_archived` narrows a list only
//!
//! [`resolve_list_or_get`] takes an [`ArchivedScope`] and refuses `Only` with an `id`
//! (`applies to list reads only`): a single-record read would return a live
//! row. The write resolvers take no flag; a host calls
//! [`reject_only_archived`](crate::controllers::crud_query::reject_only_archived)
//! on the raw query for those routes, `/stats` and restore.
//!
//! # The bulk cap is not atomic
//!
//! A bulk update or delete counts the matching records and, if the count is
//! within the cap, runs the write as a second statement. A record created
//! between the two can push the write past the cap, and one deleted between
//! them can make it smaller. [`Repository`] has no "write at most N rows"
//! operation, so the crate cannot close the gap; `lib-crud-py` has the same
//! check-then-act shape. The cap bounds an honest caller's blast radius, not
//! an adversary racing the check. A host that needs a hard limit enforces it
//! inside its own `update_many`/`delete_many` (for example `LIMIT` in a CTE),
//! or runs the count and the write in one transaction at its own isolation
//! level.

use crate::crud::{CrudService, owner_scoped};
use crate::error::CrudError;
use crate::models::HasId;
use crate::repositories::Repository;
use crate::repositories::filtering::{FilterClause, SortClause};
use crate::views::FieldError;
use crate::views::bulk::{BulkDeleteResult, BulkResult, BulkUpdateResult};

/// Which rows a list read sees, from the `include_archived` and
/// `only_archived` flags. `only_archived` implies `include_archived`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchivedScope {
    /// Live rows only (the default).
    Live,
    /// Live and archived rows (`include_archived=true`).
    Include,
    /// Archived rows only (`only_archived=true`).
    Only,
}

impl ArchivedScope {
    /// The scope the two flags select; `only_archived` wins.
    #[must_use]
    pub const fn from_flags(include_archived: bool, only_archived: bool) -> Self {
        if only_archived {
            Self::Only
        } else if include_archived {
            Self::Include
        } else {
            Self::Live
        }
    }
}

/// A resolved `list`/`get`: one record (an `?id=` request) or a filtered/
/// sorted page of them.
pub enum ListOrGet<M> {
    /// The one record an `?id=` request addressed.
    One(M),
    /// A filtered, sorted page of records.
    Many(Vec<M>),
}

/// A resolved update: one record (an `?id=` request) or the result of a
/// bulk update over the given filters.
#[derive(Debug)]
pub enum UpdateOutcome<M> {
    /// The updated record.
    One(M),
    /// The result of a bulk update.
    Bulk(BulkUpdateResult),
}

/// A resolved delete: one record deleted (an `?id=` request, -> 204) or
/// the result of a bulk delete over the given filters.
#[derive(Debug)]
pub enum DeleteOutcome {
    /// The one record was deleted.
    One,
    /// The result of a bulk delete.
    Bulk(BulkDeleteResult),
}

const NO_TARGET_FIELD: &str = "filter";

fn no_target_error() -> CrudError {
    CrudError::UnprocessableEntity(vec![FieldError::new(
        NO_TARGET_FIELD,
        "id or at least one filter is required for a bulk action",
    )])
}

fn too_many_matched_error(matched: u64, max_matched: u64) -> CrudError {
    CrudError::UnprocessableEntity(vec![FieldError::new(
        NO_TARGET_FIELD,
        format!(
            "bulk action would affect {matched} records, over the {max_matched}-record limit -- narrow the filters"
        ),
    )])
}

/// Refuse a bulk action that names no filter or matches more than
/// `max_matched` of `owner_id`'s records. Check-then-act: see the module
/// docs.
async fn check_bulk_target<R>(
    crud: &CrudService<R>,
    owner_id: &str,
    filters: &[FilterClause],
    max_matched: u64,
) -> Result<(), CrudError>
where
    R: Repository,
{
    if filters.is_empty() {
        return Err(no_target_error());
    }
    let matched = crud
        .count(&owner_scoped(owner_id, filters.to_vec()), false, false)
        .await?;
    if matched > max_matched {
        return Err(too_many_matched_error(matched, max_matched));
    }
    Ok(())
}

fn bulk_result<M: HasId>(affected: &[M]) -> BulkResult {
    BulkResult {
        matched: affected.len(),
        ids: affected.iter().map(HasId::id).collect(),
    }
}

/// `?id=` and filters are alternatives: refuse a request that sends both.
fn ensure_id_without_filters(filters: &[FilterClause]) -> Result<(), CrudError> {
    if filters.is_empty() {
        return Ok(());
    }
    Err(CrudError::UnprocessableEntity(vec![FieldError::new(
        NO_TARGET_FIELD,
        "id cannot be combined with filters",
    )]))
}

fn not_found(id: i32) -> CrudError {
    CrudError::NotFound(format!("record {id} not found"))
}

/// Return one record by id, or a filtered/sorted page of matching records.
///
/// # Errors
///
/// [`CrudError::NotFound`] when `id` names no record;
/// [`CrudError::UnprocessableEntity`] when `id` comes with filters or with
/// `only_archived`;
/// [`CrudError::Internal`] when the backend fails.
pub async fn resolve_list_or_get<R>(
    crud: &CrudService<R>,
    id: Option<i32>,
    skip: u64,
    limit: u64,
    archived: ArchivedScope,
    filters: Vec<FilterClause>,
    sort: Vec<SortClause>,
) -> Result<ListOrGet<R::Model>, CrudError>
where
    R: Repository,
{
    if let Some(id) = id {
        if archived == ArchivedScope::Only {
            return Err(CrudError::UnprocessableEntity(vec![FieldError::new(
                "only_archived",
                "applies to list reads only",
            )]));
        }
        ensure_id_without_filters(&filters)?;
        let record = crud.get(id, archived == ArchivedScope::Include).await?;
        let record = record.ok_or_else(|| not_found(id))?;
        return Ok(ListOrGet::One(record));
    }
    let records = crud
        .list(
            skip,
            limit,
            archived == ArchivedScope::Include,
            archived == ArchivedScope::Only,
            filters,
            sort,
        )
        .await?;
    Ok(ListOrGet::Many(records))
}

/// Update one record by id, or bulk-update every record matching the
/// given filters (scoped to `owner_id`, capped by `max_matched`).
///
/// # Errors
///
/// [`CrudError::NotFound`] when `id` names no record owned by `owner_id`;
/// [`CrudError::UnprocessableEntity`] when `id` comes with filters, when
/// neither an id nor a filter was given, or the filters match more than `max_matched` records;
/// [`CrudError::Internal`] when the backend fails.
pub async fn resolve_update<R>(
    crud: &CrudService<R>,
    id: Option<i32>,
    owner_id: &str,
    filters: Vec<FilterClause>,
    data: R::Update,
    max_matched: u64,
) -> Result<UpdateOutcome<R::Model>, CrudError>
where
    R: Repository,
    R::Model: HasId,
{
    if let Some(id) = id {
        ensure_id_without_filters(&filters)?;
        let updated = resolve_update_by_id(crud, id, owner_id, data).await?;
        return Ok(UpdateOutcome::One(updated));
    }
    check_bulk_target(crud, owner_id, &filters, max_matched).await?;
    let updated = crud.update_many(owner_id, filters, data).await?;
    Ok(UpdateOutcome::Bulk(bulk_result(&updated)))
}

/// Update one record by id -- the id-only half of [`resolve_update`],
/// factored out for callers that only ever update by id and would otherwise
/// have to handle an [`UpdateOutcome::Bulk`] arm their own logic can never
/// produce.
///
/// # Errors
///
/// [`CrudError::NotFound`] when `id` names no record owned by `owner_id`;
/// [`CrudError::Internal`] when the backend fails.
pub async fn resolve_update_by_id<R>(
    crud: &CrudService<R>,
    id: i32,
    owner_id: &str,
    data: R::Update,
) -> Result<R::Model, CrudError>
where
    R: Repository,
{
    let updated = crud.update(id, owner_id, data).await?;
    updated.ok_or_else(|| not_found(id))
}

/// Delete one record by id, or bulk-delete every record matching the
/// given filters (scoped to `owner_id`, capped by `max_matched`).
///
/// # Errors
///
/// As [`resolve_update`]: not found, an unbounded or oversized bulk target,
/// or a backend failure.
pub async fn resolve_delete<R>(
    crud: &CrudService<R>,
    id: Option<i32>,
    owner_id: &str,
    filters: Vec<FilterClause>,
    max_matched: u64,
) -> Result<DeleteOutcome, CrudError>
where
    R: Repository,
    R::Model: HasId,
{
    if let Some(id) = id {
        ensure_id_without_filters(&filters)?;
        let deleted = crud.delete(id, owner_id).await?;
        if !deleted {
            return Err(not_found(id));
        }
        return Ok(DeleteOutcome::One);
    }
    check_bulk_target(crud, owner_id, &filters, max_matched).await?;
    let deleted = crud.delete_many(owner_id, filters).await?;
    Ok(DeleteOutcome::Bulk(bulk_result(&deleted)))
}

#[cfg(test)]
mod tests {
    use super::ArchivedScope;

    #[test]
    fn the_flags_select_a_scope_and_only_archived_wins() {
        assert_eq!(ArchivedScope::from_flags(false, false), ArchivedScope::Live);
        assert_eq!(
            ArchivedScope::from_flags(true, false),
            ArchivedScope::Include
        );
        assert_eq!(ArchivedScope::from_flags(false, true), ArchivedScope::Only);
        assert_eq!(ArchivedScope::from_flags(true, true), ArchivedScope::Only);
    }
}
