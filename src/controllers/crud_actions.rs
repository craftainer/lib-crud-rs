//! Shared id/filter/bulk decision logic for a resource's CRUD routes: the
//! `?id=` versus filter-based branching, the bulk-action size cap and the
//! error mapping. Generic over any `R: Repository`, so every resource's
//! router (JSON, XML, HTML, ...) reuses the same decisions instead of
//! duplicating them.

use crate::crud::CrudService;
use crate::error::CrudError;
use crate::models::HasId;
use crate::repositories::Repository;
use crate::repositories::filtering::{FilterClause, SortClause};
use crate::views::FieldError;
use crate::views::bulk::{BulkDeleteResult, BulkUpdateResult};

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

fn not_found(id: i32) -> CrudError {
    CrudError::NotFound(format!("record {id} not found"))
}

/// Return one record by id, or a filtered/sorted page of matching records.
///
/// # Errors
///
/// [`CrudError::NotFound`] when `id` names no record; [`CrudError::Internal`]
/// when the backend fails.
pub async fn resolve_list_or_get<R>(
    crud: &CrudService<R>,
    id: Option<i32>,
    skip: u64,
    limit: u64,
    include_archived: bool,
    filters: Vec<FilterClause>,
    sort: Vec<SortClause>,
) -> Result<ListOrGet<R::Model>, CrudError>
where
    R: Repository,
{
    if let Some(id) = id {
        let record = crud.get(id, include_archived).await?;
        let record = record.ok_or_else(|| not_found(id))?;
        return Ok(ListOrGet::One(record));
    }
    let records = crud
        .list(skip, limit, include_archived, filters, sort)
        .await?;
    Ok(ListOrGet::Many(records))
}

/// Update one record by id, or bulk-update every record matching the
/// given filters (scoped to `owner_id`, capped by `max_matched`).
///
/// # Errors
///
/// [`CrudError::NotFound`] when `id` names no record owned by `owner_id`;
/// [`CrudError::UnprocessableEntity`] when neither an id nor a filter was
/// given, or the filters match more than `max_matched` records;
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
        let updated = resolve_update_by_id(crud, id, owner_id, data).await?;
        return Ok(UpdateOutcome::One(updated));
    }
    if filters.is_empty() {
        return Err(no_target_error());
    }
    let matched = crud.count(&filters, false).await?;
    if matched > max_matched {
        return Err(too_many_matched_error(matched, max_matched));
    }
    let updated = crud.update_many(owner_id, filters, data).await?;
    let ids = updated.iter().map(HasId::id).collect();
    Ok(UpdateOutcome::Bulk(BulkUpdateResult {
        matched: updated.len(),
        ids,
    }))
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
        let deleted = crud.delete(id, owner_id).await?;
        if !deleted {
            return Err(not_found(id));
        }
        return Ok(DeleteOutcome::One);
    }
    if filters.is_empty() {
        return Err(no_target_error());
    }
    let matched = crud.count(&filters, false).await?;
    if matched > max_matched {
        return Err(too_many_matched_error(matched, max_matched));
    }
    let deleted = crud.delete_many(owner_id, filters).await?;
    let ids = deleted.iter().map(HasId::id).collect();
    Ok(DeleteOutcome::Bulk(BulkDeleteResult {
        matched: deleted.len(),
        ids,
    }))
}
