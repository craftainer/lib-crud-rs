//! Shared id/filter/bulk decision logic for the CRUD router -- port of
//! `controllers/crud_actions.py`. Only `heroes.rs` calls this today (this
//! port has one resource so far), but it's generic over any `R:
//! Repository` rather than Hero-specific, so a future sibling router
//! (item 4's XML router) reuses the same decision logic instead of
//! duplicating it -- the same role `crud_actions.py` plays for
//! `build_json_router`/`build_xml_router` in the reference.

use crate::crud::CrudService;
use crate::generic::models::HasId;
use crate::generic::repositories::filtering::{FilterClause, SortClause};
use crate::generic::repositories::Repository;
use crate::generic::views::bulk::{BulkDeleteResult, BulkUpdateResult};
use crate::generic::views::FieldError;
use crate::problem_details::AppError;

/// A resolved `list`/`get`: one record (an `?id=` request) or a filtered/
/// sorted page of them.
pub enum ListOrGet<M> {
    One(M),
    Many(Vec<M>),
}

/// A resolved update: one record (an `?id=` request) or the result of a
/// bulk update over the given filters.
pub enum UpdateOutcome<M> {
    One(M),
    Bulk(BulkUpdateResult),
}

/// A resolved delete: one record deleted (an `?id=` request, -> 204) or
/// the result of a bulk delete over the given filters.
pub enum DeleteOutcome {
    One,
    Bulk(BulkDeleteResult),
}

const NO_TARGET_FIELD: &str = "filter";

fn no_target_error() -> AppError {
    AppError::UnprocessableEntity(vec![FieldError::new(
        NO_TARGET_FIELD,
        "id or at least one filter is required for a bulk action",
    )])
}

fn too_many_matched_error(matched: u64, max_matched: u64) -> AppError {
    AppError::UnprocessableEntity(vec![FieldError::new(
        NO_TARGET_FIELD,
        format!(
            "bulk action would affect {matched} records, over the {max_matched}-record limit -- narrow the filters"
        ),
    )])
}

fn not_found(id: i32) -> AppError {
    AppError::NotFound(format!("record {id} not found"))
}

/// Return one record by id, or a filtered/sorted page of matching records.
pub async fn resolve_list_or_get<R>(
    crud: &CrudService<R>,
    id: Option<i32>,
    skip: u64,
    limit: u64,
    include_archived: bool,
    filters: Vec<FilterClause>,
    sort: Vec<SortClause>,
) -> Result<ListOrGet<R::Model>, AppError>
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
pub async fn resolve_update<R>(
    crud: &CrudService<R>,
    id: Option<i32>,
    owner_id: &str,
    filters: Vec<FilterClause>,
    data: R::Update,
    max_matched: u64,
) -> Result<UpdateOutcome<R::Model>, AppError>
where
    R: Repository,
    R::Model: HasId,
{
    if let Some(id) = id {
        let updated = crud.update(id, owner_id, data).await?;
        let updated = updated.ok_or_else(|| not_found(id))?;
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

/// Delete one record by id, or bulk-delete every record matching the
/// given filters (scoped to `owner_id`, capped by `max_matched`).
pub async fn resolve_delete<R>(
    crud: &CrudService<R>,
    id: Option<i32>,
    owner_id: &str,
    filters: Vec<FilterClause>,
    max_matched: u64,
) -> Result<DeleteOutcome, AppError>
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crud::CrudService;
    use crate::generic::repositories::filtering::{FilterOp, FilterValue};
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    // A tiny synthetic fake resource ("Gadget", never a real one this repo
    // ships), not Hero -- mirrors template-fastapi's own
    // `_FakeGadgetRepository` in its generic-CRUD unit tests. Keeps this
    // generic package's tests from depending on `crate::hero` at all,
    // which is the whole point of the split (`src/README.md`'s "Generic
    // vs. Hero-specific split"): a lint that forbids `generic` -> `hero`
    // imports must hold for tests too, or it isn't really enforcing
    // anything.
    #[derive(Debug, Clone)]
    struct FakeGadget {
        id: i32,
        owner_id: String,
    }

    impl HasId for FakeGadget {
        fn id(&self) -> i32 {
            self.id
        }
    }

    #[derive(Default)]
    struct FakeGadgetUpdate {
        owner_id: Option<String>,
    }

    #[derive(Default)]
    struct FakeGadgetRepository {
        records: Mutex<BTreeMap<i32, FakeGadget>>,
        next_id: Mutex<i32>,
    }

    impl FakeGadgetRepository {
        fn new() -> Self {
            Self::default()
        }

        /// The only filter shape these tests exercise -- an `owner_id`
        /// equality clause -- interpreted directly, the same way
        /// `hero::HeroMemoryRepository` interprets its own filters.
        fn matches(record: &FakeGadget, filters: &[FilterClause]) -> bool {
            filters.iter().all(|clause| match &clause.value {
                FilterValue::Str(value) if clause.field == "owner_id" => {
                    matches!(clause.op, FilterOp::Eq) && &record.owner_id == value
                }
                _ => panic!("FakeGadgetRepository only understands owner_id eq filters"),
            })
        }
    }

    #[async_trait::async_trait]
    impl Repository for FakeGadgetRepository {
        type Model = FakeGadget;
        type Create = String;
        type Update = FakeGadgetUpdate;

        async fn list(
            &self,
            _opts: crate::generic::repositories::ListOptions,
        ) -> Result<Vec<Self::Model>, crate::generic::repositories::RepoError> {
            Ok(self.records.lock().unwrap().values().cloned().collect())
        }

        async fn count(
            &self,
            filters: &[FilterClause],
            _include_archived: bool,
        ) -> Result<u64, crate::generic::repositories::RepoError> {
            Ok(self
                .records
                .lock()
                .unwrap()
                .values()
                .filter(|record| Self::matches(record, filters))
                .count() as u64)
        }

        async fn get(
            &self,
            id: i32,
            _include_archived: bool,
        ) -> Result<Option<Self::Model>, crate::generic::repositories::RepoError> {
            Ok(self.records.lock().unwrap().get(&id).cloned())
        }

        async fn create(
            &self,
            owner_id: &str,
            _data: Self::Create,
        ) -> Result<Self::Model, crate::generic::repositories::RepoError> {
            let mut next_id = self.next_id.lock().unwrap();
            *next_id += 1;
            let record = FakeGadget {
                id: *next_id,
                owner_id: owner_id.to_string(),
            };
            self.records
                .lock()
                .unwrap()
                .insert(record.id, record.clone());
            Ok(record)
        }

        async fn update(
            &self,
            id: i32,
            _owner_id: &str,
            data: Self::Update,
        ) -> Result<Option<Self::Model>, crate::generic::repositories::RepoError> {
            let mut records = self.records.lock().unwrap();
            let Some(record) = records.get_mut(&id) else {
                return Ok(None);
            };
            if let Some(owner_id) = data.owner_id {
                record.owner_id = owner_id;
            }
            Ok(Some(record.clone()))
        }

        async fn update_many(
            &self,
            filters: &[FilterClause],
            data: Self::Update,
        ) -> Result<Vec<Self::Model>, crate::generic::repositories::RepoError> {
            let mut records = self.records.lock().unwrap();
            let ids: Vec<i32> = records
                .values()
                .filter(|record| Self::matches(record, filters))
                .map(|record| record.id)
                .collect();
            let mut updated = Vec::new();
            for id in ids {
                let record = records.get_mut(&id).unwrap();
                if let Some(owner_id) = &data.owner_id {
                    record.owner_id = owner_id.clone();
                }
                updated.push(record.clone());
            }
            Ok(updated)
        }

        async fn delete(
            &self,
            id: i32,
            _owner_id: &str,
        ) -> Result<bool, crate::generic::repositories::RepoError> {
            Ok(self.records.lock().unwrap().remove(&id).is_some())
        }

        async fn delete_many(
            &self,
            filters: &[FilterClause],
        ) -> Result<Vec<Self::Model>, crate::generic::repositories::RepoError> {
            let mut records = self.records.lock().unwrap();
            let ids: Vec<i32> = records
                .values()
                .filter(|record| Self::matches(record, filters))
                .map(|record| record.id)
                .collect();
            Ok(ids
                .into_iter()
                .filter_map(|id| records.remove(&id))
                .collect())
        }
    }

    fn service() -> CrudService<FakeGadgetRepository> {
        CrudService::new(FakeGadgetRepository::new())
    }

    fn filter() -> Vec<FilterClause> {
        vec![FilterClause {
            field: "owner_id".to_string(),
            op: FilterOp::Eq,
            value: FilterValue::Str("alice".to_string()),
        }]
    }

    async fn seed(crud: &CrudService<FakeGadgetRepository>, n: usize) {
        for _ in 0..n {
            crud.create("alice", "unused".to_string()).await.unwrap();
        }
    }

    #[tokio::test]
    async fn resolve_delete_by_id_returns_not_found_when_missing() {
        let crud = service();
        let result = resolve_delete(&crud, Some(999), "alice", vec![], 1000).await;
        let err = match result {
            Ok(_) => panic!("deleting a nonexistent id must 404"),
            Err(err) => err,
        };
        assert!(matches!(err, AppError::NotFound(_)));
    }

    #[tokio::test]
    async fn resolve_update_rejects_a_bulk_action_over_the_max_matched_cap() {
        let crud = service();
        seed(&crud, 3).await;
        let result = resolve_update(
            &crud,
            None,
            "alice",
            filter(),
            FakeGadgetUpdate {
                owner_id: Some("bob".to_string()),
            },
            2,
        )
        .await;
        let err = match result {
            Ok(_) => panic!("matching more records than max_matched must be rejected"),
            Err(err) => err,
        };
        assert!(matches!(err, AppError::UnprocessableEntity(_)));
    }

    #[tokio::test]
    async fn resolve_delete_rejects_a_bulk_action_over_the_max_matched_cap() {
        let crud = service();
        seed(&crud, 3).await;
        let result = resolve_delete(&crud, None, "alice", filter(), 2).await;
        let err = match result {
            Ok(_) => panic!("matching more records than max_matched must be rejected"),
            Err(err) => err,
        };
        assert!(matches!(err, AppError::UnprocessableEntity(_)));
    }
}
