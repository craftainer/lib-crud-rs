//! The generic CRUD interface: a thin, fully generic service built from a
//! `repositories::Repository`. Port of `interfaces/base.py`'s
//! `CRUDInterface` -- see `docs/adrs/0001` for why this abstraction is
//! deliberately chosen over "three similar lines is better than a
//! premature abstraction" (this repo's own equivalent of that CLAUDE.md
//! rule): a template's job is to make the *next* resource cheap, not just
//! to demonstrate one. `CrudService<R>` has zero Hero-specific code -- it
//! only knows the generic parameters `M`/`C`/`U` its `Repository` impl
//! carries (`docs/nfrs/0004-generic-crud-excludes-resource-logic.md`).

use crate::generic::repositories::filtering::{FilterClause, FilterOp, FilterValue, SortClause};
use crate::generic::repositories::{ListOptions, RepoError, Repository};

/// The default `?limit=` when a caller supplies none.
pub const DEFAULT_LIMIT: u64 = 100;
/// The maximum `?limit=` a caller may request -- mirrors `crud_router.py`'s
/// `_MAX_LIMIT`.
pub const MAX_LIMIT: u64 = 1000;

pub struct CrudService<R> {
    repository: R,
}

impl<R> CrudService<R>
where
    R: Repository,
{
    pub fn new(repository: R) -> Self {
        Self { repository }
    }

    pub async fn list(
        &self,
        skip: u64,
        limit: u64,
        include_archived: bool,
        filters: Vec<FilterClause>,
        sort: Vec<SortClause>,
    ) -> Result<Vec<R::Model>, RepoError> {
        let limit = limit.clamp(1, MAX_LIMIT);
        self.repository
            .list(ListOptions {
                skip,
                limit,
                include_archived,
                filters,
                sort,
            })
            .await
    }

    /// How many non-archived (unless `include_archived`) records match
    /// `filters` -- callers use this to cap a bulk update/delete before it
    /// runs (`docs/adrs/0013`), matching `_check_bulk_action_size` in the
    /// reference.
    pub async fn count(
        &self,
        filters: &[FilterClause],
        include_archived: bool,
    ) -> Result<u64, RepoError> {
        self.repository.count(filters, include_archived).await
    }

    pub async fn get(
        &self,
        id: i32,
        include_archived: bool,
    ) -> Result<Option<R::Model>, RepoError> {
        self.repository.get(id, include_archived).await
    }

    pub async fn create(&self, owner_id: &str, data: R::Create) -> Result<R::Model, RepoError> {
        self.repository.create(owner_id, data).await
    }

    pub async fn update(
        &self,
        id: i32,
        owner_id: &str,
        data: R::Update,
    ) -> Result<Option<R::Model>, RepoError> {
        self.repository.update(id, owner_id, data).await
    }

    /// Apply `data` to every non-archived record matching `filters`,
    /// scoped to `owner_id` -- mirrors `update`'s ownership scoping
    /// (ADR 0007/0011) by adding an `owner_id` equality clause before
    /// delegating, rather than trusting the caller-supplied filters to
    /// include one.
    pub async fn update_many(
        &self,
        owner_id: &str,
        filters: Vec<FilterClause>,
        data: R::Update,
    ) -> Result<Vec<R::Model>, RepoError> {
        self.repository
            .update_many(&owner_scoped(owner_id, filters), data)
            .await
    }

    pub async fn delete(&self, id: i32, owner_id: &str) -> Result<bool, RepoError> {
        self.repository.delete(id, owner_id).await
    }

    /// Soft-delete every non-archived record matching `filters`, scoped to
    /// `owner_id` -- same ownership reasoning as `update_many` above.
    pub async fn delete_many(
        &self,
        owner_id: &str,
        filters: Vec<FilterClause>,
    ) -> Result<Vec<R::Model>, RepoError> {
        self.repository
            .delete_many(&owner_scoped(owner_id, filters))
            .await
    }
}

fn owner_scoped(owner_id: &str, mut filters: Vec<FilterClause>) -> Vec<FilterClause> {
    filters.push(FilterClause {
        field: "owner_id".to_string(),
        op: FilterOp::Eq,
        value: FilterValue::Str(owner_id.to_string()),
    });
    filters
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::Mutex;

    /// A minimal `Repository` fake, independent of any real resource --
    /// `CrudService<R>` has zero resource-specific code (NFR-0004), so its
    /// own tests shouldn't need a real one either. Records the last
    /// `ListOptions` it was called with so `list()`'s limit-clamping can be
    /// asserted directly.
    #[derive(Default)]
    struct FakeRepository {
        last_list_opts: Mutex<Option<ListOptions>>,
        last_count_filters: Mutex<Option<Vec<FilterClause>>>,
        last_update_many_filters: Mutex<Option<Vec<FilterClause>>>,
        last_delete_many_filters: Mutex<Option<Vec<FilterClause>>>,
    }

    #[async_trait]
    impl Repository for FakeRepository {
        type Model = i32;
        type Create = i32;
        type Update = i32;

        async fn list(&self, opts: ListOptions) -> Result<Vec<i32>, RepoError> {
            *self.last_list_opts.lock().unwrap() = Some(opts);
            Ok(vec![])
        }

        async fn count(
            &self,
            filters: &[FilterClause],
            _include_archived: bool,
        ) -> Result<u64, RepoError> {
            *self.last_count_filters.lock().unwrap() = Some(filters.to_vec());
            Ok(filters.len() as u64)
        }

        async fn get(&self, id: i32, _include_archived: bool) -> Result<Option<i32>, RepoError> {
            Ok(Some(id))
        }

        async fn create(&self, _owner_id: &str, data: i32) -> Result<i32, RepoError> {
            Ok(data)
        }

        async fn update(
            &self,
            id: i32,
            _owner_id: &str,
            _data: i32,
        ) -> Result<Option<i32>, RepoError> {
            Ok(Some(id))
        }

        async fn update_many(
            &self,
            filters: &[FilterClause],
            data: i32,
        ) -> Result<Vec<i32>, RepoError> {
            *self.last_update_many_filters.lock().unwrap() = Some(filters.to_vec());
            Ok(vec![data])
        }

        async fn delete(&self, _id: i32, _owner_id: &str) -> Result<bool, RepoError> {
            Ok(true)
        }

        async fn delete_many(&self, filters: &[FilterClause]) -> Result<Vec<i32>, RepoError> {
            *self.last_delete_many_filters.lock().unwrap() = Some(filters.to_vec());
            Ok(vec![1])
        }
    }

    #[tokio::test]
    async fn list_clamps_a_zero_limit_up_to_one() {
        let service = CrudService::new(FakeRepository::default());
        service.list(0, 0, false, vec![], vec![]).await.unwrap();
        let opts = service
            .repository
            .last_list_opts
            .lock()
            .unwrap()
            .clone()
            .unwrap();
        assert_eq!(opts.limit, 1);
    }

    #[tokio::test]
    async fn list_clamps_a_limit_above_max_down_to_max() {
        let service = CrudService::new(FakeRepository::default());
        service
            .list(0, 10_000, false, vec![], vec![])
            .await
            .unwrap();
        let opts = service
            .repository
            .last_list_opts
            .lock()
            .unwrap()
            .clone()
            .unwrap();
        assert_eq!(opts.limit, MAX_LIMIT);
    }

    #[tokio::test]
    async fn list_passes_an_in_range_limit_through_unchanged() {
        let service = CrudService::new(FakeRepository::default());
        service.list(5, 50, true, vec![], vec![]).await.unwrap();
        let opts = service
            .repository
            .last_list_opts
            .lock()
            .unwrap()
            .clone()
            .unwrap();
        assert_eq!(opts.limit, 50);
        assert_eq!(opts.skip, 5);
        assert!(opts.include_archived);
    }

    #[tokio::test]
    async fn list_passes_filters_and_sort_through_unchanged() {
        let service = CrudService::new(FakeRepository::default());
        let filters = vec![FilterClause {
            field: "name".to_string(),
            op: FilterOp::Eq,
            value: FilterValue::Str("Spectra".to_string()),
        }];
        let sort = vec![SortClause {
            field: "id".to_string(),
            descending: true,
        }];
        service
            .list(0, 10, false, filters.clone(), sort.clone())
            .await
            .unwrap();
        let opts = service
            .repository
            .last_list_opts
            .lock()
            .unwrap()
            .clone()
            .unwrap();
        assert_eq!(opts.filters, filters);
        assert_eq!(opts.sort, sort);
    }

    #[tokio::test]
    async fn get_create_update_delete_delegate_to_the_repository() {
        let service = CrudService::new(FakeRepository::default());
        assert_eq!(service.get(1, false).await.unwrap(), Some(1));
        assert_eq!(service.create("owner", 9).await.unwrap(), 9);
        assert_eq!(service.update(2, "owner", 3).await.unwrap(), Some(2));
        assert!(service.delete(1, "owner").await.unwrap());
    }

    #[tokio::test]
    async fn count_delegates_to_the_repository() {
        let service = CrudService::new(FakeRepository::default());
        let filters = vec![FilterClause {
            field: "id".to_string(),
            op: FilterOp::Gte,
            value: FilterValue::Int(0),
        }];
        assert_eq!(service.count(&filters, false).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn update_many_and_delete_many_append_an_owner_id_filter() {
        let service = CrudService::new(FakeRepository::default());
        service.update_many("alice", vec![], 7).await.unwrap();
        let filters = service
            .repository
            .last_update_many_filters
            .lock()
            .unwrap()
            .clone()
            .unwrap();
        assert_eq!(
            filters,
            vec![FilterClause {
                field: "owner_id".to_string(),
                op: FilterOp::Eq,
                value: FilterValue::Str("alice".to_string()),
            }]
        );

        service.delete_many("bob", vec![]).await.unwrap();
        let filters = service
            .repository
            .last_delete_many_filters
            .lock()
            .unwrap()
            .clone()
            .unwrap();
        assert_eq!(
            filters,
            vec![FilterClause {
                field: "owner_id".to_string(),
                op: FilterOp::Eq,
                value: FilterValue::Str("bob".to_string()),
            }]
        );
    }
}
