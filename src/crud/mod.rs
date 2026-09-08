//! The generic CRUD interface: a thin, fully generic service built from a
//! `repositories::Repository`. Port of `interfaces/base.py`'s
//! `CRUDInterface` -- see `docs/adrs/0001` for why this abstraction is
//! deliberately chosen over "three similar lines is better than a
//! premature abstraction" (this repo's own equivalent of that CLAUDE.md
//! rule): a template's job is to make the *next* resource cheap, not just
//! to demonstrate one. `CrudService<R>` has zero Hero-specific code -- it
//! only knows the generic parameters `M`/`C`/`U` its `Repository` impl
//! carries (`docs/nfrs/0004-generic-crud-excludes-resource-logic.md`).

use crate::repositories::{ListOptions, RepoError, Repository};

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
    ) -> Result<Vec<R::Model>, RepoError> {
        let limit = limit.clamp(1, MAX_LIMIT);
        self.repository
            .list(ListOptions {
                skip,
                limit,
                include_archived,
            })
            .await
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

    pub async fn delete(&self, id: i32, owner_id: &str) -> Result<bool, RepoError> {
        self.repository.delete(id, owner_id).await
    }
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

        async fn delete(&self, _id: i32, _owner_id: &str) -> Result<bool, RepoError> {
            Ok(true)
        }
    }

    #[tokio::test]
    async fn list_clamps_a_zero_limit_up_to_one() {
        let service = CrudService::new(FakeRepository::default());
        service.list(0, 0, false).await.unwrap();
        let opts = service.repository.last_list_opts.lock().unwrap().unwrap();
        assert_eq!(opts.limit, 1);
    }

    #[tokio::test]
    async fn list_clamps_a_limit_above_max_down_to_max() {
        let service = CrudService::new(FakeRepository::default());
        service.list(0, 10_000, false).await.unwrap();
        let opts = service.repository.last_list_opts.lock().unwrap().unwrap();
        assert_eq!(opts.limit, MAX_LIMIT);
    }

    #[tokio::test]
    async fn list_passes_an_in_range_limit_through_unchanged() {
        let service = CrudService::new(FakeRepository::default());
        service.list(5, 50, true).await.unwrap();
        let opts = service.repository.last_list_opts.lock().unwrap().unwrap();
        assert_eq!(opts.limit, 50);
        assert_eq!(opts.skip, 5);
        assert!(opts.include_archived);
    }

    #[tokio::test]
    async fn get_create_update_delete_delegate_to_the_repository() {
        let service = CrudService::new(FakeRepository::default());
        assert_eq!(service.get(1, false).await.unwrap(), Some(1));
        assert_eq!(service.create("owner", 9).await.unwrap(), 9);
        assert_eq!(service.update(2, "owner", 3).await.unwrap(), Some(2));
        assert!(service.delete(1, "owner").await.unwrap());
    }
}
