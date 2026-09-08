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
