//! The generic half of storage-agnostic CRUD access, backing `crud`. See
//! `docs/adrs/0001-mvc-layering-with-a-generic-crud-interface.md` for why
//! this layer exists as a trait with two implementations per resource
//! (SeaORM-backed, in-memory) rather than one hand-written per-resource
//! data-access class. Hero's own implementations live in
//! `crate::hero::repositories`.
//!
//! Rust-specific deviation from the Python original's `Repository`
//! protocol (documented in that ADR's port): Python's `SQLAlchemyRepository`
//! is generic over *any* mapped model at runtime, because SQLAlchemy's
//! `Base.registry` and Python's dynamic attribute access let one class
//! introspect an arbitrary model's columns. SeaORM's `EntityTrait` is
//! statically generated per entity, so a single Rust `impl` cannot be
//! generic over "any SeaORM entity" without unwieldy trait-bound
//! machinery. Instead, `Repository` is generic via **associated types**
//! (read model `Model`, create payload `Create`, update payload `Update`),
//! and each resource provides one small `impl` per backend (SeaORM +
//! in-memory) that maps those types onto its own entity's columns.
//! `crud::CrudService<R>` itself stays fully generic and contains zero
//! resource-specific code -- the same payoff `NFR-0004`
//! (`docs/nfrs/0004-generic-crud-excludes-resource-logic.md`) describes,
//! just with the entity-specific column mapping living one layer lower
//! than in the Python original.

pub mod filtering;

use async_trait::async_trait;

use filtering::{FilterClause, SortClause};

/// Pagination/visibility/filter/sort options shared by every resource's
/// `list` -- `filters`/`sort` added by `docs/adrs/0013` (Tier C item 3).
#[derive(Debug, Clone, Default)]
pub struct ListOptions {
    pub skip: u64,
    pub limit: u64,
    /// Include soft-deleted (`archived_at IS NOT NULL`) rows -- ADR 0012.
    pub include_archived: bool,
    pub filters: Vec<FilterClause>,
    pub sort: Vec<SortClause>,
}

/// A repository-layer failure. Deliberately narrow: the CRUD service and
/// controllers only need to distinguish "not found"/"forbidden by
/// ownership" (both surfaced as `None`/`false` return values, not this
/// error) from "the backend itself failed".
#[derive(Debug, thiserror::Error)]
pub enum RepoError {
    #[error("backend error: {0}")]
    Backend(String),
}

/// Storage-agnostic CRUD access for one resource. Every mutating method
/// takes `owner_id`: reads are open (Hero's `OwnerScope(read_scoped=
/// false)`, ADR 0011), writes are always scoped to the caller.
#[async_trait]
pub trait Repository: Send + Sync {
    type Model: Send + Sync;
    type Create: Send + Sync;
    type Update: Send + Sync;

    async fn list(&self, opts: ListOptions) -> Result<Vec<Self::Model>, RepoError>;
    /// How many non-archived (unless `include_archived`) rows match
    /// `filters` -- used to cap a bulk update/delete before it runs
    /// (`docs/adrs/0013`), same role as `count()` in the reference.
    async fn count(
        &self,
        filters: &[FilterClause],
        include_archived: bool,
    ) -> Result<u64, RepoError>;
    async fn get(&self, id: i32, include_archived: bool) -> Result<Option<Self::Model>, RepoError>;
    async fn create(&self, owner_id: &str, data: Self::Create) -> Result<Self::Model, RepoError>;
    /// `None` if no row matched `id` (not found or not owned by `owner_id`).
    async fn update(
        &self,
        id: i32,
        owner_id: &str,
        data: Self::Update,
    ) -> Result<Option<Self::Model>, RepoError>;
    /// Apply `data` to every non-archived row matching `filters`; returns
    /// the updated rows.
    async fn update_many(
        &self,
        filters: &[FilterClause],
        data: Self::Update,
    ) -> Result<Vec<Self::Model>, RepoError>;
    /// `false` if no row matched `id` (not found, not owned, or already
    /// archived).
    async fn delete(&self, id: i32, owner_id: &str) -> Result<bool, RepoError>;
    /// Soft-delete every non-archived row matching `filters`; returns the
    /// rows that were deleted (post-delete state, matching the
    /// reference's `delete_many`).
    async fn delete_many(&self, filters: &[FilterClause]) -> Result<Vec<Self::Model>, RepoError>;
}

/// A macro (not a generic blanket `impl`, which trips an async-trait/HRTB
/// limitation -- `Box<dyn Repository<...> + '_>` ends up "not general
/// enough" wherever the resulting type is used through another generic
/// bound) that defines a concrete, boxed-trait-object-backed `Repository`
/// for one resource. Lets a resource's own `AppState` (e.g.
/// `crate::hero::controllers::AppState`) hold one `CrudService<
/// DynXRepository>` field whose concrete backend (SeaORM vs. in-memory)
/// was picked once at startup based on `Mode`, instead of a
/// generic-over-backend `AppState` (which axum's `State` extractor can't
/// express) or two parallel router trees.
#[macro_export]
macro_rules! dyn_repository {
    ($name:ident, model = $model:ty, create = $create:ty, update = $update:ty) => {
        pub struct $name(
            pub  Box<
                dyn $crate::generic::repositories::Repository<
                    Model = $model,
                    Create = $create,
                    Update = $update,
                >,
            >,
        );

        #[async_trait::async_trait]
        impl $crate::generic::repositories::Repository for $name {
            type Model = $model;
            type Create = $create;
            type Update = $update;

            async fn list(
                &self,
                opts: $crate::generic::repositories::ListOptions,
            ) -> Result<Vec<$model>, $crate::generic::repositories::RepoError> {
                self.0.list(opts).await
            }

            async fn count(
                &self,
                filters: &[$crate::generic::repositories::filtering::FilterClause],
                include_archived: bool,
            ) -> Result<u64, $crate::generic::repositories::RepoError> {
                self.0.count(filters, include_archived).await
            }

            async fn get(
                &self,
                id: i32,
                include_archived: bool,
            ) -> Result<Option<$model>, $crate::generic::repositories::RepoError> {
                self.0.get(id, include_archived).await
            }

            async fn create(
                &self,
                owner_id: &str,
                data: $create,
            ) -> Result<$model, $crate::generic::repositories::RepoError> {
                self.0.create(owner_id, data).await
            }

            async fn update(
                &self,
                id: i32,
                owner_id: &str,
                data: $update,
            ) -> Result<Option<$model>, $crate::generic::repositories::RepoError> {
                self.0.update(id, owner_id, data).await
            }

            async fn update_many(
                &self,
                filters: &[$crate::generic::repositories::filtering::FilterClause],
                data: $update,
            ) -> Result<Vec<$model>, $crate::generic::repositories::RepoError> {
                self.0.update_many(filters, data).await
            }

            async fn delete(
                &self,
                id: i32,
                owner_id: &str,
            ) -> Result<bool, $crate::generic::repositories::RepoError> {
                self.0.delete(id, owner_id).await
            }

            async fn delete_many(
                &self,
                filters: &[$crate::generic::repositories::filtering::FilterClause],
            ) -> Result<Vec<$model>, $crate::generic::repositories::RepoError> {
                self.0.delete_many(filters).await
            }
        }
    };
}
