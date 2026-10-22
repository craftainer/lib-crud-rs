//! The generic half of storage-agnostic CRUD access, backing
//! [`CrudService`](crate::crud::CrudService).
//!
//! [`Repository`] is generic through **associated types** (read model
//! `Model`, create payload `Create`, update payload `Update`) rather than
//! being generic over "any table": each resource provides one small
//! implementation per backend that maps those types onto its own storage.
//! `CrudService<R>` itself then stays free of resource-specific code.

pub mod filtering;

use async_trait::async_trait;

use filtering::{FilterClause, SortClause};

/// Pagination, visibility, filter and sort options shared by every
/// resource's `list`.
#[derive(Debug, Clone, Default)]
pub struct ListOptions {
    /// Records to skip.
    pub skip: u64,
    /// Maximum records to return.
    pub limit: u64,
    /// Include soft-deleted (archived) rows.
    pub include_archived: bool,
    /// Conditions every returned record satisfies.
    pub filters: Vec<FilterClause>,
    /// Sort order, most significant first.
    pub sort: Vec<SortClause>,
}

/// A repository-layer failure. Deliberately narrow: the CRUD service and
/// controllers only need to distinguish "not found"/"forbidden by
/// ownership" (both surfaced as `None`/`false` return values, not this
/// error) from "the backend itself failed".
#[derive(Debug, thiserror::Error)]
pub enum RepoError {
    /// The storage backend failed.
    #[error("backend error: {0}")]
    Backend(String),
}

/// Storage-agnostic CRUD access for one resource. Every mutating method
/// takes `owner_id`: reads are open, writes are always scoped to the caller.
///
/// # The `owner_id` filter contract
///
/// `update` and `delete` receive the owner as an argument. `update_many` and
/// `delete_many` receive none: [`CrudService`](crate::crud::CrudService)
/// appends one extra clause to the caller's filters instead,
///
/// ```text
/// FilterClause { field: "owner_id", op: FilterOp::Eq, value: FilterValue::Str(owner) }
/// ```
///
/// and the same clause is added to the `filters` of the `count` that sizes a
/// bulk action. An implementation therefore must:
///
/// - treat a clause whose `field` is `"owner_id"` as a comparison against the
///   column or attribute that holds the record's owner, whatever it is
///   called in storage (the name `owner_id` is the filter vocabulary, not a
///   schema requirement);
/// - apply it like any other clause, and never drop, reorder past or
///   special-case it: an implementation that ignores an unknown field would
///   let a bulk action touch every owner's records;
/// - fail with [`RepoError::Backend`] rather than ignore the clause when the
///   resource has no owner column.
///
/// A resource that wants its owner column to be a query filter lists it in
/// its [`FieldSpec`](crate::controllers::crud_query::FieldSpec) table;
/// otherwise the only `owner_id` clause a repository sees is the one the
/// service appends.
///
/// # Bulk actions are not atomic with their cap
///
/// The cap on a bulk action is checked with `count` before `update_many` or
/// `delete_many` runs (see
/// [`crud_actions`](crate::controllers::crud_actions)). This trait has no
/// "at most N rows" write, so the two steps are separate and the cap can be
/// exceeded by a concurrent insert. An implementation that must enforce a
/// hard limit does so itself, inside its write.
#[async_trait]
pub trait Repository: Send + Sync {
    /// The read model a record is returned as.
    type Model: Send + Sync;
    /// The payload that creates a record.
    type Create: Send + Sync;
    /// The (partial) payload that updates a record.
    type Update: Send + Sync;

    /// One page of records matching `opts`.
    ///
    /// # Errors
    ///
    /// [`RepoError`] when the backend fails.
    async fn list(&self, opts: ListOptions) -> Result<Vec<Self::Model>, RepoError>;

    /// How many non-archived (unless `include_archived`) rows match
    /// `filters`; used to cap a bulk update/delete before it runs. `filters`
    /// carry the `owner_id` clause described on [`Repository`].
    ///
    /// # Errors
    ///
    /// [`RepoError`] when the backend fails.
    async fn count(
        &self,
        filters: &[FilterClause],
        include_archived: bool,
    ) -> Result<u64, RepoError>;

    /// Record `id`, or `None` when it does not exist.
    ///
    /// # Errors
    ///
    /// [`RepoError`] when the backend fails.
    async fn get(&self, id: i32, include_archived: bool) -> Result<Option<Self::Model>, RepoError>;

    /// Create a record owned by `owner_id`.
    ///
    /// # Errors
    ///
    /// [`RepoError`] when the backend fails.
    async fn create(&self, owner_id: &str, data: Self::Create) -> Result<Self::Model, RepoError>;

    /// Update record `id`; `None` if no row matched (not found or not owned
    /// by `owner_id`).
    ///
    /// # Errors
    ///
    /// [`RepoError`] when the backend fails.
    async fn update(
        &self,
        id: i32,
        owner_id: &str,
        data: Self::Update,
    ) -> Result<Option<Self::Model>, RepoError>;

    /// Apply `data` to every non-archived row matching `filters`; returns
    /// the updated rows.
    ///
    /// # Errors
    ///
    /// [`RepoError`] when the backend fails.
    async fn update_many(
        &self,
        filters: &[FilterClause],
        data: Self::Update,
    ) -> Result<Vec<Self::Model>, RepoError>;

    /// Soft-delete record `id`; `false` if no row matched (not found, not
    /// owned, or already archived).
    ///
    /// # Errors
    ///
    /// [`RepoError`] when the backend fails.
    async fn delete(&self, id: i32, owner_id: &str) -> Result<bool, RepoError>;

    /// Soft-delete every non-archived row matching `filters`; returns the
    /// rows that were deleted (post-delete state).
    ///
    /// # Errors
    ///
    /// [`RepoError`] when the backend fails.
    async fn delete_many(&self, filters: &[FilterClause]) -> Result<Vec<Self::Model>, RepoError>;
}

/// A macro (not a generic blanket `impl`, which trips an async-trait/HRTB
/// limitation -- `Box<dyn Repository<...> + '_>` ends up "not general
/// enough" wherever the resulting type is used through another generic
/// bound) that defines a concrete, boxed-trait-object-backed `Repository`
/// for one resource. Lets the host's application state hold one
/// `CrudService<DynXRepository>` field whose concrete backend (SQL vs.
/// in-memory) was picked once at startup, instead of a
/// generic-over-backend state type (which axum's `State` extractor can't
/// express) or two parallel router trees.
#[macro_export]
macro_rules! dyn_repository {
    ($name:ident, model = $model:ty, create = $create:ty, update = $update:ty) => {
        /// A boxed, type-erased `Repository`, generated by `dyn_repository!`.
        pub struct $name(
            pub  Box<
                dyn $crate::repositories::Repository<
                        Model = $model,
                        Create = $create,
                        Update = $update,
                    >,
            >,
        );

        #[$crate::async_trait::async_trait]
        impl $crate::repositories::Repository for $name {
            type Model = $model;
            type Create = $create;
            type Update = $update;

            async fn list(
                &self,
                opts: $crate::repositories::ListOptions,
            ) -> Result<Vec<$model>, $crate::repositories::RepoError> {
                self.0.list(opts).await
            }

            async fn count(
                &self,
                filters: &[$crate::repositories::filtering::FilterClause],
                include_archived: bool,
            ) -> Result<u64, $crate::repositories::RepoError> {
                self.0.count(filters, include_archived).await
            }

            async fn get(
                &self,
                id: i32,
                include_archived: bool,
            ) -> Result<Option<$model>, $crate::repositories::RepoError> {
                self.0.get(id, include_archived).await
            }

            async fn create(
                &self,
                owner_id: &str,
                data: $create,
            ) -> Result<$model, $crate::repositories::RepoError> {
                self.0.create(owner_id, data).await
            }

            async fn update(
                &self,
                id: i32,
                owner_id: &str,
                data: $update,
            ) -> Result<Option<$model>, $crate::repositories::RepoError> {
                self.0.update(id, owner_id, data).await
            }

            async fn update_many(
                &self,
                filters: &[$crate::repositories::filtering::FilterClause],
                data: $update,
            ) -> Result<Vec<$model>, $crate::repositories::RepoError> {
                self.0.update_many(filters, data).await
            }

            async fn delete(
                &self,
                id: i32,
                owner_id: &str,
            ) -> Result<bool, $crate::repositories::RepoError> {
                self.0.delete(id, owner_id).await
            }

            async fn delete_many(
                &self,
                filters: &[$crate::repositories::filtering::FilterClause],
            ) -> Result<Vec<$model>, $crate::repositories::RepoError> {
                self.0.delete_many(filters).await
            }
        }
    };
}
