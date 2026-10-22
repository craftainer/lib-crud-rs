//! Shared fixtures for the integration tier: a Postgres-backed `widgets`
//! resource that implements [`Repository`] with plain `sqlx`, so the generic
//! layer is exercised end to end against a real database without an
//! application on top.
//!
//! Each [`PgWidgets::connect`] opens its own single-connection pool and
//! creates a `TEMP` table, so tests run in parallel against one database
//! without seeing each other's rows, and nothing outlives the test.

use async_trait::async_trait;
use chrono::NaiveDateTime;
use craftainer_crud::repositories::filtering::{FilterClause, FilterOp, FilterValue, SortClause};
use craftainer_crud::repositories::{ListOptions, RepoError, Repository};
use sqlx::QueryBuilder;
use sqlx::postgres::{PgPool, PgPoolOptions, Postgres};

/// A stored widget.
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct Widget {
    pub id: i32,
    pub owner_id: String,
    pub name: String,
    pub quantity: i64,
    pub created_at: NaiveDateTime,
    pub archived_at: Option<NaiveDateTime>,
}

impl craftainer_crud::models::HasId for Widget {
    fn id(&self) -> i32 {
        self.id
    }
}

/// Create payload.
#[derive(Debug, Clone)]
pub struct NewWidget {
    pub name: String,
    pub quantity: i64,
}

impl NewWidget {
    pub fn new(name: &str, quantity: i64) -> Self {
        Self {
            name: name.to_string(),
            quantity,
        }
    }
}

/// Update payload (PATCH semantics: `None` leaves the column alone).
#[derive(Debug, Clone, Default)]
pub struct WidgetPatch {
    pub name: Option<String>,
    pub quantity: Option<i64>,
}

/// The columns a filter or sort may name, mapped to their SQL type.
const COLUMNS: &[&str] = &["id", "owner_id", "name", "quantity", "created_at"];

pub struct PgWidgets {
    pool: PgPool,
    fail_bulk_writes: bool,
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn backend(err: &sqlx::Error) -> RepoError {
    RepoError::Backend(err.to_string())
}

impl PgWidgets {
    /// Connect to the devcontainer Postgres (`POSTGRES_*`) and create the
    /// session-local table.
    pub async fn connect() -> Self {
        let url = format!(
            "postgres://{}:{}@{}:{}/{}",
            env_or("POSTGRES_USER", "crud"),
            env_or("POSTGRES_PASSWORD", "crud"),
            env_or("POSTGRES_HOST", "postgres"),
            env_or("POSTGRES_PORT", "5432"),
            env_or("POSTGRES_DB", "crud_test"),
        );
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("the devcontainer stack's Postgres must be running");
        sqlx::query(
            "CREATE TEMP TABLE widgets (
                id SERIAL PRIMARY KEY,
                owner_id TEXT NOT NULL,
                name TEXT NOT NULL,
                quantity BIGINT NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
                archived_at TIMESTAMP
            )",
        )
        .execute(&pool)
        .await
        .expect("create the temp table");
        Self {
            pool,
            fail_bulk_writes: false,
        }
    }

    /// Close the pool, so every later call fails like a lost database.
    pub async fn closed(self) -> Self {
        self.pool.close().await;
        self
    }

    /// Make `update_many`/`delete_many` fail while reads keep working, like
    /// a database that went read-only between the size check and the write.
    pub fn failing_bulk_writes(mut self) -> Self {
        self.fail_bulk_writes = true;
        self
    }
}

fn check_column(field: &str) -> Result<(), RepoError> {
    if COLUMNS.contains(&field) {
        Ok(())
    } else {
        Err(RepoError::Backend(format!("unknown column {field:?}")))
    }
}

fn push_value(builder: &mut QueryBuilder<Postgres>, value: &FilterValue) -> Result<(), RepoError> {
    match value {
        FilterValue::Str(v) => builder.push_bind(v.clone()),
        FilterValue::Int(v) => builder.push_bind(*v),
        FilterValue::Float(v) => builder.push_bind(*v),
        FilterValue::DateTime(v) => builder.push_bind(*v),
        FilterValue::Bool(v) => builder.push_bind(*v),
        FilterValue::List(_) => return Err(RepoError::Backend("nested list".to_string())),
    };
    Ok(())
}

/// Escape `LIKE` wildcards so `contains` means a literal substring.
fn like_pattern(value: &FilterValue) -> String {
    let FilterValue::Str(raw) = value else {
        return String::new();
    };
    let escaped = raw
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_");
    format!("%{escaped}%")
}

fn push_filter(
    builder: &mut QueryBuilder<Postgres>,
    clause: &FilterClause,
) -> Result<(), RepoError> {
    check_column(&clause.field)?;
    builder.push(" AND ").push(clause.field.as_str());
    let comparison = match clause.op {
        FilterOp::Eq => " = ",
        FilterOp::Ne => " <> ",
        FilterOp::Lt => " < ",
        FilterOp::Lte => " <= ",
        FilterOp::Gt => " > ",
        FilterOp::Gte => " >= ",
        FilterOp::In => {
            builder.push(" IN (");
            if let FilterValue::List(values) = &clause.value {
                for (index, value) in values.iter().enumerate() {
                    if index > 0 {
                        builder.push(", ");
                    }
                    push_value(builder, value)?;
                }
            }
            builder.push(")");
            return Ok(());
        }
        FilterOp::Contains => {
            builder
                .push(" LIKE ")
                .push_bind(like_pattern(&clause.value));
            return Ok(());
        }
        FilterOp::Icontains => {
            builder
                .push(" ILIKE ")
                .push_bind(like_pattern(&clause.value));
            return Ok(());
        }
    };
    builder.push(comparison);
    push_value(builder, &clause.value)
}

fn push_filters(
    builder: &mut QueryBuilder<Postgres>,
    filters: &[FilterClause],
) -> Result<(), RepoError> {
    for clause in filters {
        push_filter(builder, clause)?;
    }
    Ok(())
}

fn push_sort(builder: &mut QueryBuilder<Postgres>, sort: &[SortClause]) -> Result<(), RepoError> {
    builder.push(" ORDER BY ");
    for clause in sort {
        check_column(&clause.field)?;
        builder.push(clause.field.as_str());
        builder.push(if clause.descending {
            " DESC, "
        } else {
            " ASC, "
        });
    }
    builder.push("id ASC");
    Ok(())
}

#[async_trait]
impl Repository for PgWidgets {
    type Model = Widget;
    type Create = NewWidget;
    type Update = WidgetPatch;

    async fn list(&self, opts: ListOptions) -> Result<Vec<Widget>, RepoError> {
        let mut builder = QueryBuilder::new("SELECT * FROM widgets WHERE TRUE");
        if !opts.include_archived {
            builder.push(" AND archived_at IS NULL");
        }
        push_filters(&mut builder, &opts.filters)?;
        push_sort(&mut builder, &opts.sort)?;
        builder
            .push(" LIMIT ")
            .push_bind(i64::try_from(opts.limit).unwrap_or(i64::MAX))
            .push(" OFFSET ")
            .push_bind(i64::try_from(opts.skip).unwrap_or(i64::MAX));
        builder
            .build_query_as::<Widget>()
            .fetch_all(&self.pool)
            .await
            .map_err(|err| backend(&err))
    }

    async fn count(
        &self,
        filters: &[FilterClause],
        include_archived: bool,
    ) -> Result<u64, RepoError> {
        let mut builder = QueryBuilder::new("SELECT COUNT(*) FROM widgets WHERE TRUE");
        if !include_archived {
            builder.push(" AND archived_at IS NULL");
        }
        push_filters(&mut builder, filters)?;
        let (count,): (i64,) = builder
            .build_query_as()
            .fetch_one(&self.pool)
            .await
            .map_err(|err| backend(&err))?;
        Ok(u64::try_from(count).unwrap_or(0))
    }

    async fn get(&self, id: i32, include_archived: bool) -> Result<Option<Widget>, RepoError> {
        sqlx::query_as::<_, Widget>(
            "SELECT * FROM widgets WHERE id = $1 AND (archived_at IS NULL OR $2)",
        )
        .bind(id)
        .bind(include_archived)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| backend(&err))
    }

    async fn create(&self, owner_id: &str, data: NewWidget) -> Result<Widget, RepoError> {
        sqlx::query_as::<_, Widget>(
            "INSERT INTO widgets (owner_id, name, quantity) VALUES ($1, $2, $3) RETURNING *",
        )
        .bind(owner_id)
        .bind(data.name)
        .bind(data.quantity)
        .fetch_one(&self.pool)
        .await
        .map_err(|err| backend(&err))
    }

    async fn update(
        &self,
        id: i32,
        owner_id: &str,
        data: WidgetPatch,
    ) -> Result<Option<Widget>, RepoError> {
        sqlx::query_as::<_, Widget>(
            "UPDATE widgets SET name = COALESCE($1, name), quantity = COALESCE($2, quantity) \
             WHERE id = $3 AND owner_id = $4 AND archived_at IS NULL RETURNING *",
        )
        .bind(data.name)
        .bind(data.quantity)
        .bind(id)
        .bind(owner_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| backend(&err))
    }

    async fn update_many(
        &self,
        filters: &[FilterClause],
        data: WidgetPatch,
    ) -> Result<Vec<Widget>, RepoError> {
        if self.fail_bulk_writes {
            return Err(RepoError::Backend("read-only".to_string()));
        }
        let mut builder = QueryBuilder::new("UPDATE widgets SET name = COALESCE(");
        builder
            .push_bind(data.name)
            .push(", name), quantity = COALESCE(")
            .push_bind(data.quantity)
            .push(", quantity) WHERE archived_at IS NULL");
        push_filters(&mut builder, filters)?;
        builder.push(" RETURNING *");
        builder
            .build_query_as::<Widget>()
            .fetch_all(&self.pool)
            .await
            .map_err(|err| backend(&err))
    }

    async fn delete(&self, id: i32, owner_id: &str) -> Result<bool, RepoError> {
        let result = sqlx::query(
            "UPDATE widgets SET archived_at = now() AT TIME ZONE 'utc' \
             WHERE id = $1 AND owner_id = $2 AND archived_at IS NULL",
        )
        .bind(id)
        .bind(owner_id)
        .execute(&self.pool)
        .await
        .map_err(|err| backend(&err))?;
        Ok(result.rows_affected() > 0)
    }

    async fn delete_many(&self, filters: &[FilterClause]) -> Result<Vec<Widget>, RepoError> {
        if self.fail_bulk_writes {
            return Err(RepoError::Backend("read-only".to_string()));
        }
        let mut builder = QueryBuilder::new(
            "UPDATE widgets SET archived_at = now() AT TIME ZONE 'utc' WHERE archived_at IS NULL",
        );
        push_filters(&mut builder, filters)?;
        builder.push(" RETURNING *");
        builder
            .build_query_as::<Widget>()
            .fetch_all(&self.pool)
            .await
            .map_err(|err| backend(&err))
    }
}
