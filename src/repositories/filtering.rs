//! Storage-agnostic filter/sort vocabulary shared by every `Repository`
//! implementation -- port of `repositories/filtering.py`. Plain value
//! objects only: each concrete repository interprets `FilterClause`/
//! `SortClause` itself (`hero_sea_orm.rs` maps a clause onto a SeaORM
//! `Column`/`Condition`; `hero_memory.rs` applies it directly against a
//! `hero::Model` field), the same way every other `Repository` method
//! already works.
//!
//! Deviation from the Python original, documented rather than silently
//! dropped: `FilterOp::Regex` isn't ported. The reference's own comment on
//! `field__regex=` calls out that an unbounded pattern is a ReDoS vector
//! and leans on a runtime evaluation budget (`InMemoryRepository`'s alarm
//! timeout, `SQLAlchemyRepository`'s per-transaction `statement_timeout`)
//! to bound it -- that budget infrastructure is out of scope for this
//! item, so rather than ship a regex operator with no such bound, this
//! port omits it. Likewise `FilterOp` has no equivalent of the reference's
//! Enum/Literal field kind (`docs/controllers/crud_query.py`'s
//! `FieldKind.ENUM`): Hero, the only resource this port has, has no
//! Enum/Literal field, and deriving one generically without Rust macros
//! (there's no runtime schema reflection the way Pydantic gives Python)
//! isn't worth building against zero call sites.

use chrono::NaiveDateTime;

/// A comparison a `FilterClause` applies to one field.
///
/// `Ne`/`Lt`/`Gt` are never constructed by `controllers::crud_query`'s
/// wire-format parser -- like the reference's own `FilterOp`, this enum is
/// the complete storage-agnostic vocabulary each `Repository` impl
/// interprets, not only the subset the current query-string suffix table
/// (`eq`/`min`/`max`/`in`/`contains`/`icontains`) happens to expose.
/// `#[allow(dead_code)]` on those three variants is that gap, not a claim
/// they're reachable through the HTTP layer today.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterOp {
    Eq,
    #[allow(dead_code)]
    Ne,
    #[allow(dead_code)]
    Lt,
    Lte,
    #[allow(dead_code)]
    Gt,
    Gte,
    In,
    Contains,
    Icontains,
}

/// One field's parsed filter value -- the Rust equivalent of Python's
/// dynamically-typed `Any`, since a `FilterClause` must carry values of
/// different concrete types depending on which field it targets.
#[derive(Debug, Clone, PartialEq)]
pub enum FilterValue {
    Str(String),
    Int(i64),
    DateTime(NaiveDateTime),
    Bool(bool),
    /// `FilterOp::In`'s membership list -- always a list of one of the
    /// scalar variants above, never nested.
    List(Vec<FilterValue>),
}

/// One field/operator/value comparison to apply when listing or
/// bulk-targeting records.
#[derive(Debug, Clone, PartialEq)]
pub struct FilterClause {
    pub field: String,
    pub op: FilterOp,
    pub value: FilterValue,
}

/// One field to sort by, ascending unless `descending` is set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortClause {
    pub field: String,
    pub descending: bool,
}
