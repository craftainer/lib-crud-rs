//! Storage-agnostic filter/sort vocabulary shared by every
//! [`Repository`](super::Repository) implementation. Plain value objects
//! only: each concrete repository interprets a [`FilterClause`] or
//! [`SortClause`] itself (a SQL repository maps it onto a column and a
//! predicate, an in-memory one applies it to a field).
//!
//! Two things are deliberately absent. There is no
//! `Regex` operator: an unbounded pattern is a `ReDoS` vector and this crate
//! has no evaluation budget to bound it. There is no enum field kind:
//! without runtime schema reflection a generic one is not worth building
//! until a resource needs it.

use chrono::NaiveDateTime;

/// A comparison a `FilterClause` applies to one field.
///
/// `Ne`/`Lt`/`Gt` are never constructed by
/// [`parse_filters`](crate::controllers::crud_query::parse_filters): this
/// enum is the complete vocabulary a repository interprets, not only the
/// subset the query-string suffix table (`eq`/`min`/`max`/`in`/`contains`/
/// `icontains`) exposes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterOp {
    /// Equal to.
    Eq,
    /// Not equal to.
    Ne,
    /// Less than.
    Lt,
    /// Less than or equal to.
    Lte,
    /// Greater than.
    Gt,
    /// Greater than or equal to.
    Gte,
    /// Member of a [`FilterValue::List`].
    In,
    /// Substring match, case-sensitive.
    Contains,
    /// Substring match, case-insensitive.
    Icontains,
}

/// One field's parsed filter value -- a `FilterClause` must carry
/// values of different concrete types depending on which field it targets.
#[derive(Debug, Clone, PartialEq)]
pub enum FilterValue {
    /// Text.
    Str(String),
    /// An integer.
    Int(i64),
    /// A timestamp.
    DateTime(NaiveDateTime),
    /// A boolean.
    Bool(bool),
    /// `FilterOp::In`'s membership list -- always a list of one of the
    /// scalar variants above, never nested.
    List(Vec<FilterValue>),
}

/// One field/operator/value comparison to apply when listing or
/// bulk-targeting records.
#[derive(Debug, Clone, PartialEq)]
pub struct FilterClause {
    /// The field compared.
    pub field: String,
    /// The comparison.
    pub op: FilterOp,
    /// The value compared against.
    pub value: FilterValue,
}

/// One field to sort by, ascending unless `descending` is set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortClause {
    /// The field sorted by.
    pub field: String,
    /// Sort descending instead of ascending.
    pub descending: bool,
}
