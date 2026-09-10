//! Query-string parsing for the generic CRUD router -- port of
//! `controllers/crud_query.py`. `FieldSpec`/`parse_filters`/`parse_sort`
//! are fully generic over any resource's field table; only the table
//! itself (e.g. `heroes::HERO_FIELD_SPECS`) is resource-specific, the same
//! division the reference draws between this module (generic) and each
//! resource's own Pydantic schema (resource-specific).
//!
//! Wire format matches the reference: `field=value` is an equality match;
//! `field__min=`/`field__max=` express a range; `field__in=a,b,c` is
//! membership; `field__contains=`/`field__icontains=` are string
//! operators. `sort=field,-other_field` is comma-separated, a leading `-`
//! meaning descending.
//!
//! Deviations from the reference, documented rather than silently
//! dropped:
//! - No `FilterOp::Regex`/`field__regex=` -- see
//!   `repositories/filtering.rs`'s module doc for why.
//! - No Enum/Literal field kind -- Hero, the only resource this port has,
//!   has none, and there's no Pydantic-style runtime schema reflection in
//!   Rust to derive one generically from.
//! - Every numeric-kind field here is either a bare integer or a
//!   datetime, so `FieldKind` splits `Number`/`DateTime` rather than the
//!   reference's single "number" kind dispatching on a captured Python
//!   type at cast time.

use std::collections::HashMap;

use chrono::{NaiveDateTime, Utc};

use crate::generic::repositories::filtering::{FilterClause, FilterOp, FilterValue, SortClause};
use crate::generic::views::FieldError;

/// Query params never treated as a filter/sort key, regardless of a
/// resource's own field table.
const RESERVED_PARAMS: &[&str] = &["id", "skip", "limit", "sort", "include_archived"];

/// What "kind" a filterable field is, driving which operators it accepts
/// and how a raw query-string value is cast.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldKind {
    Number,
    DateTime,
    String,
    Boolean,
}

/// One field a resource's routes accept as a filter/sort target.
#[derive(Debug, Clone, Copy)]
pub struct FieldSpec {
    pub name: &'static str,
    pub kind: FieldKind,
}

impl FieldSpec {
    pub const fn number(name: &'static str) -> Self {
        Self {
            name,
            kind: FieldKind::Number,
        }
    }
    pub const fn datetime(name: &'static str) -> Self {
        Self {
            name,
            kind: FieldKind::DateTime,
        }
    }
    pub const fn string(name: &'static str) -> Self {
        Self {
            name,
            kind: FieldKind::String,
        }
    }
    #[allow(dead_code)] // no Hero field is Boolean yet; kept for parity with the reference's kind table
    pub const fn boolean(name: &'static str) -> Self {
        Self {
            name,
            kind: FieldKind::Boolean,
        }
    }
}

fn ops_for(kind: FieldKind) -> &'static [&'static str] {
    match kind {
        FieldKind::Number | FieldKind::DateTime => &["eq", "min", "max", "in"],
        FieldKind::String => &["eq", "contains", "icontains"],
        FieldKind::Boolean => &["eq", "in"],
    }
}

fn suffix_to_op(suffix: &str) -> Option<FilterOp> {
    match suffix {
        "eq" => Some(FilterOp::Eq),
        "min" => Some(FilterOp::Gte),
        "max" => Some(FilterOp::Lte),
        "in" => Some(FilterOp::In),
        "contains" => Some(FilterOp::Contains),
        "icontains" => Some(FilterOp::Icontains),
        _ => None,
    }
}

/// Split `field__suffix` into `(field, suffix)`, defaulting to the `eq`
/// suffix when there's no `__`.
fn split_key(key: &str) -> (&str, &str) {
    match key.rsplit_once("__") {
        Some((field, suffix)) => (field, suffix),
        None => (key, "eq"),
    }
}

/// Parse an ISO datetime, normalizing a tz-aware value to naive UTC --
/// every timestamp this app stores is naive-but-conceptually-UTC (see
/// `models::hero::Model`), matching `crud_query.py`'s own
/// `_cast_datetime`.
fn cast_datetime(raw: &str) -> Option<NaiveDateTime> {
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(raw) {
        return Some(dt.with_timezone(&Utc).naive_utc());
    }
    raw.parse::<NaiveDateTime>().ok()
}

fn cast_one(kind: FieldKind, raw: &str) -> Option<FilterValue> {
    match kind {
        FieldKind::String => Some(FilterValue::Str(raw.to_string())),
        FieldKind::Number => raw.trim().parse::<i64>().ok().map(FilterValue::Int),
        FieldKind::DateTime => cast_datetime(raw.trim()).map(FilterValue::DateTime),
        FieldKind::Boolean => match raw.trim().to_lowercase().as_str() {
            "true" | "1" => Some(FilterValue::Bool(true)),
            "false" | "0" => Some(FilterValue::Bool(false)),
            _ => None,
        },
    }
}

/// Parse every non-reserved query param into a `FilterClause`, validated
/// against `specs`. An unrecognized field name, an operator not valid for
/// that field's kind, or a value that doesn't parse as that field's type
/// collects a `FieldError` rather than being silently ignored -- the
/// caller renders the result as `AppError::UnprocessableEntity`.
pub fn parse_filters(
    specs: &[FieldSpec],
    params: &HashMap<String, String>,
) -> Result<Vec<FilterClause>, Vec<FieldError>> {
    let mut clauses = Vec::new();
    let mut errors = Vec::new();
    for (key, raw) in params {
        if RESERVED_PARAMS.contains(&key.as_str()) {
            continue;
        }
        let (field, suffix) = split_key(key);
        let spec = specs.iter().find(|spec| spec.name == field);
        let Some(spec) = spec.filter(|spec| ops_for(spec.kind).contains(&suffix)) else {
            errors.push(FieldError::new(
                "query",
                format!("unrecognized filter {key:?}"),
            ));
            continue;
        };
        // `suffix` is already validated against `ops_for(spec.kind)` above,
        // so every remaining suffix maps to a real FilterOp.
        let op = suffix_to_op(suffix).expect("suffix already validated against ops_for");

        let value = if op == FilterOp::In {
            let mut values = Vec::new();
            let mut invalid = false;
            for part in raw.split(',') {
                match cast_one(spec.kind, part) {
                    Some(value) => values.push(value),
                    None => {
                        invalid = true;
                        break;
                    }
                }
            }
            if invalid {
                errors.push(FieldError::new(
                    "query",
                    format!("invalid filter value for {key:?}"),
                ));
                continue;
            }
            FilterValue::List(values)
        } else {
            match cast_one(spec.kind, raw) {
                Some(value) => value,
                None => {
                    errors.push(FieldError::new(
                        "query",
                        format!("invalid filter value for {key:?}"),
                    ));
                    continue;
                }
            }
        };
        clauses.push(FilterClause {
            field: field.to_string(),
            op,
            value,
        });
    }
    if errors.is_empty() {
        Ok(clauses)
    } else {
        Err(errors)
    }
}

/// Parse a `sort=a,-b` query param into `SortClause`s, validated against
/// `specs`.
pub fn parse_sort(
    specs: &[FieldSpec],
    params: &HashMap<String, String>,
) -> Result<Vec<SortClause>, Vec<FieldError>> {
    let Some(raw) = params.get("sort") else {
        return Ok(vec![]);
    };
    let mut clauses = Vec::new();
    let mut errors = Vec::new();
    for part in raw.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let (descending, field) = match part.strip_prefix('-') {
            Some(rest) => (true, rest),
            None => (false, part),
        };
        if !specs.iter().any(|spec| spec.name == field) {
            errors.push(FieldError::new(
                "sort",
                format!("unrecognized field {field:?}"),
            ));
            continue;
        }
        clauses.push(SortClause {
            field: field.to_string(),
            descending,
        });
    }
    if errors.is_empty() {
        Ok(clauses)
    } else {
        Err(errors)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SPECS: &[FieldSpec] = &[
        FieldSpec::number("id"),
        FieldSpec::number("power_level"),
        FieldSpec::string("name"),
        FieldSpec::datetime("created_at"),
        FieldSpec::boolean("is_locked"),
    ];

    fn params(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn a_bare_field_name_is_an_equality_filter() {
        let clauses = parse_filters(SPECS, &params(&[("name", "Spectra")])).unwrap();
        assert_eq!(clauses.len(), 1);
        assert_eq!(clauses[0].field, "name");
        assert_eq!(clauses[0].op, FilterOp::Eq);
        assert_eq!(clauses[0].value, FilterValue::Str("Spectra".to_string()));
    }

    #[test]
    fn min_and_max_suffixes_map_to_gte_and_lte() {
        let clauses = parse_filters(SPECS, &params(&[("id__min", "1"), ("id__max", "9")])).unwrap();
        let ops: Vec<_> = clauses.iter().map(|c| c.op).collect();
        assert!(ops.contains(&FilterOp::Gte));
        assert!(ops.contains(&FilterOp::Lte));
    }

    #[test]
    fn in_suffix_splits_on_commas() {
        let clauses = parse_filters(SPECS, &params(&[("id__in", "1,2,3")])).unwrap();
        assert_eq!(clauses[0].op, FilterOp::In);
        let FilterValue::List(values) = &clauses[0].value else {
            panic!("expected a list value");
        };
        assert_eq!(values.len(), 3);
    }

    #[test]
    fn contains_and_icontains_are_valid_string_ops() {
        let clauses = parse_filters(
            SPECS,
            &params(&[("name__contains", "Spec"), ("name__icontains", "spec")]),
        )
        .unwrap();
        assert_eq!(clauses.len(), 2);
    }

    #[test]
    fn boolean_field_casts_true_false_1_0() {
        for (raw, expected) in [("true", true), ("1", true), ("false", false), ("0", false)] {
            let clauses = parse_filters(SPECS, &params(&[("is_locked", raw)])).unwrap();
            assert_eq!(clauses[0].value, FilterValue::Bool(expected));
        }
    }

    #[test]
    fn datetime_field_accepts_rfc3339_and_normalizes_to_naive_utc() {
        let clauses =
            parse_filters(SPECS, &params(&[("created_at", "2027-01-01T00:00:00Z")])).unwrap();
        assert!(matches!(clauses[0].value, FilterValue::DateTime(_)));
    }

    #[test]
    fn an_unrecognized_field_is_a_field_error() {
        let errors = parse_filters(SPECS, &params(&[("nope", "1")])).unwrap_err();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].field, "query");
    }

    #[test]
    fn an_operator_not_valid_for_the_fields_kind_is_a_field_error() {
        let errors = parse_filters(SPECS, &params(&[("name__min", "1")])).unwrap_err();
        assert_eq!(errors.len(), 1);
    }

    #[test]
    fn a_value_that_doesnt_parse_as_the_fields_type_is_a_field_error() {
        let errors = parse_filters(SPECS, &params(&[("power_level", "not-a-number")])).unwrap_err();
        assert_eq!(errors.len(), 1);
    }

    #[test]
    fn an_invalid_value_inside_an_in_list_is_a_field_error() {
        let errors = parse_filters(SPECS, &params(&[("id__in", "1,nope,3")])).unwrap_err();
        assert_eq!(errors.len(), 1);
    }

    #[test]
    fn reserved_params_are_never_treated_as_filters() {
        let clauses = parse_filters(
            SPECS,
            &params(&[
                ("id", "1"),
                ("skip", "5"),
                ("limit", "10"),
                ("include_archived", "true"),
            ]),
        )
        .unwrap();
        // "id" here is ambiguous with the reserved param of the same name --
        // RESERVED_PARAMS wins, so it's never parsed as a filter.
        assert!(clauses.is_empty());
    }

    #[test]
    fn no_sort_param_returns_an_empty_list() {
        assert_eq!(parse_sort(SPECS, &params(&[])).unwrap(), vec![]);
    }

    #[test]
    fn sort_parses_comma_separated_fields_with_a_leading_dash_for_descending() {
        let clauses = parse_sort(SPECS, &params(&[("sort", "name,-id")])).unwrap();
        assert_eq!(
            clauses,
            vec![
                SortClause {
                    field: "name".to_string(),
                    descending: false
                },
                SortClause {
                    field: "id".to_string(),
                    descending: true
                },
            ]
        );
    }

    #[test]
    fn sort_rejects_an_unrecognized_field() {
        let errors = parse_sort(SPECS, &params(&[("sort", "nope")])).unwrap_err();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].field, "sort");
    }

    #[test]
    fn sort_skips_an_empty_segment_from_a_stray_comma() {
        let clauses = parse_sort(SPECS, &params(&[("sort", "name,,id")])).unwrap();
        assert_eq!(
            clauses,
            vec![
                SortClause {
                    field: "name".to_string(),
                    descending: false
                },
                SortClause {
                    field: "id".to_string(),
                    descending: false
                },
            ]
        );
    }

    #[test]
    fn suffix_to_op_is_none_for_an_unrecognized_suffix() {
        assert_eq!(suffix_to_op("bogus"), None);
    }

    #[test]
    fn cast_datetime_falls_back_to_a_naive_non_rfc3339_timestamp() {
        let clauses =
            parse_filters(SPECS, &params(&[("created_at", "2024-01-01T12:00:00")])).unwrap();
        assert_eq!(clauses.len(), 1);
        assert!(matches!(clauses[0].value, FilterValue::DateTime(_)));
    }

    #[test]
    fn cast_one_boolean_is_none_for_an_unrecognized_value() {
        let errors = parse_filters(SPECS, &params(&[("is_locked", "maybe")])).unwrap_err();
        assert_eq!(errors.len(), 1);
    }
}
