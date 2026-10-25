//! Query-string parsing for the generic CRUD routes. [`FieldSpec`],
//! [`parse_filters`] and [`parse_sort`] are fully generic over any
//! resource's field table; only the table itself is resource-specific.
//!
//! Wire format: `field=value` is an equality match; `field__min=` and
//! `field__max=` express a range; `field__in=a,b,c` is membership;
//! `field__contains=` and `field__icontains=` are string operators.
//! `sort=field,-other_field` is comma-separated, a leading `-` meaning
//! descending.
//!
//! There is no `field__regex=` (see [`crate::repositories::filtering`]) and
//! no enum field kind. [`FieldKind`] splits `Number` (integers), `Float` and
//! `DateTime` rather than using one "number" kind that dispatches on a
//! runtime type; Python's `lib-crud-py` makes the same split by casting with
//! the field's own `int`/`float` type, so `1.5` is a valid value for a
//! `Float` field and a 422 for a `Number` one.
//!
//! # Order
//!
//! Params are a slice of `(key, value)` pairs in the order they appear on
//! the wire (an Axum handler extracts them with
//! `Query<Vec<(String, String)>>`), not a map: a repeated key is a repeated
//! clause, clauses come back in request order and so do errors, which is what
//! `lib-crud-py` does (`QueryParams.multi_items()`). A hash map would make
//! both depend on hashing.

use chrono::{NaiveDateTime, Utc};

use crate::repositories::filtering::{FilterClause, FilterOp, FilterValue, SortClause};
use crate::views::FieldError;

/// Query params never treated as a filter/sort key, regardless of a
/// resource's own field table: the reserved keys of the CRUD manifest
/// (`skip`, `limit`, `sort`, `id`, `include_archived`, `only_archived`,
/// `include_unpublished`).
/// `include_unpublished` is reserved even though this crate has no draft or
/// schedule lifecycle, so a client built for the manifest can send it to any
/// server of the family.
pub const RESERVED_PARAMS: &[&str] = &[
    "id",
    "skip",
    "limit",
    "sort",
    "include_archived",
    "only_archived",
    "include_unpublished",
];

/// What "kind" a filterable field is, driving which operators it accepts
/// and how a raw query-string value is cast.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldKind {
    /// An integer.
    Number,
    /// A floating-point number.
    Float,
    /// A timestamp.
    DateTime,
    /// Free text.
    String,
    /// A boolean.
    Boolean,
}

/// One field a resource's routes accept as a filter/sort target.
#[derive(Debug, Clone, Copy)]
pub struct FieldSpec {
    /// The query-string key and the model field name.
    pub name: &'static str,
    /// How values are cast and which operators are accepted.
    pub kind: FieldKind,
}

impl FieldSpec {
    /// A numeric field.
    #[must_use]
    pub const fn number(name: &'static str) -> Self {
        Self {
            name,
            kind: FieldKind::Number,
        }
    }
    /// A floating-point field.
    #[must_use]
    pub const fn float(name: &'static str) -> Self {
        Self {
            name,
            kind: FieldKind::Float,
        }
    }
    /// A timestamp field.
    #[must_use]
    pub const fn datetime(name: &'static str) -> Self {
        Self {
            name,
            kind: FieldKind::DateTime,
        }
    }
    /// A text field.
    #[must_use]
    pub const fn string(name: &'static str) -> Self {
        Self {
            name,
            kind: FieldKind::String,
        }
    }
    /// A boolean field.
    #[must_use]
    pub const fn boolean(name: &'static str) -> Self {
        Self {
            name,
            kind: FieldKind::Boolean,
        }
    }
}

pub(crate) fn ops_for(kind: FieldKind) -> &'static [&'static str] {
    match kind {
        FieldKind::Number | FieldKind::Float | FieldKind::DateTime => &["eq", "min", "max", "in"],
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

/// Parse an ISO datetime, normalizing a tz-aware value to naive UTC: every
/// timestamp the crate handles is naive but conceptually UTC.
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
        // Finite only: Python's `float()` also takes `nan`/`inf`, but a NaN
        // bound into SQL compares greater than every number and turns a
        // range filter (or a bulk target) into the wrong set.
        FieldKind::Float => raw
            .trim()
            .parse::<f64>()
            .ok()
            .filter(|value| value.is_finite())
            .map(FilterValue::Float),
        FieldKind::DateTime => cast_datetime(raw.trim()).map(FilterValue::DateTime),
        FieldKind::Boolean => cast_bool(raw).map(FilterValue::Bool),
    }
}

/// The boolean vocabulary shared by boolean filters and the flag keys:
/// `true`/`1` and `false`/`0`, case-insensitive.
fn cast_bool(raw: &str) -> Option<bool> {
    match raw.trim().to_lowercase().as_str() {
        "true" | "1" => Some(true),
        "false" | "0" => Some(false),
        _ => None,
    }
}

/// The last value of `key`, as `QueryParams.get` returns it in `lib-crud-py`.
fn last_value<'a, K: AsRef<str>, V: AsRef<str>>(
    params: &'a [(K, V)],
    key: &str,
) -> Option<&'a str> {
    params
        .iter()
        .rev()
        .find(|(k, _)| k.as_ref() == key)
        .map(|(_, v)| v.as_ref())
}

/// Parse a `?<key>=true|false` flag, `None` when the key is absent.
fn parse_flag<K: AsRef<str>, V: AsRef<str>>(
    params: &[(K, V)],
    key: &'static str,
) -> Result<Option<bool>, Vec<FieldError>> {
    match last_value(params, key) {
        None => Ok(None),
        Some(raw) => cast_bool(raw)
            .map(Some)
            .ok_or_else(|| vec![FieldError::new(key, "invalid filter value")]),
    }
}

/// Parse the `?only_archived=true` flag an archivable list accepts: `true`
/// narrows a list to the rows with `archived_at` set. It implies
/// `include_archived`, so `true` next to an explicit `include_archived=false`
/// is contradictory. Absent or `false` is `false` and changes nothing.
///
/// # Errors
///
/// A [`FieldError`] on `only_archived` for a malformed value or for the
/// contradictory pair (`cannot be combined with include_archived=false`);
/// on `include_archived` when `only_archived=true` accompanies a malformed
/// `include_archived`. A caller wraps them in
/// [`CrudError::UnprocessableEntity`](crate::error::CrudError::UnprocessableEntity).
pub fn parse_only_archived<K: AsRef<str>, V: AsRef<str>>(
    params: &[(K, V)],
) -> Result<bool, Vec<FieldError>> {
    let only_archived = parse_flag(params, "only_archived")?.unwrap_or(false);
    if only_archived && parse_flag(params, "include_archived")? == Some(false) {
        return Err(vec![FieldError::new(
            "only_archived",
            "cannot be combined with include_archived=false",
        )]);
    }
    Ok(only_archived)
}

/// Parse the `?include_archived=true` flag. Also validates `only_archived`
/// (see [`parse_only_archived`]), so every route that reads this flag rejects
/// the contradictory pair and a malformed `only_archived` alike.
///
/// # Errors
///
/// As [`parse_only_archived`], plus a [`FieldError`] on `include_archived`
/// for a malformed value.
pub fn parse_include_archived<K: AsRef<str>, V: AsRef<str>>(
    params: &[(K, V)],
) -> Result<bool, Vec<FieldError>> {
    parse_only_archived(params)?;
    Ok(parse_flag(params, "include_archived")?.unwrap_or(false))
}

/// Refuse `?only_archived=true` on a route that cannot honour it: only a list
/// read narrows by it, and a write, a restore, `/stats` or a single-record
/// read that carried it would otherwise act on live rows when the caller
/// meant the trash.
///
/// # Errors
///
/// As [`parse_only_archived`], and a [`FieldError`] on `only_archived`
/// (`applies to list reads only`) when it is `true`.
pub fn reject_only_archived<K: AsRef<str>, V: AsRef<str>>(
    params: &[(K, V)],
) -> Result<(), Vec<FieldError>> {
    if parse_only_archived(params)? {
        return Err(vec![FieldError::new(
            "only_archived",
            "applies to list reads only",
        )]);
    }
    Ok(())
}

/// Parse every non-reserved query param into a `FilterClause`, validated
/// against `specs`. An unrecognized field name, an operator not valid for
/// that field's kind, or a value that doesn't parse as that field's type
/// collects a `FieldError` rather than being silently ignored.
///
/// # Errors
///
/// Every offending parameter, in request order, as [`FieldError`]s; a caller wraps them in
/// [`CrudError::UnprocessableEntity`](crate::error::CrudError::UnprocessableEntity).
pub fn parse_filters<K: AsRef<str>, V: AsRef<str>>(
    specs: &[FieldSpec],
    params: &[(K, V)],
) -> Result<Vec<FilterClause>, Vec<FieldError>> {
    let mut clauses = Vec::new();
    let mut errors = Vec::new();
    for (key, raw) in params {
        let (key, raw) = (key.as_ref(), raw.as_ref());
        if RESERVED_PARAMS.contains(&key) {
            continue;
        }
        let (field, suffix) = split_key(key);
        let known = specs
            .iter()
            .find(|spec| spec.name == field)
            .filter(|spec| ops_for(spec.kind).contains(&suffix))
            .and_then(|spec| suffix_to_op(suffix).map(|op| (spec, op)));
        let Some((spec, op)) = known else {
            errors.push(FieldError::new(
                "query",
                format!("unrecognized filter {key:?}"),
            ));
            continue;
        };
        let value = if op == FilterOp::In {
            let mut values = Vec::new();
            let mut invalid = false;
            for part in raw.split(',') {
                if let Some(value) = cast_one(spec.kind, part) {
                    values.push(value);
                } else {
                    invalid = true;
                    break;
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
            let Some(value) = cast_one(spec.kind, raw) else {
                errors.push(FieldError::new(
                    "query",
                    format!("invalid filter value for {key:?}"),
                ));
                continue;
            };
            value
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
///
/// # Errors
///
/// Every unrecognized sort field, as [`FieldError`]s.
pub fn parse_sort<K: AsRef<str>, V: AsRef<str>>(
    specs: &[FieldSpec],
    params: &[(K, V)],
) -> Result<Vec<SortClause>, Vec<FieldError>> {
    // The last `sort` wins, as `QueryParams.get` does in `lib-crud-py`.
    let Some(raw) = params
        .iter()
        .rev()
        .find(|(key, _)| key.as_ref() == "sort")
        .map(|(_, value)| value.as_ref())
    else {
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
        FieldSpec::float("score"),
        FieldSpec::string("name"),
        FieldSpec::datetime("created_at"),
        FieldSpec::boolean("is_locked"),
    ];

    fn params(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
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
        assert!(matches!(&clauses[0].value, FilterValue::List(values) if values.len() == 3));
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
        let expected =
            NaiveDateTime::parse_from_str("2027-01-01 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap();
        assert_eq!(clauses[0].value, FilterValue::DateTime(expected));
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
                ("only_archived", "true"),
                ("include_unpublished", "true"),
                ("sort", "name"),
            ]),
        )
        .unwrap();
        // "id" here is ambiguous with the reserved param of the same name --
        // RESERVED_PARAMS wins, so it's never parsed as a filter.
        assert_eq!(clauses, Vec::<FilterClause>::new());
    }

    #[test]
    fn every_manifest_reserved_key_is_reserved() {
        for key in [
            "skip",
            "limit",
            "sort",
            "id",
            "include_archived",
            "only_archived",
            "include_unpublished",
        ] {
            assert!(RESERVED_PARAMS.contains(&key), "{key}");
        }
    }

    #[test]
    fn a_float_field_accepts_decimal_values_in_every_operator() {
        let clauses = parse_filters(
            SPECS,
            &params(&[
                ("score", "1.5"),
                ("score__min", "-0.25"),
                ("score__max", "1e3"),
                ("score__in", "1,2.5"),
            ]),
        )
        .unwrap();
        assert_eq!(clauses[0].value, FilterValue::Float(1.5));
        assert_eq!(clauses[1].value, FilterValue::Float(-0.25));
        assert_eq!(clauses[2].value, FilterValue::Float(1000.0));
        assert_eq!(
            clauses[3].value,
            FilterValue::List(vec![FilterValue::Float(1.0), FilterValue::Float(2.5)])
        );
    }

    #[test]
    fn an_integer_field_still_rejects_a_decimal_value() {
        let errors = parse_filters(SPECS, &params(&[("power_level", "1.5")])).unwrap_err();
        assert_eq!(errors.len(), 1);
        for bad in ["abc", "NaN", "inf", "-infinity", "1e999"] {
            let errors = parse_filters(SPECS, &params(&[("score", bad)])).unwrap_err();
            assert_eq!(errors.len(), 1, "{bad}");
        }
    }

    #[test]
    fn clauses_and_errors_come_back_in_request_order() {
        let pairs = [
            ("name", "a"),
            ("id__max", "9"),
            ("nope", "1"),
            ("id__min", "1"),
            ("bad", "2"),
            ("is_locked", "true"),
        ];
        let clauses =
            parse_filters(SPECS, &params(&[pairs[0], pairs[1], pairs[3], pairs[5]])).unwrap();
        let keys: Vec<_> = clauses.iter().map(|c| (c.field.as_str(), c.op)).collect();
        assert_eq!(
            keys,
            vec![
                ("name", FilterOp::Eq),
                ("id", FilterOp::Lte),
                ("id", FilterOp::Gte),
                ("is_locked", FilterOp::Eq),
            ]
        );
        let errors = parse_filters(SPECS, &params(&pairs)).unwrap_err();
        let messages: Vec<_> = errors.iter().map(|e| e.msg.as_str()).collect();
        assert_eq!(
            messages,
            vec![
                "unrecognized filter \"nope\"",
                "unrecognized filter \"bad\""
            ]
        );
    }

    #[test]
    fn a_repeated_key_is_a_repeated_clause() {
        let clauses = parse_filters(SPECS, &params(&[("id__min", "1"), ("id__min", "5")])).unwrap();
        assert_eq!(clauses.len(), 2);
        assert_eq!(clauses[1].value, FilterValue::Int(5));
    }

    #[test]
    fn the_last_sort_param_wins() {
        let clauses = parse_sort(SPECS, &params(&[("sort", "name"), ("sort", "-id")])).unwrap();
        assert_eq!(clauses.len(), 1);
        assert_eq!(clauses[0].field, "id");
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
        let expected =
            NaiveDateTime::parse_from_str("2024-01-01 12:00:00", "%Y-%m-%d %H:%M:%S").unwrap();
        assert_eq!(clauses[0].value, FilterValue::DateTime(expected));
    }

    #[test]
    fn cast_one_boolean_is_none_for_an_unrecognized_value() {
        let errors = parse_filters(SPECS, &params(&[("is_locked", "maybe")])).unwrap_err();
        assert_eq!(errors.len(), 1);
    }

    fn only_archived_error(pairs: &[(&str, &str)]) -> (String, String) {
        let errors = parse_only_archived(&params(pairs)).unwrap_err();
        assert_eq!(errors.len(), 1);
        (errors[0].field.to_string(), errors[0].msg.clone())
    }

    #[test]
    fn only_archived_is_false_when_absent_or_false() {
        assert!(!parse_only_archived(&params(&[])).unwrap());
        for raw in ["false", "0", "FALSE", " False "] {
            assert!(!parse_only_archived(&params(&[("only_archived", raw)])).unwrap());
        }
    }

    #[test]
    fn only_archived_takes_true_and_1_case_insensitively() {
        for raw in ["true", "1", "TRUE", "True"] {
            assert!(parse_only_archived(&params(&[("only_archived", raw)])).unwrap());
        }
    }

    #[test]
    fn a_malformed_only_archived_is_invalid_filter_value() {
        for raw in ["maybe", "", "yes", "2"] {
            assert_eq!(
                only_archived_error(&[("only_archived", raw)]),
                (
                    "only_archived".to_string(),
                    "invalid filter value".to_string()
                ),
                "{raw}"
            );
        }
    }

    #[test]
    fn the_last_only_archived_wins() {
        let pairs = params(&[("only_archived", "false"), ("only_archived", "true")]);
        assert!(parse_only_archived(&pairs).unwrap());
    }

    #[test]
    fn only_archived_true_with_include_archived_false_is_contradictory() {
        for raw in ["false", "0"] {
            assert_eq!(
                only_archived_error(&[("only_archived", "true"), ("include_archived", raw)]),
                (
                    "only_archived".to_string(),
                    "cannot be combined with include_archived=false".to_string()
                )
            );
        }
    }

    #[test]
    fn only_archived_true_with_include_archived_true_or_none_is_fine() {
        let pairs = params(&[("only_archived", "true"), ("include_archived", "true")]);
        assert!(parse_only_archived(&pairs).unwrap());
        let pairs = params(&[("only_archived", "false"), ("include_archived", "false")]);
        assert!(!parse_only_archived(&pairs).unwrap());
    }

    #[test]
    fn only_archived_true_with_a_malformed_include_archived_names_include_archived() {
        assert_eq!(
            only_archived_error(&[("only_archived", "true"), ("include_archived", "maybe")]),
            (
                "include_archived".to_string(),
                "invalid filter value".to_string()
            )
        );
    }

    #[test]
    fn parse_include_archived_also_validates_only_archived() {
        assert!(!parse_include_archived(&params(&[])).unwrap());
        assert!(parse_include_archived(&params(&[("include_archived", "1")])).unwrap());
        assert!(
            parse_include_archived(&params(&[
                ("include_archived", "true"),
                ("only_archived", "true")
            ]))
            .unwrap()
        );
        let errors = parse_include_archived(&params(&[("include_archived", "maybe")])).unwrap_err();
        assert_eq!(errors[0].field, "include_archived");
        let errors = parse_include_archived(&params(&[("only_archived", "maybe")])).unwrap_err();
        assert_eq!(errors[0].field, "only_archived");
        let errors = parse_include_archived(&params(&[
            ("include_archived", "false"),
            ("only_archived", "true"),
        ]))
        .unwrap_err();
        assert_eq!(
            errors[0].msg,
            "cannot be combined with include_archived=false"
        );
    }

    #[test]
    fn reject_only_archived_refuses_true_and_accepts_false_or_absent() {
        assert!(reject_only_archived(&params(&[])).is_ok());
        assert!(reject_only_archived(&params(&[("only_archived", "false")])).is_ok());
        assert_eq!(
            only_archived_reject_error(&[("only_archived", "1")]),
            "applies to list reads only"
        );
        // The contradictory pair and a malformed value keep their own message.
        assert_eq!(
            only_archived_reject_error(&[("only_archived", "true"), ("include_archived", "0")]),
            "cannot be combined with include_archived=false"
        );
        assert_eq!(
            only_archived_reject_error(&[("only_archived", "x")]),
            "invalid filter value"
        );
    }

    fn only_archived_reject_error(pairs: &[(&str, &str)]) -> String {
        let errors = reject_only_archived(&params(pairs)).unwrap_err();
        assert_eq!(errors[0].field, "only_archived");
        errors[0].msg.clone()
    }
}
