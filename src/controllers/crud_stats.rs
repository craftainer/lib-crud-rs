//! Query parsing and OLS forecasting for the generic `/stats` and `/predict`
//! routes. A resource's field table ([`FieldSpec`]) decides which fields
//! take part; the handlers that read the data stay in the host.
//!
//! [`forecast`] is a plain ordinary-least-squares linear regression over
//! `(bucket_time, value)` pairs: standard-library arithmetic only, no ML
//! crate. It is a naive trend projection, not a trained model.

use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime, NaiveTime};

use crate::controllers::crud_query::{FieldKind, FieldSpec};
use crate::error::CrudError;
use crate::views::FieldError;

/// The fewest future buckets a prediction may ask for.
pub const MIN_PERIODS: u32 = 1;
/// The most future buckets a prediction may ask for: a longer naive linear
/// projection is near-meaningless, so this caps `?periods=` the way
/// [`MAX_LIMIT`](crate::crud::MAX_LIMIT) caps `?limit=`.
pub const MAX_PERIODS: u32 = 52;
/// The number of future buckets projected when the caller names none.
pub const DEFAULT_PERIODS: u32 = 4;

/// How many matching records `/stats`/`/predict` should read to compute
/// their aggregates: a sanity bound, not a precise one, since these are
/// reporting features rather than paginated listings.
pub const MAX_HISTORY_RECORDS: u64 = 1000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// The width of one time bucket of a stats or prediction series.
pub enum TimeBucket {
    /// One calendar day.
    Day,
    /// One week.
    Week,
    /// One calendar month.
    Month,
}

impl TimeBucket {
    /// Parse a `?bucket=` value (`day`, `week` or `month`).
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "day" => Some(Self::Day),
            "week" => Some(Self::Week),
            "month" => Some(Self::Month),
            _ => None,
        }
    }

    /// The wire name of this bucket width.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Day => "day",
            Self::Week => "week",
            Self::Month => "month",
        }
    }
}

/// Fields eligible for numeric stats/predict: `FieldKind::Number` and
/// `FieldKind::Float` -- `FieldKind::DateTime` is excluded (averaging or
/// summing a timestamp has no sensible meaning as an aggregate statistic).
#[must_use]
pub fn numeric_fields(specs: &[FieldSpec]) -> Vec<&'static str> {
    specs
        .iter()
        .filter(|spec| matches!(spec.kind, FieldKind::Number | FieldKind::Float))
        .map(|spec| spec.name)
        .collect()
}

/// Fields eligible for a categorical value distribution.
#[must_use]
pub fn categorical_fields(specs: &[FieldSpec]) -> Vec<&'static str> {
    specs
        .iter()
        .filter(|spec| spec.kind == FieldKind::Boolean)
        .map(|spec| spec.name)
        .collect()
}

/// Count each boolean/enum `field`'s value distribution over `records`,
/// resource-agnostic the same way `numeric_fields`/`categorical_fields`
/// are: `value_of` is the one place a caller's per-field access becomes
/// concrete (mirrors that resource's own `numeric_value`/`boolean_value`
/// helpers).
///
/// The order is deterministic and is `lib-crud-py`'s in-memory one: fields in
/// the order of `fields`, and within a field its values in order of first
/// appearance in `records`.
pub fn categorical_counts<T>(
    records: &[T],
    fields: &[&'static str],
    value_of: impl Fn(&T, &str) -> Option<bool>,
) -> Vec<crate::views::stats::CategoricalValueCount> {
    let mut rows = Vec::new();
    for &field in fields {
        let mut counts: Vec<(String, u64)> = Vec::new();
        for record in records {
            let Some(value) = value_of(record, field).map(|value| value.to_string()) else {
                continue;
            };
            match counts.iter_mut().find(|(seen, _)| *seen == value) {
                Some((_, count)) => *count += 1,
                None => counts.push((value, 1)),
            }
        }
        rows.extend(counts.into_iter().map(|(value, count)| {
            crate::views::stats::CategoricalValueCount {
                field,
                value,
                count,
            }
        }));
    }
    rows
}

/// Parse an optional `?bucket=` value.
///
/// # Errors
///
/// [`CrudError::UnprocessableEntity`] when the value is not a known bucket.
pub fn parse_bucket(raw: Option<&String>) -> Result<Option<TimeBucket>, CrudError> {
    match raw {
        None => Ok(None),
        Some(raw) => TimeBucket::parse(raw).map(Some).ok_or_else(|| {
            CrudError::UnprocessableEntity(vec![FieldError::new("bucket", "invalid bucket")])
        }),
    }
}

/// Validate `?field=` names a numeric field of `specs`, or return `None`
/// if omitted.
///
/// # Errors
///
/// [`CrudError::UnprocessableEntity`] when the field is not numeric.
pub fn parse_predict_field(
    specs: &[FieldSpec],
    raw: Option<&String>,
) -> Result<Option<&'static str>, CrudError> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    numeric_fields(specs)
        .into_iter()
        .find(|field| *field == raw)
        .map(Some)
        .ok_or_else(|| {
            CrudError::UnprocessableEntity(vec![FieldError::new("field", "not a numeric field")])
        })
}

/// Parse an optional `?periods=` value, defaulting to [`DEFAULT_PERIODS`].
///
/// # Errors
///
/// [`CrudError::UnprocessableEntity`] when the value is not an integer in
/// `1..=`[`MAX_PERIODS`].
pub fn parse_periods(raw: Option<&String>) -> Result<u32, CrudError> {
    let Some(raw) = raw else {
        return Ok(DEFAULT_PERIODS);
    };
    let periods: u32 = raw.parse().map_err(|_| {
        CrudError::UnprocessableEntity(vec![FieldError::new("periods", "must be an integer")])
    })?;
    if !(MIN_PERIODS..=MAX_PERIODS).contains(&periods) {
        return Err(CrudError::UnprocessableEntity(vec![FieldError::new(
            "periods",
            format!("must be between {MIN_PERIODS} and {MAX_PERIODS}"),
        )]));
    }
    Ok(periods)
}

/// One bucket of a generic float-valued series -- what `forecast`
/// operates over. Also used directly for a time-bucketed record-count
/// series (`value` is the count, as a float).
#[derive(Debug, Clone, Copy)]
pub struct BucketValue {
    /// Start of the bucket.
    pub bucket_start: NaiveDateTime,
    /// The bucket's value.
    pub value: f64,
}

/// One projected future bucket: its start and the forecast's projected
/// value.
#[derive(Debug, Clone, Copy)]
pub struct Prediction {
    /// Start of the projected bucket.
    pub bucket_start: NaiveDateTime,
    /// The projected value.
    pub value: f64,
}

/// Truncate `value` to the start of its UTC calendar bucket.
#[must_use]
pub fn bucket_start(bucket: TimeBucket, value: NaiveDateTime) -> NaiveDateTime {
    let day_start = value.date().and_time(NaiveTime::MIN);
    match bucket {
        TimeBucket::Day => day_start,
        TimeBucket::Week => {
            day_start - Duration::days(i64::from(day_start.weekday().num_days_from_monday()))
        }
        TimeBucket::Month => day_start - Duration::days(i64::from(day_start.day0())),
    }
}

/// Return `start` advanced by `steps` whole buckets of the given width.
fn advance(start: NaiveDateTime, bucket: TimeBucket, steps: i64) -> NaiveDateTime {
    match bucket {
        TimeBucket::Day => start + Duration::days(steps),
        TimeBucket::Week => start + Duration::weeks(steps),
        TimeBucket::Month => {
            let month_index = i64::from(start.month0()) + steps;
            let year = start.year() + i32::try_from(month_index.div_euclid(12)).unwrap_or(0);
            let month = u32::try_from(month_index.rem_euclid(12)).unwrap_or(0) + 1;
            NaiveDate::from_ymd_opt(year, month, 1)
                .expect("advancing by whole months always lands on a valid day-1 date")
                .and_time(start.time())
        }
    }
}

/// Fewer than 2 time buckets of history exist -- caught by the caller and
/// re-raised as a `422`, the same status/shape every other caller-input
/// problem in this layer uses.
#[derive(Debug)]
pub struct InsufficientHistory {
    /// How many buckets of history exist.
    pub have: usize,
}

/// Whole buckets between two bucket starts (`first` and `value`, both already
/// truncated by [`bucket_start`]): days for `Day`, days divided by 7 (floor)
/// for `Week`, calendar months (`12 * year difference + month difference`)
/// for `Month`.
fn bucket_offset(first: NaiveDateTime, value: NaiveDateTime, bucket: TimeBucket) -> i64 {
    if bucket == TimeBucket::Month {
        return i64::from(value.year() - first.year()) * 12 + i64::from(value.month())
            - i64::from(first.month());
    }
    let days = (value - first).num_days();
    if bucket == TimeBucket::Week {
        days.div_euclid(7)
    } else {
        days
    }
}

/// Project `periods` future buckets via ordinary-least-squares linear
/// regression on bucket *time*. `series` must already be ordered
/// oldest-first.
///
/// `x_i` is the whole number of buckets between the first bucket's start and
/// bucket `i`'s start (`Day`: days; `Week`: days / 7, floor; `Month`:
/// `12 * (year_i - year_0) + (month_i - month_0)`), so an empty bucket leaves
/// a gap in `x` instead of shifting later points. With `n` points, `y_i` the
/// bucket value and every sum accumulated left to right in plain `f64`
/// arithmetic (`+=`; no iterator `sum`, no compensated or pairwise
/// summation), exactly as `lib-crud-py`'s `forecast` does so both servers
/// return the same bits for the same series:
///
/// ```text
/// mean_x = (x_0 + ... + x_{n-1}) / n    mean_y = (y_0 + ... + y_{n-1}) / n
/// sxx = sum (x_i - mean_x)^2            sxy = sum (x_i - mean_x)(y_i - mean_y)
/// slope = 0.0 if sxx == 0 else sxy / sxx
/// intercept = mean_y - slope * mean_x
/// value_k = slope * (x_{n-1} + k) + intercept          for k = 1..=periods
/// ```
///
/// Projected bucket `k` starts `k` whole buckets after the last one. Each
/// `bucket_start` must already be truncated to its bucket ([`bucket_start`])
/// and the series strictly ascending, one point per bucket; otherwise two
/// points share an `x` and the trend is meaningless.
///
/// # Errors
///
/// [`InsufficientHistory`] when `series` has fewer than two buckets.
#[expect(
    clippy::cast_precision_loss,
    reason = "series are at most a few hundred buckets; far below f64's 2^52 exact-integer range"
)]
pub fn forecast(
    series: &[BucketValue],
    periods: u32,
    bucket: TimeBucket,
) -> Result<Vec<Prediction>, InsufficientHistory> {
    const MINIMUM_BUCKETS: usize = 2;
    let (Some(first), Some(last)) = (series.first(), series.last()) else {
        return Err(InsufficientHistory { have: series.len() });
    };
    if series.len() < MINIMUM_BUCKETS {
        return Err(InsufficientHistory { have: series.len() });
    }
    let xs: Vec<f64> = series
        .iter()
        .map(|point| bucket_offset(first.bucket_start, point.bucket_start, bucket) as f64)
        .collect();
    let ys: Vec<f64> = series.iter().map(|point| point.value).collect();
    let (slope, intercept) = slope_intercept(&xs, &ys);
    let last_x = xs[xs.len() - 1];
    let mut predictions = Vec::with_capacity(periods as usize);
    for step in 1..=periods {
        let value = slope * (last_x + f64::from(step)) + intercept;
        predictions.push(Prediction {
            bucket_start: advance(last.bucket_start, bucket, i64::from(step)),
            value,
        });
    }
    Ok(predictions)
}

/// Ordinary-least-squares slope and intercept for `xs`/`ys` (same length,
/// non-empty), every sum accumulated left to right (see [`forecast`]).
/// Guards the zero-variance case (every `x` equal) by returning a flat `0.0`
/// slope instead of dividing by zero.
#[expect(
    clippy::cast_precision_loss,
    reason = "series are at most a few hundred buckets; far below f64's 2^52 exact-integer range"
)]
fn slope_intercept(xs: &[f64], ys: &[f64]) -> (f64, f64) {
    let n = xs.len() as f64;
    let mut total_x = 0.0;
    let mut total_y = 0.0;
    for (x, y) in xs.iter().zip(ys) {
        total_x += x;
        total_y += y;
    }
    let mean_x = total_x / n;
    let mean_y = total_y / n;
    let mut sxx = 0.0;
    let mut sxy = 0.0;
    for (x, y) in xs.iter().zip(ys) {
        sxx += (x - mean_x) * (x - mean_x);
        sxy += (x - mean_x) * (y - mean_y);
    }
    let slope = if sxx == 0.0 { 0.0 } else { sxy / sxx };
    let intercept = mean_y - slope * mean_x;
    (slope, intercept)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dt(y: i32, m: u32, d: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(y, m, d)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
    }

    #[test]
    fn time_bucket_parses_valid_and_rejects_invalid() {
        assert_eq!(TimeBucket::parse("day"), Some(TimeBucket::Day));
        assert_eq!(TimeBucket::parse("week"), Some(TimeBucket::Week));
        assert_eq!(TimeBucket::parse("month"), Some(TimeBucket::Month));
        assert_eq!(TimeBucket::parse("year"), None);
    }

    #[test]
    fn bucket_start_truncates_to_day_week_month() {
        let value = NaiveDate::from_ymd_opt(2027, 3, 17)
            .unwrap()
            .and_hms_opt(13, 45, 0)
            .unwrap();
        assert_eq!(bucket_start(TimeBucket::Day, value), dt(2027, 3, 17));
        // 2027-03-17 is a Wednesday -- week start (Monday) is 2027-03-15.
        assert_eq!(bucket_start(TimeBucket::Week, value), dt(2027, 3, 15));
        assert_eq!(bucket_start(TimeBucket::Month, value), dt(2027, 3, 1));
    }

    #[test]
    fn advance_wraps_months_across_a_year_boundary() {
        let start = dt(2027, 11, 1);
        assert_eq!(advance(start, TimeBucket::Month, 1), dt(2027, 12, 1));
        assert_eq!(advance(start, TimeBucket::Month, 2), dt(2028, 1, 1));
        assert_eq!(advance(start, TimeBucket::Month, 14), dt(2029, 1, 1));
    }

    #[test]
    fn advance_steps_by_whole_weeks() {
        let start = dt(2027, 3, 15);
        assert_eq!(advance(start, TimeBucket::Week, 2), dt(2027, 3, 29));
    }

    #[test]
    fn as_str_round_trips_every_bucket() {
        assert_eq!(TimeBucket::Day.as_str(), "day");
        assert_eq!(TimeBucket::Week.as_str(), "week");
        assert_eq!(TimeBucket::Month.as_str(), "month");
    }

    #[test]
    fn forecast_errors_with_fewer_than_two_buckets() {
        let series = [BucketValue {
            bucket_start: dt(2027, 1, 1),
            value: 5.0,
        }];
        let err = forecast(&series, 1, TimeBucket::Day).unwrap_err();
        assert_eq!(err.have, 1);
    }

    #[test]
    fn forecast_projects_a_straight_line_trend() {
        // A perfect line: value = bucket_index (0, 1, 2, 3).
        let series: Vec<BucketValue> = (0..4_i32)
            .map(|i| BucketValue {
                bucket_start: dt(2027, 1, 1) + Duration::days(i64::from(i)),
                value: f64::from(i),
            })
            .collect();
        let predictions = forecast(&series, 2, TimeBucket::Day).unwrap();
        assert_eq!(predictions.len(), 2);
        assert!((predictions[0].value - 4.0).abs() < 1e-9);
        assert!((predictions[1].value - 5.0).abs() < 1e-9);
        assert_eq!(predictions[0].bucket_start, dt(2027, 1, 5));
    }

    fn series_on(days: &[i64], values: &[f64]) -> Vec<BucketValue> {
        days.iter()
            .zip(values)
            .map(|(&day, &value)| BucketValue {
                bucket_start: dt(2027, 1, 1) + Duration::days(day),
                value,
            })
            .collect()
    }

    #[test]
    fn forecast_regresses_on_bucket_time_so_a_gap_counts() {
        // Pins the formula shared with lib-crud-py: x = 0, 1, 4, 5 and
        // y = 3, 1, 7, 4 give mean_x = 2.5, mean_y = 3.75, sxx = 17,
        // sxy = 11.5, slope = 23/34, intercept = 3.75 - 2.5 * 23/34, so the
        // value at x = 6 (k = 1) is 104/17.
        let series = series_on(&[0, 1, 4, 5], &[3.0, 1.0, 7.0, 4.0]);
        let predictions = forecast(&series, 2, TimeBucket::Day).unwrap();
        assert!((predictions[0].value - 104.0 / 17.0).abs() < 1e-15);
        assert_eq!(predictions[0].bucket_start, dt(2027, 1, 7));
        assert_eq!(predictions[1].bucket_start, dt(2027, 1, 8));
    }

    #[test]
    fn forecast_gap_changes_the_trend_versus_position() {
        // Values 0, 1, 2 on days 1, 2 and 10: x = 0, 1, 9, so the line is
        // flatter than the position-based 3.0.
        let series = series_on(&[0, 1, 9], &[0.0, 1.0, 2.0]);
        let predictions = forecast(&series, 1, TimeBucket::Day).unwrap();
        assert!((predictions[0].value - 3.0).abs() > 0.5);
        assert_eq!(predictions[0].bucket_start, dt(2027, 1, 11));
    }

    #[test]
    fn bucket_offset_counts_whole_days_weeks_and_months() {
        let first = dt(2026, 11, 2);
        assert_eq!(bucket_offset(first, dt(2026, 11, 9), TimeBucket::Day), 7);
        assert_eq!(bucket_offset(first, dt(2026, 11, 23), TimeBucket::Week), 3);
        assert_eq!(bucket_offset(first, dt(2026, 11, 29), TimeBucket::Week), 3);
        assert_eq!(bucket_offset(first, dt(2026, 10, 26), TimeBucket::Week), -1);
        let first = dt(2026, 11, 1);
        assert_eq!(bucket_offset(first, dt(2027, 2, 1), TimeBucket::Month), 3);
        assert_eq!(bucket_offset(first, dt(2028, 1, 1), TimeBucket::Month), 14);
    }

    #[test]
    fn forecast_on_weekly_and_monthly_buckets_uses_bucket_units() {
        // Weekly: weeks 0, 1, 3 -> y = 0, 1, 3: a gap of one week.
        let weeks: Vec<BucketValue> = [(0, 0.0), (1, 1.0), (3, 3.0)]
            .iter()
            .map(|&(week, value)| BucketValue {
                bucket_start: dt(2027, 1, 4) + Duration::weeks(week),
                value,
            })
            .collect();
        let out = forecast(&weeks, 1, TimeBucket::Week).unwrap();
        // x = 0, 1, 3: slope = 1, intercept = 4/3 - 4/3 = 0, value at 4 is 4.
        assert!((out[0].value - 4.0).abs() < 1e-12);
        assert_eq!(out[0].bucket_start, dt(2027, 2, 1));
        // Monthly across a year end: months 0, 1, 3 (Nov, Dec, Feb).
        let months: Vec<BucketValue> = [(2026, 11, 0.0), (2026, 12, 1.0), (2027, 2, 3.0)]
            .iter()
            .map(|&(y, m, value)| BucketValue {
                bucket_start: dt(y, m, 1),
                value,
            })
            .collect();
        let out = forecast(&months, 1, TimeBucket::Month).unwrap();
        assert!((out[0].value - 4.0).abs() < 1e-12);
        assert_eq!(out[0].bucket_start, dt(2027, 3, 1));
    }

    #[test]
    fn forecast_of_an_empty_series_is_insufficient() {
        assert_eq!(forecast(&[], 1, TimeBucket::Day).unwrap_err().have, 0);
    }

    #[test]
    fn forecast_handles_a_flat_series_with_zero_slope() {
        let series: Vec<BucketValue> = (0..3)
            .map(|i| BucketValue {
                bucket_start: dt(2027, 1, 1) + Duration::days(i),
                value: 7.0,
            })
            .collect();
        let predictions = forecast(&series, 1, TimeBucket::Day).unwrap();
        assert!((predictions[0].value - 7.0).abs() < 1e-9);
    }

    #[test]
    fn slope_intercept_returns_a_flat_zero_slope_for_all_equal_xs() {
        // Degenerate input `forecast` never produces for distinct bucket
        // starts -- the guard still has to return `0.0` rather than `NaN`.
        let (slope, intercept) = slope_intercept(&[5.0, 5.0, 5.0], &[1.0, 2.0, 3.0]);
        assert_eq!(slope, 0.0);
        assert!((intercept - 2.0).abs() < 1e-9);
    }

    #[test]
    fn numeric_and_categorical_fields_classify_from_field_specs() {
        let specs = &[
            FieldSpec::number("id"),
            FieldSpec::string("name"),
            FieldSpec::boolean("is_locked"),
            FieldSpec::datetime("created_at"),
        ];
        assert_eq!(numeric_fields(specs), vec!["id"]);
        let with_float = &[FieldSpec::number("id"), FieldSpec::float("score")];
        assert_eq!(numeric_fields(with_float), vec!["id", "score"]);
        assert_eq!(categorical_fields(specs), vec!["is_locked"]);
    }

    #[test]
    fn categorical_counts_tallies_each_fields_value_distribution() {
        struct Record {
            is_locked: bool,
        }
        let records = vec![
            Record { is_locked: true },
            Record { is_locked: true },
            Record { is_locked: false },
        ];
        let counts = categorical_counts(&records, &["is_locked"], |record, _field| {
            Some(record.is_locked)
        });
        // First-seen order, with no sorting by the test.
        assert_eq!(counts.len(), 2);
        assert_eq!(counts[0].field, "is_locked");
        assert_eq!(counts[0].value, "true");
        assert_eq!(counts[0].count, 2);
        assert_eq!(counts[1].value, "false");
        assert_eq!(counts[1].count, 1);
    }

    #[test]
    fn categorical_counts_order_is_fields_then_first_seen_on_every_run() {
        let records: Vec<(bool, bool)> = (0..40).map(|i| (i % 3 == 0, i % 2 == 0)).collect();
        let run = || {
            categorical_counts(&records, &["a", "b"], |record, field| {
                Some(if field == "a" { record.0 } else { record.1 })
            })
            .into_iter()
            .map(|row| (row.field, row.value, row.count))
            .collect::<Vec<_>>()
        };
        let expected = vec![
            ("a", "true".to_string(), 14),
            ("a", "false".to_string(), 26),
            ("b", "true".to_string(), 20),
            ("b", "false".to_string(), 20),
        ];
        for _ in 0..20 {
            assert_eq!(run(), expected);
        }
    }

    #[test]
    fn categorical_counts_skips_records_without_a_value_for_the_field() {
        let records = [Some(true), None, Some(true)];
        let counts = categorical_counts(&records, &["flag"], |record, _field| *record);
        assert_eq!(counts.len(), 1);
        assert_eq!(counts[0].count, 2);
    }

    #[test]
    fn parse_bucket_rejects_an_unrecognized_value() {
        let raw = "fortnight".to_string();
        assert!(parse_bucket(Some(&raw)).is_err());
        assert_eq!(parse_bucket(None).unwrap(), None);
    }

    #[test]
    fn parse_predict_field_rejects_a_non_numeric_field() {
        let specs = &[FieldSpec::number("id"), FieldSpec::string("name")];
        let raw = "name".to_string();
        assert!(parse_predict_field(specs, Some(&raw)).is_err());
        let raw = "id".to_string();
        assert_eq!(parse_predict_field(specs, Some(&raw)).unwrap(), Some("id"));
        assert_eq!(parse_predict_field(specs, None).unwrap(), None);
    }

    #[test]
    fn parse_periods_enforces_the_min_max_bounds_and_default() {
        assert_eq!(parse_periods(None).unwrap(), DEFAULT_PERIODS);
        let raw = "0".to_string();
        assert!(parse_periods(Some(&raw)).is_err());
        let raw = "53".to_string();
        assert!(parse_periods(Some(&raw)).is_err());
        let raw = "10".to_string();
        assert_eq!(parse_periods(Some(&raw)).unwrap(), 10);
        let raw = "not-a-number".to_string();
        assert!(parse_periods(Some(&raw)).is_err());
    }
}
