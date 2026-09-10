//! Query parsing and OLS forecasting for the generic `/stats`/`/predict`
//! routes -- port of `controllers/crud_stats.py`, narrowed to operate
//! directly against Hero rather than being generic over any `Repository`
//! (this instance has one resource; see `docs/adrs/0015` for why that
//! narrowing is the honest choice here rather than premature genericity
//! over a capability with a single call site).
//!
//! `forecast()` is a plain ordinary-least-squares linear regression over
//! `(bucket_index, value)` pairs -- stdlib arithmetic only, no ML crate,
//! matching the reference's own `forecast()` and its ADR
//! (`docs/adrs/0017-linear-forecast-and-json-only-events-for-crud-
//! stats.md` in template-fastapi, ported here as `docs/adrs/0015`).

use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime};

use crate::generic::controllers::crud_query::{FieldKind, FieldSpec};
use crate::generic::views::FieldError;
use crate::problem_details::AppError;

/// A predicted series longer than this would be a near-meaningless naive
/// linear projection anyway -- caps `?periods=` the same way
/// `crud::MAX_LIMIT` caps `?limit=`.
pub const MIN_PERIODS: u32 = 1;
pub const MAX_PERIODS: u32 = 52;
pub const DEFAULT_PERIODS: u32 = 4;

/// How many matching records `/stats`/`/predict` read to compute their
/// aggregates -- a sanity bound, not a precise one (this is a demo/
/// reporting feature, not a paginated listing), matching the reference's
/// own `MAX_HISTORY_RECORDS`.
pub const MAX_HISTORY_RECORDS: u64 = 1000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeBucket {
    Day,
    Week,
    Month,
}

impl TimeBucket {
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "day" => Some(Self::Day),
            "week" => Some(Self::Week),
            "month" => Some(Self::Month),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Day => "day",
            Self::Week => "week",
            Self::Month => "month",
        }
    }
}

/// Fields eligible for numeric stats/predict: `FieldKind::Number` only --
/// `FieldKind::DateTime` is excluded the same way the reference excludes
/// date/datetime fields from `numeric_fields` (averaging/summing a
/// timestamp has no sensible meaning as an aggregate statistic).
pub fn numeric_fields(specs: &[FieldSpec]) -> Vec<&'static str> {
    specs
        .iter()
        .filter(|spec| spec.kind == FieldKind::Number)
        .map(|spec| spec.name)
        .collect()
}

/// Fields eligible for a categorical value distribution.
pub fn categorical_fields(specs: &[FieldSpec]) -> Vec<&'static str> {
    specs
        .iter()
        .filter(|spec| spec.kind == FieldKind::Boolean)
        .map(|spec| spec.name)
        .collect()
}

pub fn parse_bucket(raw: Option<&String>) -> Result<Option<TimeBucket>, AppError> {
    match raw {
        None => Ok(None),
        Some(raw) => TimeBucket::parse(raw).map(Some).ok_or_else(|| {
            AppError::UnprocessableEntity(vec![FieldError::new("bucket", "invalid bucket")])
        }),
    }
}

/// Validate `?field=` names a numeric field of `specs`, or return `None`
/// if omitted.
pub fn parse_predict_field(
    specs: &[FieldSpec],
    raw: Option<&String>,
) -> Result<Option<&'static str>, AppError> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    numeric_fields(specs)
        .into_iter()
        .find(|field| *field == raw)
        .map(Some)
        .ok_or_else(|| {
            AppError::UnprocessableEntity(vec![FieldError::new("field", "not a numeric field")])
        })
}

pub fn parse_periods(raw: Option<&String>) -> Result<u32, AppError> {
    let Some(raw) = raw else {
        return Ok(DEFAULT_PERIODS);
    };
    let periods: u32 = raw.parse().map_err(|_| {
        AppError::UnprocessableEntity(vec![FieldError::new("periods", "must be an integer")])
    })?;
    if !(MIN_PERIODS..=MAX_PERIODS).contains(&periods) {
        return Err(AppError::UnprocessableEntity(vec![FieldError::new(
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
    pub bucket_start: NaiveDateTime,
    pub value: f64,
}

/// One projected future bucket: its start and the forecast's projected
/// value.
#[derive(Debug, Clone, Copy)]
pub struct Prediction {
    pub bucket_start: NaiveDateTime,
    pub value: f64,
}

/// Truncate `value` to the start of its UTC calendar bucket -- mirrors
/// the reference's own `_bucket_start` helper.
pub fn bucket_start(bucket: TimeBucket, value: NaiveDateTime) -> NaiveDateTime {
    let day_start = value
        .date()
        .and_hms_opt(0, 0, 0)
        .expect("midnight is always valid");
    match bucket {
        TimeBucket::Day => day_start,
        TimeBucket::Week => {
            day_start - Duration::days(day_start.weekday().num_days_from_monday() as i64)
        }
        TimeBucket::Month => NaiveDate::from_ymd_opt(day_start.year(), day_start.month(), 1)
            .expect("year/month from an existing date is always valid")
            .and_hms_opt(0, 0, 0)
            .expect("midnight is always valid"),
    }
}

/// Return `start` advanced by `steps` whole buckets of the given width.
fn advance(start: NaiveDateTime, bucket: TimeBucket, steps: i64) -> NaiveDateTime {
    match bucket {
        TimeBucket::Day => start + Duration::days(steps),
        TimeBucket::Week => start + Duration::weeks(steps),
        TimeBucket::Month => {
            let month_index = start.month0() as i64 + steps;
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
    pub have: usize,
}

/// Project `periods` future buckets via ordinary-least-squares linear
/// regression. `series` must already be ordered oldest-first.
pub fn forecast(
    series: &[BucketValue],
    periods: u32,
    bucket: TimeBucket,
) -> Result<Vec<Prediction>, InsufficientHistory> {
    const MINIMUM_BUCKETS: usize = 2;
    if series.len() < MINIMUM_BUCKETS {
        return Err(InsufficientHistory { have: series.len() });
    }
    let n = series.len();
    let xs: Vec<f64> = (0..n).map(|i| i as f64).collect();
    let ys: Vec<f64> = series.iter().map(|point| point.value).collect();
    let mean_x = xs.iter().sum::<f64>() / n as f64;
    let mean_y = ys.iter().sum::<f64>() / n as f64;
    let denominator: f64 = xs.iter().map(|x| (x - mean_x).powi(2)).sum();
    let slope = if denominator == 0.0 {
        0.0
    } else {
        xs.iter()
            .zip(&ys)
            .map(|(x, y)| (x - mean_x) * (y - mean_y))
            .sum::<f64>()
            / denominator
    };
    let intercept = mean_y - slope * mean_x;
    let last_start = series[n - 1].bucket_start;
    let mut predictions = Vec::with_capacity(periods as usize);
    for step in 1..=periods {
        let x = (n - 1) as f64 + f64::from(step);
        let value = slope * x + intercept;
        predictions.push(Prediction {
            bucket_start: advance(last_start, bucket, i64::from(step)),
            value,
        });
    }
    Ok(predictions)
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
        let series: Vec<BucketValue> = (0..4)
            .map(|i| BucketValue {
                bucket_start: dt(2027, 1, 1) + Duration::days(i),
                value: i as f64,
            })
            .collect();
        let predictions = forecast(&series, 2, TimeBucket::Day).unwrap();
        assert_eq!(predictions.len(), 2);
        assert!((predictions[0].value - 4.0).abs() < 1e-9);
        assert!((predictions[1].value - 5.0).abs() < 1e-9);
        assert_eq!(predictions[0].bucket_start, dt(2027, 1, 5));
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
    fn numeric_and_categorical_fields_classify_from_field_specs() {
        let specs = &[
            FieldSpec::number("id"),
            FieldSpec::string("name"),
            FieldSpec::boolean("is_locked"),
            FieldSpec::datetime("created_at"),
        ];
        assert_eq!(numeric_fields(specs), vec!["id"]);
        assert_eq!(categorical_fields(specs), vec!["is_locked"]);
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
