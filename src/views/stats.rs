//! Response shapes for the generic `GET <prefix>/stats`/`GET
//! <prefix>/predict` routes.

use chrono::NaiveDateTime;
use serde::Serialize;

/// One numeric field's aggregate statistics: count/min/max/avg/sum.
#[derive(Debug, Serialize)]
pub struct NumericFieldStat {
    /// The field.
    pub field: &'static str,
    /// Records with a value.
    pub count: u64,
    /// Smallest value.
    pub minimum: Option<f64>,
    /// Largest value.
    pub maximum: Option<f64>,
    /// Mean value.
    pub average: Option<f64>,
    /// Sum of values.
    pub total: Option<f64>,
}

/// One (field, value) pair's count, one row of a categorical field's
/// distribution. Empty for a resource with no boolean or enum field.
#[derive(Debug, Serialize)]
pub struct CategoricalValueCount {
    /// The field.
    pub field: &'static str,
    /// The value.
    pub value: String,
    /// Records holding it.
    pub count: u64,
}

/// One bucket of a time-bucketed record-count series.
#[derive(Debug, Serialize)]
pub struct TimeBucketCount {
    /// Start of the bucket.
    pub bucket_start: NaiveDateTime,
    /// Records in the bucket.
    pub count: u64,
}

/// Record-lifecycle breakdown. Only the soft-delete (archived) state exists
/// in this crate, so that is all it reports.
#[derive(Debug, Serialize)]
pub struct LifecycleStats {
    /// Soft-deleted records.
    pub archived: u64,
}

/// The full `GET <prefix>/stats` response body.
#[derive(Debug, Serialize)]
pub struct ResourceStats {
    /// Total records.
    pub total: u64,
    /// Per numeric field aggregates.
    pub numeric: Vec<NumericFieldStat>,
    /// Per categorical value counts.
    pub categorical: Vec<CategoricalValueCount>,
    /// Record counts over time, when a bucket was requested.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_series: Option<Vec<TimeBucketCount>>,
    /// Lifecycle breakdown.
    pub lifecycle: LifecycleStats,
}

/// One point of a generic float-valued series -- a known bucket or a
/// projected one.
#[derive(Debug, Serialize)]
pub struct SeriesPoint {
    /// Start of the bucket.
    pub bucket_start: NaiveDateTime,
    /// The bucket's value.
    pub value: f64,
}

/// The full `GET <prefix>/predict` response body. `method` is always
/// `"linear_regression"` -- present explicitly so a client never
/// mistakes this for a real, trained model's forecast . `field` is `None` when the forecast targets record count over
/// time (the default) rather than a specific numeric field.
#[derive(Debug, Serialize)]
pub struct Prediction {
    /// The forecast field, or `None` for record count.
    pub field: Option<&'static str>,
    /// The bucket width (`day`, `week` or `month`).
    pub bucket: &'static str,
    /// Always `linear_regression`.
    pub method: &'static str,
    /// The last observed point.
    pub last_known: SeriesPoint,
    /// The projected points.
    pub predictions: Vec<SeriesPoint>,
}
