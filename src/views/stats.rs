//! Response shapes for the generic `GET <prefix>/stats`/`GET
//! <prefix>/predict` routes -- port of `views/stats.py`.

use chrono::NaiveDateTime;
use serde::Serialize;

/// One numeric field's aggregate statistics: count/min/max/avg/sum.
#[derive(Debug, Serialize)]
pub struct NumericFieldStat {
    pub field: &'static str,
    pub count: u64,
    pub minimum: Option<f64>,
    pub maximum: Option<f64>,
    pub average: Option<f64>,
    pub total: Option<f64>,
}

/// One (field, value) pair's count, one row of a categorical field's
/// distribution. Never populated for Hero (it has no boolean/enum
/// field), but kept generic rather than hardcoded empty -- a future
/// resource with a categorical field gets this for free.
#[derive(Debug, Serialize)]
pub struct CategoricalValueCount {
    pub field: &'static str,
    pub value: String,
    pub count: u64,
}

/// One bucket of a time-bucketed record-count series.
#[derive(Debug, Serialize)]
pub struct TimeBucketCount {
    pub bucket_start: NaiveDateTime,
    pub count: u64,
}

/// Record-lifecycle mixin breakdown -- narrower than the reference's
/// `LifecycleStats` (`archived`/`draft`/`locked`/`scheduled_pending`/
/// `scheduled_expired`): Hero only carries the `Archivable` mixin in this
/// port (`models::hero`'s own doc comment), so this only ever reports
/// `archived`.
#[derive(Debug, Serialize)]
pub struct LifecycleStats {
    pub archived: u64,
}

/// The full `GET <prefix>/stats` response body.
#[derive(Debug, Serialize)]
pub struct ResourceStats {
    pub total: u64,
    pub numeric: Vec<NumericFieldStat>,
    pub categorical: Vec<CategoricalValueCount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_series: Option<Vec<TimeBucketCount>>,
    pub lifecycle: LifecycleStats,
}

/// One point of a generic float-valued series -- a known bucket or a
/// projected one.
#[derive(Debug, Serialize)]
pub struct SeriesPoint {
    pub bucket_start: NaiveDateTime,
    pub value: f64,
}

/// The full `GET <prefix>/predict` response body. `method` is always
/// `"linear_regression"` -- present explicitly so a client never
/// mistakes this for a real, trained model's forecast (`docs/adrs/
/// 0015`). `field` is `None` when the forecast targets record count over
/// time (the default) rather than a specific numeric field.
#[derive(Debug, Serialize)]
pub struct Prediction {
    pub field: Option<&'static str>,
    pub bucket: &'static str,
    pub method: &'static str,
    pub last_known: SeriesPoint,
    pub predictions: Vec<SeriesPoint>,
}
