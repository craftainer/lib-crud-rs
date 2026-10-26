//! Conditional requests: entity-tags, `If-None-Match` revalidation and
//! `If-Match` preconditions (RFC 9110 sections 8.8.3 and 13.1).
//!
//! This is the building-block half of the feature, in the same spirit as
//! [`controllers`](crate::controllers): the crate has no CRUD routers, so the
//! host's handlers call these functions. Nothing here is applied
//! automatically, which is why [`Features::conditional_requests`] stays the
//! host's switch: set it once the handlers really call them.
//!
//! The formats are those of `lib-crud-py`, so an SDK that works against the
//! Python server works against this one:
//!
//! - [`record_etag`]: a strong tag, `"<32 hex>"`, the SHA-256 of
//!   `<id>:<updated_at>` (the timestamp written as Python's `isoformat()`
//!   writes a naive datetime: whole seconds when the microseconds are zero).
//! - [`list_etag`]: a weak tag, `W/"<32 hex>"`, the SHA-256 of the records'
//!   `<id>:<updated_at>` strings joined with `|`.
//! - [`not_modified`]: `If-None-Match` with *weak* comparison, so a `GET` that
//!   matches gets a bodiless `304` carrying the same `ETag` and
//!   `Cache-Control: private, no-cache`.
//! - [`check_if_match`]: `If-Match` with *strong* comparison, so a stale tag
//!   (or a weak one, which can never match) is [`CrudError::PreconditionFailed`],
//!   a `412` problem response. `*` matches any existing record.
//!
//! A typical handler:
//!
//! ```
//! use axum::http::{HeaderMap, StatusCode};
//! use chrono::NaiveDateTime;
//! use craftainer_crud::conditional::{Versioned, check_if_match, not_modified, record_etag};
//! use craftainer_crud::models::HasId;
//!
//! struct Widget { id: i32, updated_at: NaiveDateTime }
//! impl HasId for Widget { fn id(&self) -> i32 { self.id } }
//! impl Versioned for Widget { fn updated_at(&self) -> NaiveDateTime { self.updated_at } }
//!
//! let widget = Widget { id: 1, updated_at: NaiveDateTime::default() };
//! let etag = record_etag(&widget);
//!
//! // GET: revalidate.
//! let mut request = HeaderMap::new();
//! request.insert("if-none-match", etag.parse().unwrap());
//! let response = not_modified(&request, &etag).expect("the tag is current");
//! assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
//!
//! // PATCH or DELETE: the precondition holds, so the write goes ahead.
//! let mut request = HeaderMap::new();
//! request.insert("if-match", etag.parse().unwrap());
//! check_if_match(&request, &etag, "Widget").expect("the tag is current");
//! ```
//!
//! [`Features::conditional_requests`]: crate::manifest::Features::conditional_requests

use std::fmt::Write as _;

use axum::http::header::{CACHE_CONTROL, ETAG, IF_MATCH, IF_NONE_MATCH};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use chrono::NaiveDateTime;
use sha2::{Digest, Sha256};

use crate::error::CrudError;
use crate::models::HasId;

/// `Cache-Control` of every `ETag`-bearing `GET`: stored by the client only
/// (responses are per caller), revalidated before every reuse.
pub const REVALIDATE_CACHE_CONTROL: &str = "private, no-cache";

/// A record whose current state is identified by its id and `updated_at`:
/// what [`record_etag`] and [`list_etag`] hash.
///
/// `updated_at` must change whenever anything the record's representation
/// shows changes, and be the value the representation carries (microsecond
/// precision at most; finer digits are not hashed).
pub trait Versioned: HasId {
    /// When the record was last modified.
    fn updated_at(&self) -> NaiveDateTime;
}

/// A short stable hex digest of `source`, for an entity-tag's opaque value
/// (SHA-256, first 32 hex digits, as `lib-crud-py` does).
pub(crate) fn digest(source: &[u8]) -> String {
    let hash = Sha256::digest(source);
    let mut hex = String::with_capacity(32);
    for byte in &hash[..16] {
        write!(hex, "{byte:02x}").expect("writing to a String cannot fail");
    }
    hex
}

/// `<id>:<updated_at>`, with the timestamp as Python's `datetime.isoformat()`
/// writes it (no fraction at all when the microseconds are zero).
fn version<T: Versioned>(record: &T) -> String {
    let updated_at = record.updated_at();
    let micros = updated_at.and_utc().timestamp_subsec_micros();
    let stamp = updated_at.format("%Y-%m-%dT%H:%M:%S");
    if micros == 0 {
        format!("{}:{stamp}", record.id())
    } else {
        format!("{}:{stamp}.{micros:06}", record.id())
    }
}

/// The strong `ETag` of one record: `"<digest of id:updated_at>"`. Use it on
/// a single-record `GET`, and on the response to the `PATCH` that changed it.
#[must_use]
pub fn record_etag<T: Versioned>(record: &T) -> String {
    format!("\"{}\"", digest(version(record).as_bytes()))
}

/// The weak `ETag` of one page of records, in order: `W/"<digest>"`. Weak
/// because two pages with the same ids and timestamps need not be
/// byte-identical (a view can change shape without `updated_at` moving).
#[must_use]
pub fn list_etag<T: Versioned>(records: &[T]) -> String {
    let joined = records.iter().map(version).collect::<Vec<_>>().join("|");
    format!("W/\"{}\"", digest(joined.as_bytes()))
}

/// The `ETag` and `Cache-Control: private, no-cache` headers of a response
/// that carries `etag`. A value that cannot be a header value (the tags built
/// here always can) leaves `ETag` out rather than failing.
#[must_use]
pub fn revalidation_headers(etag: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    if let Ok(value) = HeaderValue::from_str(etag) {
        headers.insert(ETAG, value);
    }
    headers.insert(
        CACHE_CONTROL,
        HeaderValue::from_static(REVALIDATE_CACHE_CONTROL),
    );
    headers
}

/// Every entity-tag (and `*`) in the values of the header `name`, in order,
/// like `lib-crud-py`'s `(?:W/)?"[^"]*"|\*` scan: anything between tags
/// (commas, spaces, junk) is skipped.
fn entity_tags<'a>(headers: &'a HeaderMap, name: &axum::http::HeaderName) -> Vec<&'a str> {
    let mut tags = Vec::new();
    for value in headers.get_all(name).iter().filter_map(|v| v.to_str().ok()) {
        let bytes = value.as_bytes();
        let mut at = 0;
        while at < bytes.len() {
            let quote = if bytes[at..].starts_with(b"W/\"") {
                Some(at + 2)
            } else if bytes[at] == b'"' {
                Some(at)
            } else {
                None
            };
            let closing = quote.and_then(|q| {
                bytes[q + 1..]
                    .iter()
                    .position(|&b| b == b'"')
                    .map(|n| q + 1 + n)
            });
            if let Some(closing) = closing {
                tags.push(&value[at..=closing]);
                at = closing + 1;
            } else {
                if bytes[at] == b'*' {
                    tags.push("*");
                }
                at += 1;
            }
        }
    }
    tags
}

fn is_weak(tag: &str) -> bool {
    tag.starts_with("W/")
}

fn opaque(tag: &str) -> &str {
    tag.strip_prefix("W/").unwrap_or(tag)
}

/// Whether the request's `If-None-Match` matches `etag`: weak comparison
/// (RFC 9110 8.8.3.2), so the `W/` prefix is ignored on both sides, and `*`
/// matches anything.
pub(crate) fn if_none_match(req_headers: &HeaderMap, etag: &str) -> bool {
    let wanted = opaque(etag);
    entity_tags(req_headers, &IF_NONE_MATCH)
        .into_iter()
        .any(|tag| tag == "*" || opaque(tag) == wanted)
}

/// A bodiless `304 Not Modified` when the request's `If-None-Match` matches
/// `etag`, carrying the same `ETag` and `Cache-Control` a full response
/// would; `None` means "send the full response" (with
/// [`revalidation_headers`]).
///
/// Weak comparison (RFC 9110 8.8.3.2) and `*` as in section 13.1.2. The `304`
/// carries only `ETag` and `Cache-Control`: a handler whose full response sets
/// others that a `304` must repeat (RFC 9110 15.4.5, for example `Vary`) adds
/// them to the returned response. Call it
/// from `GET` (and `HEAD`) handlers only: for other methods a failed
/// `If-None-Match` is a `412`, which this does not model, as in
/// `lib-crud-py`.
#[must_use]
pub fn not_modified(req_headers: &HeaderMap, etag: &str) -> Option<Response> {
    if_none_match(req_headers, etag)
        .then(|| (StatusCode::NOT_MODIFIED, revalidation_headers(etag)).into_response())
}

/// Check the request's `If-Match` against `current_etag`, the tag of the
/// record as it is now, before a `PATCH` or `DELETE` of that one record.
///
/// Strong comparison (RFC 9110 8.8.3.2): the tags must be byte-identical and
/// neither weak, so a `W/` tag never matches; `*` matches any existing
/// record. No `If-Match` header passes. A header with no matching tag is
/// [`CrudError::PreconditionFailed`] naming `resource` (`412`).
///
/// Only a single-record write is conditional (a bulk action has no one
/// representation to compare against), and a missing record has no tag, so
/// call this only once the record was found and let a missing one fall
/// through to the write's own `404`. The check and the write are separate
/// statements: this closes the read-modify-write window a client holds open,
/// not a race inside the gap between them.
///
/// # Errors
///
/// [`CrudError::PreconditionFailed`] when `If-Match` is present and does not
/// match.
pub fn check_if_match(
    req_headers: &HeaderMap,
    current_etag: &str,
    resource: &str,
) -> Result<(), CrudError> {
    if !req_headers.contains_key(IF_MATCH) {
        return Ok(());
    }
    let matches = entity_tags(req_headers, &IF_MATCH)
        .into_iter()
        .any(|tag| tag == "*" || (!is_weak(tag) && tag == current_etag));
    if matches {
        Ok(())
    } else {
        Err(CrudError::PreconditionFailed(format!(
            "{resource} was modified since the supplied If-Match"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use chrono::NaiveDate;

    struct Widget {
        id: i32,
        updated_at: NaiveDateTime,
    }

    impl HasId for Widget {
        fn id(&self) -> i32 {
            self.id
        }
    }

    impl Versioned for Widget {
        fn updated_at(&self) -> NaiveDateTime {
            self.updated_at
        }
    }

    fn widget(id: i32, micros: u32) -> Widget {
        Widget {
            id,
            updated_at: NaiveDate::from_ymd_opt(2026, 1, 2)
                .unwrap()
                .and_hms_micro_opt(3, 4, 5, micros)
                .unwrap(),
        }
    }

    fn headers(name: &'static str, values: &[&str]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for value in values {
            map.append(name, HeaderValue::from_str(value).unwrap());
        }
        map
    }

    fn sha(text: &str) -> String {
        digest(text.as_bytes())
    }

    #[test]
    fn digest_is_the_first_32_hex_digits_of_sha256() {
        assert_eq!(
            sha("abc"),
            "ba7816bf8f01cfea414140de5dae2223",
            "SHA-256(abc) begins ba7816bf8f01cfea414140de5dae2223"
        );
    }

    #[test]
    fn record_etag_is_strong_and_hashes_id_and_isoformat_updated_at() {
        assert_eq!(
            record_etag(&widget(7, 0)),
            format!("\"{}\"", sha("7:2026-01-02T03:04:05"))
        );
        assert_eq!(
            record_etag(&widget(7, 123_456)),
            format!("\"{}\"", sha("7:2026-01-02T03:04:05.123456"))
        );
        assert_eq!(
            record_etag(&widget(7, 120)),
            format!("\"{}\"", sha("7:2026-01-02T03:04:05.000120"))
        );
    }

    #[test]
    fn record_etag_changes_with_id_or_updated_at() {
        let base = record_etag(&widget(1, 5));
        assert_eq!(base, record_etag(&widget(1, 5)));
        assert_ne!(base, record_etag(&widget(2, 5)));
        assert_ne!(base, record_etag(&widget(1, 6)));
    }

    #[test]
    fn list_etag_is_weak_and_hashes_the_joined_versions_in_order() {
        let page = [widget(1, 0), widget(2, 0)];
        assert_eq!(
            list_etag(&page),
            format!(
                "W/\"{}\"",
                sha("1:2026-01-02T03:04:05|2:2026-01-02T03:04:05")
            )
        );
        let reversed = [widget(2, 0), widget(1, 0)];
        assert_ne!(list_etag(&page), list_etag(&reversed));
        assert_eq!(list_etag::<Widget>(&[]), format!("W/\"{}\"", sha("")));
    }

    #[test]
    fn revalidation_headers_carry_the_tag_and_private_no_cache() {
        let map = revalidation_headers("\"abc\"");
        assert_eq!(map[ETAG], "\"abc\"");
        assert_eq!(map[CACHE_CONTROL], "private, no-cache");
        assert_eq!(REVALIDATE_CACHE_CONTROL, "private, no-cache");
    }

    #[test]
    fn revalidation_headers_skip_an_unrepresentable_tag() {
        let map = revalidation_headers("bad\ntag");
        assert!(!map.contains_key(ETAG));
        assert_eq!(map[CACHE_CONTROL], "private, no-cache");
    }

    #[test]
    fn entity_tags_scans_tags_stars_and_skips_junk() {
        let map = headers(
            "if-match",
            &[r#""a", W/"b" ,  *, junk, "unterminated"#, r#"x"c""#, "W/"],
        );
        assert_eq!(
            entity_tags(&map, &IF_MATCH),
            vec![r#""a""#, r#"W/"b""#, "*", r#""c""#]
        );
        assert_eq!(
            entity_tags(&HeaderMap::new(), &IF_MATCH),
            Vec::<&str>::new()
        );
        let opaque_bytes =
            HeaderMap::from_iter([(IF_MATCH, HeaderValue::from_bytes(b"\xff\"a\"").unwrap())]);
        assert_eq!(entity_tags(&opaque_bytes, &IF_MATCH), Vec::<&str>::new());
    }

    #[test]
    fn if_none_match_uses_weak_comparison() {
        let strong = "\"abc\"";
        let weak = "W/\"abc\"";
        assert!(if_none_match(&headers("if-none-match", &[strong]), strong));
        assert!(if_none_match(&headers("if-none-match", &[weak]), strong));
        assert!(if_none_match(&headers("if-none-match", &[strong]), weak));
        assert!(if_none_match(&headers("if-none-match", &[weak]), weak));
        assert!(!if_none_match(
            &headers("if-none-match", &["\"other\""]),
            strong
        ));
        assert!(!if_none_match(&HeaderMap::new(), strong));
    }

    #[test]
    fn if_none_match_accepts_star_and_tag_lists_across_header_lines() {
        let etag = "\"abc\"";
        assert!(if_none_match(&headers("if-none-match", &["*"]), etag));
        assert!(if_none_match(
            &headers("if-none-match", &["\"x\", W/\"abc\", \"y\""]),
            etag
        ));
        assert!(if_none_match(
            &headers("if-none-match", &["\"x\"", "\"abc\""]),
            etag
        ));
        assert!(!if_none_match(
            &headers("if-none-match", &["\"x\", \"y\""]),
            etag
        ));
    }

    #[tokio::test]
    async fn not_modified_keeps_etag_and_cache_control_and_has_no_body() {
        let etag = record_etag(&widget(1, 0));
        let response = not_modified(&headers("if-none-match", &[&etag]), &etag).unwrap();
        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(response.headers()[ETAG], etag.as_str());
        assert_eq!(response.headers()[CACHE_CONTROL], "private, no-cache");
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(body.is_empty());
    }

    #[test]
    fn not_modified_is_none_when_nothing_matches() {
        let etag = record_etag(&widget(1, 0));
        assert!(not_modified(&HeaderMap::new(), &etag).is_none());
        assert!(not_modified(&headers("if-none-match", &["\"stale\""]), &etag).is_none());
    }

    #[test]
    fn not_modified_matches_a_list_tag_weakly_and_star() {
        let etag = list_etag(&[widget(1, 0)]);
        assert!(not_modified(&headers("if-none-match", &[&etag]), &etag).is_some());
        assert!(not_modified(&headers("if-none-match", &["*"]), &etag).is_some());
    }

    fn precondition_failed(result: Result<(), CrudError>) -> String {
        result.unwrap_err().to_string()
    }

    #[test]
    fn check_if_match_passes_without_the_header() {
        assert!(check_if_match(&HeaderMap::new(), "\"abc\"", "Widget").is_ok());
    }

    #[test]
    fn check_if_match_passes_on_a_current_strong_tag() {
        let etag = record_etag(&widget(1, 0));
        assert!(check_if_match(&headers("if-match", &[&etag]), &etag, "Widget").is_ok());
    }

    #[test]
    fn check_if_match_passes_on_star_and_on_a_tag_in_a_list() {
        let etag = record_etag(&widget(1, 0));
        assert!(check_if_match(&headers("if-match", &["*"]), &etag, "Widget").is_ok());
        let list = format!("\"old\", {etag}, \"older\"");
        assert!(check_if_match(&headers("if-match", &[&list]), &etag, "Widget").is_ok());
        assert!(check_if_match(&headers("if-match", &["\"old\"", &etag]), &etag, "Widget").is_ok());
    }

    #[test]
    fn check_if_match_fails_with_412_detail_on_a_stale_tag() {
        let etag = record_etag(&widget(1, 0));
        let stale = record_etag(&widget(1, 1));
        let detail = precondition_failed(check_if_match(
            &headers("if-match", &[&stale]),
            &etag,
            "Widget",
        ));
        assert_eq!(
            detail,
            "precondition failed: Widget was modified since the supplied If-Match"
        );
    }

    #[test]
    fn check_if_match_fails_on_a_header_without_any_tag() {
        let etag = record_etag(&widget(1, 0));
        precondition_failed(check_if_match(
            &headers("if-match", &["nonsense"]),
            &etag,
            "W",
        ));
        precondition_failed(check_if_match(&headers("if-match", &[""]), &etag, "W"));
    }

    #[test]
    fn check_if_match_never_matches_a_weak_tag() {
        let etag = record_etag(&widget(1, 0));
        let weak = format!("W/{etag}");
        precondition_failed(check_if_match(&headers("if-match", &[&weak]), &etag, "W"));
        // Even when the current tag is itself weak and byte-identical.
        let weak_current = list_etag(&[widget(1, 0)]);
        precondition_failed(check_if_match(
            &headers("if-match", &[&weak_current]),
            &weak_current,
            "W",
        ));
        // `*` still matches an existing record with a weak current tag.
        assert!(check_if_match(&headers("if-match", &["*"]), &weak_current, "W").is_ok());
    }

    #[tokio::test]
    async fn precondition_failed_renders_as_412_problem_details() {
        let etag = record_etag(&widget(1, 0));
        let response = check_if_match(&headers("if-match", &["\"stale\""]), &etag, "Widget")
            .unwrap_err()
            .into_response();
        assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);
        assert_eq!(
            response.headers()[axum::http::header::CONTENT_TYPE],
            "application/problem+json"
        );
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["title"], "Precondition Failed");
        assert_eq!(body["status"], 412);
        assert_eq!(body["type"], "about:blank");
        assert_eq!(
            body["detail"],
            "Widget was modified since the supplied If-Match"
        );
    }
}
