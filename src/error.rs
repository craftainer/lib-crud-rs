//! The error type the generic CRUD layer returns, and its RFC 9457
//! (`application/problem+json`) rendering.
//!
//! [`CrudError`] is deliberately narrow: only the failures the generic layer
//! itself can produce. A host application keeps its own error type (for
//! example one that also has `Unauthorized`/`Forbidden`) and converts with
//! `From<CrudError>`; or it returns [`CrudError`] straight from a handler,
//! since it implements [`IntoResponse`].

use std::sync::atomic::{AtomicBool, Ordering};

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

use crate::repositories::RepoError;
use crate::views::FieldError;

/// RFC 9457 body shape: `type`/`title`/`status`/`detail`.
#[derive(Debug, Serialize)]
struct ProblemDetail {
    #[serde(rename = "type")]
    kind: &'static str,
    title: &'static str,
    status: u16,
    detail: Detail,
}

/// `detail` is either a message or a list of field errors.
#[derive(Debug, Serialize)]
#[serde(untagged)]
enum Detail {
    Text(String),
    Fields(Vec<FieldError>),
}

/// A failure of the generic CRUD layer.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CrudError {
    /// The addressed record does not exist (HTTP 404).
    #[error("not found: {0}")]
    NotFound(String),
    /// The request was well-formed but invalid, with one entry per offending
    /// field (HTTP 422).
    #[error("unprocessable entity: {} field error(s)", .0.len())]
    UnprocessableEntity(Vec<FieldError>),
    /// A backing service (for example the event broker) is unavailable
    /// (HTTP 503).
    #[error("service unavailable: {0}")]
    ServiceUnavailable(String),
    /// The caller is over a limit the layer enforces, for example the cap on
    /// anonymous event streams (HTTP 429).
    #[error("too many requests: {0}")]
    TooManyRequests(String),
    /// Anything else, typically a storage failure (HTTP 500). The detail is
    /// redacted in the response unless [`configure_detail_redaction`] turned
    /// redaction off.
    #[error("internal error: {0}")]
    Internal(String),
}

static REDACT_INTERNAL_DETAIL: AtomicBool = AtomicBool::new(true);

// configure_detail_redaction flips a process-wide static -- serialize any
// test, in any module, that touches it.
#[cfg(test)]
pub(crate) static REDACTION_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Choose whether [`CrudError::Internal`] responses hide their message.
///
/// Redaction is on by default. A host calls this once at startup, typically
/// with `false` only in development, so internal detail never reaches a
/// production client.
pub fn configure_detail_redaction(redact: bool) {
    REDACT_INTERNAL_DETAIL.store(redact, Ordering::Relaxed);
}

/// `detail` when [`configure_detail_redaction`] has turned redaction off,
/// otherwise the generic `hidden` text: for a detail that comes from
/// infrastructure (a broker address, a driver message) rather than from the
/// caller's own request.
#[must_use]
pub fn redacted_detail(detail: String, hidden: &str) -> String {
    if REDACT_INTERNAL_DETAIL.load(Ordering::Relaxed) {
        hidden.to_string()
    } else {
        detail
    }
}

impl IntoResponse for CrudError {
    fn into_response(self) -> Response {
        let (status, title, detail) = match self {
            Self::NotFound(msg) => (StatusCode::NOT_FOUND, "Not Found", Detail::Text(msg)),
            Self::UnprocessableEntity(errors) => (
                StatusCode::UNPROCESSABLE_ENTITY,
                "Unprocessable Content",
                Detail::Fields(errors),
            ),
            Self::ServiceUnavailable(msg) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "Service Unavailable",
                Detail::Text(msg),
            ),
            Self::TooManyRequests(msg) => (
                StatusCode::TOO_MANY_REQUESTS,
                "Too Many Requests",
                Detail::Text(msg),
            ),
            Self::Internal(msg) => {
                tracing::error!(error = %msg, "unhandled internal error");
                let detail = redacted_detail(msg, "Internal Server Error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal Server Error",
                    Detail::Text(detail),
                )
            }
        };

        let body = ProblemDetail {
            kind: "about:blank",
            title,
            status: status.as_u16(),
            detail,
        };
        let mut response = (status, axum::Json(body)).into_response();
        response.headers_mut().insert(
            axum::http::header::CONTENT_TYPE,
            axum::http::HeaderValue::from_static("application/problem+json"),
        );
        response
    }
}

impl From<RepoError> for CrudError {
    fn from(err: RepoError) -> Self {
        Self::Internal(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::REDACTION_LOCK;
    use super::*;
    use axum::body::to_bytes;

    async fn body_json(response: Response) -> serde_json::Value {
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn not_found_maps_to_404_with_problem_json_content_type() {
        let response = CrudError::NotFound("widget 1 not found".to_string()).into_response();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CONTENT_TYPE)
                .unwrap(),
            "application/problem+json"
        );
        let body = body_json(response).await;
        assert_eq!(body["title"], "Not Found");
        assert_eq!(body["status"], 404);
        assert_eq!(body["detail"], "widget 1 not found");
        assert_eq!(body["type"], "about:blank");
    }

    #[tokio::test]
    async fn unprocessable_entity_serializes_field_errors_as_a_list() {
        let response = CrudError::UnprocessableEntity(vec![
            FieldError::new("name", "must be 1-200 characters"),
            FieldError::new("powers", "must contain at least one power"),
        ])
        .into_response();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = body_json(response).await;
        let detail = body["detail"].as_array().unwrap();
        assert_eq!(detail.len(), 2);
        assert_eq!(detail[0]["field"], "name");
        assert_eq!(detail[1]["field"], "powers");
    }

    #[tokio::test]
    async fn service_unavailable_maps_to_503() {
        let response =
            CrudError::ServiceUnavailable("Authentication service unavailable".to_string())
                .into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn too_many_requests_maps_to_429() {
        let response = CrudError::TooManyRequests("slow down".to_string()).into_response();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(body_json(response).await["detail"], "slow down");
    }

    #[test]
    fn redacted_detail_follows_the_redaction_switch() {
        let _guard = REDACTION_LOCK.lock().unwrap();
        configure_detail_redaction(true);
        assert_eq!(redacted_detail("secret".to_string(), "hidden"), "hidden");
        configure_detail_redaction(false);
        assert_eq!(redacted_detail("secret".to_string(), "hidden"), "secret");
    }

    #[tokio::test]
    async fn internal_error_detail_is_redacted_outside_dev_mode() {
        // into_response() is where redaction actually happens (it reads
        // REDACT_INTERNAL_DETAIL synchronously) -- the guard only needs to
        // span that call, not the later `.await` on the body, so it never
        // crosses an await point (clippy::await_holding_lock).
        let response = {
            let _guard = REDACTION_LOCK.lock().unwrap();
            configure_detail_redaction(true);
            let response = CrudError::Internal("db pool exhausted: secret-ish detail".to_string())
                .into_response();
            configure_detail_redaction(false);
            response
        };
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = body_json(response).await;
        assert_eq!(body["detail"], "Internal Server Error");
    }

    #[tokio::test]
    async fn internal_error_detail_is_shown_in_dev_mode() {
        let response = {
            let _guard = REDACTION_LOCK.lock().unwrap();
            configure_detail_redaction(false);
            CrudError::Internal("db pool exhausted".to_string()).into_response()
        };
        let body = body_json(response).await;
        assert_eq!(body["detail"], "db pool exhausted");
    }

    #[test]
    fn repo_error_converts_to_an_internal_app_error() {
        let repo_err = RepoError::Backend("connection reset".to_string());
        let app_err: CrudError = repo_err.into();
        assert!(matches!(app_err, CrudError::Internal(msg) if msg.contains("connection reset")));
    }
}
