//! RFC 9457 Problem Details error responses -- port of `problem_details.py`.
//! `AppError` is the single error type every controller returns; its
//! `IntoResponse` impl is the one place that renders
//! `application/problem+json`, matching `register_problem_handlers(app)`
//! being called once from `main.py`.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

use crate::config::Mode;
use crate::views::FieldError;

/// RFC 9457 body shape -- `type`/`title`/`status`/`detail`/`instance`.
#[derive(Debug, Serialize)]
struct ProblemDetail {
    #[serde(rename = "type")]
    kind: &'static str,
    title: &'static str,
    status: u16,
    detail: ProblemDetail_Detail,
}

// serde_json can't serialize a bare enum untagged as "either a string or a
// list of field errors" without a small wrapper -- this is that wrapper,
// matching `problem_details.py`'s `detail: str | list`.
#[derive(Debug, Serialize)]
#[serde(untagged)]
#[allow(non_camel_case_types)]
enum ProblemDetail_Detail {
    Text(String),
    Fields(Vec<FieldError>),
}

/// The one error type every controller/extractor returns. Deliberately
/// narrow (mirrors the handful of `HTTPException`s the Python routers
/// actually raise) rather than a per-resource error enum.
#[derive(Debug)]
pub enum AppError {
    NotFound(String),
    Unauthorized(String),
    Forbidden(String),
    UnprocessableEntity(Vec<FieldError>),
    ServiceUnavailable(String),
    Internal(String),
}

/// Set once at startup (`main.rs`) so `AppError::Internal`'s `IntoResponse`
/// can redact its detail outside `Mode::Dev` (NFR-0015) without threading
/// `Settings` through every controller.
static REDACT_INTERNAL_DETAIL: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(true);

pub fn configure_detail_redaction(mode: Mode) {
    REDACT_INTERNAL_DETAIL.store(mode != Mode::Dev, std::sync::atomic::Ordering::Relaxed);
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, title, detail): (StatusCode, &'static str, ProblemDetail_Detail) = match self {
            AppError::NotFound(msg) => (
                StatusCode::NOT_FOUND,
                "Not Found",
                ProblemDetail_Detail::Text(msg),
            ),
            AppError::Unauthorized(msg) => (
                StatusCode::UNAUTHORIZED,
                "Unauthorized",
                ProblemDetail_Detail::Text(msg),
            ),
            AppError::Forbidden(msg) => (
                StatusCode::FORBIDDEN,
                "Forbidden",
                ProblemDetail_Detail::Text(msg),
            ),
            AppError::UnprocessableEntity(errors) => (
                StatusCode::UNPROCESSABLE_ENTITY,
                "Unprocessable Content",
                ProblemDetail_Detail::Fields(errors),
            ),
            AppError::ServiceUnavailable(msg) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "Service Unavailable",
                ProblemDetail_Detail::Text(msg),
            ),
            AppError::Internal(msg) => {
                tracing::error!(error = %msg, "unhandled internal error");
                let detail = if REDACT_INTERNAL_DETAIL.load(std::sync::atomic::Ordering::Relaxed) {
                    "Internal Server Error".to_string()
                } else {
                    msg
                };
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal Server Error",
                    ProblemDetail_Detail::Text(detail),
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
            "application/problem+json".parse().unwrap(),
        );
        if status == StatusCode::UNAUTHORIZED {
            response.headers_mut().insert(
                axum::http::header::WWW_AUTHENTICATE,
                "Bearer".parse().unwrap(),
            );
        }
        response
    }
}

impl From<crate::repositories::RepoError> for AppError {
    fn from(err: crate::repositories::RepoError) -> Self {
        AppError::Internal(err.to_string())
    }
}
