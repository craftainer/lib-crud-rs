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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::views::FieldError;
    use axum::body::to_bytes;
    use std::sync::Mutex;

    // configure_detail_redaction flips a process-wide static -- serialize
    // any test that touches it, same pattern as config.rs's ENV_LOCK.
    static REDACTION_LOCK: Mutex<()> = Mutex::new(());

    async fn body_json(response: Response) -> serde_json::Value {
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn not_found_maps_to_404_with_problem_json_content_type() {
        let response = AppError::NotFound("hero 1 not found".to_string()).into_response();
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
        assert_eq!(body["detail"], "hero 1 not found");
        assert_eq!(body["type"], "about:blank");
    }

    #[tokio::test]
    async fn unauthorized_maps_to_401_and_sets_www_authenticate() {
        let response = AppError::Unauthorized("bad token".to_string()).into_response();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::WWW_AUTHENTICATE)
                .unwrap(),
            "Bearer"
        );
    }

    #[tokio::test]
    async fn forbidden_maps_to_403_without_www_authenticate() {
        let response = AppError::Forbidden("Insufficient role".to_string()).into_response();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert!(response
            .headers()
            .get(axum::http::header::WWW_AUTHENTICATE)
            .is_none());
    }

    #[tokio::test]
    async fn unprocessable_entity_serializes_field_errors_as_a_list() {
        let response = AppError::UnprocessableEntity(vec![
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
            AppError::ServiceUnavailable("Authentication service unavailable".to_string())
                .into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn internal_error_detail_is_redacted_outside_dev_mode() {
        // into_response() is where redaction actually happens (it reads
        // REDACT_INTERNAL_DETAIL synchronously) -- the guard only needs to
        // span that call, not the later `.await` on the body, so it never
        // crosses an await point (clippy::await_holding_lock).
        let response = {
            let _guard = REDACTION_LOCK.lock().unwrap();
            configure_detail_redaction(Mode::Production);
            let response = AppError::Internal("db pool exhausted: secret-ish detail".to_string())
                .into_response();
            configure_detail_redaction(Mode::Dev);
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
            configure_detail_redaction(Mode::Dev);
            AppError::Internal("db pool exhausted".to_string()).into_response()
        };
        let body = body_json(response).await;
        assert_eq!(body["detail"], "db pool exhausted");
    }

    #[test]
    fn repo_error_converts_to_an_internal_app_error() {
        let repo_err = crate::repositories::RepoError::Backend("connection reset".to_string());
        let app_err: AppError = repo_err.into();
        assert!(matches!(app_err, AppError::Internal(msg) if msg.contains("connection reset")));
    }
}
