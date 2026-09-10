//! The generic half of the View layer: resource-agnostic DTO building
//! blocks. Sits above `generic::models` in the layering order (see
//! `src/README.md`) -- may import `models` types for `From` conversions,
//! never the reverse. Hero's own DTOs live in `crate::hero::views`.

pub mod bulk;
pub mod stats;

/// One field-level validation failure -- rendered into RFC 9457's `detail`
/// as a list, mirroring FastAPI/Pydantic's `RequestValidationError.errors()`
/// shape (`loc`/`msg`) closely enough for a client to key off `field`.
#[derive(Debug, Clone, serde::Serialize)]
pub struct FieldError {
    pub field: &'static str,
    pub msg: String,
}

impl FieldError {
    pub fn new(field: &'static str, msg: impl Into<String>) -> Self {
        Self {
            field,
            msg: msg.into(),
        }
    }
}
