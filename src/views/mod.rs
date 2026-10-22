//! The generic half of the view layer: resource-agnostic DTO building
//! blocks. A host's own per-resource DTOs live in the host.

pub mod bulk;
pub mod stats;

/// One field-level validation failure -- rendered into RFC 9457's `detail`
/// as a list, so a client can key off `field`.
#[derive(Debug, Clone, serde::Serialize)]
pub struct FieldError {
    /// The offending field.
    pub field: &'static str,
    /// What is wrong with it.
    pub msg: String,
}

impl FieldError {
    /// A failure of `field` described by `msg`.
    pub fn new(field: &'static str, msg: impl Into<String>) -> Self {
        Self {
            field,
            msg: msg.into(),
        }
    }
}
