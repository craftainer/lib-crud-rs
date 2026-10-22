//! Resource-agnostic model-layer contracts only: no concrete entity lives
//! here. The lowest layer: it never imports from the other modules.

/// A model with a stable integer identity, so generic code above this layer
/// (for example [`controllers::crud_actions`](crate::controllers::crud_actions))
/// can report which records a bulk action touched without knowing the
/// concrete model's shape.
pub trait HasId {
    /// The record's id.
    fn id(&self) -> i32;
}
