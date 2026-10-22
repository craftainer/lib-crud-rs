//! The generic Model layer: resource-agnostic model-layer contracts only
//! -- no concrete SeaORM entity lives here (Hero's is
//! `crate::hero::models::hero`). Lowest layer besides `config`/`oidc`
//! (see `src/README.md`'s "Layering") -- never imports from
//! `views`/`repositories`/`crud`/`health`/`controllers`, generic or
//! resource-specific.

/// A model with a stable integer identity -- lets generic, resource-
/// agnostic code above this layer (`generic::controllers::crud_actions`)
/// report which records a bulk action touched without needing resource-
/// specific knowledge of the concrete model's shape. Defined here (the
/// lowest layer) rather than in `repositories`/`crud` so a model's own
/// file can implement it without those higher layers needing to be
/// visible to `models` (`src/README.md`'s layering is one-directional).
pub trait HasId {
    fn id(&self) -> i32;
}
