# generic/models/

The generic half of the Model layer (see `src/README.md`'s "Formalizing
the generic-CRUD split"): resource-agnostic model-layer contracts only.
Lowest layer besides `config`/`oidc` — never imports from
`views`/`repositories`/`crud`/`health`/`controllers`.

- `mod.rs` — `HasId`, the trait a model implements to give it a stable
  integer identity, used generically by
  `generic::controllers::crud_actions` to report which records a bulk
  action touched without resource-specific knowledge of the model's
  shape.

No concrete SeaORM entity lives here — that's resource-specific and
lives in e.g. `crate::hero::models`. A new resource's model implements
`HasId` in its own file, without this module changing.

## Don't

- Add a concrete entity or any resource-specific field/logic here — see
  `docs/nfrs/NFR-0004-generic-crud-excludes-resource-logic.md`.
