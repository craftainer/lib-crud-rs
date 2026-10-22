# models/

Resource-agnostic model-layer contracts only. `HasId` gives a model a stable
integer identity so generic code (`controllers::crud_actions`) can report
which records a bulk action touched without knowing the model's shape.

## Don't

- Add a concrete entity or any resource-specific field or logic.
