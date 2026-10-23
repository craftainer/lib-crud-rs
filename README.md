# lib-crud-rs

> [!NOTE]
> This repository is vibe coded with [Claude](https://claude.com/product/claude-code).

`craftainer-crud` (imported as `craftainer_crud`): the generic async CRUD
layer for [Axum](https://github.com/tokio-rs/axum) services, extracted from
[`template-axum`](https://github.com/craftainer/template-axum). It is an
instance of
[`template-rust-crate`](https://github.com/craftainer/template-rust-crate)
(so it gets the devcontainer, CI, release workflow and template sync from
there); see [`docs/TEMPLATE.md`](docs/TEMPLATE.md) for the conventions shared
by every instance and
[`docs/adrs/0002-extract-the-generic-crud-layer.md`](docs/adrs/0002-extract-the-generic-crud-layer.md)
for why the crate exists and where its boundary is.

The crate is the resource-agnostic half of a CRUD service. A host application
supplies one `Repository` implementation per resource (any storage) and the
crate supplies the rest:

- `crud::CrudService`, a thin service over any repository;
- `repositories::filtering` and `controllers::crud_query`, the filter and
  sort vocabulary and its query-string parser;
- `controllers::crud_actions`, list-or-get, update and delete with bulk
  actions and a size cap;
- `controllers::crud_stats` and `views::stats`, building blocks for stats and
  prediction endpoints;
- `events` and `controllers::crud_events`, a publish/subscribe bus and its
  Server-Sent Events stream;
- `manifest`, the CRUD manifest (contract I1): register a
  `ResourceDescriptor` per resource, mount `manifest_router` (caller-filtered,
  weak `ETag`) and `api_catalog_router` (`/.well-known/api-catalog`);
- `error::CrudError`, the error every one of them returns, rendered as
  `application/problem+json`.

A host keeps its own error type and converts with `From<CrudError>`.

The query and bulk behaviour follows
[`lib-crud-py`](https://github.com/craftainer/lib-crud-py), the server source
of truth for the SDKs; [ADR 0003](docs/adrs/0003-match-lib-crud-py-and-harden-events.md)
lists where and why. Things a host needs to know:

- Extract query params as `Query<Vec<(String, String)>>` and pass the slice to
  `parse_filters`/`parse_sort`: clauses and errors keep the request's order.
- `?id=` together with filters is a `422` (Python silently ignores the
  filters; see the ADR).
- The bulk cap is a check followed by a write, not one atomic step.
- Event streams use server-minted, per-resource subscriber ids, cap the
  streams that start without one, answer `503` when the broker is down and
  `429` over the cap. Pass `EventBus::with_subscriber_secret` the same secret on
  every instance (and across restarts) if clients should resume. Delivery
  covers events that reached the broker; a publish can still be dropped before
  that (see `events`).
- The forecast regresses on bucket time (a gap in the series counts), the
  same formula as `lib-crud-py`.
- The manifest is built from what the host registers; see
  [ADR 0004](docs/adrs/0004-crud-manifest.md) for where it can differ from
  `lib-crud-py` (no `regex` operator, no enum or date field kinds, bulk
  event names).
- A repository must honour the `owner_id` equality clause that bulk actions
  append (see `Repository`).

## Features

- `mqtt` (off by default): `EventBus::connect`, an MQTT-backed bus with
  persistent subscriber sessions. Without it only the in-memory bus
  (`EventBus::mock`) exists.

## Development

Open the repository in the devcontainer: it starts Postgres and Mosquitto,
which the integration tests need (`tests/README.md`). Then run
`prek run --all-files --hook-stage manual`; that runs `rustfmt`,
`clippy -D warnings`, the tests with a 100% line-coverage gate, `cargo-deny`
and the template-sync manifest check.

## Publishing to crates.io

`make publish-packages` is a no-op (with a `::warning::` in the log) until you opt in, per repository, by setting the
repository variable `CARGO_PUBLISH` to any non-empty value (Settings,
Secrets and variables, Actions, Variables). The version lives only in the
git tag `release.yml` computes; `Cargo.toml` stays at `0.0.0` and
`make build` stamps `RELEASE_VERSION` into the build without committing it.

crates.io has no "pending publisher": a trusted publisher can only be added
to a crate that already exists. So the first release of each crate uses a
short-lived bootstrap token, and trusted publishing takes over after it:

1. Create a crates.io API token scoped to the new crate name
   (`publish-new` only for the first release) with a short expiry, and store
   it as the repository secret `CARGO_REGISTRY_TOKEN`.
2. Set `CARGO_PUBLISH=1` and run the release workflow. `make publish-packages` sees
   the token and publishes with it.
3. On crates.io, open the crate's Settings, Trusted Publishing, and add a
   GitHub publisher: the owner, the repository, the workflow `release.yml`,
   and no environment.
4. Delete the `CARGO_REGISTRY_TOKEN` secret and revoke the token. From the
   next release, `make publish-packages` finds no token and exchanges the job's
   GitHub OIDC identity for a 30-minute crates.io token, publishes with it
   and revokes it.

## License

Licensed under the [GNU General Public License v3.0](LICENSE).
