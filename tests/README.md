# tests/

The integration tier. Each file links against the crate's public API only, as
a dependent would, and runs against the devcontainer's backing services.

- `postgres_crud.rs` — the generic layer end to end against Postgres
  (`POSTGRES_*`), through the plain-`sqlx` `widgets` repository in
  `support/`. Each test opens a single-connection pool and a `TEMP` table, so
  tests run in parallel without seeing each other's rows.
- `mqtt_events.rs` — the event bus against a real Mosquitto
  (`MQTT_HOST`/`MQTT_PORT`). Needs the `mqtt` feature (`required-features`
  in `Cargo.toml`). It is the only place the persistent-session delivery
  guarantee can be verified.
- `manifest_conformance.rs` — the CRUD manifest conformance fixtures through
  the builder and the route, validated against the I1 JSON Schema. Needs no
  backing service.
- `fixtures/` — vendored data (the manifest contract); see its README.
- `support/` — the shared fixtures (not a test target).

Unit tests live next to the code they test.

## Do

- Use unique topics and table names (or temp tables) per test.
- Keep fixtures neutral: no application's resource names.
- Cover each generic function from one test binary: the 100% line gate takes
  the best single instantiation of a generic function, not the union of
  several, so a failure-path test must use the same repository type as the
  success-path tests.
