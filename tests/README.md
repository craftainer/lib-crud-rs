# tests/

The integration tier. Each file links against the crate's public API only, as
a dependent would, and runs against the devcontainer's backing services.

- `postgres_crud.rs` — the generic layer end to end against Postgres
  (`POSTGRES_*`), through the plain-`sqlx` `widgets` repository in
  `support/`. Each test opens a single-connection pool and a `TEMP` table, so
  tests run in parallel without seeing each other's rows. It includes the
  `expected_version` races (two writers with one version, exactly one wins);
  the single-connection pool serializes the statements, so the race is
  interleaved at the `await` points and a check-then-write implementation
  would let both through.
- `mqtt_events.rs` — the event bus against a real Mosquitto
  (`MQTT_HOST`/`MQTT_PORT`). Needs the `mqtt` feature (`required-features`
  in `Cargo.toml`). It is the only place the persistent-session delivery
  guarantee can be verified, including the MQTT 5.0 session TTL (replay within
  it, session gone after it, unset unchanged, transient unaffected), the
  default one-hour session, shutdown draining events queued on the shared
  connection, `PendingEvents` (only a committed transaction's events arrive)
  and the per-caller stream cap. The dev broker has no TLS listener or
  credentials, so the TLS and login paths are covered by the unit tests in
  `src/events.rs` (settings, file reading, a refused connection), not here.
- `manifest_conformance.rs` — the CRUD manifest conformance fixtures through
  the builder and the route, validated against the I1 JSON Schema. Needs no
  backing service.
- `stats_cache.rs` — the stats/predict response cache through the public API
  (hit, miss, bypass, per-owner keys, invalidation, TTL, a failing cache).
  Needs no backing service.
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
