# 0010. A response cache for stats and predict

## Status

Accepted. Mirrors `lib-crud-py`'s `StatsCache` (`interfaces/cache.py`, `crud_stats.cache_key`
and the `X-Stats-Cache` handling in `crud_router.py`).

## Context

`lib-crud-py` lets a host pass a `stats_cache` to its router factories: `/stats` and
`/predict` bodies are then served from a shared cache keyed by route, query, response schema
and ownership scope, and invalidated per resource after writes. The Python router calls the
cache itself. This crate has no routers or stats handlers (the host reads the data), so the
feature is a building block a host's handlers call.

## Decision

A new module `stats_cache` (no new dependency: `async-trait`, `sha2`, `serde_json` were already
used):

- `trait StatsCache: Send + Sync` (async via `async_trait`): `get(resource, key) ->
  Result<Option<Value>, CacheError>`, `set(resource, key, Value)`, `invalidate(resource)`.
  Bodies are `serde_json::Value`, not bytes: the host serialises once, when it responds.
- `CacheError` is a plain message type. It converts into `CrudError::Internal`, so a handler
  returning `CrudError` can use `?`. Cache errors propagate to the handler, as in Python; they
  are not swallowed as misses.
- `MemoryStatsCache`: process-local, per-entry TTL, 60 s by default, `with_ttl(Duration)`.
  For one instance, tests and development; a shared store is the host's own `StatsCache`.
- `CachedStats { cache: Option<Arc<dyn StatsCache>>, resource }` with
  `get_or_build(key, build) -> Result<(Value, CacheState), E>` where `E: From<CacheError>`
  and `build` is `FnOnce() -> impl Future<Output = Result<Value, E>>`. `CacheState` (`#[non_exhaustive]`, so a state can be added in a minor release) is `Hit`,
  `Miss` or `Bypass` (no cache configured; `CachedStats::uncached`), with `as_str()` for an
  `X-Stats-Cache`-style header. `invalidate()` drops the resource's entries.
- `cache_key(kind, schema_id, &CacheScope, &BTreeMap<String, Value>) -> String`: SHA-256 (hex)
  of sorted JSON over those four parts. `CacheScope` is `Shared` or `Owner { field, value }`;
  the owner value is hashed inside the material. The schema id is passed explicitly (Rust has
  no `module.qualname`). Keys are behaviour-equivalent to Python's (same inputs separate and
  group the same way), not byte-identical; nothing shares a cache across the two servers.

## Consequences

- Additive; a minor release.
- Nothing is applied automatically: the host builds the key from its normalised query, builds
  the body inside `get_or_build`, and calls `invalidate` after a write. Setting a response
  header from `CacheState` is the host's too.
- Only non-per-user aggregates belong in the cache; an owner-scoped resource must pass
  `CacheScope::Owner` so owners never share an entry.
- `MemoryStatsCache` has no size bound; expired entries are dropped on any `set`, when
  read, and when the resource is invalidated.
- Differences from Python: typed `Value` bodies, an explicit schema id and scope, `Bypass` as a
  state of its own (Python omits the header), and the in-memory implementation shipped here
  (Python's Redis one lives in the template).
