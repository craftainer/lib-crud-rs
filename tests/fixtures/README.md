# tests/fixtures/

Data the tests read; not a test target.

- `manifest/` and `schemas/crud-manifest/v1.json`: the CRUD manifest v1
  contract (I1) and its shared conformance fixtures, copied unchanged from
  [`lib-crud-py`](https://github.com/craftainer/lib-crud-py) at commit
  `475d441` (`main` after v0.2.0; the schema and fixtures are identical to
  v0.2.0's). `server.json` is the setup, each `<case>.request.json` a
  caller and `<case>.manifest.json` the manifest that caller must get.
  `tests/manifest_conformance.rs` runs them; see
  [ADR 0004](../../docs/adrs/0004-crud-manifest.md) for where Rust differs.

## Do

- Update these files only by copying them again from `lib-crud-py` and
  changing the commit above.
