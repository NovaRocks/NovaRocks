# NovaRocks patches over apache/iceberg-rust 0.11.0

Upstream tag: `v0.11.0`
Upstream commit: `20eb0feddac8f40bb9e9108a0744b7f356d69480`
Upstream repository: https://github.com/apache/iceberg-rust
Registry package: `iceberg-catalog-hms-0.11.0.crate`
Registry SHA-256: `b377f757ed01d7e08bd4dfc0dfdfecaf00d50576b8942c205a6565ac1b2df172`

The original registry archive was checked against the index/API checksum and
its VCS commit/path. Source differences were checked against the rc.2-to-release
upstream changes; normalized manifests and package-generated files were checked
separately. The original archive was not regenerated from Git.

The published Cargo.lock, Cargo.toml.orig and license inventory retain their
release bytes. Standalone checks use a candidate source copy and a separate
validation lock; the root workspace alone owns the production dependency lock.
Do not format vendor sources. Design: ADR-0215
(docs/adr/ADR-0215-released-iceberg-package-provenance.md).

## Dependency policy

Retain upstream registry dependency versions and lints. The root
`[patch.crates-io]` selects `iceberg` and `iceberg-catalog-hms` at version `0.11.0`.
The unmodified property macro and dev-only REST SDK use crates.io; the REST SDK
is outside production normal/build dependency closures. Do not vendor runtime/network/Arrow
crates or add resource accounting hooks. Only the approved protocol/API patch
inventory may diverge.

## Validation

- Compare the initial source commit against the recorded package.
- Use workspace dependency patches for consumer checks and tests.
- Independently check this crate with sibling Iceberg patches when resolvable;
  record unavailable upstream development dependencies as a validation gap.

## Patch inventory

### H3: typed missing-table result

- Purpose: preserve `load_table`'s exact `O2(NoSuchObjectException)` mapping to
  `TableNotFound`; other declared Thrift exceptions and transport failures retain
  `Unexpected`. The upstream runtime/KMS table construction is unchanged.
- Files: `src/catalog.rs` (`HmsCatalog::load_table`).
- Upstream status: no PR/issue tracked. 0.11.0 still classifies this load error as
  `Unexpected`; `get_namespace`'s corresponding namespace mapping is already
  upstream and has no local patch.
- Production dependency/owner: NovaRocks Iceberg connector's HMS catalog control
  and load paths distinguish explicit absence from unavailable catalog service by
  `ErrorKind`; the Iceberg connector maintainers own this compatibility contract.
- Exit: remove in an HMS upstream-convergence follow-up owned by those maintainers
  once the adopted upstream load API preserves the typed absence/transport
  distinction and the consumer coverage passes without this patch. No accepted
  IRU subtask currently claims that deletion.
- Coverage: existing NovaRocks `catalog::error` unit tests cover the consumer's
  `TableNotFound` versus `Unexpected` classification; `iceberg-hms` SQL covers
  normal table loading. Standalone crate checking validates the match against
  the generated Thrift exception vocabulary. There is no isolated RPC-absence
  test in this port; no new test hooks or mock classification helper are added.

### H4: classify HMS table listings from entity parameters

- Purpose: preserve serial `get_table_objects_by_name` batches of at most 100
  names on the existing client. Emit only entities whose `parameters["table_type"]`
  is `iceberg` ignoring ASCII case, validate the entire returned batch before
  projection, and omit entities that disappeared after the initial name list.
  This is an RPC item-count bound; it does not claim allocation or response-byte
  accounting. 0.11.0's namespace-existence precheck remains in place.
- Files: `src/catalog.rs` (`HmsCatalog::list_tables`), `src/lib.rs`,
  `src/table_projection.rs` (copied unchanged from the 0.9 vendor).
- Upstream status: no upstream PR/issue tracked; NovaRocks M07 PR #1173 introduced
  this classification. 0.11.0's `get_all_tables` still returns unclassified names.
- Production dependency/owner: NovaRocks Iceberg connector's catalog listing
  contract; Iceberg connector/MEM-1 maintainers own classification and listing
  admission. No metadata-file loads, retries, client owners, view capability,
  paging capability, or allocation hooks are added.
- Exit: the MEM-1 follow-up that replaces the HMS listing boundary owns deletion,
  after its replacement classifies actual HMS entities with the same omission,
  validation, and error semantics and passes the existing coverage. Alternatively,
  remove through that owner when adopted upstream provides the same behavior.
- Coverage: the nine existing `table_projection_tests` are carried unchanged;
  NovaRocks connector tests compile this source through the existing `#[path]`
  seam (the new path is wired in T6). They cover classification, historical case,
  missing entities, ordering, identity, duplicates, and the 100-name item bound.
  Existing `hms_listing_observer_tests` cover listing admission/exit ownership.
