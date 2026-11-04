# NovaRocks patches over apache/iceberg-rust 0.11.0

Upstream tag: `v0.11.0`
Upstream commit: `20eb0feddac8f40bb9e9108a0744b7f356d69480`
Upstream repository: https://github.com/apache/iceberg-rust
Registry package: `iceberg-0.11.0.crate`
Registry SHA-256: `25dd5d1aa2fa92d2c9bc426f72bc16717a27eb73255805beb8845afcca31e58d`

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

Retain upstream registry dependency versions and lints except for the bounded
Arrow/Parquet upstream backport recorded below (ADR-0204). The root
`[patch.crates-io]` selects `iceberg` and `iceberg-catalog-hms` at version `0.11.0`.
The unmodified property macro and dev-only REST SDK use crates.io; the REST SDK
is outside production normal/build dependency closures. Do not vendor runtime/network/Arrow
crates or add resource accounting hooks. Only the approved protocol/API patch
inventory and exact upstream dependency-upgrade backports may diverge.
Backports do not authorize new NovaRocks changes inside third-party code.

## Validation

- Compare the initial source commit against the recorded package.
- Use workspace dependency patches for consumer checks and tests.
- Independently check this crate with sibling Iceberg patches when resolvable;
  record unavailable upstream development dependencies as a validation gap.

## Patch inventory

No patches applied in the initial source commit.

## U3 — snapshot reference map accessor

- Purpose: preserve the 0.9 `TableMetadata::refs()` read-only accessor.
- Files: `src/spec/table_metadata.rs`.
- Upstream status: no tracked upstream issue; 0.11.0 exposes only individual reference lookup.
- Production dependency: NovaRocks dependency validation, ref preparation, complete branch/table/call retention policy and reclamation protection observe the authoritative full branch/tag reference set.
- Exit: remove when upstream exposes the same complete reference-map accessor, or a reviewed consumer migration removes the need for it.
- Coverage: connector dependency/ref tests, fixed retention graphs and actual reclamation root/freshness tests; V2 standalone core check.

## P7a — missing v3 next-row-id read compatibility

- Purpose: preserve the 0.9 read default of `INITIAL_ROW_ID` when v3 metadata omits `next-row-id`; serialization continues emitting it.
- Files: `src/spec/table_metadata.rs`.
- Upstream status: no tracked upstream issue; 0.11.0 requires the field.
- Production dependency: reading existing Spark REST v3 metadata without the field.
- Exit: IRU-8 replaces the external semantic-identity compatibility boundary and removes this patch.
- Coverage: migrated `test_table_metadata_v3_missing_next_row_id_defaults_to_initial`; existing v3 metadata serde tests.

## Retired patches

U4 (direct child directory listing) has been removed. Hadoop discovery now
uses NovaRocks' own bounded filesystem helpers; the four former U4 source files
match upstream commit `20eb0feddac8f40bb9e9108a0744b7f356d69480` byte for byte.
The SDK Storage/FileIO interfaces no longer carry a directory extension.


## Upstream backports — Arrow/Parquet 60

Policy: ADR-0204 permits bounded Iceberg dependency-upgrade backports from
upstream. Merged changes are preferred; an unmerged PR is reviewed to the same
standard as a local patch. Stop and use a future upstream baseline if the
backport requires a broad upstream refactor. This permission is specific to the
Iceberg family and does not extend to other vendored crates.

- [#2958](https://github.com/apache/iceberg-rust/pull/2958), merged as
  `665c64e4`: the Arrow 59 dependency step is subsumed by the Arrow 60
  declarations below.
- [#3257](https://github.com/apache/iceberg-rust/pull/3257), open and unmerged
  when checked on 2026-11-04, head
  `0d5eb123495f765ea151e78c8964919d554d4b98`: copy the upstream source changes
  without local modifications in these files:
  - `src/arrow/reader/positional_deletes.rs`
  - `src/arrow/reader/row_filter.rs`
  - `src/arrow/reader/row_lineage.rs`
  - `src/arrow/value.rs`
  - `src/expr/visitors/page_index_evaluator.rs`
- `Cargo.toml`: semantically translate the upstream workspace dependency
  declarations into the normalized package manifest: Arrow
  `arith/array/buffer/cast/ord/schema/select/string` and Parquet use `60.0`;
  development-only `serde_arrow` uses `0.15` with `arrow-60`. Features and lints
  otherwise remain unchanged. `Cargo.toml.orig` and `DEPENDENCIES.rust.tsv`
  retain the official 0.11.0 release as provenance.
- U3 and P7a remain unchanged: both only modify `src/spec/table_metadata.rs`.

The page-index evaluator change in #3257 selects all rows when the offset index
is absent. It affects the SDK ArrowReader, which NovaRocks does not use for
production reads; NovaRocks retains its own physical reader.

Deletion and refresh conditions:

1. If upstream merges a rewritten version, resynchronize this backport to the
   merged content and rerun the affected validation.
2. When an upstream official release contains these changes, the Iceberg
   dependency maintainer coordinates a rebaseline of the full family under
   ADR-0174 and deletes this backport section. The expected 0.12 release is on
   a different branch from the official 0.11.0 baseline; a version-only bump is
   insufficient.
3. If upstream closes #3257 without merging it, treat the backport as a local
   patch and return to design review; do not silently retain it.

Coverage: standalone core `cargo check --all-targets`, affected Arrow reader,
Arrow value, page-index evaluator and table-metadata tests, and NovaRocks's
Iceberg connector and native SQL read/write verification. Actual run results
are recorded in the DEP-SEC-5 execution evidence, not inferred from this list.
