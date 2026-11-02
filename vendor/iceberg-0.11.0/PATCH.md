# NovaRocks patches over apache/iceberg-rust 0.11.0-rc.2

Upstream tag: `v0.11.0-rc.2`
Upstream commit: `859048563bc71a3167d0545cb66756d26c64e4ca`
Upstream repository: https://github.com/apache/iceberg-rust
Normalized package: `iceberg-0.11.0.crate`
Package SHA-256: `83bd584247a59b6ce2d2f6ce08188640977568aff9b7240c4a5e45a1c6214142`

Generation command (Rust/Cargo 1.98.1, from the verified upstream checkout):

```sh
cargo +1.98.1 package --no-verify --exclude-lockfile --offline \
  -p iceberg-property-macro -p iceberg -p iceberg-catalog-rest -p iceberg-catalog-hms
```

`--exclude-lockfile` avoids resolving unrelated unpublished/workspace development
dependencies while preserving Cargo's normalized manifest and original published
source files. Each extracted file was compared byte-for-byte with this package.
The initial source commit is unmodified except for this inventory.

## Dependency policy

Retain upstream registry dependency versions and lints except for the bounded
Arrow/Parquet upstream backport recorded below (ADR-0204). The root
`[patch.crates-io]` selects `iceberg`, `iceberg-catalog-hms` and
`iceberg-property-macro` at version `0.11.0`; the macro has no source patches.
The unpatched REST SDK is a connector dev-only path dependency, outside the
production normal/build dependency closures. Do not vendor runtime/network/Arrow
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
- Upstream status: no tracked upstream issue; rc.2 exposes only individual reference lookup.
- Production dependency: NovaRocks dependency validation, ref preparation, complete branch/table/call retention policy and reclamation protection observe the authoritative full branch/tag reference set.
- Exit: remove when upstream exposes the same complete reference-map accessor, or a reviewed consumer migration removes the need for it.
- Coverage: connector dependency/ref tests, fixed retention graphs and actual reclamation root/freshness tests; V2 standalone core check.

## P7a — missing v3 next-row-id read compatibility

- Purpose: preserve the 0.9 read default of `INITIAL_ROW_ID` when v3 metadata omits `next-row-id`; serialization continues emitting it.
- Files: `src/spec/table_metadata.rs`.
- Upstream status: no tracked upstream issue; rc.2 requires the field.
- Production dependency: reading existing Spark REST v3 metadata without the field.
- Exit: IRU-8 replaces the external semantic-identity compatibility boundary and removes this patch.
- Coverage: migrated `test_table_metadata_v3_missing_next_row_id_defaults_to_initial`; existing v3 metadata serde tests.

## Retired patches

U4 (direct child directory listing) has been removed. Hadoop discovery now
uses NovaRocks' own bounded filesystem helpers; the four former U4 source files
match upstream commit `859048563bc71a3167d0545cb66756d26c64e4ca` byte for byte.
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
  when checked on 2026-10-28, head
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
  retain the rc.2 baseline as provenance.
- U3 and P7a remain unchanged: both only modify `src/spec/table_metadata.rs`.
- [#3535](https://github.com/apache/iceberg-rust/pull/3535) is not backported.
  Disabling Iceberg's Zstd default features would not change the unified
  Zstd 0.13 features while `mysql_common 0.32.4` enables `default`/`legacy`.
  The change is already on the upstream 0.11.x branch and will arrive through
  a coordinated rebaseline.

The page-index evaluator change in #3257 selects all rows when the offset index
is absent. It affects the SDK ArrowReader, which NovaRocks does not use for
production reads; NovaRocks retains its own physical reader.

Deletion and refresh conditions:

1. If upstream merges a rewritten version, resynchronize this backport to the
   merged content and rerun the affected validation.
2. When an upstream official release contains these changes, the Iceberg
   dependency maintainer coordinates a rebaseline of the full family under
   ADR-0174 and deletes this backport section. The expected 0.12 release is on
   a different branch from the recorded rc.2 baseline; a version-only bump is
   insufficient.
3. If upstream closes #3257 without merging it, treat the backport as a local
   patch and return to design review; do not silently retain it.

Coverage: standalone core `cargo check --all-targets`, affected Arrow reader,
Arrow value, page-index evaluator and table-metadata tests, and NovaRocks's
Iceberg connector and native SQL read/write verification. Actual run results
are recorded in the DEP-SEC-5 execution evidence, not inferred from this list.
