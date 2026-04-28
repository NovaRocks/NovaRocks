# NovaRocks patches over upstream iceberg-rust 0.9.0

Upstream source: https://crates.io/crates/iceberg/0.9.0

These patches are the minimum required to let NovaRocks implement custom
Transaction actions for INSERT OVERWRITE and DELETE flows that iceberg-rust 0.9
does not yet ship as built-in actions (`overwrite_files`, `row_delta`).

When upstream lands native equivalents — likely in 0.10/0.11 — this whole
vendor directory and the corresponding `[patch.crates-io]` block in the root
`Cargo.toml` should be deleted, and the NovaRocks `OverwriteCommit` and
`RowDeltaCommit` impls (`src/connector/iceberg/commit/{overwrite,row_delta}.rs`)
should be re-pointed at the upstream actions.

Tracked under spec §0.4 / Plan Task 9.

## Patch 1 — `src/transaction/action.rs`

Raise `TransactionAction` trait visibility from `pub(crate)` to `pub` so that
downstream crates can implement the trait.

```diff
- #[async_trait]
- pub(crate) trait TransactionAction: AsAny + Sync + Send {
+ #[async_trait]
+ pub trait TransactionAction: AsAny + Sync + Send {
```

## Patch 2 — `src/catalog/mod.rs`

Raise `TableCommit::builder().build()` visibility from `pub(crate)` to `pub`
so that downstream crates can construct `TableCommit` directly when invoking
`Catalog::update_table` from a custom action.

```diff
- #[builder(build_method(vis = "pub(crate)"))]
+ #[builder(build_method(vis = "pub"))]
  pub struct TableCommit {
```

## Verification after rebase

When bumping the vendored copy to a newer iceberg-rust patch release:

1. `diff -ru` against the new upstream source to confirm only those two lines
   diverge (plus this `PATCH.md` and the inline `// NovaRocks patch:` comments).
2. `cargo build -p novarocks` from the worktree root.
3. `cargo test -p novarocks --lib commit:: -- --nocapture` should still pass.

If upstream changes the surrounding code substantially, re-apply by hand and
update this file.
