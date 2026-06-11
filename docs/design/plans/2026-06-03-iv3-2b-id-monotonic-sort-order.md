# IV3-2b: `last-partition-id` / `last-column-id` Monotonic Assertion + Sort-Order Resolvability — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add NovaRocks-side fail-fast assertions that an Iceberg DDL commit never regresses `last-column-id` or `last-partition-id`, and that a table's `default-sort-order-id` always resolves to an existing sort order (defensive guard against foreign/legacy metadata that would otherwise panic in `default_sort_order()`).

**Architecture:** Add three small pure validation functions to the existing `src/connector/iceberg/commit/validation.rs` module (the canonical home for pre-commit `ensure_*` checks), unit-test each in isolation, then wire them into the schema-evolution commit path (`schema_update.rs`), the partition-spec evolution path (`registry.rs`), and the write-support gate (`validation.rs::ensure_iceberg_write_supported`).

**Tech Stack:** Rust, vendored `iceberg-rust 0.9.0` (`iceberg::spec::TableMetadata` accessors `last_column_id()`, `last_partition_id()`, `sort_order_by_id()`, `default_sort_order_id()`).

**Scope note:** Part A2 of the combined spec `docs/design/specs/2026-06-03-iv3-2-iv3-8-iceberg-metadata-design.md` (IV3-2 §A4 + §A5). Sort-order *DDL* (ALTER … SET SORT ORDER) is not implemented in NovaRocks; §A5 therefore reduces to a resolvability guard, not multi-ALTER evolution management. Prerequisite: Plan 1 (`2026-06-03-iv3-2-snapshot-summary-totals.md`) — same `validation.rs` module, no edit conflicts.

---

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `src/connector/iceberg/commit/validation.rs` | Pre-commit `ensure_*` checks | **Add** 3 functions + unit tests |
| `src/connector/iceberg/catalog/schema_update.rs` | Schema-evolution commit | Call `ensure_column_id_not_regressed` in `SchemaUpdateTxnAction::commit` |
| `src/connector/iceberg/catalog/registry.rs` | Partition-spec evolution commit | Call `ensure_partition_id_not_regressed` after `update_table` |

---

## Task 1: `ensure_column_id_not_regressed`

**Files:**
- Modify: `src/connector/iceberg/commit/validation.rs`

- [ ] **Step 1: Write the failing test**

Add to the `#[cfg(test)] mod tests` block in `validation.rs`:

```rust
    #[test]
    fn column_id_monotonic_ok_and_regression_fails() {
        assert!(super::ensure_column_id_not_regressed(10, 12).is_ok());
        assert!(super::ensure_column_id_not_regressed(10, 10).is_ok());
        let err = super::ensure_column_id_not_regressed(10, 9).unwrap_err();
        assert!(err.contains("last-column-id"), "got: {err}");
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p novarocks --lib commit::validation::tests::column_id_monotonic 2>&1 | tail -15`
Expected: FAIL — `cannot find function ensure_column_id_not_regressed`.

- [ ] **Step 3: Implement the function**

Add to `validation.rs` (top-level, near the other `pub fn ensure_*`):

```rust
/// Fail-fast guard: the new schema's `last-column-id` high-watermark must not
/// regress below the table's current value. Iceberg requires this id be
/// monotonically increasing; a regression would corrupt field-id assignment.
pub fn ensure_column_id_not_regressed(current: i32, next: i32) -> Result<(), String> {
    if next < current {
        return Err(format!(
            "iceberg schema evolution would regress last-column-id from {current} to {next}; \
             column ids must be monotonically increasing"
        ));
    }
    Ok(())
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p novarocks --lib commit::validation::tests::column_id_monotonic 2>&1 | tail -15`
Expected: PASS.

- [ ] **Step 5: Wire into the schema-evolution commit**

In `src/connector/iceberg/catalog/schema_update.rs`, inside `SchemaUpdateTxnAction::commit` (the block that computes `next_last_column_id`), immediately after:

```rust
        let next_last_column_id =
            std::cmp::max(metadata.last_column_id(), new_schema.highest_field_id());
```

add:

```rust
        crate::connector::iceberg::commit::validation::ensure_column_id_not_regressed(
            metadata.last_column_id(),
            next_last_column_id,
        )
        .map_err(|e| iceberg::Error::new(iceberg::ErrorKind::DataInvalid, e))?;
```

- [ ] **Step 6: Run the schema_update tests + commit**

Run: `cargo test -p novarocks --lib catalog::schema_update 2>&1 | tail -15`
Expected: PASS.

```bash
git add src/connector/iceberg/commit/validation.rs src/connector/iceberg/catalog/schema_update.rs
git commit -m "feat(iceberg): assert last-column-id never regresses on schema evolution"
```

---

## Task 2: `ensure_partition_id_not_regressed`

**Files:**
- Modify: `src/connector/iceberg/commit/validation.rs`
- Modify: `src/connector/iceberg/catalog/registry.rs` (partition-spec evolution commit, lines ~595–631)

- [ ] **Step 1: Write the failing test**

Add to `validation.rs` tests:

```rust
    #[test]
    fn partition_id_monotonic_ok_and_regression_fails() {
        assert!(super::ensure_partition_id_not_regressed(1000, 1001).is_ok());
        assert!(super::ensure_partition_id_not_regressed(1000, 1000).is_ok());
        let err = super::ensure_partition_id_not_regressed(1001, 1000).unwrap_err();
        assert!(err.contains("last-partition-id"), "got: {err}");
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p novarocks --lib commit::validation::tests::partition_id_monotonic 2>&1 | tail -15`
Expected: FAIL.

- [ ] **Step 3: Implement the function**

Add to `validation.rs`:

```rust
/// Fail-fast guard: after a partition-spec evolution commit, the reloaded
/// table's `last-partition-id` must not have regressed. iceberg-rust assigns
/// partition field ids during `AddSpec`; this asserts that the committed
/// result preserved monotonicity (catalog round-trip sanity).
pub fn ensure_partition_id_not_regressed(previous: i32, reloaded: i32) -> Result<(), String> {
    if reloaded < previous {
        return Err(format!(
            "iceberg partition-spec evolution regressed last-partition-id from {previous} to \
             {reloaded}; partition field ids must be monotonically increasing"
        ));
    }
    Ok(())
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p novarocks --lib commit::validation::tests::partition_id_monotonic 2>&1 | tail -15`
Expected: PASS.

- [ ] **Step 5: Wire into the partition-spec evolution commit**

In `src/connector/iceberg/catalog/registry.rs`, the partition alter path currently reads the old metadata and commits:

```rust
    let metadata = table.metadata();
    let base_default_spec_id = metadata.default_partition_spec_id();
    // ...
    block_on_iceberg(async { catalog.update_table(commit).await })
        .map_err(|e| format!("alter iceberg partition spec runtime failed: {e}"))?
        .map_err(|e| format!("alter iceberg partition spec failed: {e}"))?;
```

Capture the old `last_partition_id` before the commit (add right after `let metadata = table.metadata();`):

```rust
    let prev_last_partition_id = metadata.last_partition_id();
```

and replace the discard-the-result commit with one that binds the updated table and asserts:

```rust
    let updated = block_on_iceberg(async { catalog.update_table(commit).await })
        .map_err(|e| format!("alter iceberg partition spec runtime failed: {e}"))?
        .map_err(|e| format!("alter iceberg partition spec failed: {e}"))?;
    crate::connector::iceberg::commit::validation::ensure_partition_id_not_regressed(
        prev_last_partition_id,
        updated.metadata().last_partition_id(),
    )?;
```

(`Catalog::update_table` returns `Result<Table>`; `updated.metadata()` is the post-commit `TableMetadata`. If the surrounding code shape differs slightly, the invariant is: bind the `update_table` result as `updated` and assert against `prev_last_partition_id`.)

- [ ] **Step 6: Run partition-spec tests + commit**

Run: `cargo test -p novarocks --lib partition_spec 2>&1 | tail -15` and `cargo test -p novarocks --lib catalog::registry 2>&1 | tail -15`
Expected: PASS.

```bash
git add src/connector/iceberg/commit/validation.rs src/connector/iceberg/catalog/registry.rs
git commit -m "feat(iceberg): assert last-partition-id never regresses on spec evolution"
```

---

## Task 3: `ensure_default_sort_order_resolvable`

**Files:**
- Modify: `src/connector/iceberg/commit/validation.rs`

- [ ] **Step 1: Write the failing test**

Add to `validation.rs` tests (this uses a real `Table`; reuse the existing test-table builder in this module — the file already constructs `Table` fixtures for the other `ensure_*` tests; mirror that. The assertion is on the helper given a metadata whose `default_sort_order_id` resolves vs not):

```rust
    #[test]
    fn default_sort_order_resolvable_ok_for_unsorted_table() {
        // The default unsorted order (id 0) is always present in valid metadata.
        let table = test_table_unpartitioned(); // existing helper in this module
        assert!(super::ensure_default_sort_order_resolvable(&table).is_ok());
    }
```

(If the existing test helper has a different name, use whichever builder the other `ensure_*` tests in this file already call to get a `Table`.)

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p novarocks --lib commit::validation::tests::default_sort_order_resolvable 2>&1 | tail -15`
Expected: FAIL — `cannot find function ensure_default_sort_order_resolvable`.

- [ ] **Step 3: Implement the function**

Add to `validation.rs` (mirrors the existing `ensure_no_variant_in_sort_order` style, which already reads `metadata.default_sort_order()`):

```rust
/// Fail-fast guard: the table's `default-sort-order-id` must reference an
/// existing sort order. iceberg-rust's `TableMetadata::default_sort_order()`
/// panics if the id is dangling; this surfaces a clean error instead, e.g.
/// for foreign/legacy metadata with an inconsistent sort-order set.
pub fn ensure_default_sort_order_resolvable(table: &Table) -> Result<(), String> {
    let metadata = table.metadata();
    let id = metadata.default_sort_order_id();
    if metadata.sort_order_by_id(id).is_none() {
        return Err(format!(
            "iceberg table default-sort-order-id {id} does not reference any existing sort order"
        ));
    }
    Ok(())
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p novarocks --lib commit::validation::tests::default_sort_order_resolvable 2>&1 | tail -15`
Expected: PASS.

- [ ] **Step 5: Wire into the write-support gate**

In `validation.rs`, inside `pub fn ensure_iceberg_write_supported(table: &Table) -> Result<IcebergWriteMode, String>`, add a call near the other `ensure_*` guards it already runs (e.g. alongside `ensure_no_variant_in_sort_order(table)?;` if present, otherwise before the final `Ok(...)`):

```rust
    ensure_default_sort_order_resolvable(table)?;
```

- [ ] **Step 6: Run the validation + a write smoke test + commit**

Run: `cargo test -p novarocks --lib commit::validation 2>&1 | tail -20`
Expected: PASS.

```bash
git add src/connector/iceberg/commit/validation.rs
git commit -m "feat(iceberg): guard default-sort-order-id resolvability before writes"
```

---

## Task 4: fmt/clippy sweep

- [ ] **Step 1:** Run `cargo fmt && cargo clippy -p novarocks --lib 2>&1 | tail -20`
Expected: no fmt diff; no new clippy warnings.

- [ ] **Step 2:** Commit any fmt fixes:

```bash
git add -A && git commit -m "chore(iceberg): fmt after id/sort-order guards" || echo "nothing to commit"
```

---

## Self-Review (completed during planning)

**Spec coverage (vs design §4 A4/A5 / §11 IV3-2 #4/#5):**
- §A4 `last-partition-id`/`last-column-id` monotonic, fail-fast → Tasks 1 & 2 ✓ (acceptance #4)
- §A5 sort-order consistency → Task 3 `ensure_default_sort_order_resolvable` ✓; **scope-reduced** because sort-order DDL does not exist in NovaRocks (no multi-ALTER path to keep coherent) — documented in the scope note. Acceptance #5 ("multi-ALTER sort-order metadata consistent") is vacuously satisfied today and re-covered if/when sort-order DDL lands.

**Placeholder scan:** The only soft references are "the existing test-table builder in this module" (Task 3 Step 1) and the precise call-site shape in `registry.rs` (Task 2 Step 5) — both resolved by the executing agent against the open file; the function bodies, signatures, and wiring instructions are concrete.

**Type/name consistency:** `ensure_column_id_not_regressed(i32, i32)`, `ensure_partition_id_not_regressed(i32, i32)`, `ensure_default_sort_order_resolvable(&Table)` — names and signatures referenced identically at definition, test, and call sites.
