# IV3-6 PR-4: Variant Path Pushdown — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the IV3-6 PR-4 `VariantPathPushdown` path for Iceberg scans: rewrite eligible typed `variant_get` / `try_variant_get` calls on scan variant columns into synthetic scan slots, carry those path specs through thrift and HDFS lowering, and materialize the synthetic values in the parquet reader. This PR deliberately stops before row-group/page pruning and runtime-filter mapping; those are PR-5.

**Architecture:** The optimizer adds `ScanVariantColumn` descriptors to `ScanNode`, deduplicates identical `(source_column_id, canonical_path, requested_type, strict)` requests, and rewrites the expression to a synthetic `ColumnRef`. Codegen allocates synthetic output slots and attaches structured `TVariantPathColumn` specs to `THdfsScanNode`. Lowering splits these synthetic slots out of physical file columns, ensures the source variant slot is present as an output or hidden read slot, and passes `VariantPathSpec` to `HdfsScanConfig` / `ParquetScanConfig`. The parquet reader evaluates each spec after reading the source variant column: exact shredded typed-value layouts may return a zero-copy/cast-free child array when safe, otherwise it falls back to the same kernel-backed semantics used by `variant_get`. Scan predicates remain normal scan conjuncts and are still evaluated for correctness; pruning is not added in this PR.

**Tech Stack:** Rust; NovaRocks standalone SQL optimizer/codegen/lowering; generated Rust thrift from `idl/thrift/PlanNodes.thrift`; Arrow `RecordBatch` / `ArrayRef`; parquet variant experimental APIs already used by `src/exec/expr/function/variant/variant_get.rs` and `src/formats/parquet/variant_read.rs`; SQL regression runner under `tests/sql-test-runner`.

**Background you must know (verified against current `origin/main` on 2026-06-12):**
- PR-2 is merged in PR #289; `variant_get` / `try_variant_get` are available and the Iceberg commit collector skips variant bounds.
- PR-4 follows the design in `docs/superpowers/specs/2026-06-10-iv3-6-variant-design.md`: run after predicate pushdown and before required-column tagging, add thrift carrier on `THdfsScanNode`, and do no row-group/page pruning until PR-5.
- `ScanNode` already carries scan hints (`dict_columns`) in `src/sql/planner/plan.rs`; those are mirrored through logical/physical conversion and codegen.
- Query rewrite ordering is in `src/sql/optimizer/rewrite/registry.rs`. The new stage belongs after `PredicatePushdownAfterMoveAround` and before `AggregatePushdown` / `TagRequiredColumns`.
- HDFS lowering has hidden-slot precedent in `src/lower/node/hdfs_scan.rs`, including `next_hidden_slot_id` / hidden Iceberg virtual handling. Synthetic variant output slots must not be treated as physical `hive_column_names`.
- Rust thrift is generated at build time from `idl/thrift/*.thrift` by `src/build.rs`; there is no checked-in `src/plan_nodes.rs` to edit.
- Existing parquet variant conversion is in `src/formats/parquet/variant_read.rs`, and whole-column conversion currently happens in `src/formats/parquet/mod.rs` via `convert_variant_columns` before schema normalization.

**Commit policy:** every task ends in its own commit. Commit messages are English.

---

### Task 1: Planner data model and stage registration

**Files:**
- Modify: `src/sql/planner/plan.rs`
- Modify: `src/sql/optimizer/operator.rs` or the file that defines `PhysicalScanOp`
- Modify: `src/sql/optimizer/convert.rs`
- Modify: `src/sql/optimizer/cascades_rules/implement.rs`
- Modify: `src/sql/optimizer/rewrite/rules/mod.rs`
- Modify: `src/sql/optimizer/rewrite/registry.rs`
- Modify tests that instantiate `ScanNode`

- [ ] **Step 1: RED - add model/registry tests first**
  - Add a focused unit test that constructs a `ScanNode` with one `ScanVariantColumn`, converts it through the logical-to-physical path used by `ScanToPhysical`, and asserts the descriptor is preserved.
  - Update `query_pipeline_uses_expected_stage_order_and_rules` to expect a new `VariantPathPushdown` stage after `PredicatePushdownAfterMoveAround` and before `AggregatePushdown`.
  - Run:
    ```bash
    cargo test --lib query_pipeline_uses_expected_stage_order_and_rules -- --nocapture
    cargo test --lib variant_path_scan_descriptor_survives_physical_conversion -- --nocapture
    ```
  - Expected: the stage test fails because the stage is absent; the descriptor test fails to compile because the model is absent.

- [ ] **Step 2: GREEN - add the model and empty rule stage**
  - Add `ScanVariantColumn`:
    - `source_column_id: ColumnId`
    - `source_column: String`
    - `synthetic_column_id: ColumnId`
    - `synthetic_column: String`
    - `canonical_path: String`
    - `requested_type: DataType`
    - `strict: bool`
  - Add `variant_columns: Vec<ScanVariantColumn>` to `ScanNode` and `PhysicalScanOp`.
  - Preserve it in logical/physical conversion, clone/copy helpers, and scan test constructors.
  - Add `variant_path_pushdown` module with a no-op `VariantPathPushdownRule` registered in the requested stage.
  - Update rule-name validation expected lists.

- [ ] **Step 3: GREEN verification**
  - Run:
    ```bash
    cargo test --lib query_pipeline_uses_expected_stage_order_and_rules -- --nocapture
    cargo test --lib variant_path_scan_descriptor_survives_physical_conversion -- --nocapture
    cargo test --lib rewrite_registry_recognizes_migrated_query_rules -- --nocapture
    ```

- [ ] **Step 4: Commit**
  ```bash
  git add src/sql
  git commit -m "feat(optimizer): add variant path scan descriptors"
  ```

---

### Task 2: Optimizer `VariantPathPushdown` rewrite

**Files:**
- Create: `src/sql/optimizer/rewrite/rules/variant_path_pushdown/mod.rs`
- Create: `src/sql/optimizer/rewrite/rules/variant_path_pushdown/rule.rs`
- Modify: `src/sql/optimizer/rewrite/rules/mod.rs`
- Modify: `src/sql/optimizer/rewrite/registry.rs`

- [ ] **Step 1: RED - expression rewrite tests**
  - Add unit tests for:
    - `Filter(Scan)` where predicate is `variant_get(v, '$.a', 'bigint') = 10`: after rewrite, scan output contains one synthetic column and the predicate references that synthetic `ColumnRef`.
    - `Project(Scan)` with the same expression: project item is rewritten to the synthetic `ColumnRef`.
    - identical requests in predicate and projection deduplicate to one synthetic descriptor.
    - unsupported cases stay unchanged: non-Iceberg scan, non-column source, non-literal path/type, two-arg `variant_get`, array-index path, unsupported requested type, and source column not typed as variant/large binary.
  - Run:
    ```bash
    cargo test --lib variant_path_pushdown -- --nocapture
    ```
  - Expected: fail because the rule is no-op or absent.

- [ ] **Step 2: GREEN - implement the rule**
  - Traverse top-down over `Filter`, `Project`, and already-pushed `Scan.predicates`.
  - Match only:
    - function name `variant_get` or `try_variant_get`
    - exactly three arguments
    - arg0 is `ColumnRef` produced directly by an Iceberg scan
    - arg1 and arg2 are string literals
    - canonical path contains only object fields, no array index
    - requested type is one of `boolean`, `bigint`, `double`, `string`, `date`
  - Use analyzer/codegen type mapping already introduced by PR-2 to normalize requested type to Arrow `DataType`.
  - `strict = true` for `variant_get`, `false` for `try_variant_get`.
  - Generate stable synthetic names such as `__nr_var_<source>_<ordinal>`, fresh `ColumnId`, and hidden `OutputColumn` entries.
  - Deduplicate by `(source_column_id, canonical_path, requested_type, strict)`.
  - Replace matched function calls with typed synthetic `ColumnRef`.
  - Leave residual semantics unchanged for all unsupported cases.

- [ ] **Step 3: GREEN verification**
  - Run:
    ```bash
    cargo test --lib variant_path_pushdown -- --nocapture
    cargo test --lib query_pipeline_uses_expected_stage_order_and_rules -- --nocapture
    ```

- [ ] **Step 4: Commit**
  ```bash
  git add src/sql/optimizer src/sql/planner
  git commit -m "feat(optimizer): rewrite variant_get to scan path slots"
  ```

---

### Task 3: EXPLAIN visibility and optimizer plan goldens

**Files:**
- Modify: `src/sql/explain.rs`
- Add: `sql-tests/optimizer/sql/variant_path_pushdown.sql`
- Add/update: `sql-tests/optimizer/result/variant_path_pushdown.result`

- [ ] **Step 1: RED - explain unit and SQL golden**
  - Add a unit test around scan explain formatting that expects verbose/costs/analyze output to include:
    ```text
    variant columns: __nr_var_... := variant_get(v, '$.a', 'bigint')
    ```
  - Add an optimizer SQL test with `-- @explain_contains=variant columns:` and a disabled-rule comparison using:
    ```sql
    SET disable_optimizer_rules = 'VariantPathPushdown';
    ```
  - Run:
    ```bash
    cargo test --lib explain_variant_path_columns -- --nocapture
    cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --suite optimizer --only variant_path_pushdown --mode verify
    ```
  - Expected: fail because explain output/golden are not present yet.

- [ ] **Step 2: GREEN - format variant path scan hints**
  - Add `variant columns:` only for `Verbose`, `Costs`, and `Analyze`, matching the design.
  - Format from the structured `ScanVariantColumn`, not from raw expression debug output.
  - Keep normal scan `Pruned type`, dict, min-max, and RF explain lines unchanged.

- [ ] **Step 3: GREEN verification**
  - Run:
    ```bash
    cargo test --lib explain_variant_path_columns -- --nocapture
    cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --suite optimizer --only variant_path_pushdown --mode verify
    ```

- [ ] **Step 4: Commit**
  ```bash
  git add src/sql/explain.rs sql-tests/optimizer
  git commit -m "test(optimizer): cover variant path pushdown explain output"
  ```

---

### Task 4: Thrift carrier and codegen emission

**Files:**
- Modify: `idl/thrift/PlanNodes.thrift`
- Modify: `src/sql/codegen/fragment_builder.rs`
- Modify: `src/sql/codegen/nodes.rs` only if helper builders need constructor updates

- [ ] **Step 1: RED - codegen/thrift tests**
  - Add a codegen unit test that builds an Iceberg scan with one `ScanVariantColumn`, runs `visit_scan`, and asserts:
    - `THdfsScanNode.variant_path_columns` is populated.
    - the synthetic slot id matches the slot bound to the synthetic `ColumnRef`.
    - the source slot id references the source variant column.
    - `hive_column_names` does not include the synthetic column.
  - Run:
    ```bash
    cargo test --lib visit_scan_emits_variant_path_columns_for_iceberg -- --nocapture
    ```
  - Expected: fail to compile because the thrift field is absent.

- [ ] **Step 2: GREEN - add thrift structs and codegen emission**
  - Add a new thrift struct near `THdfsScanNode`, for example:
    ```thrift
    struct TVariantPathColumn {
      1: optional Types.TSlotId source_slot_id
      2: optional Types.TSlotId output_slot_id
      3: optional string source_column
      4: optional string output_column
      5: optional string canonical_path
      6: optional string requested_type
      7: optional bool strict
    }
    ```
  - Add `29: optional list<TVariantPathColumn> variant_path_columns` to `THdfsScanNode`.
  - In `visit_scan`, allocate source slots as normal and allocate synthetic output slots for `op.variant_columns` without adding them to physical `hive_column_names`.
  - Register synthetic bindings in `ExprScope` by `synthetic_column_id` and synthetic name.
  - Ensure source slots are present when only synthetic columns are required; if the source variant column was pruned out, codegen/lowering must still provide the hidden source read path.

- [ ] **Step 3: GREEN verification**
  - Run:
    ```bash
    cargo test --lib visit_scan_emits_variant_path_columns_for_iceberg -- --nocapture
    cargo build
    ```

- [ ] **Step 4: Commit**
  ```bash
  git add idl/thrift/PlanNodes.thrift src/sql/codegen
  git commit -m "feat(codegen): carry variant path columns on hdfs scans"
  ```

---

### Task 5: HDFS lowering and scan config plumbing

**Files:**
- Modify: `src/lower/node/hdfs_scan.rs`
- Modify: `src/connector/hdfs.rs`
- Modify: `src/formats/mod.rs` or `src/formats/parquet/mod.rs` config plumbing
- Add shared runtime spec module if needed, preferably close to `src/formats/parquet/variant_read.rs`

- [ ] **Step 1: RED - lowering tests**
  - Add unit tests for `lower_hdfs_scan_node`-level parsing:
    - valid `TVariantPathColumn` produces a `VariantPathSpec`.
    - missing source/output slot errors clearly.
    - synthetic output slot is excluded from physical `data_columns`.
    - source variant slot is retained as hidden read input when not in output layout.
  - Run:
    ```bash
    cargo test --lib lower_hdfs_scan_variant_path -- --nocapture
    ```
  - Expected: fail because lowering does not parse the thrift field.

- [ ] **Step 2: GREEN - parse and pass specs**
  - Define `VariantPathSpec` with:
    - source slot/name/field
    - output slot/name/field
    - canonical path
    - requested Arrow type
    - strict flag
  - Parse `hdfs.variant_path_columns` with fail-fast validation.
  - Exclude output synthetic slots from physical data columns.
  - Add a hidden source read slot using the existing hidden-slot pattern when the source variant column is not already output.
  - Add `variant_path_columns` to `HdfsScanConfig` and `ParquetScanConfig`.
  - Keep `min_max_conjuncts` and runtime-filter mappings unchanged; PR-5 owns pruning/RF mapping.

- [ ] **Step 3: GREEN verification**
  - Run:
    ```bash
    cargo test --lib lower_hdfs_scan_variant_path -- --nocapture
    cargo build
    ```

- [ ] **Step 4: Commit**
  ```bash
  git add src/lower src/connector src/formats
  git commit -m "feat(lower): pass variant path specs to parquet scans"
  ```

---

### Task 6: Parquet reader synthetic variant path materialization

**Files:**
- Modify: `src/formats/parquet/variant_read.rs`
- Modify: `src/formats/parquet/mod.rs`

- [ ] **Step 1: RED - reader behavior tests**
  - Add tests that build in-memory batches with variant struct columns and assert:
    - `variant_get(v, '$.a', 'bigint')` synthetic output equals the PR-2 expression semantics.
    - `try_variant_get` returns NULL on cast failure where strict mode errors.
    - missing path returns NULL.
    - shredded typed-value layout can use the direct child array for whitelisted type/path when it exactly matches.
    - fallback path works for unshredded or non-matching layout.
  - Run:
    ```bash
    cargo test --lib parquet::variant_read -- variant_path --nocapture
    ```
  - Expected: fail because the helper does not exist.

- [ ] **Step 2: GREEN - implement materialization**
  - Add a helper that appends/reorders synthetic variant path arrays after the source variant columns are read.
  - Use the exact shredded typed-value fast path only when:
    - the parquet field is the variant root with the expected Iceberg field id/extension metadata.
    - the path is object-field-only and maps to a `typed_value` subtree.
    - requested type is whitelisted and Arrow type matches without lossy cast.
  - Otherwise collapse/rebuild to the engine variant representation and evaluate through the same kernel semantics as PR-2.
  - Normalize the final batch to the chunk schema after synthetic columns are appended.
  - Preserve dictionary encoding and existing `convert_variant_columns` behavior for normal variant outputs.

- [ ] **Step 3: GREEN verification**
  - Run:
    ```bash
    cargo test --lib parquet::variant_read -- variant_path --nocapture
    cargo test --lib variant_get -- --nocapture
    cargo build
    ```

- [ ] **Step 4: Commit**
  ```bash
  git add src/formats/parquet
  git commit -m "feat(parquet): materialize variant path scan columns"
  ```

---

### Task 7: End-to-end SQL coverage

**Files:**
- Modify/add: `sql-tests/iceberg-dml/sql/variant_get.sql` or a focused new case under `sql-tests/iceberg-dml/sql/`
- Modify/add corresponding result golden
- Optional docs update: `docs/guides/iceberg-v3/variant.md`

- [ ] **Step 1: RED - SQL case first**
  - Add an Iceberg SQL test where a table contains a variant column and queries use:
    - `WHERE variant_get(v, '$.a', 'bigint') = ...`
    - `SELECT variant_get(v, '$.a', 'bigint')`
    - `try_variant_get` with an uncastable row
    - rule-disabled comparison for identical results
  - Add `-- @explain_contains=variant columns:` on the enabled path.
  - Run with the generated worktree env:
    ```bash
    source docker/iceberg-rest/runtime/current/env.sh
    cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
      --config "$NOVAROCKS_SQL_TEST_CONFIG" \
      --suite iceberg-dml --only variant_get --mode verify
    ```
  - Expected: fail before runtime support.

- [ ] **Step 2: GREEN - update golden and docs**
  - Record/update the golden only after the runtime path is correct and explain includes the scan variant line.
  - If docs are touched, state that PR-4 adds scan-time materialization but not pruning.

- [ ] **Step 3: GREEN verification**
  - Run:
    ```bash
    source docker/iceberg-rest/runtime/current/env.sh
    cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
      --config "$NOVAROCKS_SQL_TEST_CONFIG" \
      --suite iceberg-dml --only variant_get --mode verify
    cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
      --suite optimizer --only variant_path_pushdown --mode verify
    ```

- [ ] **Step 4: Commit**
  ```bash
  git add sql-tests docs/guides/iceberg-v3/variant.md
  git commit -m "test(iceberg): verify variant path pushdown end to end"
  ```

---

### Task 8: Final integration gate and PR prep

**Files:** no required source changes unless a verification failure reveals a bug.

- [ ] **Step 1: Full local formatting/build gate**
  ```bash
  cargo fmt
  cargo test --lib variant_path_pushdown -- --nocapture
  cargo test --lib lower_hdfs_scan_variant_path -- --nocapture
  cargo test --lib parquet::variant_read -- variant_path --nocapture
  cargo test --lib variant_get -- --nocapture
  cargo build
  ```

- [ ] **Step 2: SQL gate**
  ```bash
  source docker/iceberg-rest/runtime/current/env.sh
  cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
    --config "$NOVAROCKS_SQL_TEST_CONFIG" \
    --suite iceberg-dml --only variant_get --mode verify
  cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
    --suite optimizer --only variant_path_pushdown --mode verify
  ```

- [ ] **Step 3: Self-review**
  - Confirm no row-group/page pruning was added.
  - Confirm no runtime-filter mapping to source variant columns was added.
  - Confirm source variant columns remain available for scan-time materialization even when only the synthetic output is required.
  - Confirm strict/try semantics match PR-2 expression semantics.

- [ ] **Step 4: Commit final cleanup if needed**
  ```bash
  git add <files>
  git commit -m "chore(variant): finalize path pushdown validation"
  ```

- [ ] **Step 5: Ready for PR**
  - Push branch.
  - Open a draft PR against `main` using the StarRocks/NovaRocks PR template.
