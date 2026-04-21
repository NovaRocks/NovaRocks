# Standalone Module Refactor — Design

**Date**: 2026-04-21
**Scope**: Reorganize `src/standalone/` to eliminate the 6830-line `engine.rs` god file and give each sub-system a dedicated directory.
**Non-goal**: No behavior change. No new abstraction traits. No refactoring of generic runtime/pipeline code beyond relocating `ExecutionCoordinator`.

---

## 1. Motivation

Current layout of `src/standalone/` (13 files, ~15k lines):

| File | Lines | Concern |
|------|-------|---------|
| `engine.rs` | **6830** | **god file** — Session API, DDL, DML, stream load, sqlparser→Expr, local parquet I/O, iceberg batch normalization, aggregate merge, const eval, MV recognition, name resolution, persistence glue, query planning/execution |
| `iceberg.rs` | 1701 | catalog registry + schema/type mapping + insert batch |
| `server.rs` | 1498 | MySQL wire protocol + session IO + arrow→mysql encoding |
| `store.rs` | 1434 | SQLite metadata persistence (managed lake) |
| `lake_recovery.rs` | 995 | ManagedLake config/catalog/runtime + reconcile |
| `coordinator.rs` | 693 | `ExecutionCoordinator` (pipeline runner) — not standalone-specific |
| `lake_ddl.rs` | 487 | managed lake create_table |
| `lake_txn.rs` | 395 | managed lake insert/publish |
| `hadoop_catalog.rs` | 377 | Iceberg Hadoop catalog |
| `iceberg_s3_storage.rs` | 333 | Iceberg S3 storage |
| `catalog.rs` | 269 | InMemoryCatalog (local tables) |
| `iceberg_add_files.rs` | 255 | ADD FILES command |

Two core problems:

1. **`engine.rs` mixes 10+ unrelated concerns** in one file. Adding a new SQL statement type requires navigating 6800 lines to find the right spot.
2. **File-name prefixes (`iceberg_*`, `lake_*`) are being used as a substitute for directories.** The sub-systems already exist conceptually but aren't reflected in structure.

A secondary problem: `coordinator.rs` is generic (single fragment pipeline runner) but lives in `standalone/`. It should move to `src/runtime/`.

## 2. Target Layout

### Inside `src/standalone/` after PR1 + PR2

```text
src/standalone/
├── mod.rs                           re-exports only (public surface unchanged)
├── server/
│   ├── mod.rs                       run_standalone_server + options
│   ├── shim.rs                      NovaRocksMysqlShim + session IO
│   ├── encoding.rs                  arrow → mysql value conversion
│   └── statement.rs                 execute_statement_text + SQL recognition/splitting utilities
├── engine/
│   ├── mod.rs                       StandaloneNovaRocks / Session / Options / State + top-level dispatch impl
│   ├── planner.rs                   choose_standalone_execution / execute_query / execute_plan / explain_query
│   ├── name_resolve.rs              resolve_local_* / resolve_iceberg_*
│   ├── persistence.rs               persist_* / restore_* / metadata store glue / block_on_standalone_async
│   ├── iceberg_glue.rs              engine-side iceberg batch normalization/merge
│   ├── sqlparse/
│   │   ├── mod.rs
│   │   ├── expr.rs                  sqlparser → Expr/Literal + literal helpers + const eval
│   │   ├── statement.rs             execute_create_* / execute_drop_* / execute_insert_statement / table-name extraction
│   │   ├── materialized_view.rs     MV name recognition/canonicalization
│   │   └── generate_series.rs       generate_series helpers
│   └── local/
│       ├── mod.rs                   LocalTableSemantics + InMemoryCatalog + semantics bookkeeping
│       ├── parquet.rs               read_local_parquet_data / write_parquet_to_path / schema cast
│       ├── insert.rs                insert path + local literal → array builders
│       ├── stream_load.rs           CSV/JSON parse + stream_load_local_table
│       └── aggregate.rs             merge_aggregate_table_*
├── iceberg/
│   ├── mod.rs                       re-exports
│   ├── registry.rs                  IcebergCatalogRegistry + catalog lifecycle (PR2 may split further)
│   ├── schema.rs                    [PR2] schema / type mapping / logical-type property parsing
│   ├── insert.rs                    [PR2] insert path + literal → array builders
│   ├── stats.rs                     [PR2] data file stats extraction
│   ├── s3_storage.rs                (= current iceberg_s3_storage.rs)
│   ├── add_files.rs                 (= current iceberg_add_files.rs)
│   └── hadoop_catalog.rs            (= current hadoop_catalog.rs)
└── lake/
    ├── mod.rs                       re-exports
    ├── config.rs                    ManagedLakeConfig (split out of lake_recovery.rs)
    ├── catalog.rs                   ManagedLakeCatalog + ManagedTableRuntime + reconcile (split out of lake_recovery.rs)
    ├── ddl.rs                       (= current lake_ddl.rs)
    ├── txn.rs                       (= current lake_txn.rs)
    └── store.rs                     (= current store.rs — SQLite metadata)
```

### Outside `src/standalone/` (PR1)

```text
src/runtime/
├── query_result.rs                  [NEW] QueryResult + QueryResultColumn + impl
└── coordinator.rs                   [NEW] ExecutionCoordinator (moved from standalone/coordinator.rs)
```

`src/standalone/mod.rs` continues to `pub use crate::runtime::query_result::{QueryResult, QueryResultColumn}` so external callers see no API change.

## 3. `engine.rs` Split Mapping

Every function in the current `engine.rs` has a target file. Line ranges refer to the current `engine.rs` (as of 2026-04-21).

### `engine/mod.rs` (~1500 lines target)

- `StandaloneOptions`, `StatementResult`, `StandaloneStreamLoadRequest`, `StandaloneStreamLoadResult`, `StandaloneManagedTabletInfo`, `StandaloneManagedTableInfo` (46–114)
- `StandaloneState` + `Default` impl (178–206)
- `StandaloneMaterializedView` (213)
- `StandaloneNovaRocks` + impl (220, 229–413)
- `StandaloneSession` type + top-level dispatch methods of its impl (225, selected parts of 414–1250)
- `build_string_query_result` / `append_string_query_rows` (125–177)
- `record_batch_to_chunk` (4931)

The giant `impl StandaloneSession` (lines 414–1250, 837 lines) is split across files using Rust's multi-`impl` support: each sub-module writes `impl StandaloneSession { fn handle_... }` for the methods it owns. `mod.rs` retains only top-level dispatch (the method that picks which handler to call).

### `engine/planner.rs` (~400 lines — engine.rs 5318–5744)

- `StandaloneExecutionPlan` enum
- `choose_standalone_execution` / `single_fragment_plan` / `strip_top_level_stream_root_wrapper` / `top_level_stream_root_wrapper_child_id`
- `explain_query` / `execute_query` / `execute_plan`
- `build_table_stats_from_plan` / `collect_scan_stats`
- `ensure_standalone_exchange_server` / `wait_for_standalone_exchange_server`
- `split_explain_costs_sql`

### `engine/name_resolve.rs` (~150 lines — engine.rs 4544–4658)

- `ResolvedLocalTableName` / `ResolvedIcebergNamespaceName` / `ResolvedIcebergTableName`
- `resolve_local_table_name` / `resolve_iceberg_namespace_name` / `resolve_iceberg_table_name` / `resolve_iceberg_table_name_explicit`
- `normalize_optional_identifier`

### `engine/persistence.rs` (~300 lines — engine.rs 4660–4930)

- `resolve_metadata_store` / `resolve_managed_lake_config` / `resolve_relative_path`
- `restore_metadata_if_needed` / `restore_local_catalog` / `restore_iceberg_catalogs` / `restore_managed_lake`
- `persist_local_database_if_needed` / `persist_local_table_if_needed` / `delete_local_table_if_needed` / `delete_local_database_if_needed`
- `persist_iceberg_catalog_if_needed` / `persist_iceberg_namespace_if_needed` / `persist_iceberg_table_if_needed` / `delete_iceberg_catalog_if_needed` / `delete_iceberg_namespace_if_needed` / `delete_iceberg_table_if_needed`
- `block_on_standalone_async`

### `engine/sqlparse/expr.rs` (~1200 lines — engine.rs 1491–2690 + 4201–4446)

- `sqlparser_expr_to_custom_expr` / `sqlparser_expr_to_literal` / `sqlparser_function_to_literal`
- `negate_literal` / `sql_number_literal` / `is_integral_sql_number` / `literal_to_i128_for_integer`
- `bytes_to_latin1_string` / `latin1_string_to_bytes` / `function_expr_args`
- `eval_literal_arithmetic` / `cast_literal` / `is_select_without_from`
- `evaluate_constant_select` / `evaluate_const_expr` / `extract_numeric_scalar`
- `sql_type_to_arrow_type`
- `compare_literals` / `LiteralKey` + `literal_to_key` / `literal_from_batch` / `format_decimal128_value`
- `parse_kv_properties` / `parse_prop_string_or_ident` / `strip_optional_identifier_quotes` / `canonicalize_sql_for_match`

### `engine/sqlparse/statement.rs` (~800 lines — engine.rs 1348–2300 + 4949–5160 + 5172–5316)

- `convert_sqlparser_insert_to_custom`
- `execute_create_database_statement` / `execute_create_table_statement`
- `execute_drop_catalog_statement` / `execute_drop_database_statement` / `execute_drop_table_statement`
- `execute_truncate_table_statement` / `execute_insert_statement`
- `try_parse_local_parquet_create_table` / `looks_like_add_files` / `parse_add_files_sql`
- `extract_table_names_*` / `extract_three_part_*` / `strip_catalog_from_three_part_names`

### `engine/sqlparse/materialized_view.rs` (~100 lines — engine.rs 1251–1346)

- `materialized_view_key` / `parse_create_materialized_view_name` / `parse_drop_materialized_view_name` / `parse_refresh_materialized_view_name` / `looks_like_show_alter_materialized_view` / `supports_bitmap_count_rewrite`

### `engine/sqlparse/generate_series.rs` (~200 lines — engine.rs 1442–1490 + 2203–2388 + 3191–3216)

- `parse_generate_series_function_expr`
- `insert_generate_series_rows` / `insert_generate_series_rows_local`
- `evaluate_generate_series_row` / `evaluate_generate_series_expr`

### `engine/local/mod.rs` (~250 lines)

- `LocalTableSemantics` (engine.rs 207)
- Merge of current `src/standalone/catalog.rs`: `InMemoryCatalog` / `normalize_identifier` / `build_parquet_table`
- `create_local_table_from_columns` / `update_local_table_semantics` / `get_local_table_semantics` / `remove_local_table_semantics` / `remove_local_database_semantics` (engine.rs 2686–2834)
- `apply_local_table_semantics_if_needed` (engine.rs 4036)
- `ensure_dual_table` / `ensure_dual_in_database` (engine.rs 3954–3997)

### `engine/local/parquet.rs` (~500 lines — engine.rs 3217–3953)

- `read_local_parquet_data`
- `cast_batch_to_schema` / `cast_list_struct_to_map_for_local_schema` / `cast_array_for_local_schema`
- `parquet_storage_type_for_local_batch` / `encode_array_for_local_parquet_storage` / `normalize_local_parquet_batch` / `write_parquet_to_path`
- `parse_date_string_to_days` / `parse_datetime_string_to_micros`

### `engine/local/insert.rs` (~600 lines — engine.rs 2238–2300 + 2835–2901 + 3243–3680)

- `reorder_insert_rows` / `build_insert_column_mapping` / `reorder_insert_row`
- `insert_into_local_table`
- `build_local_insert_batch` / `build_local_literal_array`
- `parse_decimal_string_to_i128`

### `engine/local/stream_load.rs` (~250 lines — engine.rs 2902–3190)

- `stream_load_local_table` / `parse_stream_load_columns`
- `parse_csv_stream_load_rows` / `single_byte_stream_load_delimiter`
- `parse_json_stream_load_rows` / `parse_stream_load_jsonpaths` / `parse_json_rows` / `extract_json_path` / `json_value_to_field`

### `engine/local/aggregate.rs` (~150 lines — engine.rs 4058–4200)

- `merge_aggregate_table_rows_if_needed` / `merge_aggregate_table_row` / `merge_aggregate_table_value`

### `engine/iceberg_glue.rs` (~250 lines — engine.rs 3998–4057 + 4446–4543)

- `load_full_iceberg_batch` / `apply_iceberg_table_semantics_if_needed`
- `normalize_iceberg_source_batch` / `iceberg_field_indices` / `normalize_iceberg_array_type` / `concat_or_empty_batches`

### Moved to `src/runtime/query_result.rs` (~60 lines — engine.rs 52–177)

- `QueryResult` / `QueryResultColumn` + `impl QueryResult`
- `standalone/mod.rs` adds `pub use crate::runtime::query_result::{QueryResult, QueryResultColumn};`

## 4. Dependency Discipline

The new `engine/` subtree must have a strict one-way dependency graph:

```text
engine/mod.rs
  ↓  uses
engine/planner.rs
  ↓  uses
engine/sqlparse/*    engine/local/*    engine/iceberg_glue.rs
  ↓  use
engine/name_resolve.rs    engine/persistence.rs
```

No module may `use` a sibling or ancestor of equal/higher layer. `mod.rs` imports everything below it but nothing imports `mod.rs` internals except through public items.

`engine/iceberg_glue.rs` is the only place under `engine/` that talks to `standalone::iceberg::*`. It does not depend on `engine/local/*`.

## 5. PR Strategy

### PR1 — Directory reorganization + `engine.rs` split

Steps (each step is its own commit or logically grouped commits; `cargo check` runs between steps):

1. Create new directory skeletons (`server/`, `engine/`, `engine/sqlparse/`, `engine/local/`, `iceberg/`, `lake/`) with empty `mod.rs` files that `pub use` existing paths — preserves compilation.
2. Move `QueryResult` / `QueryResultColumn` to `src/runtime/query_result.rs`. Update `standalone/mod.rs` re-export.
3. Move `coordinator.rs` to `src/runtime/coordinator.rs`. Update callers (there is one — `engine.rs`).
4. Move current `iceberg_*.rs` / `hadoop_catalog.rs` into `iceberg/` subdirectory. Pure file moves; adjust `mod.rs`.
5. Split `lake_recovery.rs` into `lake/config.rs` + `lake/catalog.rs`. Move other `lake_*.rs` and `store.rs` into `lake/` subdirectory.
6. Move `server.rs` into `server/` subdirectory, splitting into `mod.rs` / `shim.rs` / `encoding.rs` / `statement.rs`.
7. Split `engine.rs` into `engine/` subtree, leaf-first:
   - `local/aggregate.rs`
   - `sqlparse/materialized_view.rs`
   - `sqlparse/generate_series.rs`
   - `sqlparse/expr.rs`
   - `local/parquet.rs` / `local/insert.rs` / `local/stream_load.rs` / `local/mod.rs`
   - `sqlparse/statement.rs`
   - `iceberg_glue.rs`
   - `name_resolve.rs` / `persistence.rs`
   - `planner.rs`
   - remaining `engine/mod.rs`
8. Merge current `standalone/catalog.rs` into `engine/local/mod.rs`.
9. Final `cargo fmt` + `cargo clippy` + `cargo test` + suite verification.

### PR2 — Split `iceberg/registry.rs`

Steps:

1. Extract `iceberg/stats.rs` (independent).
2. Extract `iceberg/schema.rs`.
3. Extract `iceberg/insert.rs` (depends on `schema.rs`).
4. `registry.rs` becomes the slim catalog/lifecycle file.
5. `cargo fmt` + `cargo clippy` + `cargo test` + suite verification.

## 6. Verification

Applies to both PRs:

- `cargo fmt --check`
- `cargo clippy --all-targets -- -D warnings`
- `cargo test`
- `--suite ssb --mode verify` (release build, zero diff)
- `--suite tpc-h --mode verify` (release build, zero diff)
- `--suite tpc-ds --mode verify` (release build, zero diff)
- stream-load smoke test against a local table (not covered by suites)
- `standalone/mod.rs` public re-exports byte-identical to pre-refactor (verified by `git diff`)
- No new `#[allow(dead_code)]` introduced
- No TODO markers added for deferred work

## 7. Risks and Mitigations

| Risk | Mitigation |
|------|-----------|
| Cyclic imports between engine sub-modules | Enforce one-way dependency graph in Section 4; `cargo check` after each move |
| Split `impl StandaloneSession` causes visibility issues | Move one method at a time, `cargo check` between moves |
| `coordinator` + `QueryResult` move creates a red intermediate state | Move both in the same commit |
| Rebase conflicts with in-flight work on `engine.rs` | PR1 merges atomically; confirm clean `main` before starting |
| Rarely-exercised paths (MV, stream load, add_files) regress | Add explicit smoke tests to verification checklist |
| Surface-level public API accidentally changes | Diff `standalone/mod.rs` `pub use` before and after; must be byte-identical |

## 8. Out of Scope

- Introducing a `TableBackend` trait unifying local / iceberg / managed-lake
- Refactoring `ExecutionCoordinator` internals (only location changes)
- Any behavior change, performance change, or feature change
- Reorganizing sub-systems *outside* `standalone/` (other than the specific `runtime/` additions)
- Writing new tests beyond the verification checklist
