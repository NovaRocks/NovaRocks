# Iceberg 3-part Name & Metadata-Table Query-Prep Fix

Date: 2026-05-08
Status: Design

## Background

Two SQL regression cases on `origin/main` have been failing for a while.
Both share a common root cause inside the standalone-server SELECT
query-preparation stage; the analyzer-level errors users see are
downstream symptoms.

- `sql-tests/iceberg/sql/iceberg_in_list_predicate.sql` (step 4)
  - The outer SELECT has no `FROM`. All `cat.db.tbl` references live in
    projection subqueries (`COALESCE((SELECT … FROM cat.db.tbl WHERE …), '')`).
  - Analyzer fails with `unsupported table name: cat.db.tbl` at
    `src/sql/analyzer/resolve_from.rs:168`.
- `sql-tests/iceberg/sql/iceberg_metadata_snapshots.sql` (step 6)
  - `SELECT count(*) FROM cat.db.tbl$snapshots` is rewritten by the parser
    pre-normalizer to a 4-part name `cat.db.tbl.__nr_meta_snapshots__`.
  - In a `current_catalog=Some(cat)` session the iceberg loader fails with
    `load iceberg table cat.<current_db>.__nr_meta_snapshots__ failed: no
    metadata files for <current_db>.__nr_meta_snapshots__`.
  - In a `current_catalog=None` session the 4-part name reaches the
    analyzer, the metadata-suffix is recognized, but the `[_cat, db, tbl]`
    arm tries to resolve the base table from the local in-memory catalog
    where it has never been registered.

The actual bugs are in the *query-prep layer* that walks the AST to
discover external Iceberg tables, registers them into the local catalog,
and rewrites 3-part qualified names into 2-part names for the analyzer:

- `src/sql/parser/query_refs.rs::extract_three_part_refs_from_set_expr`
  walks `select.from` and `joins` only; it does not recurse into
  projection / WHERE / HAVING expressions or their subqueries.
  `strip_catalog_in_set_expr` has the same omission. So 3-part references
  in projection subqueries are invisible to the prep stage.
- The same module's `extract_three_part_refs_from_factor` and
  `strip_catalog_in_factor` use `parts.len() == 3`. The parser-rewritten
  metadata-table form is 4-part, so it is silently ignored.
- `src/engine/query_prep.rs::query_table_names` chooses *either* the
  1-part *or* the 3-part extractor based on `current_catalog`. With
  `current_catalog=Some(cat)` the 1-part extractor returns the last
  identifier of any TableFactor — for a 4-part metadata reference this
  is the synthetic `__nr_meta_*__` suffix, which is then resolved as a
  real table name and fails to load.

## Goals

- Make `iceberg_in_list_predicate` and `iceberg_metadata_snapshots` pass
  with `--mode verify`.
- Keep the rest of the `iceberg` and `iceberg-compatibility` suites
  passing with `--mode verify`.
- Do not modify the catalog dispatcher or anything under
  `sql-tests/iceberg-rest/`.
- No backwards-compatibility code (per project policy: NovaRocks has no
  historical users).

## Non-goals

- Refactoring `query_refs.rs` into a single shared AST visitor. Keep the
  fix scoped.
- Improving 3-part-reference handling for `current_catalog=Some(cat)`
  sessions beyond what is needed for the metadata-table case. Other
  3-part cases under `current_catalog=Some` may still register against
  the wrong namespace; that is an existing limitation outside this fix.
- Re-recording `.result` files. The expected outputs are already
  recorded; the fix needs to produce matching output.

## Design

### Approach

Surgical edits to `src/sql/parser/query_refs.rs` and one site in
`src/engine/query_prep.rs`. Reuse `split_metadata_suffix` from
`src/sql/analyzer/iceberg_metadata.rs` to recognize the
`__nr_meta_*__` suffix uniformly.

### Component changes

`src/sql/analyzer/iceberg_metadata.rs`
- Make `split_metadata_suffix` reachable from `crate::sql::parser`. It is
  already `pub fn`, so this is just an import in the parser module.

`src/sql/parser/query_refs.rs`

1. `extract_three_part_refs_from_factor`
   - Compute `(base_parts, _) = split_metadata_suffix(&parts)`.
   - If `base_parts.len() == 3`, push `(base[0], base[1], base[2])`.
   - Otherwise, do nothing. (base.len() < 3 is handled by the 1-part
     path; > 3 falls through to the analyzer's existing error.)

2. `extract_three_part_refs_from_set_expr`
   - Add recursion into `select.projection` items, `select.selection`
     (WHERE), and `select.having` via a new
     `extract_three_part_refs_from_expr(expr, refs)` helper.
   - The helper mirrors the enum coverage of
     `extract_table_names_from_expr`: `Expr::Subquery`, `Expr::Exists`,
     `Expr::InSubquery`, `Expr::BinaryOp`, `Expr::UnaryOp`, `Expr::Nested`,
     `Expr::Between`. Subquery bodies are walked via the existing
     `extract_three_part_refs_from_set_expr`.

3. `strip_catalog_in_factor`
   - Compute `(base_parts, _)` via `split_metadata_suffix`.
   - If `base_parts.len() == 3`, remove `parts[0]` (the leading catalog).
     The `__nr_meta_*__` suffix, if present, stays as the last part —
     so a 4-part `cat.db.tbl.__nr_meta_*__` becomes
     `db.tbl.__nr_meta_*__` (3-part).

4. `strip_catalog_in_set_expr`
   - Add recursion into projections, WHERE, HAVING via a new
     `strip_catalog_in_expr(expr)` mirroring step 2's coverage.

5. `extract_table_names_from_table_factor`
   - If the last identifier of the TableFactor name is `__nr_meta_*__`
     (matched via `split_metadata_suffix` returning a Some suffix), do
     not push anything. Such references are handled by the 3-part
     extractor.

`src/engine/query_prep.rs::query_table_names`

- Always call `extract_three_part_table_refs(query)` and convert each
  result to a 3-part `ObjectName`.
- When `current_catalog.is_some()`, additionally call
  `extract_table_names_from_query(query)` and convert each result to a
  1-part `ObjectName`.
- Merge both lists (3-part entries first), then dedup by
  `ObjectName.parts`. Existing per-name resolution and registration
  loop downstream is unchanged.

### Data flow (annotated)

Case 1 (`current_catalog=None`, 3-part refs in projection subqueries):
```
parse → engine/mod.rs:739 extract_three_part_table_refs
       (now recurses into projection subqueries) → [(cat, db, tbl)]
       register_iceberg_tables_for_query(...)
       strip_catalog_from_three_part_names (now recurses) →
         subquery `cat.db.tbl` → `db.tbl`
       execute_query(rewritten) → analyzer [db, tbl] arm OK
```

Case 2a (`current_catalog=None`, 4-part metadata):
```
parse + rewrite → cat.db.tbl.__nr_meta_*__
extract_three_part_refs_from_factor:
  split_metadata_suffix → base=[cat, db, tbl]
  base.len()==3 → emit (cat, db, tbl)
register → local catalog (db, tbl)
strip_catalog_in_factor:
  base.len()==3 → drop parts[0] → db.tbl.__nr_meta_*__
execute_query → analyzer split_metadata_suffix → base=[db, tbl]
  → [db, tbl] arm → OK
```

Case 2b (`current_catalog=Some(cat)`, 4-part metadata):
```
query_table_names returns [(cat, db, tbl)] (3-part) ∪ [] (1-part)
register → local catalog (db, tbl)
strip not applied (current_catalog branch in engine/mod.rs not entered)
execute_query → analyzer split_metadata_suffix base=[cat, db, tbl]
  → [_cat, db, tbl] arm uses (db, tbl) → OK
```

### Error handling

No intentional change in error semantics. Three categories of
unsupported names continue to error at the same place as before:

- 5-part or unstructured names → analyzer's
  `iceberg metadata table requires …` or `unsupported table name`.
- Non-existent base tables → iceberg backend
  `load iceberg table cat.db.tbl failed: …`.
- Unrecognized `__nr_meta_xyz__` metatypes → parser-level rewrite already
  whitelists `snapshots/history/refs/partitions` and errors otherwise.

The only observable change is that 3-part references in projection
subqueries that point to a non-existent table now surface a more useful
iceberg-loader error (`load iceberg table cat.db.tbl failed: …`)
instead of `unsupported table name: cat.db.tbl`.

## Testing

Unit tests in `src/sql/parser/query_refs.rs::tests`:

- `extract_three_part_table_refs` returns refs from projection
  subqueries, `WHERE x IN (SELECT … FROM cat.db.tbl)`, `EXISTS (...)`,
  CTE bodies.
- `extract_three_part_table_refs` returns `(cat, db, tbl)` for a 4-part
  `cat.db.tbl.__nr_meta_snapshots__` factor.
- `extract_three_part_table_refs` does not return anything for a 3-part
  metadata factor `db.tbl.__nr_meta_*__` (base.len() == 2).
- `strip_catalog_from_three_part_names` rewrites projection-subquery
  `cat.db.tbl` to `db.tbl` and 4-part metadata `cat.db.tbl.__nr_meta_*__`
  to `db.tbl.__nr_meta_*__`.
- `extract_table_names_from_query` skips a TableFactor whose last part
  is `__nr_meta_*__`.

SQL regression (against the local `docker/iceberg-rest/` fixture):

- `--suite iceberg --only iceberg_in_list_predicate,iceberg_metadata_snapshots --mode verify` passes.
- `--suite iceberg --mode verify` (full) shows no new failures.
- `--suite iceberg-compatibility --mode verify` shows no new failures.

Build/lint hygiene: `cargo fmt`, `cargo clippy`, `cargo build`,
`cargo test` for the directly touched crates.

## Risks & boundary

- `query_table_names` always running the 3-part extractor (instead of
  selecting one extractor by session catalog) may register additional
  3-part references in `current_catalog=Some(cat)` sessions that were
  previously skipped. This is the correct behavior, but it widens the
  effective surface of the prep stage. The full `iceberg` and
  `iceberg-compatibility` suite runs are the gate for catching any
  regression.
- The metadata-suffix awareness lives in `split_metadata_suffix`, used
  in both the analyzer and now the parser-side prep. If the suffix
  encoding changes in the future, both call sites should follow.
