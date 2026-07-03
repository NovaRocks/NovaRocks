# low-cardinality

End-to-end coverage for low-cardinality dictionary metadata and carrier
compatibility after R0. Cases here exercise:

- `ANALYZE FULL TABLE` populates `dictionary.snapshot` metadata for
  string-typed columns, while standalone SQL results continue to follow plain
  string semantics;
- write paths (INSERT / UPDATE / MERGE / TRUNCATE / DELETE) advance table
  snapshots so stale dictionary metadata does not affect query correctness;
- DROP TABLE / DROP DATABASE remove dictionary metadata;
- runtime filters stay value-domain correct over low-cardinality string data;
- runtime observability reports dictionary carrier input, kept, and hydrated
  counters without restoring legacy native rewrite plan shapes.

R0 retired the standalone native low-cardinality rewrite path. Standalone SQL
plans should not contain FE-compatible `DECODE` nodes or scan dictionary hints;
cases that need plan-shape protection use `@explain_not_contains` on the query
under test. `EXPLAIN COSTS` is **not** suitable in standalone mode —
`try_explain_costs` short-circuits to an ESTIMATE / cardinality summary and
never renders the physical plan tree.

## Storage (Iceberg v3)

All cases here run on **Iceberg v3** via `init.sql`'s
`lowcard_cat_${suite_uuid0}` external catalog. `ANALYZE FULL` builds Iceberg
dictionary metadata; a subsequent write advances the table snapshot so stale
metadata must not change the rows returned by standalone SQL (see `stale`).

The legacy 128-bit `LARGEINT` compressed-key cases that cannot be represented
on Iceberg (`LARGEINT -> DECIMAL(38,0)` would lose the 128-bit range) live in
the sibling **`low-cardinality-native`** suite on StarRocks native storage.
