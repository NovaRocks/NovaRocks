# low-cardinality

End-to-end coverage for the low-cardinality dictionary rewrite pipeline.
Cases here exercise:

- `ANALYZE FULL TABLE` populates a `dictionary.snapshot` for string-typed
  columns and the optimizer rewrites group-by / equi-join / order-preserving
  sort to operate on dict ids;
- write paths (INSERT / UPDATE / MERGE / TRUNCATE / DELETE) flip the snapshot
  to STALE so subsequent queries fall back to plain string operators;
- DROP TABLE / DROP DATABASE remove dictionary metadata;
- `SET disable_optimizer_rules = 'LowCardinalityDictionaryRewrite'`
  unconditionally suppresses the rewrite.

Plan-shape assertions go through `EXPLAIN VERBOSE` + `@result_contains=DECODE`
(uppercase). `EXPLAIN COSTS` is **not** suitable in standalone mode —
`try_explain_costs` short-circuits to an ESTIMATE / cardinality summary and
never renders the physical plan tree.

Background: this rewrite landed in PR #191; the rewriter and codegen are wired
together (Tasks 3–8 of `docs/design/plans/2026-05-26-low-cardinality-dictionary-rewrite.md`)
but the runtime integration plus the bulk of regression cases live here so
they can evolve independently of the optimizer suite.

## Storage (Iceberg v3)

All cases here run on **Iceberg v3** via `init.sql`'s
`lowcard_cat_${suite_uuid0}` external catalog — they validate the dictionary
rewrite + execution end-to-end on Iceberg-backed tables (Option A: iceberg/HDFS
scan dict-encode execution,
`docs/design/plans/2026-05-31-iceberg-scan-dict-execution-option-a.md`).
`ANALYZE FULL` builds the iceberg dictionary; a subsequent write advances the
table snapshot and the dictionary's snapshot watermark no longer matches, so
the rewrite is skipped (see `stale`).

The legacy 128-bit `LARGEINT` compressed-key cases that cannot be represented
on Iceberg (`LARGEINT -> DECIMAL(38,0)` would lose the 128-bit range) live in
the sibling **`low-cardinality-native`** suite on StarRocks native storage.
