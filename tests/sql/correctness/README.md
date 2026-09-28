<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# SQL Conformance Corpus

`tests/sql/correctness/` is NovaRocks' executable small-data SQL conformance corpus. Its
three manifests are derived from runnable cases rather than maintained as a
separate compatibility spreadsheet.

## Suite map

Pick suites by the engine area a change touches; do not run the whole corpus to
verify one fix.  `--suite all` currently selects 33 suites / 776 cases, and seven
more suites are `explicit_only` and must be named.  Ask
`cargo run --manifest-path tests/sql/runner/Cargo.toml -- --list-suites` for the
authoritative current list.

| Suite | Engine area it exercises | Typical change entry | Extra requirement |
|---|---|---|---|
| `aggregate` | Aggregation operators, aggregate functions, two-phase aggregation, GROUPING SETS / CUBE / ROLLUP | `novarocks/execution/src/exec/operators/aggregate/**`, `novarocks/execution/src/exec/expr/agg/**` | — |
| `analytic` | Window functions, frames, window TopN | `novarocks/execution/src/exec/operators/analytic_*.rs` | — |
| `complex-type` | ARRAY / MAP / STRUCT / JSON values, comparison and grouping over them | `novarocks/execution/src/exec/expr/{array_expr,struct_expr}.rs`, `novarocks/execution/src/exec/expr/function/array/**` | — |
| `cte` | CTE binding, inlining, recursive CTE | `novarocks/sql/src/analysis/cte.rs`, `novarocks/sql/src/optimizer/cte_rewrite.rs` | — |
| `decimal` | DECIMAL256 arithmetic, cast, overflow, predicates | `novarocks/execution/src/exec/expr/decimal.rs`, `novarocks/sql/src/semantic/**` | — |
| `distributed-resilience` | BE loss, FE crash, query control and cleanup under real process failure | `novarocks/worker/src/task_registry.rs`, `novarocks/frontend-application/src/task_execution/**` | `--cluster-mode cross-process --cluster-size 3` |
| `filter` | Predicate evaluation, type coercion in predicates, filter pushdown | `novarocks/execution/src/exec/operators/filter_processor.rs`, `novarocks/execution/src/exec/expr/comparison.rs` | — |
| `function` | Scalar, bitmap, HLL and binary functions, signature resolution | `novarocks/execution/src/exec/expr/function/**`, `novarocks/sql/src/functions/registry.rs` | — |
| `iceberg` | Iceberg read path, metadata tables, branches and tags | `novarocks/connector/iceberg/**` | — |
| `iceberg-compatibility` | Cross-engine reads of tables Spark wrote through REST Catalog | `novarocks/connector/iceberg/**` | provisioned REST Catalog + Spark fixture |
| `iceberg-ddl` | Iceberg DDL, schema evolution, CREATE TABLE LIKE | `novarocks/connector/iceberg/**`, `novarocks/sql/src/planning/**` | — |
| `iceberg-dml` | INSERT / DELETE / UPDATE / MERGE against Iceberg, type round-trips | `novarocks/connector/iceberg/**`, `novarocks/execution/src/exec/operators/table_writer.rs` | — |
| `iceberg-hms` | Native Hive Metastore catalog admission for document-managed MVs | `novarocks/connector/iceberg/src/document_storage/**`, `novarocks/frontend-application/src/mv/**` | `explicit_only`; cross-process, 3 BE, `-j 1`; start the separate `docker/iceberg-hive/` fixture |
| `iceberg-ivm` | Incremental MV maintenance over Iceberg (COW / MOR, projections, PK) | `novarocks/mv-application/**`, `novarocks/execution/src/exec/mv/**` | cross-process, 3 BE, `-j 1`; isolated REST Catalog and MinIO |
| `iceberg-mv-apply` | Change-stream apply into an MV target | `novarocks/mv-application/**` | cross-process, 3 BE, `-j 1`; isolated REST Catalog and MinIO |
| `iceberg-mv-scheduler` | MV refresh policies, intervals, pause / resume | `novarocks/mv-application/**` | cross-process, 3 BE, `-j 1`; isolated REST Catalog and MinIO |
| `iceberg-rest` | NovaRocks-only REST Catalog end-to-end write and read | `novarocks/connector/iceberg/**` | REST Catalog |
| `join` | Hash / nested-loop joins, join order, outer-join nullability, bucket shuffle | `novarocks/execution/src/exec/operators/{hashjoin,nljoin}/**`, `novarocks/sql/src/optimizer/**` | — |
| `lake-publication` | Native lake publication gate under a publication-catalog fault fixture | `novarocks/frontend-application/src/**` | `explicit_only`; cross-process, 3 BE, `-j 1` |
| `limit` | LIMIT / OFFSET, global limit across fragments | `novarocks/execution/src/exec/operators/limit_processor.rs` | — |
| `lnp-3a-mv-rebuild` | Product-topology acceptance: MV rebuild after a lake wipe | `novarocks/mv-application/**` | `explicit_only`; cross-process, 3 BE, `-j 1`; isolated REST Catalog and MinIO |
| `lnp-3c-runtime-cut` | Product-topology acceptance: runtime-state cut across an FE restart | `novarocks/frontend-application/src/state_family/**` | `explicit_only`; cross-process, 3 BE, `-j 1` |
| `lnp-3d-mv-accelerator` | Product-topology acceptance: Accelerator wipe, restart and isolation | `novarocks/mv-application/**`, `novarocks/catalog-application/**` | `explicit_only`; cross-process, 3 BE, `-j 1`; isolated REST Catalog and MinIO |
| `mv-storage-contract` | MV storage-contract product gate: documents, lake-only recovery, operator continuation | `novarocks/mv-application/**`, `novarocks/frontend-application/src/mv/**` | `explicit_only`; cross-process, 3 BE, `-j 1`; the runner starts it **its own** REST Catalog and MinIO (see below) |
| `mv-storage-physical-occ` | External target commit wins against a frozen MV publication without an MV P attachment | `novarocks/frontend-application/src/mv/**`, `tests/fixtures/iceberg-rest-publication/**` | `explicit_only`; cross-process, 3 BE, `-j 1`; separate private REST Catalog and MinIO |
| `mv-publication-v11` | Both MV/external commit orders inside REST after requirement validation, including full, incremental append, metadata-only, and repartition MV publications | `novarocks/frontend-application/src/mv/**`, `tests/fixtures/iceberg-rest-publication/**` | `explicit_only`; cross-process, 3 BE, `-j 1`; the runner builds the checked-in hook image and uses private REST and MinIO |
| `low-cardinality` | Dictionary encoding fast paths and their value domains | `novarocks/execution/src/exec/dict_encode.rs`, `novarocks/execution/src/exec/expr/{dict_decode,dict_peel}.rs` | — |
| `materialized-view` | MV lifecycle and metadata surface | `novarocks/mv-application/**` | REST Catalog |
| `mv-rewrite` | Transparent MV query rewrite, freshness, rollup matching | `novarocks/sql/src/optimizer/**` (`MvRewrite`) | isolated REST Catalog and MinIO |
| `optimizer` | Plan-shape goldens: rules, pushdown, broadcast risk, EXPLAIN output | `novarocks/sql/src/optimizer/**`, `novarocks/sql/src/explain/**` | — |
| `optimizer-dist` | The same plan-shape facts as they appear under a distributed plan | `novarocks/sql/src/optimizer/**` | — |
| `paimon` | Read-only Paimon append-only and `deduplicate` PK reads | `novarocks/connector/paimon/**` | `explicit_only`; provisioned external Spark/Paimon fixture |
| `project` | Projection, cast semantics, arithmetic and string expression edges | `novarocks/execution/src/exec/operators/project_processor.rs`, `novarocks/execution/src/exec/expr/cast.rs` | — |
| `runtime-filter` | Runtime filter build / probe, bitset filters, value domains | `novarocks/execution/src/runtime_filter/**`, `novarocks/execution/src/exec/operators/runtime_filter/**` | — |
| `runtime-filter-distributed` | Runtime filters that cross the process boundary | the same paths plus `novarocks/worker/src/**` | — |
| `session` | Session and client-compatibility statements | `novarocks/mysql-adapter/**`, `novarocks/query-application/src/**` | — |
| `set-op` | UNION / INTERSECT / EXCEPT, including NULL and cast behavior | `novarocks/execution/src/exec/operators/setop/**` | — |
| `sort` | Sort, TopN, ranking, NULL and non-finite float ordering | `novarocks/execution/src/exec/operators/sort/**` | — |
| `sql-reject` | Statements NovaRocks must reject, SQL error codes and phases | `novarocks/sql/src/admission.rs`, `novarocks/user-error/**` | — |
| `statistics` | ANALYZE, NDV, min/max, Puffin statistics round-trip | `novarocks/statistics-application/**`, `novarocks/connector/iceberg/**` | REST Catalog |
| `subquery` | Scalar subquery semantics and unnesting | `novarocks/sql/src/optimizer/**` | — |
| `table-function` | UNNEST and table-function join shapes | `novarocks/execution/src/exec/operators/table_function_processor.rs` | — |

Iceberg suites that create an external catalog need the object store; the suites
marked "REST Catalog" also need the REST service. `docker/iceberg-rest/up.sh`
provides both, while `iceberg-hms` additionally needs `docker/iceberg-hive/up.sh`.
Suites with their own `README.md` keep the authority on their internals; this
table only routes a change to the right suite.

When a change does not map onto any row, that is a signal about the change, not
about the table: either it has no SQL-visible behavior (verify it with the
owning crate's Rust tests) or the corpus has a gap worth filling.

### Suites that get their own REST Catalog

`docker/iceberg-rest` deliberately shares its Docker services across worktrees,
which also means one REST Catalog database and one namespace listing for the
whole machine.  Object-storage prefixes are per worktree and generated names
carry a uuid, so nothing collides -- but every attachment still enumerates
every worktree's tables.

A suite that restarts a frontend and lets it rediscover its own materialized
views adopts the other worktrees' views too. Even without a restart, a suite's
`DROP CATALOG` guard sees those foreign MV references and refuses cleanup.
Those suites are listed in `ISOLATED_REST_CATALOG_SUITES`
(`tests/sql/runner/src/lib.rs`). The runner starts a private REST Catalog and
MinIO for them
(`tests/cluster-harness/src/isolated_iceberg_rest.rs`), overriding
`iceberg_rest_uri`, `iceberg_rest_warehouse` and the object-store placeholders
and environment for the whole run.  Such a suite cannot share a run with an
ordinary one, and the runner says so rather than silently redirecting it.

Nothing extra is needed to run one -- the fixture is started and torn down by
the runner -- but Docker must be available, and the run costs one container
start.
`mv-publication-v11` additionally builds its checked-in REST hook image from
locally provisioned base images without pulling from a registry. The hook
replaces only that run's private REST container and publishes a loopback control
port; the runner records the actual image ID and removes the whole private
project after the case.

## Taxonomy

- **accept**: every existing suite is the acceptance baseline.  These cases
  document SQL NovaRocks currently accepts and the resulting behavior.  Do not
  move an existing case merely to classify it.
- **reject**: `sql-reject/` contains statements NovaRocks must reject.  Its
  cases cover malformed syntax, recognized-but-unsupported syntax, and
  capability rejection.  A reject case must fail; an unexpected success is a
  test failure.
- **extension**: a NovaRocks-specific statement stays in the suite that
  exercises it and carries an `@nova_extension` directive.  The runner derives
  the extension manifest from those executable annotations; do not maintain a
  hand-written duplicate list.

## Error assertion tiers

Each reject assertion belongs to one of two mechanically distinct tiers:

- **drift** locks the observed behavior so that a later change is visible.  It
  is not a claim that the observed error is the final user contract.  Omit the
  tier only for legacy cases; new reject cases should declare
  `@expect_error_tier=drift` explicitly.
- **target** is the post-cutover user contract.  It requires both a SQL error
  code and a location in the original user SQL, using
  `@expect_sql_code=<lowercase.dot.code>` and
  `@expect_error_at=<line>:<column>`.  The location is 1-based and the column
  is a byte column in the original SQL text.  Do not use target assertions for
  known normalized-text location drift.

The runner also accepts `@expect_sql_phase=<Lex|Parse|Validate|Analyze|Admit>`
to check the phase registered for the asserted SQL code.  It resolves phase
through the SQL-error descriptor manifest, never from error-message text or a
code-name convention.  SQLP-0 starts with an empty production descriptor
registry, so no published suite may use a target SQL code until the owning
domain registers it.  Unknown SQL codes fail while parsing the suite.

`@expect_error` and `@expect_error_code` remain available for drift assertions
and can coexist with the SQL-specific directives.  Prefer the narrowest
current assertion that captures the observed behavior without inventing a
future contract.

## Layout

Each suite follows the runner convention:

```text
<suite>/
  init.sql       # optional setup hook
  cleanup.sql    # optional teardown hook
  sql/           # one or more runnable cases
  result/        # golden results for successful statements, when needed
```

The `sql-reject` skeleton deliberately contains a parser-only drift case.  It
keeps the suite non-empty and runnable before the broader reject corpus lands.

Result comparison is implicitly skipped for steps whose final statement is
DDL, DML, or a session command without a rowset. The runner uses the same SQL
statement splitter as execution: `USE db; SELECT ...` requires a recorded
result and comparison, while `USE db; SET ...` remains implicitly skipped.
An explicit `@skip_result_check=true` still skips comparison for the whole step.
