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
| `iceberg-compatibility` / `spark_rest_delete_applicability` | 同提交 position/DV/equality 的序号边界与独立 Java 行袋 | `novarocks/connector/iceberg/src/delete_semantics/**`、`typed_read/**` | 真实 Java writer + manifest 闭包；native 1FE+3BE |
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

相同输入的 worktree 附着同一版本化 catalog，因而共享其数据库和 namespace 列表；不同输入的 catalog 版本可以并存。  Object-storage prefixes are per worktree and generated names
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

隔离 fixture 由 runner 启停，需要已 provision 的 BOM 与本机 Docker；不回退固定端点。端点统一投影到 runner 配置及受控子进程环境。它有自己的唯一项目，并设置 `NOVA_ENV_UPDATE_CURRENT=false`，不会占用共享 current。
`mv-publication-v11` 在创建前从本机 provisioned base 构建 checked-in hook image，以 publication-hook profile 启动私有 REST，并发布 loopback control URI；不会在 stock 就绪后替换容器。runner 记录 profile、实际镜像身份与 control URI，结束后删除其私有项目。hook 镜像的供给快照统一仍属于后续工作。

共享套件使用 publication 中的实际端点和 `[env].fixture_env_file`；stable runtime 目录只用于运行数据。共享 catalog 删除前先退出 HMS 等外部 endpoint；force 不豁免该检查。新对象存储需要重建 benchmark READY 数据，旧 REST/Hive 不自动迁移或删除。

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

UEA-4G 的额外原生验收入口是 system scenario `connector/iceberg-delete-applicability`，属于显式阶段。`NOVAROCKS_UEA4G_NATIVE_MANIFEST` 指向已冻结输入清单，清单以 SHA-256 绑定独立 Java corpus 和规模收据；S3 凭证从现有 fixture 环境注入，清单不保存密钥。场景重放准确 snapshot 的行袋，并以真实多文件输入检查三个 BE 的 split/page-source/退出事实，保存对象范围与资源收敛收据。性能对照及 provider 内闭包、union、完成屏障和物理范围测试分别验收，不能由 SQL 行数或空闲 BE 数代替。

`iceberg-ivm/iceberg_ivm_delete_applicability` 使用官方 Iceberg writer 构造五个端点阶段，检查 same-commit DV/equality、同一不可变 Puffin 的不同 blob、删除 artifact 等价替代与整个数据文件移除；每阶段比较增量 MV、独立 FULL MV 和关闭 MV rewrite 的基表行袋。该 suite 使用隔离 REST fixture，需单独运行。

release 对照入口为 `connector/iceberg-delete-performance` 和 `tests/system-test-runner/scripts/uea4g-performance.py`。驱动在执行前冻结二进制、源码、配置、输入与八次运行顺序；同一 cold/warm 配置在 baseline/candidate 间交替，第二遍反转顺序。全部 raw samples 保留，baseline 错行的非 control case 不计算性能比例，candidate 错行直接失败；control 噪声超过冻结门限时要求按同一参数重做实验。性能场景只能使用 `--launch-profile performance`；行袋、闭包与性能收据各自证明对应契约。
