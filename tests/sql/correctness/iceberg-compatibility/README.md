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

# Iceberg Compatibility SQL Suite

This suite validates cross-engine Iceberg compatibility.

本地 MinIO 模式下，runner 为本次 suite 启动独有的 REST Catalog 和 MinIO，并将原 fixture env-file、端点和凭据投影给 Spark 与 NovaRocks；node-local 模式保留原 publication 与能力检查。各 case 在本次 namespace 内生成数据，不依赖共享预置表；隔离时由原 fixture handle 提供存储权限并负责清理。此 suite 应单独调用，不与普通共享环境 suite 混跑。

The cases create Iceberg format-v3 tables with Spark through the workspace REST
Catalog and MinIO object store, then read the tables through NovaRocks. Current
coverage includes:

- basic Parquet reads
- primitive/date/timestamp/decimal/NULL reads
- ARRAY/MAP/STRUCT/nested field reads
- partitioned-table filtering and aggregation
- Spark-side schema evolution, including DROP plus re-ADD of the same name
- Spark-side partition evolution across historical specs
- Spark-written row-level DELETE, UPDATE, and MERGE visibility
- Spark-created refs with NovaRocks time-travel reads
- NovaRocks snapshot/history metadata-table reads over Spark commits
- NovaRocks Iceberg MV refresh over Spark-written base-table commits

Run it against the generated local environment (see
[`docker/iceberg-rest/README.md`](../../docker/iceberg-rest/README.md) for
how to bring it up):

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql/runner/Cargo.toml -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-compatibility --mode verify
```

## IRU-9 Variant and write-boundary coverage

The `java_variant_*` cases use the pinned Java API fixture in
`tests/sql/fixtures/iceberg-variant/` to read unshredded/shredded Variant,
exercise the manifest-statistics hint, isolate nested projection, and check
Variant write refusal and DV deletion. `java_variant_write_rejected` also checks
default/v3 CTAS, LIKE, new top-level/nested Variant definitions and ordinary-to-
Variant type evolution refusals. Its existing external table remains usable for
ordinary ADD/rename/comment/drop/property changes and same-type Variant MODIFY;
the final Java snapshot/data-file check permits those metadata changes while
refusing accidental data publication. `java_variant_dv_delete` retains its exact
post-DV row oracle and continues through ordinary-id equality deletion and
TRUNCATE. Existing Variant tables are not made wholly read-only.
`java_uuid_write_rejected` checks UUID write/equality-key refusal while retaining
supported deletion; the Variant creation policy does not alter UUID operations.
`java_timestamptz_write_adaptation` writes to a Java-authored target under a
non-UTC session timezone and exercises the same target in equality deletion.
The exhaustive independent footer and Spark type matrices are waived for this
IRU-9 execution; these cases do not claim that validation.

The same fixture's `empty-int`, `empty-bigint`, and `empty-region` modes supply
the three `iceberg-dml/variant_*` cases and `optimizer/variant_path_pushdown`.
Those cases bind an explicit REST catalog locally; they do not switch their
ordinary suite backend from Hadoop. Empty tables keep exact original field
nullability and have no data files or snapshot, including for the row-lineage
and region-partition targets. Pure Variant function SQL and both pushdown
oracles remain unchanged; existing function or empty-snapshot failures remain
visible instead of being accepted as fixture setup failures.

SQL runner 经 Trino HTTP 协议访问 FE；配置使用 `[cluster].trino_port`（生成环境为 `NOVA_ENV_TRINO_PORT`），命令行覆盖使用 `--trino-port`。golden 的类型、单元格与迁移规则见 [SQL README](../../README.md#golden-格式)。
