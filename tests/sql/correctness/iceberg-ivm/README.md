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

Iceberg IVM SQL tests for NovaRocks.

This suite is for materialized views whose target storage engine is Iceberg:
`CREATE MATERIALIZED VIEW ... PROPERTIES('storage_engine' = 'iceberg')`.

Scope:
- Iceberg-backed MV target creation in the active Iceberg catalog/database
- manual refresh into a normal Iceberg target table
- append-only incremental refresh over an Iceberg base table
- row-lineage based incremental apply for base DELETE, UPDATE, and equality-delete changes
- metadata-only / no-op refresh behavior
- target catalog visibility and DROP cleanup

Recommended invocation:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql/runner/Cargo.toml -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm \
  --mode verify
```

SQL runner 经 Trino HTTP 协议访问 FE；配置使用 `[cluster].trino_port`（生成环境为 `NOVA_ENV_TRINO_PORT`），命令行覆盖使用 `--trino-port`。golden 的类型、单元格与迁移规则见 [SQL README](../../README.md#golden-格式)。
