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

Iceberg SQL tests for NovaRocks.

This suite targets the locally supported Iceberg scope:
- Hadoop catalog
- Parquet read/write path
- Configurable S3-compatible object storage

Recommended invocation:

```bash
cargo run --manifest-path tests/sql/runner/Cargo.toml -- \
  --suite iceberg \
  --config tests/sql/runner/conf/default.toml \
  --mode verify
```

Required config variables follow the local `tests/sql/runner/conf/default.toml` style:
- `iceberg_catalog_type`
- `iceberg_catalog_warehouse`
- `oss_ak`
- `oss_sk`
- `oss_endpoint`

The runner also supports `${uuid0}` style placeholders and will auto-expand them
per test run to avoid catalog/database/table name collisions.

SQL runner 经 Trino HTTP 协议访问 FE；配置使用 `[cluster].trino_port`（生成环境为 `NOVA_ENV_TRINO_PORT`），命令行覆盖使用 `--trino-port`。golden 的类型、单元格与迁移规则见 [SQL README](../../README.md#golden-格式)。
