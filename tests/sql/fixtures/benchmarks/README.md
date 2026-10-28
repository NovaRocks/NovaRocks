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

# Benchmark Data Bootstrap

This directory contains source-controlled scripts for standard benchmark data
bootstrap. Large generated data is not stored in git.

Generated local outputs are ignored under:

- `cache/`
- `generated/`
- `parquet/`

## Scope

The bootstrap path supports these standard benchmark data sets:

- SSB SF1 through `ssb-dbgen`
- TPC-H SF1 through `tpch-dbgen`
- TPC-DS 1GB through `dsdgen`

Spark producer 从固定机器容器的 AWS 环境读取访问材料，bootstrap 不把凭证拼入 Spark 命令行。`NOVAROCKS_BENCHMARK_SPARK_MEMORY` 可显式设置当前环境所需的 driver/executor heap；它不改变数据规模、p4布局、fixture key判据或结果oracle。

Benchmark data must come from standard generators. Spark is only used to
convert generated pipe-delimited raw files into Iceberg tables. TPC-H and
TPC-DS schemas are parsed from the generator-provided DDL files so table
definitions stay aligned with the pinned generator revision.

The Spark conversion pins Parquet row group and page sizing. This keeps the
generated benchmark files close to the physical layout expected by the current
reader and avoids pathological tiny-page scans from Spark defaults.

The standard SF1/1GB fixture also pins the tested `p4` table layout:

| Suite | Tables | Range partitions | Target file size |
|---|---|---:|---:|
| SSB | `lineorder` | 4 | 64 MiB |
| TPC-H | `lineitem` / `orders` | 4 / 1 | 256 MiB |
| TPC-DS | `store_sales`, `catalog_sales`, `web_sales` / `inventory` | 4 / 1 | 256 MiB |

These values are part of the immutable fixture contract. Changing them creates
a new fixture key; it never mutates or deletes an existing READY fixture.

## Manual Bootstrap

Start or reuse the shared Iceberg REST, MinIO, and Spark test fixture:

```bash
docker/iceberg-rest/up.sh
source docker/iceberg-rest/runtime/current/env.sh
```

Start NovaRocks standalone with the generated worktree config:

```bash
NO_PROXY=127.0.0.1,localhost \
cargo run -p novarocks-server -- standalone --role all-in-one \
  --fe-config "$NOVAROCKS_FE_CONFIG" --be-config "$NOVAROCKS_BE_CONFIG"
```

In another shell, resolve the immutable fixture key and run the bootstrap
driver with that exact resolver output. This path does not connect to the
NovaRocks MySQL endpoint or create an external catalog:

```bash
source docker/iceberg-rest/runtime/current/env.sh
resolved_dataset="$(mktemp)"
python3 tests/sql/fixtures/benchmarks/resolve_benchmark_fixture.py \
  --suite ssb --scale 1 \
  --shared-root "$NOVA_ENV_SHARED_BENCHMARK_ROOT" > "$resolved_dataset"
tests/sql/fixtures/benchmarks/bootstrap_benchmark_data.sh \
  --suite ssb \
  --scale 1 \
  --resolved-dataset "$resolved_dataset" \
  --ensure
rm -f "$resolved_dataset"
```

Use `--suite tpc-h --scale 1` for TPC-H SF1 and `--suite tpc-ds --scale 1GB`
for TPC-DS 1GB. The TPC-DS `1GB` label is passed to `dsdgen` as scale `1`.

`--check` reports a typed READY result without building. `--ensure` reuses a
valid READY and builds only when READY is absent; a malformed READY fixture
fails closed. Each build writes its own staging prefix and publishes READY
with a normal PUT after the manifest and referenced objects are complete.
READY records the completed data location; simultaneous manual producers are
not coordinated. Published data and sibling contract keys are retained.

## Runner Auto Bootstrap

The SQL test runner auto bootstraps benchmark data before verifying supported
benchmark suites. For example:

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo run --manifest-path tests/sql/runner/Cargo.toml -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite ssb \
  --mode verify
```

Use `--suite tpc-h` or `--suite tpc-ds` for the other benchmark suites. Override
scales with `--benchmark-scale`, for example `--benchmark-scale tpc-ds=10GB`.

Disable benchmark auto bootstrap when you need to inspect runner behavior
without preparing data:

```bash
cargo run --manifest-path tests/sql/runner/Cargo.toml -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite ssb \
  --mode verify \
  --no-auto-bootstrap-benchmark-data
```

## Manual Generator Cache Recovery

Generator cache is recoverable from the pinned standard generator archives in
`benchmark_tools.toml`. For SSB:

```bash
mkdir -p tests/sql/fixtures/benchmarks/cache
curl -fsSL \
  https://github.com/greenlion/ssb-dbgen/archive/d006a6c49ff1a145a7d4ac7d837427627b213091.zip \
  -o tests/sql/fixtures/benchmarks/cache/ssb-dbgen-d006a6c49ff1a145a7d4ac7d837427627b213091.zip
```

Expected SHA-256:

```text
fe38fc04bfffec954dd9a5264be295768edc2227fbafc2cb58fa7ca3ad459f3d
```

Verify the cached archive:

```bash
shasum -a 256 tests/sql/fixtures/benchmarks/cache/ssb-dbgen-d006a6c49ff1a145a7d4ac7d837427627b213091.zip
```

TPC-H and TPC-DS archives are pinned by commit and downloaded from GitHub
`codeload` URLs; their expected hashes are recorded in
`tests/sql/fixtures/benchmarks/benchmark_tools.toml`.

## Dry Run

The bootstrap script can print resolved paths without generating data,
uploading data, or invoking Spark:

```bash
source docker/iceberg-rest/runtime/current/env.sh
tests/sql/fixtures/benchmarks/bootstrap_benchmark_data.sh \
  --suite ssb \
  --scale 1 \
  --mysql-port "$NOVA_ENV_MYSQL_PORT" \
  --dry-run
```

Expected output includes:

```text
DRY_RUN suite=ssb scale=1
```

For TPC-DS, dry-run output also shows the normalized generator scale:

```text
DRY_RUN suite=tpc-ds scale=1GB generator_scale=1
```

### 共享机器运行预算

共享消费者通过本次不可变 publication 读取机器事实，继承
`NOVA_FIXTURE_RUN_CONTEXT` 的 run 身份与原截止时间。嵌套 Spark 调用不会重置
预算；容器内 owner 负责准确进程组的终止与实际 wait。正常清理失败会返回失败，
不能以未来 TTL 清扫代替本次清理。私有 publication 保留其原有资源 owner。

benchmark root 内全部对象（包括 READY 引用的 warehouse 和未引用 staging）均为
保留输入；TTL 与机器升级不删除它们。bootstrap 只向本次新 staging 条件上传 raw
对象，不先递归删除 prefix。迁移前后使用 resolver、bootstrap `--check` 和完整
对象清单/内容核验确认输入；临时容器目录与本地构建文件不属于这些持久数据。
