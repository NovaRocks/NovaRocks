# Benchmark Auto Bootstrap Design

## 背景

NovaRocks 的标准 Docker 测试环境已经能稳定提供 per-worktree 的
Iceberg REST Catalog、MinIO、Spark 和 standalone-server 配置。当前痛点是
`ssb`、`tpc-h`、`tpc-ds` 这些标准 benchmark suite 依赖约 1G 的 parquet
测试数据，而这些数据体积太大，不能提交到仓库。上一次本地验证通过了手动把
`sql-tests/bootstrap/parquet` 下的数据导入 MinIO 后运行 `ssb`，但该目录
当前是 ignored，本质上不是可维护的仓库能力。

目标是把“标准测试集数据准备”变成可复现、可审计、按需触发的流程。运行
`sql-test-runner --suite ssb` 时，如果当前 Docker 测试环境没有对应数据，
runner 应自动触发 bootstrap，生成并导入标准数据，然后继续执行 suite。

## 非目标

- 不把 1G 级别测试数据提交到 git。
- 不在 NovaRocks 中实现自定义随机数据生成逻辑。
- 不使用 Spark 生成 benchmark 业务数据分布。
- 不在第一阶段完整实现并验证 `tpc-h` 和 `tpc-ds` 全量 suite 通过。
- 不声称自动生成的数据可用于正式 TPC 审计或发布成绩。

## 数据来源原则

`ssb`、`tpc-h`、`tpc-ds` 都必须来自对应标准 generator：

- `ssb`: 使用 SSB `ssb-dbgen`，例如 `dbgen -s <sf> -T a`。
- `tpc-h`: 使用 TPC-H tools 中的 `dbgen`。
- `tpc-ds`: 使用 TPC-DS tools 中的 `dsdgen`。

仓库只维护 pinned tool metadata、下载 URL、checksum、编译和调用脚本。首次
bootstrap 时下载到 ignored cache，后续复用 cache。下载失败时不切换镜像，
而是给出明确手动下载路径和期望 sha256。

参考来源：

- TPC 当前规格和工具下载入口:
  <https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp?mode=tpc-member>
- TPC-H 规格说明 DBGen 是 TPC provided software:
  <https://www.tpc.org/TPC_Documents_Current_Versions/pdf/TPC-H_v3.0.1.pdf>
- TPC-DS 规格说明 dsdgen 生成基础表数据:
  <https://www.tpc.org/TPC_Documents_Current_Versions/pdf/TPC-DS_v4.0.0.pdf>
- SSB generator:
  <https://github.com/electrum/ssb-dbgen>

## 目录结构

新增和整理 `sql-tests/bootstrap/` 下的可维护脚本与配置：

```text
sql-tests/bootstrap/
  README.md
  benchmark_tools.toml
  bootstrap_benchmark_data.sh
  bootstrap_sql_test_catalog.sh
  ddl/
  spark/
    write_standard_benchmark.py
  cache/
  generated/
```

`cache/` 和 `generated/` 必须 gitignored：

- `cache/` 存放下载后的 generator archive、解压目录和编译产物。
- `generated/` 存放 `.tbl` / `.dat` raw files、中间 parquet、raw manifest。

`benchmark_tools.toml` 记录每个工具的 version、download URL、sha256、build
command、binary relative path 和默认命令模板。脚本读取该文件，避免把 tool
版本散落在 shell 逻辑中。

## 数据生成流程

Bootstrap 脚本分为四个阶段。

### 1. Resolve Runtime

脚本默认从仓库根目录执行，读取固定入口：

```bash
source docker/iceberg-rest/runtime/current/env.sh
```

它必须使用该入口解析：

- `NOVAROCKS_SQL_TEST_CONFIG`
- `NOVA_ENV_MYSQL_PORT`
- `NOVA_ENV_COMPOSE_PROJECT`
- `NOVAROCKS_STANDALONE_CONFIG`
- `AWS_S3_ENDPOINT`
- `NOVAROCKS_ICEBERG_REST_URI`
- `NOVAROCKS_ICEBERG_REST_WAREHOUSE`
- `CATALOG_WAREHOUSE_URI` 或 runner config 中的 `iceberg_catalog_warehouse`

如果 `runtime/current/env.sh` 不存在，脚本失败并提示运行：

```bash
docker/iceberg-rest/up.sh --prepare-only
docker/iceberg-rest/up.sh
```

### 2. Prepare Tools

脚本读取 `benchmark_tools.toml`。对选中 suite 所需 generator：

1. 检查 cache 中 archive 是否存在。
2. 校验 sha256。
3. 解压到 cache 中固定目录。
4. 编译 generator。
5. 校验目标 binary 存在。

支持以下 override：

- `NOVAROCKS_BENCHMARK_TOOL_CACHE=/path`
- `NOVAROCKS_BENCHMARK_DOWNLOAD_DIR=/path`
- `NOVAROCKS_BENCHMARK_TOOL_<SUITE>_ARCHIVE=/path/to/archive`

下载失败或 checksum mismatch 时必须 fail fast。错误信息要包含 suite、tool
name、version、URL、目标 cache 路径和期望 sha256。

### 3. Generate Raw Files

脚本使用标准 generator 生成 raw 文件：

```bash
# SSB
dbgen -s <sf> -T a

# TPC-H
dbgen -s <sf>

# TPC-DS
dsdgen -scale <sf> -dir <out>
```

输出目录：

```text
sql-tests/bootstrap/generated/<suite>/<scale>/raw/
```

生成后写 raw manifest：

```json
{
  "suite": "ssb",
  "scale": "1",
  "generator": "ssb-dbgen",
  "generator_version": "...",
  "command": "dbgen -s 1 -T a",
  "files": [
    {"path": "customer.tbl", "bytes": 123, "sha256": "..."}
  ],
  "generated_at": "..."
}
```

Raw manifest 用于本地调试和重入判断。它不代表 warehouse 数据已经可用。

### 4. Load To Iceberg

Spark 只负责读取 generator 产出的 raw files，按仓库 DDL schema 做类型转换，
再写入当前 Docker 环境对应的 Iceberg warehouse。Spark 不生成业务数据。

目标 catalog 和 database：

- catalog: `sql_test_catalog`
- database: `ssb`、`tpch`、`tpcds`

写入完成后，在 warehouse 下写 Iceberg bootstrap manifest，例如：

```text
<iceberg_catalog_warehouse>/_bootstrap_manifest/ssb/sf1.json
```

Manifest 内容：

```json
{
  "suite": "ssb",
  "scale": "1",
  "catalog": "sql_test_catalog",
  "database": "ssb",
  "generator": "ssb-dbgen",
  "generator_version": "...",
  "schema_version": "2026-05-26",
  "warehouse": "s3://novarocks/<env>/iceberg-catalog",
  "tables": [
    {"name": "customer", "rows": 30000},
    {"name": "dates", "rows": 2556},
    {"name": "lineorder", "rows": 6000000},
    {"name": "part", "rows": 200000},
    {"name": "supplier", "rows": 2000}
  ]
}
```

Runner 后续通过 manifest 和 sentinel queries 判断是否可跳过 bootstrap。

## Runner 集成

`sql-test-runner` 增加 benchmark auto bootstrap 前置阶段，执行位置在 suite
配置解析完成之后、suite init hook 执行之前。

只对以下 suite 生效：

- `ssb`
- `tpc-h`
- `tpc-ds`

CLI 行为：

- 默认开启 auto bootstrap。
- `--no-auto-bootstrap-benchmark-data` 关闭自动触发。
- `--benchmark-scale <suite>=<scale>` 可覆盖 suite scale。
- `--benchmark-bootstrap-rebuild` 强制重建选中 suite 的数据。

触发流程：

1. runner 检查 suite 是否是 benchmark suite。
2. 检查目标 catalog、database、sentinel tables 和 bootstrap manifest。
3. 检查通过则继续执行 suite init。
4. 检查失败则调用：

```bash
sql-tests/bootstrap/bootstrap_benchmark_data.sh \
  --suite ssb \
  --scale 1 \
  --target-catalog sql_test_catalog
```

5. bootstrap 成功后重新检查。
6. 仍失败则 abort suite，并打印可复现的手动命令。

Runner 不实现下载、编译、Spark 写入、schema cast 等细节。它只负责检测、
触发、重新检测和报告错误。

## 并发与幂等

Bootstrap 脚本必须支持重复执行：

- 数据已存在且 manifest 匹配时直接跳过。
- scale 或 generator version 不匹配时默认生成新路径，不覆盖旧数据。
- 显式 `--rebuild` 才删除并重建目标 database。
- 使用 lock file 防止多个 runner 同时生成同一 suite/scale。

Lock 路径应包含 runtime env id、suite 和 scale：

```text
sql-tests/bootstrap/generated/locks/<env-id>-<suite>-<scale>.lock
```

第二个进程应等待第一个进程完成，然后重新检查 manifest。如果等待超时，打印
lock owner、suite、scale 和可手动清理的路径。

## 错误处理

错误信息必须给出下一步命令，而不是只暴露底层错误。

- `runtime/current/env.sh` 缺失:
  提示运行 `docker/iceberg-rest/up.sh --prepare-only` 或 `docker/iceberg-rest/up.sh`。
- Docker / MinIO / Spark 不可用:
  提示运行 `docker/iceberg-rest/up.sh`。
- generator 下载失败:
  打印 pinned URL、目标 cache 路径和 sha256，要求用户手动下载后重试。
- hash 不匹配:
  拒绝使用该文件，并打印实际 hash 与期望 hash。
- generator 编译失败:
  打印 build directory、build command 和 log 路径。
- Spark 写入失败:
  打印 suite、scale、table、warehouse path 和 Spark log 路径。
- bootstrap 后 runner 复检失败:
  打印 sentinel query 和 manifest path。

## Scale 策略

默认 scale 以本地开发可用为优先，但必须使用标准 generator 的 scale 参数。

第一阶段默认：

- `ssb`: `sf=1`
- `tpc-h`: `sf=1`
- `tpc-ds`: `sf=1GB`

后续可以通过 CLI 或环境变量覆盖：

```bash
--benchmark-scale ssb=1
--benchmark-scale tpc-h=1
--benchmark-scale tpc-ds=1GB
```

如果某个 suite 的标准规格不允许某个 scale 用于正式结果，文档要明确该数据仅
用于开发和回归测试，不用于发布 benchmark 成绩。

## Expected Results

生成器版本、scale 或 schema cast 规则变化时，expected results 需要重录。

第一阶段实现时必须：

1. 自动生成并导入 `ssb sf=1`。
2. 重录 `sql-tests/ssb/result/*.result`。
3. 验证完整 `ssb` suite 通过。

`tpc-h` 和 `tpc-ds` 的 metadata、命令结构和 manifest schema 可以随第一阶段
落地，但全量重录和完整验证应作为后续任务处理，除非实现阶段明确扩大范围。

## 测试策略

脚本级测试：

- `bash -n sql-tests/bootstrap/bootstrap_benchmark_data.sh`
- dry-run 验证命令拼接、cache 路径、scale 参数、manifest 路径。
- fake generator fixture 输出极小 `.tbl` / `.dat`，验证脚本调用标准接口。
- hash mismatch、download failure、missing runtime、missing Docker 都应有明确错误。

Runner 单元测试：

- benchmark suite 识别。
- auto bootstrap flag 默认开启。
- `--no-auto-bootstrap-benchmark-data` 跳过触发。
- scale 参数解析。
- bootstrap command 构造。
- bootstrap 成功后复检失败时 abort suite。

集成测试：

1. 清理当前 runtime 的 `ssb` database 和 manifest。
2. 启动标准 Docker 环境和 standalone-server。
3. 运行：

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite ssb --only q1.1 --mode verify
```

4. 确认 runner 自动触发 bootstrap。
5. 运行完整 `ssb` suite，目标 `13/13` 通过。

## 第一阶段交付范围

第一阶段交付：

- `benchmark_tools.toml` schema 和 SSB pinned tool metadata。
- `bootstrap_benchmark_data.sh` 的 SSB 路径。
- Spark loader 对 SSB raw files 的 schema cast 和 Iceberg 写入。
- runner 自动触发 SSB bootstrap。
- SSB expected results 重录。
- 完整 SSB suite 验证通过。
- TPC-H/TPC-DS 的配置占位、错误提示和 follow-up 文档。

第一阶段不交付：

- TPC-H/TPC-DS 全量数据生成验证。
- TPC-H/TPC-DS expected results 重录。
- 官方 benchmark 报告或审计支持。

## 开放风险

- TPC tools 下载入口可能需要用户人工确认许可。自动下载失败时必须给手动缓存
  指令，不应使用未经确认的镜像。
- TPC-DS schema cast 和日期/decimal 细节可能需要单独验证。
- Spark loader 写入 Iceberg 后的文件布局和统计信息可能影响 planner 行为，
  第一次落地必须重录 goldens。
- 本地首次生成 SSB SF1 可能耗时较长，runner 输出需要清楚展示当前阶段。
