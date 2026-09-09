# Paimon 只读 Connector

NovaRocks 的 Paimon Connector 读取由外部系统发布的当前快照。首期支持
Filesystem Catalog、Parquet 数据文件、append-only 表，以及
`merge-engine=deduplicate` 的主键表。NovaRocks 不拥有 Paimon 写入、提交、
DDL、维护或统计信息采集权限。

## 创建 Catalog

下面的例子假设 Server 已注册名为 `paimon-prod-data`、代次为 `v1` 的对象存储
credential binding。FE 与 BE 必须解析到同一个 binding；Catalog 属性中不能放入
访问密钥。

```sql
CREATE EXTERNAL CATALOG paimon_prod
PROPERTIES (
    "type" = "paimon",
    "paimon.catalog.type" = "filesystem",
    "warehouse" = "s3://company-lake/paimon",
    "aws.s3.endpoint" = "https://s3.example.com",
    "aws.s3.region" = "us-east-1",
    "aws.s3.enable_path_style_access" = "true",
    "credential.object-store-data.consumer-role" = "frontend-and-backend",
    "credential.object-store-data.mode" = "static",
    "credential.object-store-data.name" = "paimon-prod-data",
    "credential.object-store-data.generation" = "v1"
);
```

`paimon.catalog.type` 在 PAI-1 中必须为 `filesystem`，`warehouse` 必须存在。
对象存储 endpoint、region 与 path-style 设置按实际部署填写。

## 读取语义

- append-only 表返回当前快照中所有有效数据文件的行。
- 主键表支持默认 `deduplicate` merge engine，包括固定 bucket 和动态 bucket。
  NovaRocks 在 Connector 内完成跨文件 upsert/delete 合并，再把当前结果交给引擎的
  filter、limit、join 与聚合算子。
- 支持单个有符号整数 `sequence.field`；相同 sequence 的胜者遵循 Paimon 的确定性
  顺序语义。
- 查询准备阶段冻结 schema ID、snapshot ID、manifest/file set 和 merge recipe。
  查询运行期间外部发布新快照不会改变该查询；之后的新查询会看到新快照。
- schema evolution 依赖 Paimon field ID，支持 rename、reorder、nullable 增列与非键列
  删除后的历史文件读取。
- 数据文件必须是 Parquet。首期支持 uncompressed、Snappy、Zstd 和 LZ4 Raw。

## 外部保留前提

NovaRocks 冻结读取视图，但不会向 Paimon 写入 tag、consumer 状态或任何保留租约，
因此它不拥有 snapshot、manifest、schema 与数据文件的 GC/expiration 决策。外部
Paimon writer/maintenance 的保留窗口必须覆盖 NovaRocks 最长查询时长，并留出任务
排队、重试及运维时钟偏差的余量。在途查询所引用的对象若被外部清理，查询会明确
失败；它不会切换到 latest snapshot、返回部分结果，或把缺失对象当成空表。

调整 snapshot expiration 或 orphan cleanup 前，运维方必须确认没有仍可能读取目标
snapshot 的 NovaRocks 查询。需要跨系统保证长查询不受 GC 影响时，应由外部 Paimon
治理建立可验证的保留策略；PAI-1 不提供此类写侧租约。

## 明确不支持的能力

Paimon Catalog 是只读的。`INSERT`、Paimon 表 DDL、`ALTER TABLE ... OPTIMIZE` 和
`ANALYZE TABLE` 会在 FE 编译或 Connector capability 边界返回错误，不会尝试降级为
其他 provider 的实现。

首期还会拒绝 ORC/Avro 数据文件、deletion vector、aggregation merge engine、
postpone bucket、多个 sequence field、嵌套类型、`TIMESTAMP_LTZ`、data evolution、
system table、change-window、procedure、named reference 和 pinned-file-set 读取。
未知且可能影响读语义的 Paimon 表属性也会失败关闭。

## 本地验证

仓库中的 `docker/paimon-read` 使用固定版本的 Spark/Paimon 创建外部 fixture，并用
Spark 直接读取产生 oracle。运行 SQL 验收：

```bash
docker/iceberg-rest/up.sh
source docker/iceberg-rest/runtime/current/env.sh
export PAI_RUN_ID="paimon-local-$(date +%s)"
export PAI_REPORT_ROOT="$PWD/reports/paimon/$PAI_RUN_ID"
docker/paimon-read/prepare.sh \
  --run-id "$PAI_RUN_ID" \
  --output-dir "$PAI_REPORT_ROOT/fixture"

cargo run --manifest-path tests/sql/runner/Cargo.toml -- \
  --config "$PAI_REPORT_ROOT/fixture/sql-runner.toml" \
  --suite paimon --mode verify

docker/paimon-read/cleanup.sh \
  --run-id "$PAI_RUN_ID" \
  --output-dir "$PAI_REPORT_ROOT/fixture"
```

正式产品验收使用 native `1FE+3BE` system scenarios；all-in-one 只适合作为本地 smoke。
