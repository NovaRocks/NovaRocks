# Paimon Connector（读取暂停）

Paimon Provider 当前已暂停目录与读取能力。注册、catalog 配置与投影保留，catalog
可正常 materialize；目录和读取入口返回带暂停原因与恢复条件的 `Unsupported`，
不提供 pure program、不进入 pure catalogue，BE bind 也明确拒绝。

恢复须等待 paimon-rust 及其依赖支持 Arrow 60 与 zstd 0.14，并重新满足
[ADR-0138](../../adr/ADR-0138-vendored-paimon-read-host-patch.md) 的授权读取边界；
暂停规则和恢复条件见 [ADR-0204](../../adr/ADR-0204-upstream-iceberg-backports-and-provider-suspension.md)。

下面保留 catalog 配置示例，以及暂停前的读取范围与语义约定。历史读取范围包括
Filesystem Catalog、Parquet 数据文件、append-only 表，以及
`merge-engine=deduplicate` 的主键表；这些读取能力仅在恢复并重新验收后适用。
NovaRocks 不拥有 Paimon 写入、提交、DDL、维护或统计信息采集权限。

## 创建 Catalog（配置保留）

下面的例子假设 FE 与 BE 的 Server 配置都注册了名为 `paimon-prod-data`、代次为
`v1` 的对象存储 credential binding。FE 注册 `object-store-metadata` 用于读取
schema、manifest 和统计文件，BE 注册 `object-store-data` 用于执行 attempt 的数据
读取；两项绑定可以复用相同的名称和代次，但用途与消费角色必须分别声明。Catalog
属性中不能放入访问密钥。

```sql
CREATE EXTERNAL CATALOG paimon_prod
PROPERTIES (
    "type" = "paimon",
    "paimon.catalog.type" = "filesystem",
    "warehouse" = "s3://company-lake/paimon",
    "aws.s3.endpoint" = "https://s3.example.com",
    "aws.s3.region" = "us-east-1",
    "aws.s3.enable_path_style_access" = "true",
    "credential.object-store-metadata.consumer-role" = "frontend",
    "credential.object-store-metadata.mode" = "static",
    "credential.object-store-metadata.name" = "paimon-prod-data",
    "credential.object-store-metadata.generation" = "v1",
    "credential.object-store-data.consumer-role" = "backend",
    "credential.object-store-data.mode" = "static",
    "credential.object-store-data.name" = "paimon-prod-data",
    "credential.object-store-data.generation" = "v1"
);
```

`paimon.catalog.type` 在 PAI-1 中必须为 `filesystem`，`warehouse` 必须存在。
对象存储 endpoint、region 与 path-style 设置按实际部署填写。

## 历史读取语义（仅恢复后适用）

以下记录暂停前的读取约定，供恢复时重新验收。当前目录与读取操作仍返回 `Unsupported`。

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

## 外部保留前提（仅恢复后适用）

恢复读取后，NovaRocks 冻结读取视图，但不会向 Paimon 写入 tag、consumer 状态或任何保留租约，
因此它不拥有 snapshot、manifest、schema 与数据文件的 GC/expiration 决策。外部
Paimon writer/maintenance 的保留窗口必须覆盖 NovaRocks 最长查询时长，并留出任务
排队、重试及运维时钟偏差的余量。在途查询所引用的对象若被外部清理，查询会明确
失败；它不会切换到 latest snapshot、返回部分结果，或把缺失对象当成空表。

恢复读取后，调整 snapshot expiration 或 orphan cleanup 前，运维方必须确认没有仍可能读取目标
snapshot 的 NovaRocks 查询。需要跨系统保证长查询不受 GC 影响时，应由外部 Paimon
治理建立可验证的保留策略；PAI-1 不提供此类写侧租约。

## 明确不支持的能力

Paimon 仍不提供写入、DDL、维护和统计采集能力。`INSERT`、Paimon 表 DDL、`ALTER TABLE ... OPTIMIZE` 和
`ANALYZE TABLE` 会在 FE 编译或 Connector capability 边界返回错误，不会尝试降级为
其他 provider 的实现。

暂停前的读取支持矩阵还拒绝 ORC/Avro 数据文件、deletion vector、aggregation merge engine、
postpone bucket、多个 sequence field、嵌套类型、`TIMESTAMP_LTZ`、data evolution、
system table、change-window、procedure、named reference 和 pinned-file-set 读取。
恢复读取后，未知且可能影响读语义的 Paimon 表属性仍须失败关闭。

## 保留的手工 fixture 配方

Paimon 读取能力与产品测试均已暂停；保留的是 Provider 注册、catalog 配置与投影。Paimon SQL suite、既有读取组件测试以及 13 个依赖 Paimon 的 native system 场景均关闭，包含六个 connector 场景、五个 mixed catalog-planning 场景和两个 scan-readiness 场景。runner 明确拒绝选择这些场景，暂停状态不表示验收通过。

仓库中的 `docker/paimon-read` 使用固定版本的 Spark/Paimon 创建外部 fixture，并用
Spark 直接读取产生 oracle。以下保留历史配方；prepare/verify 是可手工调用的输入工具，SQL 验收命令当前被拒绝，CI 不准备 Paimon fixture。cleanup 继续可调用，用于清理匹配 run-id 的既有私有 fixture：

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

保留的产品验收场景使用 native `1FE+3BE`；恢复运行须先满足上述上游版本与授权读取边界，再明确并重新验证测试范围，当前无重新启用开关。
