# Connector-first standalone scan design

## 背景

NovaRocks standalone SQL 当前的 scan 抽象混合了两类职责：

- 表级定义：表名、列、数据源身份。
- 查询级物理输入：本次 scan 要读哪些 Iceberg data files 或 StarRocks tablets。

这种混合主要体现在 `TableDef.source` / `ScanSource`：

- StarRocks 表在 PR #198 后通过 `ScanSource::StarRocks { db_id, table_id }` 携带轻量表身份，但实际 tablet/version 布局仍通过 `InMemoryCatalog.physical_layouts` 和 `CatalogProvider::get_physical_layout` side-map 查询。
- Iceberg 表通过 `ScanSource::IcebergDataFiles { files, ... }` 直接把本次查询规划出的 data files 放在 `TableDef` 内。

这导致抽象不统一，也会让新增数据源时不断扩展核心 enum。目标设计应当更接近成熟 connector 架构：核心 planner/optimizer/codegen 不枚举具体数据源，而是通过 connector-owned opaque handles 和 capability 与数据源交互。

FE-compatible thrift path 是协议边界，必须保持不变。StarRocks FE 发送的 thrift plan 和 scan ranges 仍按现有 lowering/runtime 处理。Standalone 未来分布式执行也应生成同一类 StarRocks-compatible thrift scan node 和 `TScanRangeParams`，而不是引入与 FE path 冲突的新协议。

## 目标

1. 将 standalone scan 拆成 connector-first 架构。
2. 核心 plan 不再通过 `ScanSource` enum 枚举 StarRocks、Iceberg、未来 Paimon/Delta/Hive 等数据源。
3. `TableDef` 回归逻辑表定义，不携带本次查询的 files/tablets。
4. 本次查询的物理输入由 connector split planning 生成。
5. Standalone codegen 仍输出现有 StarRocks-compatible thrift scan node 和 scan ranges。
6. FE-compatible thrift protocol/lowering/runtime path 不改。
7. 消除 StarRocks `PhysicalTableLayout` side-map 对 scan codegen 的依赖。

## 非目标

- 不重写 FE-compatible thrift lowering。
- 不改变 StarRocks FE 与 NovaRocks backend 的协议。
- 不在第一阶段实现新的分布式调度协议。
- 不要求第一阶段完成所有 connector 的深度 predicate/file/tablet pruning。
- 不把全量 connector metadata 缓存在 plan 生命周期对象中。

## 核心抽象

核心层只认识 connector-owned opaque handles：

```rust
struct ConnectorId(String);

struct TableHandle {
    connector_id: ConnectorId,
    handle: Arc<dyn ConnectorTableHandle>,
}

struct ScanHandle {
    connector_id: ConnectorId,
    handle: Arc<dyn ConnectorScanHandle>,
}

struct Split {
    connector_id: ConnectorId,
    handle: Arc<dyn ConnectorSplit>,
}
```

语义：

- `TableHandle`：connector-owned table identity。它回答“这是什么表”。
- `ScanHandle`：connector-owned scan intent 和 pushdown state。它回答“这次 scan 已接受哪些约束”。
- `Split`：connector-owned planned physical input。它回答“这次 scan 实际读哪些物理输入”。

核心层不解析具体 handle payload。StarRocks、Iceberg、未来新数据源都在各自 connector 内部定义 concrete handle：

```rust
StarRocksTableHandle { database, table, db_id, table_id }
StarRocksScanHandle { table, schema_id, projected_columns, pushed_predicates, dict_columns }
StarRocksSplit { tablet_id, partition_id, version }

IcebergTableHandle { catalog, namespace, table, table_uuid, snapshot_id }
IcebergScanHandle { table, projected_columns, pushed_predicates, row_lineage_mode }
IcebergSplit { data_file, delete_files, partition_info, row_lineage }
```

## Connector capability

所有 connector 必须实现基础 scan API：

```rust
trait Connector {
    fn id(&self) -> ConnectorId;

    fn get_table_handle(&self, name: &ResolvedTableName) -> Result<TableHandle>;

    fn get_table_schema(&self, table: &TableHandle) -> Result<TableSchema>;

    fn begin_scan(&self, table: &TableHandle, ctx: BeginScanContext) -> Result<ScanHandle>;

    fn plan_splits(&self, scan: &ScanHandle, ctx: SplitPlanningContext) -> Result<Vec<Split>>;

    fn to_thrift_scan(
        &self,
        scan: &ScanHandle,
        splits: &[Split],
        ctx: ThriftScanContext,
    ) -> Result<ThriftScanPlan>;
}
```

可选 capability 单独建 trait，避免所有 connector 被迫支持同一组高级能力：

```rust
trait SupportsProjectionPushdown {
    fn apply_projection(
        &self,
        scan: ScanHandle,
        columns: &[ColumnRef],
    ) -> Result<PushdownResult<ScanHandle>>;
}

trait SupportsPredicatePushdown {
    fn apply_predicate(
        &self,
        scan: ScanHandle,
        predicate: &TypedExpr,
    ) -> Result<PushdownResult<ScanHandle>>;
}

trait SupportsStatistics {
    fn estimate_statistics(&self, scan: &ScanHandle) -> Result<Option<TableStatistics>>;
}

trait SupportsDictionary {
    fn dictionary_owner(&self, table: &TableHandle) -> Result<Option<DictionaryOwner>>;

    fn load_active_dictionary(
        &self,
        scan: &ScanHandle,
        column: &ColumnRef,
    ) -> Result<Option<DictionarySnapshot>>;
}

trait SupportsExplain {
    fn explain_scan(&self, scan: &ScanHandle, splits: Option<&[Split]>) -> ConnectorExplain;
}
```

`PushdownResult` 必须明确区分 connector 接受和未接受的部分：

```rust
struct PushdownResult<T> {
    handle: T,
    accepted: Vec<TypedExpr>,
    remaining: Vec<TypedExpr>,
}
```

核心 optimizer 只把 `accepted` 从上层 filter/projection 中移除。`remaining` 必须继续由 core plan 执行，避免错误地假设 connector 已经处理了语义。

## Standalone 查询数据流

### Analyzer

Analyzer 解析表名后通过 connector registry 找到 connector：

```text
ResolvedTableName -> ConnectorRegistry -> Connector
```

然后请求：

```rust
let table_handle = connector.get_table_handle(&name)?;
let schema = connector.get_table_schema(&table_handle)?;
```

Logical scan 携带 schema 和 `TableHandle`，不携带 files/tablets：

```rust
LogicalScanOp {
    table_name,
    schema,
    table_handle,
    scan_handle: None,
    columns,
    predicates: vec![],
}
```

### Optimizer

进入 optimizer 前，scan 调用 `begin_scan` 创建 `ScanHandle`。之后 optimizer rules 通过 optional capabilities 改写 scan state：

- column pruning 调用 `apply_projection`。
- predicate pushdown 调用 `apply_predicate`。
- statistics 调用 `estimate_statistics`。
- low-cardinality dictionary rewrite 调用 `dictionary_owner` / `load_active_dictionary`。

核心 rule 不 match StarRocks/Iceberg enum，也不读取 connector internal handle。

### Split planning

优化结束后，physical/codegen 前统一调用：

```rust
let splits = connector.plan_splits(&scan_handle, split_ctx)?;
```

语义统一：

- StarRocks connector 返回本次 scan 选中的 tablet splits。
- Iceberg connector 返回本次 scan 选中的 data file splits。
- 未来 connector 返回自己的 split payload。

如果当前 StarRocks 不做 tablet pruning，`plan_splits` 返回 active tablets 全量；这只是当前能力限制，不改变 split 的“本次 scan 输入”语义。

### Thrift codegen

Standalone codegen 调用 connector：

```rust
let thrift = connector.to_thrift_scan(&scan_handle, &splits, thrift_ctx)?;
```

返回：

```rust
struct ThriftScanPlan {
    node: TPlanNode,
    scan_ranges: Vec<TScanRangeParams>,
    global_dicts: Vec<TGlobalDict>,
}
```

之后继续走现有 fragment builder 和 `TPlanFragmentExecParams`。FE-compatible lowering/runtime 不需要知道这些 standalone connector handle 类型。

## StarRocks connector 行为

StarRocks connector 内部可以访问 StarRocks metadata/runtime catalog。核心 plan 不直接访问这些结构。

职责：

- `get_table_handle` 解析 `(database, table)` 到 `(db_id, table_id)`。
- `get_table_schema` 返回列定义和必要 pseudo columns。
- `begin_scan` 固定 query snapshot，记录 `schema_id` 和初始 scan state。
- `apply_projection` 记录 required columns。
- `apply_predicate` 第一阶段可以不接受谓词；后续支持 partition/tablet pruning 时返回 accepted predicates。
- `plan_splits` 返回本次 scan 的 selected tablets，每个 split 包含 `tablet_id`、`partition_id`、`version`。
- `dictionary_owner` 直接从 `StarRocksTableHandle` 返回 owner identity，不读取全局 runtime catalog 锁。
- `to_thrift_scan` 生成 `TLakeScanNode` 和 `TInternalScanRange`，保持现有 StarRocks-compatible scan range 形态。

StarRocks split 的 `tablets` 语义是本次 scan 选中的 tablets，不是全量表 metadata。全量 partitions/indexes/tablets 仍属于 StarRocks metadata/runtime module。

## Iceberg connector 行为

Iceberg connector 内部负责 REST/Hadoop/memory catalog、snapshot、manifest、delete file、row-lineage 等细节。

职责：

- `get_table_handle` 解析 catalog/namespace/table 和 snapshot selector。
- `get_table_schema` 返回 Iceberg schema、row-lineage pseudo columns。
- `begin_scan` 固定 snapshot。
- `apply_projection` 记录 projected columns。
- `apply_predicate` 接受可用于 manifest/file pruning 的 predicate，无法安全接受的 predicate 返回 remaining。
- `plan_splits` 返回本次 scan 选中的 data files，并附带 delete files、partition info、row-lineage metadata。
- `dictionary_owner` 返回 Iceberg table identity。
- `to_thrift_scan` 生成 `THdfsScanNode` 和 HDFS scan ranges。

Iceberg 不再通过 `TableDef.source` 携带 query-local files。files 只存在于 connector split planning 的输出中。

## FE-compatible 边界

FE-compatible path 保持如下契约：

```text
StarRocks FE
  -> thrift TPlanNode + TScanRangeParams
  -> NovaRocks lower_lake_scan_node / lower_hdfs_scan_node
  -> existing runtime
```

本设计不要求 FE path 使用 standalone connector handles。Standalone 只是自己生成同样兼容的 thrift scan node/ranges，以便未来 standalone 分布式执行复用现有协议层。

## 错误处理与 fail-fast

- connector capability 不支持时必须显式返回“不接受”，不能静默接受再忽略。
- `PushdownResult.remaining` 必须被核心 plan 保留。
- `to_thrift_scan` 必须校验 splits 的 connector id 与 scan handle 一致。
- StarRocks `to_thrift_scan` 必须校验 split 中 `tablet_id`、`partition_id`、`version`、`schema_id` 可表达为合法 `TInternalScanRange`。
- Iceberg `to_thrift_scan` 必须校验 file format、delete file、row-lineage metadata 与现有 lowering 支持范围一致。
- dictionary rewrite 不能在 hot path 获取 mutable runtime catalog 锁。

## Explain 与 observability

核心 explain 默认展示通用信息：

```text
SCAN connector=starrocks table=default.orders splits=32
SCAN connector=iceberg table=rest.db.t splits=12
```

如果 connector 实现 `SupportsExplain`，可以补充 connector-specific 信息：

- StarRocks：tablet count、partition count、schema id、accepted predicates。
- Iceberg：data file count、delete file count、snapshot id、manifest pruning summary。

核心 explain 不直接 downcast connector handle。

## 迁移计划

### 阶段 1：引入 connector handle 框架

新增 core traits、opaque handles、connector registry adapter、`ThriftScanPlan`。保留旧 `ScanSource`、`PhysicalTableLayout`、`get_physical_layout`，确保老路径行为不变。

### 阶段 2：迁移 StarRocks standalone scan

让 standalone StarRocks scan 走：

```text
TableHandle -> ScanHandle -> Split -> to_thrift_scan
```

StarRocks codegen 不再通过 `CatalogProvider::get_physical_layout` 回查 layout，而是用 `StarRocksSplit` 生成 `TInternalScanRange`。这一阶段完成后，`InMemoryCatalog.physical_layouts` 对 StarRocks scan codegen 不再必要。

### 阶段 3：迁移 Iceberg standalone scan

把 `ScanSource::IcebergDataFiles { files, ... }` 从 `TableDef` 中移出。Iceberg file planning 移入 connector `plan_splits`，codegen 通过 `to_thrift_scan` 生成 `THdfsScanNode` 和 HDFS scan ranges。

### 阶段 4：接入 optimizer capabilities

逐步将现有规则改为 connector capability：

- column pruning -> `apply_projection`
- predicate pushdown -> `apply_predicate`
- statistics -> `estimate_statistics`
- dictionary rewrite -> `dictionary_owner` / `load_active_dictionary`
- explain -> `explain_scan`

### 阶段 5：清理旧抽象

删除或重命名旧结构：

- `ScanSource`
- `PhysicalTableLayout`
- `CatalogProvider::get_physical_layout`
- `InMemoryCatalog.physical_layouts`

最终核心 plan 不再以 enum variant 形式枚举具体数据源。

## 测试要求

每个迁移阶段至少运行：

```text
cargo build
cargo test --lib
sql-tests filter suite
sql-tests iceberg-rest suite
targeted low-cardinality dictionary tests
```

StarRocks 阶段重点验证：

- INSERT 后 visible version 正确。
- TRUNCATE/DROP 后新查询不会读旧 splits。
- dictionary rewrite 不读取 `state.starrocks_table` 热路径锁。
- 迁移前后生成的 `TInternalScanRange` 等价。

Iceberg 阶段重点验证：

- REST catalog + MinIO。
- snapshot/time travel。
- delete files。
- row-lineage pseudo columns。
- manifest/file pruning。
- equality delete required columns。

## 设计结论

采用 connector-first 完整版方案。核心层通过 opaque `TableHandle`、`ScanHandle`、`Split` 与 connector 交互，不再在核心 enum 中枚举数据源。Standalone scan planning 生成 query-local splits，并由 connector 转换为现有 StarRocks-compatible thrift scan node/ranges。FE-compatible thrift path 保持不变。
