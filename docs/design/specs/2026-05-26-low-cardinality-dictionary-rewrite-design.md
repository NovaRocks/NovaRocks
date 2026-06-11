# Low-cardinality dictionary rewrite 设计

- 状态：待用户 review
- 日期：2026-05-26
- 范围：standalone SQL optimizer plan 层低基数字典改写；StarRocks 表与 Iceberg 表字典维护；相关历史命名与非法 scan shape 清理

## 1. 背景

StarRocks FE 已经有完整的低基数字符串字典改写实现。NovaRocks 的执行层和
StarRocks FE 兼容路径已经具备核心执行能力：

- `TPlanFragment.query_global_dicts` / `query_global_dict_exprs` 能把字典快照传到执行层。
- `DECODE_NODE` lowering 能生成执行层 `DictDecode` 表达式。
- StarRocks scan reader 已经能按 `query_global_dicts` 把字符串列编码成 dict id。

当前 standalone SQL plan 层缺失字典 rewrite，所以 aggregate suite 中与 compressed
key / decode plan shape 相关的 case 失败，例如：

- `sql-tests/aggregate/sql/agg_test_agg_compressed_key.sql`
- `sql-tests/aggregate/sql/agg_test_agg_with_limit.sql`

用户要求不是只修这两个 case，而是照 StarRocks V2 的完整思路在 NovaRocks 上实现
plan 层能力，同时利用新完成的 logical rewrite framework，使节点可以自由组合，并为
后续增量物化视图 rewrite 探路。

## 2. 设计原则

1. 字典 rewrite 是 optimizer 语义，不是 codegen 或执行层 fallback。
2. 字典必须由真实表 owner 维护；没有表 identity 的裸文件 scan 不参与，也不作为合法业务 shape 保留。
3. `InMemoryCatalog` 只承载 planner 可见 `TableDef`，不拥有字典生命周期。
4. 字典必须是完整快照。运行时遇到 dict miss 是维护或 planner bug，不能 silent fallback。
5. 字典快照在单个 query 内一致。不同 fragment 不能各自选择不同字典版本。
6. 旧 `managed-lake` 命名不进入新设计。当前这类表统一称为 StarRocks 表。
7. 不考虑历史兼容性。旧 metadata key、record kind、配置文件名、测试 fixture 可以随实现一起迁移或删除。

## 3. 非目标

- 不重写执行层 `DictDecode` 算子。
- 不复制 StarRocks Java optimizer 的 memo 架构。
- 不引入没有表 owner 的临时字典。
- 不支持裸 Parquet/local file 表作为字典 owner。
- 不为了兼容旧 SQLite metadata 保留 `managed_lake` 命名。
- 不在第一版实现跨查询 derived dictionary 持久缓存。

## 4. 必要清理

### 4.1 StarRocks 表命名收敛

当前代码里用户语义已经在收敛到 StarRocks 表：

- `ScanSource::StarRocks` 已存在。
- MV SQL storage engine 已是 `starrocks`。
- 但 backend name 仍是 `"managed"`，内部类型仍大量叫 `ManagedLake*`。

本任务实现阶段应同步清理：

- connector backend name 从 `"managed"` 改成 `"starrocks"`。
- `MvStorageEngine::StarRocks.backend_name()` 返回 `"starrocks"`。
- 相关内部类型在本任务触达范围内一起重命名：`ManagedLakeBackend`、
  `ManagedLakeCatalog`、`ManagedLakeConfig`、`ManagedLakeMetaRepository`、
  `ManagedLakeTxnRepository` 等改成 `StarRocks*`。
- 用户可见错误、SQL test、runner config、docs 中的 `managed-lake` 文案改为
  `StarRocks table` / `StarRocks storage`。
- metadata namespace / record kind 也改成 `starrocks.*`，不保留旧名兼容。

### 4.2 ScanSource 命名收敛

`ScanSource::S3ParquetFiles` 当前名字不符合实现现状。它不是 S3 专用，也不是合法的
独立表 owner。第一版应收敛为更精确的形态：

```rust
enum ScanSource {
    StarRocks,
    IcebergDataFiles {
        table: IcebergTableInfo,
        files: Vec<IcebergDataFileInfo>,
        cloud_properties: BTreeMap<String, String>,
    },
    IcebergMetadataTable {
        table: IcebergTableInfo,
        ...
    },
    IcebergDeltaTable {
        table: IcebergTableInfo,
        ...
    },
}
```

`S3FileInfo` 同步改成 `IcebergDataFileInfo`。这反映其字段事实：

- row count、column stats 来自 Iceberg manifest / Puffin；
- partition spec、partition value、manifest path 是 Iceberg metadata；
- first row id、data sequence number、delete files 是 Iceberg v3 / delete semantics。

`IcebergDataFiles` 必须携带 Iceberg table identity。`IcebergDataFiles` without
Iceberg identity 是非法代码状态，应该 fail fast 或通过类型结构避免构造。

`TableDef.iceberg_table: Option<_>` 也应同步收敛。Iceberg identity 不再作为一个
可缺省字段挂在 `TableDef` 外层，而是进入 Iceberg scan source variant。新的
`IcebergTableInfo` 至少应包含：

```text
IcebergTableInfo {
  catalog,
  namespace,
  table,
  table_uuid,
  current_snapshot_id,
  schema_id,
  location,
  schema,
  serialized_metadata,
}
```

这样类型层面就不能构造 `IcebergDataFiles + no identity`。

### 4.3 删除 no-identity file scan fixture

当前仓库仍有一些 `S3ParquetFiles + iceberg_table: None` 的测试/合成 fixture。这些
fixture 会让未来代码误以为裸文件列表是合法表类型，增加 dictionary、MV rewrite 和
统计路径的歧义。

实现阶段应删除或改写：

- 老 `standalone --table --path`、`[[standalone_server.tables]]` 相关测试残留。
- `local parquet` / `preloaded table` 历史注释。
- analyzer / explain / codegen 单测里直接构造 no-identity file scan 的 fixture。

如果某个测试只需要一个 scan leaf：

- 用 `ScanSource::StarRocks` + `PhysicalTableLayout` fixture；或
- 构造完整 `IcebergDataFiles { table: IcebergTableInfo, ... }` fixture。

## 5. 字典 owner 模型

字典只绑定两类真实 owner：

```text
DictionaryOwner::StarRocksTable
DictionaryOwner::IcebergTable
```

### 5.1 StarRocks 表 owner

StarRocks 表字典 owner 来自表元数据和物理布局：

```text
StarRocksDictionaryKey {
  database,
  table,
  db_id,
  table_id,
  schema_id,
  column_id,
  column_name,
}
```

字典可用性还要绑定可见数据版本。第一版建议用 table layout fingerprint：

```text
StarRocksDictionaryWatermark {
  schema_id,
  tablets: [(tablet_id, partition_id, visible_version)]
}
```

当写入路径能证明只追加且新 distinct values 已被字典吸收时，可以推进 watermark。
否则将该列字典标记为 stale。

### 5.2 Iceberg 表 owner

Iceberg 表字典 owner 来自 Iceberg catalog identity：

```text
IcebergDictionaryKey {
  catalog,
  namespace,
  table,
  table_uuid,
  schema_id,
  field_id,
  field_name,
}
```

字典快照绑定 Iceberg snapshot：

```text
IcebergDictionaryWatermark {
  snapshot_id,
  schema_id,
}
```

外部引擎写入导致 current snapshot 改变时，旧字典不能继续用于新 snapshot，除非
NovaRocks 字典维护器明确完成了该 snapshot 的 refresh。

## 6. 字典维护

### 6.1 DictionaryManager

新增 standalone 字典管理器，挂在 `StandaloneState`：

```text
DictionaryManager {
  load_snapshot(owner, column) -> Option<DictionarySnapshot>
  rebuild_for_analyze(table, columns) -> Vec<DictionarySnapshot>
  observe_starrocks_commit(...)
  observe_iceberg_commit(...)
  invalidate_table(...)
  drop_table(...)
}
```

持久化放进 NovaRocks metadata repository，使用统一 Avro record schema。建议记录：

```text
DictionarySnapshot {
  dictionary_id,
  owner,
  column,
  data_type,
  version,
  watermark,
  values: [(id, bytes)],
  null_id: 0,
  state: Active | Stale | Dropped,
  created_at,
  updated_at,
}
```

id 语义：

- `0` 保留给 null。
- 非 null 从 `1` 开始。
- 对需要 order-preserving 的场景，id 必须按字符串比较语义排序后分配。
- 如果无法证明 id 顺序等价于目标表达式排序语义，Sort / TopN 前必须 decode。

### 6.2 创建与刷新

`ANALYZE FULL TABLE` 是第一版字典刷新主入口。

流程：

1. 解析目标表 owner。
2. 对 eligible string-like 列扫描当前可见数据。
3. 收集完整 distinct set。
4. 若 NDV / byte size / value length 超过阈值，不创建字典，并记录 skip reason。
5. 按稳定顺序分配 id。
6. 写入 `DictionarySnapshot { state=Active, watermark=current }`。

`ANALYZE SAMPLE` 不生成可用于 rewrite 的字典，因为 sample 不能证明完整覆盖。

### 6.3 写入维护

写入路径必须更新字典状态：

- INSERT / INSERT SELECT：
  - 如果写入路径能收集新 distinct values，且合并后仍在阈值内，则追加字典并推进 watermark。
  - 如果不能完整收集、超过阈值、或 schema/type 不匹配，则标记 stale。
- INSERT OVERWRITE / TRUNCATE：
  - 原字典标记 stale 或 dropped，等待下一次 `ANALYZE FULL` rebuild。
- DELETE：
  - 删除不会引入新值，旧字典作为 superset 仍可安全使用。
  - 如果版本 watermark 策略无法区分 delete-only commit，保守标记 stale；后续可按 commit action 细化。
- UPDATE / MERGE：
  - 若可能引入 string 新值，按 INSERT 规则处理。
  - 若仅 delete-old / insert-new 无法完整收集，标记 stale。
- ALTER COLUMN type / DROP COLUMN / DROP TABLE / DROP DATABASE：
  - 相关字典 dropped。
- RENAME COLUMN：
  - StarRocks 表如果 column id 不变，可更新 display name。
  - Iceberg 表以 field id 为准，可更新 field_name。

运行时 scan 若遇到值不在 Active dictionary 中，必须返回错误。这代表 planner 使用了过期字典。

### 6.4 Derived dictionary

Project / Filter / expression rewrite 产生的 derived dictionary 不持久化。

它们在 query 内由 base dictionary 和 `query_global_dict_exprs` 派生：

```text
base dict snapshot -> derived dict expression -> query-global derived dict
```

Join / Union 需要合并字典时，合并结果也是 query-local snapshot，不写回持久 catalog。
只有 `ANALYZE FULL` 或写入维护路径能修改持久字典。

## 7. Rewrite 架构

新增 query rewrite rule：

```text
LowCardinalityDictionaryRewrite
```

放入现有 logical rewrite framework。推荐阶段：

```text
PredicatePushdownPreJoin
JoinReorder
PredicatePushdownPostJoin
AggregatePushdown
ColumnPruning
LowCardinalityDictionaryRewrite
```

原因：

- predicate / aggregate / column pruning 先决定最终需要的表达式和列。
- dictionary rewrite 再根据稳定 logical shape 插入 dict column 和 decode boundary。
- CBO physical implement 看到的是已经带 dictionary 语义的 logical plan。

内部拆三层：

```text
DictionaryCollector
DictionaryRewriteContext
DictionaryRewriter
```

### 7.1 DictionaryCollector

自底向上收集：

- scan string columns 是否有 Active dictionary snapshot；
- expression 是否可在 dict 上执行；
- aggregate group key / aggregate argument 是否允许；
- join / union 是否需要合并或 decode；
- parent 是否需要原始 string 输出。

collector 只做分析，不改 plan。

### 7.2 DictionaryRewriteContext

维护 rewrite 过程中的映射：

```text
string column -> dict column
dict column -> string column
dict column -> DictionarySnapshot
derived expression -> query-global dict expr
decode boundary requirements
```

context 是 query-local 的。它引用持久 dictionary snapshot，但不拥有持久生命周期。

### 7.3 DictionaryRewriter

根据 context 重写 logical plan：

- scan 增加隐藏 dict column 输出；
- project/filter/agg/join/topN/distribution 尽量改用 dict expression；
- 在必须恢复 string 语义的位置插入 `LogicalDecode`；
- 物理实现阶段生成 `PhysicalDecode`；
- codegen 将 `PhysicalDecode` 编译成 `TDecodeNode`。

## 8. 节点语义

### 8.1 Scan

StarRocks scan：

- 只有 `ScanSource::StarRocks` 且有有效 `PhysicalTableLayout` 时可使用 StarRocks 字典。
- dict column 类型为 `Int32`；array string 可以扩展为 `List<Int32>`。

Iceberg scan：

- 只有 `ScanSource::IcebergDataFiles { table, ... }` 可使用 Iceberg 字典。
- `IcebergDataFiles` without `table` 不存在。

### 8.2 Project

支持：

- dict column 透传；
- allowlist string function 的 derived dict；
- 输出需要 string 时插 decode。

不支持的表达式：

- 多列 string 混合；
- 非确定性函数；
- 未证明能在 dictionary domain 上等价执行的函数。

这些情况在表达式前 decode，而不是猜测改写。

### 8.3 Filter

支持：

- `col = literal` / `IN` / `IS NULL` 等可映射到 dict id 的谓词；
- 单列 allowlist function predicate 的 derived dict。

不支持：

- LIKE / regexp / complex expression 第一版可 decode 后执行，除非显式实现 dict-domain 等价。

### 8.4 Aggregate

Group by string column 使用 dict id。

第一版 aggregate function allowlist 对齐 StarRocks 保守集合：

- `COUNT`
- `COUNT DISTINCT`
- `MIN`
- `MAX`
- `ANY_VALUE`
- `ARRAY_AGG`
- `APPROX_COUNT_DISTINCT` 若执行层已支持对应类型

multi-stage aggregate 必须同步 intermediate/final 类型。若某 stage consumer 需要 string，
在 stage boundary 插 decode。

### 8.5 Sort / TopN / Limit

Limit 本身不改变字典语义。

Sort / TopN 只有在 order-preserving dictionary id 下才能直接按 dict id 排序。否则必须在
Sort / TopN 前 decode。这样避免 `agg_test_agg_with_limit.sql` 这类 case 中 limit 与 order
语义被 dict id 分配顺序污染。

### 8.6 Join

Join key 是 string 时：

- 同一个 dictionary snapshot 可以直接用 dict id join。
- 不同 snapshot 只有在 query 内完成 merged dictionary 后才能保留 dict join。
- 无法合并时，在 join key 前 decode。

### 8.7 Union / Set ops

- `UNION ALL` 可合并或传递 dictionary。
- `UNION DISTINCT` / `INTERSECT` / `EXCEPT` 第一版保守 decode，除非实现 merged dictionary
  后能证明 distinct key 等价。

### 8.8 Window / TableFunction / CTE

第一版保守策略：

- CTE producer/consumer 必须携带 dictionary metadata；无法保证多 consumer 一致时 decode。
- Window partition/order 按 Sort / Aggregate 规则判断。
- TableFunction 默认 decode；后续按 StarRocks allowlist 扩展。

## 9. Plan 与执行层接口

codegen 只消费 optimizer 产物，不做字典发现。

输出：

```text
TPlanFragment.query_global_dicts
TPlanFragment.query_global_dict_exprs
TDecodeNode.dict_id_to_string_ids
```

规则：

- `query_global_dicts` 使用 Active `DictionarySnapshot`。
- `query_global_dict_exprs` 只表达 query-local derived dictionary。
- `TDecodeNode.dict_id_to_string_ids` 由 `PhysicalDecode` 生成。
- scan node 的 dict slot 必须和 descriptor / tuple layout 对齐。

## 10. 配置与观测

新增可禁用规则名：

```sql
SET disable_optimizer_rules = 'LowCardinalityDictionaryRewrite';
```

trace / explain 应能看到：

- selected dictionary owner；
- dictionary version / watermark；
- skip reason：no active dict、stale、over threshold、unsupported type、unsupported expr；
- decode boundary。

`EXPLAIN COSTS` / verbose 输出中继续使用稳定的 `Decode` 标记，满足现有 SQL golden。

## 11. 测试计划

### 11.1 清理测试

- 删除 no-identity file scan fixture。
- 删除 local/preloaded parquet 测试残留。
- 重命名 managed-lake 文案和配置文件。
- 更新 `ScanSource::S3ParquetFiles` / `S3FileInfo` 相关单测到新类型。

### 11.2 字典维护单测

- `ANALYZE FULL` 为 StarRocks string 列生成 Active dictionary。
- `ANALYZE SAMPLE` 不生成 rewrite dictionary。
- INSERT 能追加字典或标记 stale。
- INSERT OVERWRITE / TRUNCATE drop 或 stale dictionary。
- DELETE-only 不引入 dict miss。
- ALTER/DROP 清理 dictionary。
- Iceberg snapshot 改变后旧 dictionary 不误用。

### 11.3 Rewrite 单测

- scan string column 改写为 dict column。
- group by string 使用 dict id。
- unsupported expression 自动插 decode。
- join same dict 保留 dict join，different dict decode 或 merged。
- topN/sort 在非 order-preserving dict 前 decode。
- derived dict 生成 `query_global_dict_exprs`。

### 11.4 Codegen/lowering 单测

- `PhysicalDecode` 生成 `TDecodeNode`。
- `query_global_dicts` slot id 与 scan tuple 对齐。
- derived dictionary expression 被写入 fragment。
- invalid dict metadata fail fast。

### 11.5 SQL 回归

必须修复：

- `agg_test_agg_compressed_key.sql`
- `agg_test_agg_with_limit.sql`

扩展：

- StarRocks 表 group by / limit / join / union dict case。
- Iceberg 表 analyze + group by string case。
- rule disable 后 explain 不出现 `Decode`。
- stale dictionary 后 optimizer 跳过或 fail fast，不能产出错误结果。

## 12. 实施切分建议

1. 清理命名与非法 scan shape。
2. 引入 dictionary metadata schema 和 `DictionaryManager`。
3. 接入 `ANALYZE FULL` rebuild 与写入 invalidation。
4. 新增 logical/physical decode plan 节点和 codegen。
5. 实现 `LowCardinalityDictionaryRewrite` collector/context/rewriter。
6. 补 SQL golden 和 targeted tests。

正式 implementation plan 需要在本设计 review 后单独展开。

## 13. 自检

- 设计没有把字典绑定到 `InMemoryCatalog`。
- 设计没有保留裸 file scan 作为合法字典 owner。
- StarRocks 表和 Iceberg 表 owner 都有独立 watermark。
- query-local derived dictionary 不反写持久 metadata。
- Sort/TopN 不依赖非 order-preserving dict id。
- runtime dict miss 是错误，不是 fallback。
- 历史 `managed-lake` 命名和 `S3ParquetFiles` 命名被纳入实现清理范围。
