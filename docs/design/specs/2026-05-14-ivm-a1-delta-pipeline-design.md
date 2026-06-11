# IVM-A1 Delta SELECT 到 Iceberg Sink 一体 pipeline 设计

- 状态：草稿（v2，2026-05-15 改方案）
- 日期：2026-05-14 立项；2026-05-15 把 leaf-swap 架构改为 plan-time TPlanNode + 内置 table function
- 范围：Iceberg-backed MV 增量刷新执行层
- 依赖：A4（`__change_op` delta source，#121）、A7（branch-staged refresh transaction，#125）、A9（target row identity apply，#126）、A10（unified MV target commit，#107）、A11（MV schema/field-id contract，#130/#131/#132）、A5/A6（MvBackend lifecycle split + IcebergMvBackend，#133）
- 阻塞 / 后续：A2 任意 snapshot range；A3 多基表 snapshot pin；join MV / 聚合 MV

---

## 1. 背景与问题

iceberg-backed MV 当前增量刷新由 [src/engine/mv/iceberg_refresh.rs](../../../src/engine/mv/iceberg_refresh.rs) 主导，靠两条独立的 `execute_query` 各自跑一遍 MV `physical_select_sql`：

- INSERT 侧 [mv_flow.rs:230 `execute_query_for_mv_incremental_refresh`](../../../src/engine/mv_flow.rs:230)：把 snapshot diff 拿到的 Iceberg 新 data files 包成 `IcebergFileForQuery`，注册到一次性 `InMemoryCatalog`，再跑 MV SELECT。
- DELETE 侧 [mv_flow.rs:387 `execute_query_for_mv_incremental_deletes`](../../../src/engine/mv_flow.rs:387)：把反向重建出来的内存 `Vec<RecordBatch>` 用 [mv_flow.rs:282 `write_mv_delete_temp_parquet`](../../../src/engine/mv_flow.rs:282) 落到 `std::env::temp_dir()`，再走同一套一次性 catalog + SELECT。

两条 pipeline 跑完各自吐出 `QueryResult`，[iceberg_refresh.rs:2049-2052](../../../src/engine/mv/iceberg_refresh.rs:2049) 把它们转成 `Vec<Chunk>`，再 `data_block_on` 异步写 Iceberg DataFile / 调 A9 target locator / 调 `run_iceberg_commit`。

### 这套架构的真实代价

1. **事务边界撕扯**：两次 `execute_query` 在不同 query context（`query_id`、`runtime_state`、`cancellation` 都不联动）。INSERT 侧成功 + DELETE 侧失败时，已经在内存里的 insert chunks 浪费。A7 branch-staged 只保最终 metadata 一致性，跑完的 SELECT 资源是真损失。
2. **性能损失**：parse + analyze + lowering 重复两遍；多基表 join MV 下 base 表会被各自扫两遍，没有 runtime filter 复用、shared scan、hashtable 复用。
3. **本地磁盘强依赖**：`write_mv_delete_temp_parquet` 用 `std::fs::create_dir_all` + `std::fs::File::create` 绕过 `src/fs/**` 抽象。云原生（无本地磁盘 / 只读 rootfs）部署不通。
4. **大 delta 内存峰值无解**：`materialize_changes` 一次性把所有反向重建的行进内存 `Vec<RecordBatch>`；100MB delta → 100MB+ Arrow buffer 常驻；spill 触发不到（数据已在算子外）。
5. **下游 IVM 算子无处挂**：未来 join IVM、聚合 IVM 要求 delta 以流的形式进入算子，"两次 SELECT 收 `QueryResult` 再装 chunk"这种数据流形状杜绝了 streaming 路径。

A1 把这套重构为"**一次 ExecPlan、一条 pipeline、一个 sink，refresh driver 协调 A7 staging + A10 commit**"。

---

## 2. 目标 / 非目标

### 目标

- iceberg-backed MV 增量刷新由**一次** `execute_plan` 完成，从底部 scan 到顶部 sink 是同一条 streaming pipeline
- 反向重建逻辑从 refresh layer 下沉到新算子 `IcebergDeltaScan`，pull-driven streaming，不预物化全部 delete 行
- 写 DataFile / 写 PositionDeleteGroup 的工作下沉到新算子 `IcebergMergeSinkFactory`，按 `__change_op` 路由
- IcebergDeltaScan 是**编译时一等公民**：在 SQL → analyzer → planner → optimizer → codegen → lower 整条链路上以原生节点形态贯穿，不依赖任何 plan 后置 mutation 或 side-channel
- 删除 `write_mv_delete_temp_parquet` / `execute_query_for_mv_incremental_deletes` / `materialize_changes` 的 `QueryResult` 通路、清理一次性 `InMemoryCatalog` 魔术
- iceberg-ivm SQL 测试 suite 保持 16/16 通过，并新增 2 个 case 覆盖大 delta + INSERT/UPDATE/DELETE 混合

### 非目标

- **managed-lake MV 不动**——[ivm_delta_source.rs:81 `write_mv_delete_temp_parquet`](../../../src/connector/starrocks/managed/ivm_delta_source.rs:81) 那条仍保留
- **A2 任意 `[from, to]` snapshot range 不做**——A1 仍是 prev→curr 单步
- **A3 多基表 snapshot pin 不做**——A1 仍单基表
- **Spill 实际触发验证不做**——streaming 把内存峰值压到单 chunk 给 spill 提供了前提，但 spill metric / chaos 测试另起 PR

---

## 3. 决策汇总

| # | 决策点 | 选择 | 关键理由 |
|---|---|---|---|
| 1 | PR 切分 | 一刀切（scan + sink 同 PR） | 避免两轮回归；scan 与 sink 接缝（change_op 路由）只磨合一次 |
| 2 | 覆盖面 | 仅 iceberg-MV | managed-lake MV 用另一套 sink 体系，并入会让 PR 双体量、双回归面 |
| 3 | Sink 与 commit 边界 | Sink 只写入；A7 staging + A10 commit 留 refresh driver | A7 事务边界不被 pipeline 吸走；与现有 `IcebergTableSinkFactory` "sink 产 WrittenFile、commit 在外"语义一致 |
| 4 | Scan 形态 | 形态 1：厚 trait / IVM-aware scan | 一个 source leaf 内吞所有 INSERT+DELETE 源，避免 UnionAll 在 plan 层拼接 |
| 5 | Scan 算子住哪 | 1b：新算子 `IcebergDeltaScan` | 不动 HDFS_SCAN；IVM 知识隔离；普通 Iceberg 查询零认知开销 |
| 6 | **Plan 拼接 (改 v2)** | **P3：plan-time TPlanNode + 内置 table function** | SQL/AST/Thrift 全链路一等节点；不需要 ExecPlan post-mutation；leaf-swap 路线作废 |
| 7 | Scan 执行模式 | Streaming | 满足"无本地磁盘 / 100MB+ delta / spill 可触发"全部 3 条验收 |
| 8 | Sink 算子 | S2：新建 `IcebergMergeSinkFactory` | 与 `IcebergDeltaScan` 选 1b 对称；不污染 FE-thrift-driven 的 `IcebergTableSinkFactory` |
| 9 | A9 locator 状态生命周期 | **L2：lower_plan 现场加载（改 v2）** | refresh driver 只传 snapshot range 进编译链路，locator state 跟 change_files 一起在 lower_plan 通过 `plan_changes` 计算 |
| 10 | `__change_op` 暴露 + 验收 | C-a 透明列（A4 同款）+ T-a 删 temp parquet 代码 + suite 加 2 case | 与 A4 行为一致，零认知分歧；硬保证靠删代码不靠测试 |
| 11 | **AST 表达形式 (新 v2)** | **内置 table function `__nr_ivm_delta('cat.ns.tbl', from_snap, to_snap)`** | 不动 sqlparser、不引入 NovaRocks-private AST wrapper；args 是普通 SQL 字面量；analyzer 在已有 `TableFactor::TableFunction` dispatch 点加分支 |
| 12 | **Thrift 节点 (新 v2)** | **`TPlanNodeType::ICEBERG_DELTA_SCAN_NODE = 1000` + `TIcebergDeltaScanNode`** | NovaRocks-only IDL 扩展，id offset 1000 留给私有节点，避免与上游 starrocks 冲突；只传 catalog/namespace/table/from/to 五字段，change_files 由 lower_plan 现场算 |

---

## 4. 架构与数据流

### 4.1 ExecPlan 形态

以 `CREATE MATERIALIZED VIEW mv_high_value AS SELECT region, amount FROM iceberg.db.orders WHERE amount > 100` 为例。`iceberg_mv_physical_select_sql` 改写后 `physical_select_sql` 是 `SELECT region, amount, _row_id AS __nova_base_row_id FROM iceberg.db.orders WHERE amount > 100`。

```
IcebergMergeSink                                          [新 operator]
   │   按 __change_op 路由：
   │     INSERT 行 → DataFileWriter → collector.inject_written_file
   │     DELETE 行 → 提取 __nova_base_row_id → A9 locator → collector.inject_delete_group
   ↑
ProjectOp { region, amount, __nova_base_row_id }          [现有 operator]
   ↑    __change_op 列透明透传
FilterOp { amount > 100 }                                 [现有 operator]
   ↑
IcebergDeltaScan {                                        [新 operator]
   base_table_ident: iceberg.db.orders,
   from_snapshot_id: v1,
   to_snapshot_id: v2,
   base_projection: [_row_id, region, amount],
   change_files: [
     (s3://.../00001.parquet,  role=DataFile,        change_op=+1),
     (s3://.../pd-001.parquet, role=PositionDelete,  change_op=-1, targets=...),
     (s3://.../ed-001.parquet, role=EqualityDelete,  change_op=-1, targets=...),
   ],
   apply_key_source: BaseRowId,
   iceberg_runtime: { base_table, delete_side: { base_first_row_ids, previous_delete_visibility } }
}
```

整棵 ExecPlan 在一个 `execute_plan` 调用内执行，pipeline 跨越 scan → filter → project → sink 一条流。

### 4.2 SQL / AST / Thrift / ExecPlan 全链路形态

#### refresh driver 端：构造延迟到 SQL 表达层

refresh driver 拿到 `physical_select_sql` 后，**在 sqlparser AST 层** mutate 一处：把唯一的 `FROM iceberg.db.orders` 这个 `TableFactor::Table` 替换为 `TableFactor::TableFunction`：

```sql
-- 原 MV physical_select_sql
SELECT region, amount, _row_id AS __nova_base_row_id
FROM iceberg.db.orders
WHERE amount > 100

-- refresh driver AST mutate 之后（语义等价 SQL）
SELECT region, amount, _row_id AS __nova_base_row_id
FROM __nr_ivm_delta('iceberg.db.orders', 7234156934512345678, 9123412412341234567) AS orders
WHERE amount > 100
```

`__nr_ivm_delta` 是 NovaRocks-only 的内置 table function。三个 args：
1. `'cat.ns.table'` —— 单字面量 string，base 表完整名（也可拆 3 个 string，看实现方便；建议拆开避免 analyzer 自己 split）
2. `from_snapshot_id: i64` —— 上次刷新对应的 base 表 snapshot
3. `to_snapshot_id: i64` —— 本次目标 snapshot（base 表当前 snapshot）

**`__nr_ivm_delta` 只传 5 个标量**，change_files / Puffin DV / first_row_id index / delete visibility 等结构化 / 重型数据**完全不出现在编译链路上层**，全部在 lower_plan 现场算。

#### analyzer：识别 table function，产 `Relation::IcebergDeltaScan`

analyzer 在 [resolve_from.rs:424 `TableFactor::TableFunction`](../../../src/sql/analyzer/resolve_from.rs:424) 已有的 dispatch 点新增一个分支：函数名 `__nr_ivm_delta` 时走专用路径。

```rust
// resolve_from.rs 内
sqlast::TableFactor::TableFunction { expr, alias } => {
    let func_name = expr.func_name_ascii_lower();
    if func_name == "__nr_ivm_delta" {
        return self.analyze_iceberg_delta_table_function(expr, alias);
    }
    // ... 老路：用户自定义 table function 等
}

fn analyze_iceberg_delta_table_function(
    &self,
    expr: &Function,
    alias: Option<&TableAlias>,
) -> Result<(Relation, AnalyzerScope), String> {
    // 1. 解析 3 个 args
    let (table_fqn, from_snap, to_snap) = parse_ivm_delta_args(expr.args)?;
    let (catalog, namespace, table) = split_fqn(&table_fqn)?;

    // 2. 查 base 表 TableDef（拿 schema + row-lineage 虚拟列）
    let table_def = self.catalog.get_table(&namespace, &table)?;
    ensure_row_lineage_advertised(&table_def)?;  // 否则报错"base 表非 v3，不能 IVM"

    // 3. 构造 scope
    let qualifier = alias.as_ref().map(|a| ...).unwrap_or(&table_def.name);
    let mut scope = AnalyzerScope::new();
    for col in &table_def.columns { scope.add_column(...) }
    for col in &table_def.iceberg_row_lineage_metadata_columns { scope.add_column(...) }

    // 4. 产出 Relation::IcebergDeltaScan（新增 Relation 变体）
    let relation = Relation::IcebergDeltaScan(IcebergDeltaScanRelation {
        catalog, namespace, table,
        from_snapshot_id: from_snap,
        to_snapshot_id: to_snap,
        qualifier,
        schema: table_def.columns.clone(),
        ...
    });
    Ok((relation, scope))
}
```

`from_snap` / `to_snap` 解析时**强制非负**（防御性）。

#### planner / optimizer：透传

`LogicalPlan` 加一个变体 `IcebergDeltaScan(LogicalIcebergDeltaScan { catalog, namespace, table, from, to, schema, ... })`。planner 把 `Relation::IcebergDeltaScan` 转成它。optimizer 现阶段不写专用优化规则，passthrough（未来 A2 可能在这里加 snapshot range merge 等）。

#### codegen：emit ICEBERG_DELTA_SCAN_NODE

`PhysicalPlan` 同样加变体 `IcebergDeltaScan(PhysicalIcebergDeltaScan)`。fragment_builder 看到这个变体时 emit：

```thrift
struct TIcebergDeltaScanNode {
    1: required string catalog,
    2: required string namespace,
    3: required string table,
    4: required i64 from_snapshot_id,
    5: required i64 to_snapshot_id,
}

// 节点本身仍走标准 TPlanNode 包装，与 HDFS_SCAN_NODE / OLAP_SCAN_NODE 同级
struct TPlanNode {
    1: required TPlanNodeId node_id,
    2: required TPlanNodeType node_type,
    ...
    NN: optional TIcebergDeltaScanNode iceberg_delta_scan_node,
}
```

`TPlanNodeType::ICEBERG_DELTA_SCAN_NODE = 1000` 显式 id，留 NovaRocks-only 节点用 1000+ 段，与 starrocks 上游的 0..999 永远不撞。

#### lower_plan：现场加载，构造 ExecNode

lower_plan 在识别 `node_type == ICEBERG_DELTA_SCAN_NODE` 时调 `lower_iceberg_delta_scan`：

```rust
fn lower_iceberg_delta_scan(
    thrift_node: &TIcebergDeltaScanNode,
    iceberg_catalogs: &IcebergCatalogRegistry,    // 新加的 lower_plan 参数
    ...
) -> Result<ExecNode, String> {
    // 1. 加载 iceberg 表句柄
    let entry = iceberg_catalogs.get(&thrift_node.catalog)?;
    let loaded = iceberg::catalog::load_table(&entry, &thrift_node.namespace, &thrift_node.table)?;

    // 2. plan_changes 算 change batch
    let batch = plan_changes(
        &loaded.table,
        thrift_node.from_snapshot_id,
        &[],  // pk_columns，A1 不用
    ).map_err(|e| format!("ivm-a1 lower delta-scan: plan_changes failed: {e}"))?;

    // 3. 构造 change_files（含 Puffin DV 字段 —— 直接从 batch 里读，不进 Thrift）
    let change_files = build_delta_source_files(&batch);

    // 4. 按需预加载 Seam 4 那些重型 state
    let factory = build_factory_for_table(&loaded.table, entry.object_store_config())?;
    let delete_side = if batch.has_delete_side() {
        Some(DeltaScanDeleteSide {
            base_first_row_ids: base_data_file_first_row_id_index(&loaded.table)?,
            previous_delete_visibility: load_existing_delete_visibility_by_data_file_at(
                &loaded.table,
                Some(batch.previous_snapshot_id),
                entry.object_store_config(),
            )?,
        })
    } else {
        None
    };

    // 5. 拼装 IcebergDeltaScanNode
    Ok(ExecNode { kind: ExecNodeKind::IcebergDeltaScan(IcebergDeltaScanNode {
        base_table_ident: BaseTableIdent { ... },
        from_snapshot_id: thrift_node.from_snapshot_id,
        to_snapshot_id: thrift_node.to_snapshot_id,
        output_chunk_schema: ...,  // 从 desc_tbl 推
        apply_key_source: ApplyKeySource::BaseRowId,
        change_files,
        object_store_config: entry.object_store_config().cloned(),
        iceberg_runtime: Arc::new(IcebergRuntimeHandles {
            base_table: loaded.table,
            object_store_factory: Arc::new(factory),
            delete_side,
        }),
        node_id: ...,
    })})
}
```

**lower_plan 是 plan_changes / 重型 state 加载的唯一时机**。Operator 拿到 `Arc<IcebergRuntimeHandles>` 时所有重型预加载都已就绪。

### 4.3 Refresh driver 流程（重构后）

```
1.  acquire mv_refresh_lock                                          [不变]
2.  load mv_definition; ensure A11 schema/lineage contract           [不变]
3.  snapshot diff 早期检查：
       根据 mv_definition.last_refresh_snapshots + 当前 base snapshot
       决定是否走增量路径，确定 (previous_snapshot_id, current_snapshot_id)
4.  早返回：plan_changes 空 delta → 仅推进 lineage，不进 pipeline
       (lower_plan 会再调一次 plan_changes —— 重复一次轻量调用，不优化掉)
5.  begin A7 staged refresh intent → staging branch                  [不变]
6.  ensure staging branch ready                                      [不变]
7.  build catalog：
       a. 构造 InMemoryCatalog
       b. 通过 build_iceberg_table_def_for_delta_scan(state, catalog, ns, table)
          注册 base 表（empty storage + advertise row-lineage 虚拟列）
       c. 同时注册 target MV 表（如果 MV SELECT 引用了，A1 简单 MV 不会）
8.  解析 physical_select_sql → sqlparser::ast::Query
9.  AST mutate：
       walk Query，找到 FROM iceberg.db.orders（base 表），替换为
       TableFactor::TableFunction {
           name: "__nr_ivm_delta",
           args: [
               'iceberg.db.orders',
               <previous_snapshot_id>,
               <current_snapshot_id>,
           ],
           alias: orders,
       }
10. collector = new_iceberg_mv_commit_collector(target_table, ident, staging_branch, op_kind)
11. merge_sink = IcebergMergeSinkFactory::new(IcebergMergeSinkPlan {
       target_table, collector: Arc::clone(&collector), apply_key_column, ...,
    })
12. execute_query(
       &nova_query, &catalog, current_database, exchange_port, query_opts,
       Some(Box::new(merge_sink)),    // ← 自定义 sink 参数（Seam 1 唯一保留改动）
    )?
       └─ 走标准编译链路：analyzer 看到 __nr_ivm_delta → Relation::IcebergDeltaScan
          → planner LogicalIcebergDeltaScan → optimizer passthrough
          → fragment_builder emit ICEBERG_DELTA_SCAN_NODE
          → lower_plan 调 plan_changes / 加载 runtime → ExecNodeKind::IcebergDeltaScan
          → pipeline 跑 sink populates collector
13. commit_iceberg_mv_with_populated_collector(
       table, catalog, entry, ident, collector, target_ref, marker,
    ).await
       → new_snapshot_id                                              [Seam 3]
14. publish + record + finalize                                        [不变，A7]
```

### 4.4 代码搬家清单

| 当前位置 | A1 后 |
|---|---|
| [changes.rs:568-679 `materialize_changes`](../../../src/connector/iceberg/changes.rs:568) 反向重建 | 搬入 `IcebergDeltaScan` operator scanners |
| [changes.rs `scan_deletes_with_*`](../../../src/connector/iceberg/changes.rs) helpers | 保留为 `IcebergDeltaScan` 调用的纯 helper |
| [mv_flow.rs:230 `execute_query_for_mv_incremental_refresh`](../../../src/engine/mv_flow.rs:230) | **删除** |
| [mv_flow.rs:282 `write_mv_delete_temp_parquet`](../../../src/engine/mv_flow.rs:282) | **删除** |
| [mv_flow.rs:387 `execute_query_for_mv_incremental_deletes`](../../../src/engine/mv_flow.rs:387) | **删除** |
| [iceberg_refresh.rs:2030-2076 双 execute_query](../../../src/engine/mv/iceberg_refresh.rs:2030) | 单 `execute_query` + 自定义 sink |
| [iceberg_refresh.rs:2166 `write_chunks_as_iceberg_data_files`](../../../src/engine/mv/iceberg_refresh.rs:2166) | 抽出 chunk → DataFile 的 streaming helper 给 sink 用；外部显式调用点删 |
| [iceberg_refresh.rs:2169 `locate_target_rows_by_apply_key`](../../../src/engine/mv/iceberg_refresh.rs:2169) | 保留为函数；sink 端调 |
| 一次性 `InMemoryCatalog::default()` + `register(IcebergFileForQuery)` + `strip_catalog_from_three_part_names` | **删除**（plan-time 路径用 `build_iceberg_table_def_for_delta_scan` 替代） |

---

## 5. 新算子契约

### 5.1 `IcebergDeltaScan`

```rust
// src/exec/node/iceberg_delta_scan.rs（新增）
pub struct IcebergDeltaScanNode {
    pub base_table_ident: BaseTableIdent,              // catalog/namespace/table 字符串
    pub from_snapshot_id: i64,
    pub to_snapshot_id: i64,
    pub output_chunk_schema: ChunkSchemaRef,           // 含 _row_id + SELECT 引用列
    pub apply_key_source: ApplyKeySource,              // BaseRowId（A9）
    pub change_files: Vec<DeltaSourceFile>,            // 由 lower_plan 现场算
    pub object_store_config: Option<ObjectStoreConfig>,
    pub iceberg_runtime: Arc<IcebergRuntimeHandles>,   // 由 lower_plan 加载
    pub node_id: i32,
}

pub struct IcebergRuntimeHandles {
    pub base_table: iceberg::table::Table,
    pub object_store_factory: Arc<OpendalRangeReaderFactory>,
    pub delete_side: Option<DeltaScanDeleteSide>,      // None 当 batch 无 delete
}

pub struct DeltaScanDeleteSide {
    pub base_first_row_ids: HashMap<String, i64>,                 // 全 base 表 manifest 扫一次
    pub previous_delete_visibility: ExistingDeleteVisibilityByDataFile,
}

pub enum DeltaSourceRole {
    DataFile,                                          // → __change_op = +1
    PositionDelete { targets: Vec<PositionDeleteTargetData> },
    EqualityDelete { equality_field_ids: Vec<i32>, targets: Vec<EqualityDeleteTargetData> },
    DeletedDataFile { previous_data_file_visibility: Option<DeletedFileVisibility> },
}

pub struct DeltaSourceFile {
    pub path: String,
    pub size: i64,
    pub role: DeltaSourceRole,
    pub partition_spec_id: Option<i32>,
    pub partition_key: Option<String>,
    pub first_row_id: Option<i64>,
    pub data_sequence_number: Option<i64>,
}
```

**注意**：`change_files`、`iceberg_runtime` 都是 lower_plan 阶段填好后传给 operator 的——operator 拿到时已经是只读 `Arc`，per-driver 共享。

**OperatorFactory 行为**：

- `is_source() == true`
- 创建 `IcebergDeltaScanOperator`，per-driver 持有 `pending_files: VecDeque<DeltaSourceFile>`（A1 第一版单 driver；multi-driver morsel 分配后续）
- 输出 schema = `output_chunk_schema`，每个 chunk 携带 `__change_op: Int8` 透明列

**`pull_chunk` streaming state machine**：

```rust
fn pull_chunk(&mut self, state: &RuntimeState) -> Result<Option<Chunk>, String> {
    loop {
        if self.current_scanner.is_none() {
            let Some(next) = self.pending_files.pop_front() else { return Ok(None); };
            self.current_scanner = Some(open_scanner_for_role(&self.node, next)?);
        }
        match self.current_scanner.as_mut().unwrap().next_batch()? {
            Some(batch) => {
                let op = self.current_scanner.as_ref().unwrap().change_op_value();
                let tagged = inject_change_op_column(batch, op)?;
                return Ok(Some(Chunk::try_new_with_chunk_schema(tagged, self.node.output_chunk_schema.clone())?));
            }
            None => { self.current_scanner = None; }
        }
    }
}
```

**Scanner per-role 行为**：

- `DataFile`：scan_one_added_data_file（changes.rs Task 3 helper），每个 RecordBatch 加 `__change_op = +1`
- `PositionDelete`：scan_position_delete_rows_for_targets（Parquet/Puffin DV 都支持，从 `DeltaSourceRole::PositionDelete.role` 内字段判断），加 `__change_op = -1`
- `EqualityDelete`：scan_equality_delete_rows_for_one，加 `__change_op = -1`
- `DeletedDataFile`：scan_one_deleted_data_file（带 previous_visibility 过滤），加 `__change_op = -1`

### 5.2 `IcebergMergeSinkFactory`

```rust
// src/engine/mv/iceberg_merge_sink.rs（新增）
pub struct IcebergMergeSinkPlan {
    pub target_table: iceberg::table::Table,
    pub collector: Arc<IcebergCommitCollector>,
    pub apply_key_column: String,                     // __nova_base_row_id
}

pub struct IcebergMergeSinkFactory {
    name: String,
    plan: Arc<IcebergMergeSinkPlan>,
}
```

**OperatorFactory 行为**：

- `is_sink() == true`
- 创建 `IcebergMergeSinkOperator`，per-driver 持有 `IcebergStreamingDataFileWriter`

**`push_chunk` 行为**：

```rust
fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
    let (insert_batch, delete_batch) = partition_chunk_by_change_op(&chunk)?;
    if let Some(batch) = insert_batch {
        data_block_on(self.writer.write_record_batch(strip_change_op(batch)?))??;
    }
    if let Some(batch) = delete_batch {
        // A9 locator 借用 target_table 的 visibility / referenced_partitions
        // 这些在 sink 端可以现场 load（与 IcebergDeltaScan 走 lower_plan 现场 load 对称）
        // 或者由 refresh driver 在创建 sink_plan 时预 load 一次传进来
        let apply_keys = extract_apply_key_values_from_record_batch(&batch, &self.plan.apply_key_column)?;
        if !apply_keys.is_empty() {
            let groups = data_block_on(locate_target_rows_by_apply_key(...))??;
            for g in groups { self.plan.collector.inject_delete_group(g); }
        }
    }
    Ok(())
}
```

**`set_finishing` 行为**：

- Flush `IcebergStreamingDataFileWriter.finish()` 收 `Vec<DataFile>`，对每个调 `collector.inject_written_file(data_file_to_written_file(&df, partition_spec_id))`
- **不调** `run_iceberg_commit`——commit 由 refresh driver 在 pipeline 之后 driveR

### 5.3 错误传播

- `IcebergDeltaScan` 中途失败（IO / 反扫错误）→ pipeline error → `execute_query` 返回 Err → refresh driver 捕获 → `abort_iceberg_mv_refresh` + cleanup（A7 已有路径）
- Sink 中途失败 → 同上；collector 累计的 staged files 由 abort path 清理
- collector 在 sink finalize 后仍未 commit → refresh driver 错误处理走 [iceberg_refresh.rs:2152 `handle_iceberg_mv_commit_error`](../../../src/engine/mv/iceberg_refresh.rs:2152)（已有）
- AST mutate 找不到唯一 base 表（理论上 A1 contract 保证唯一，但防御性）→ refresh driver 早期报错，不进 pipeline

---

## 6. IDL 扩展（NovaRocks-only）

### 6.1 `idl/thrift/PlanNodes.thrift`

```thrift
// TPlanNodeType 末尾追加 NovaRocks-only 节点，显式 id 1000+
enum TPlanNodeType {
    OLAP_SCAN_NODE,
    ...
    LAKE_CACHE_STATS_SCAN_NODE,

    // NovaRocks-only nodes start from 1000 to avoid colliding with upstream
    ICEBERG_DELTA_SCAN_NODE = 1000,
}

// IcebergDeltaScan 的 Thrift payload。change_files / 反向重建用的 visibility
// 等结构化数据 NOT 进 Thrift——lower_plan 现场调 plan_changes 计算。
struct TIcebergDeltaScanNode {
    1: required string catalog,
    2: required string namespace,
    3: required string table,
    4: required i64 from_snapshot_id,
    5: required i64 to_snapshot_id,
}

// TPlanNode 内已有 `optional` 字段挂各种 *_node payload，加一个 iceberg_delta_scan_node
struct TPlanNode {
    1: required TPlanNodeId node_id,
    2: required TPlanNodeType node_type,
    ...
    NN: optional TIcebergDeltaScanNode iceberg_delta_scan_node,  // 配 ICEBERG_DELTA_SCAN_NODE
}
```

### 6.2 兼容性

- 上游 starrocks FE 永远不会 emit `ICEBERG_DELTA_SCAN_NODE`（FE 不懂 NovaRocks IVM），NovaRocks 也不会把含此节点的 Thrift 传给 FE
- 上游 starrocks 给 `TPlanNodeType` 加新变体只占 0..999 段，NovaRocks 占 1000+ 段，**前向兼容靠 id 段隔离保证**
- 不需要给 starrocks 提交 PR

---

## 7. 验收测试（T-a）

### 7.1 实现层硬保证（不依赖测试）

A1 PR 直接**删除**：

- [mv_flow.rs:282 `write_mv_delete_temp_parquet`](../../../src/engine/mv_flow.rs:282)
- [mv_flow.rs:387 `execute_query_for_mv_incremental_deletes`](../../../src/engine/mv_flow.rs:387)
- [mv_flow.rs:230 `execute_query_for_mv_incremental_refresh`](../../../src/engine/mv_flow.rs:230)
- 相关 `std::env::temp_dir` / `std::fs::create_dir_all` / `std::fs::File::create` 调用

删干净之后"无本地磁盘也跑得通"是编译期 + 类型系统保证的事实。

### 7.2 SQL 测试新增

`tests/sql-test/iceberg-ivm/` 新增两个 case：

- **`large_delta_mixed.sql`**：base 表预填 100MB+（≥ 1M 行），执行 INSERT 1k 行 + DELETE 1k 行（用 position-delete 触发反向重建），refresh MV，验证结果与 full refresh 等价
- **`update_only.sql`**：base 表 UPDATE 一批行（Iceberg merge-on-read 产生 new data file + position-delete 对），refresh MV，验证 target 表对应行被正确替换

### 7.3 既有 suite 保持

iceberg-ivm suite 现有 case 必须全过；mv-on-iceberg suite 当前 baseline 不允许下降。

### 7.4 Spill 验证（非 A1 范围）

A1 把内存峰值压到单 chunk 是 spill 的前提；spill 实际触发的 metric 验证 + chaos 测试另起 PR。

---

## 8. 风险与缓解

| 风险 | 缓解 |
|---|---|
| Streaming state machine bug（多文件、跨 chunk 边界、locator 索引共享、driver 间 morsel 分配） | iceberg-ivm 现有 case + 新 2 case 覆盖；先单 driver 跑通再开 multi-driver |
| 反向重建 helper 从 changes.rs 搬家时语义漂移 | helper 保留单元测试；搬家时 `cargo test` 全套必跑 |
| Locator 状态在 multi-driver sink 下并发访问 | `iceberg_runtime` 包 `Arc`（只读）；`collector` 注入已带内部锁（[collector.rs:122](../../../src/connector/iceberg/commit/collector.rs:122)） |
| DataFileWriter rolling boundary 与 chunk 边界不一致 → 小文件 | 沿用 [sink.rs](../../../src/connector/iceberg/sink.rs) 现有 rolling 策略 |
| Iceberg writer API 是 async，pipeline 是 sync | sink 内 `data_block_on` 包 async，与现有 sink 一致 |
| `__nr_ivm_delta` table function 名字与用户自定义函数冲突 | 加 `__nr_` 前缀作为 reserved 命名空间；解析时优先匹配 NovaRocks 内置 |
| AST mutate 找错 / 找不到 base 表 | A1 阶段 MV 限定单 base 表（A11 contract 已强制）；找不到 / 多个匹配时 fail fast |
| `from_snapshot_id` / `to_snapshot_id` 解析为负值 | analyzer 处加 `< 0` 报错防御（iceberg spec 实践非负，但 i64 类型层面允许） |
| `__change_op` 透明列被 optimizer 误丢 | A4 透明列保留机制必须保住；新 SQL case 断言 sink 端 chunk 含 `__change_op` |
| lower_plan 现场 `plan_changes` 重复调用（refresh driver 在早返回时已调一次） | 接受重复调用；plan_changes 是 manifest 扫，单次成本可控；不引入跨调用缓存避免并发 race |

---

## 9. A1 不解决的（明示）

- managed-lake MV 的 temp parquet 路径（[ivm_delta_source.rs:81](../../../src/connector/starrocks/managed/ivm_delta_source.rs:81)）保留
- Spill 实际触发验证（runtime metric / chaos 测试）
- 多基表 / join IVM（依赖 A3；IDL 里可以预留 `ICEBERG_SNAPSHOT_SCAN_NODE = 1001` enum 变体但 lower_plan 先 unimplemented）
- 任意 `[from, to]` snapshot range（依赖 A2）
- 上游 `iceberg-rust` 新增 `IncrementalChangelogScan` 后是否切换（A1 在 NovaRocks 端手写反向重建，未来若 iceberg-rust 0.10+ 提供同款 API 可考虑迁移）

---

## 10. 参考

- POC 路线方案（Google Doc，2026-05-14 brainstorm 阶段读取）
- [IVM-A1 待办文档](file:///Users/harbor/Documents/Obsidian/NovaRocks%20TODO/IVM-A1-delta-pipeline.md)
- [IVM-A4 设计](2026-05-11-ivm-a4-changeop-execplan-design.md)
- [IVM-A7 设计](2026-05-13-ivm-a7-branch-staged-refresh-transaction-design.md)
- [IVM-A9 设计](2026-05-13-ivm-a9-iceberg-target-row-identity-apply-design.md)
- [IVM-A11 设计](2026-05-14-ivm-a11-mv-schema-field-id-contract-design.md)
- 现有 NovaRocks AST 扩展先例：[`__nr_meta_*__` metadata table dispatch](../../../src/sql/analyzer/resolve_from.rs:266) / [`AlterIcebergRefStmt`](../../../src/sql/parser/ast/iceberg_ref.rs:39)

---

## 11. 改动历史

- **2026-05-14 v1**：leaf-swap 架构（refresh driver 编译 ExecPlan 后 mutate base scan leaf 为 IcebergDeltaScan）
- **2026-05-15 v2**：放弃 leaf-swap，改为 plan-time TPlanNode + `__nr_ivm_delta` table function 全链路一等节点；理由：leaf-swap 需要 ExecPlan post-mutation 和 hint 协议，本质是隐式 side-channel；新方案让 IcebergDeltaScan 在 SQL / AST / Thrift 上都是显式可见的节点，可扩展到 join MV（增 `ICEBERG_SNAPSHOT_SCAN_NODE`）
