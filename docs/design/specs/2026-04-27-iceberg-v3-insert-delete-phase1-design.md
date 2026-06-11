# Iceberg v3 INSERT / DELETE Phase 1 Design

**日期**：2026-04-27
**状态**：Draft（待 review）
**范围**：standalone 模式下 v2 兼容的 INSERT / INSERT OVERWRITE / DELETE，写入 v2/v3 格式的 Iceberg 表

---

## 0. 背景与目标

### 0.1 背景

NovaRocks 当前对 Iceberg 的写入支持仅限 **MV refresh fast-append 路径**（[mv_refresh_iceberg.rs](../../../src/connector/starrocks/managed/mv_refresh_iceberg.rs)，commit `b93bdb6` phase4a）。直接 `INSERT INTO iceberg_table` 在 standalone 引擎里被显式拒绝（[insert_flow.rs:65-66](../../../src/engine/insert_flow.rs:65)）；`DELETE FROM iceberg_table` 没有任何 SQL 分析器/lowering 路径。

读路径已支持 v2 position deletes（[position_delete.rs](../../../src/connector/iceberg/position_delete.rs)），显式拒绝 equality deletes 与 v3 deletion vectors。

iceberg-rust 0.9.0 的 `Transaction` 公共方法仅有 `fast_append / upgrade_table_version / update_table_properties / replace_sort_order / update_location / update_statistics`，**没有** `overwrite_files / row_delta / delete_files`，且 `SnapshotProduceOperation` trait 是 `pub(crate)` 不可外部扩展。但底层原语 `ManifestWriterBuilder.build_v{2,3}_{data,deletes}() / ManifestWriter.add_file/add_delete_file/add_existing_file / ManifestListWriter::v{2,3} / Catalog::update_table(TableCommit) / TableUpdate::AddSnapshot` 全部 `pub`，足以自实现 OverwriteFiles / RowDelta 语义。

### 0.2 目标

Phase 1 在 **standalone 模式**下解锁三种 SQL：

```sql
INSERT INTO  [<catalog>.]<db>.<tbl> [(cols)] SELECT … | VALUES …
INSERT OVERWRITE [<catalog>.]<db>.<tbl> [(cols)] SELECT … | VALUES …
DELETE FROM  [<catalog>.]<db>.<tbl> WHERE <predicate>
```

写入 v2 / v3 格式的 Iceberg 表（v3 表透写 v3 manifest，由 iceberg-rust 处理）。DELETE 写 v2 兼容的 position-delete 文件（Parquet，schema = `[file_path, pos]`）。

### 0.3 非目标（Phase 1 之外，留 Phase 2+）

- v3 deletion vectors（Puffin DV）读 / 写
- equality deletes 读 / 写
- v3 row-lineage / variant 列写入支持
- FE-driven 模式（thrift `TIcebergTableSink` / `ICEBERG_DELETE_SINK` 路径）
- 静态分区 INSERT / OVERWRITE 子句（`INSERT … PARTITION(p=v)`）
- DELETE without WHERE（建议用户走 INSERT OVERWRITE）
- DELETE USING / 子查询关联 / LIMIT
- 跨表 MERGE INTO
- 局部分区 OVERWRITE（OVERWRITE WHERE …）
- 自动 compaction / orphan files GC

### 0.4 关键决策摘要

| 决策 | 选择 |
|---|---|
| 模式 | A — 仅 standalone（FE-driven 留后续） |
| INSERT 范围 | I2 — INSERT INTO + INSERT OVERWRITE（无静态分区子句） |
| DELETE 删除文件格式 | D-pos — 仅 v2 position deletes |
| 事务模型 | T2 — 单写者假设 + 失败时主动清理 staged 文件 |
| Catalog 后端 | Cat-2 — 本地 FS + S3（复用现有 [registry.rs](../../../src/connector/iceberg/catalog/registry.rs)） |
| 上游对齐 | S4 — 自实现 OverwriteCommit / RowDeltaCommit，封装在 trait 后，便于将来切换到 iceberg-rust 上游补齐的 API |
| 拆分 | D1 — Phase 1 与 Phase 2 各自独立 spec / plan / 实施 |

---

## 1. 整体架构与数据流

### 1.1 分层与职责

```
SQL: INSERT INTO / INSERT OVERWRITE / DELETE FROM iceberg_table
  │
  ▼
[src/engine/insert_flow.rs | new delete_flow.rs]   ←── 事务所有者
   ├─ 通过 IcebergCatalogRegistry 解析 catalog + table
   ├─ 加载当前 Iceberg 表的 base snapshot
   ├─ v3 兼容性校验（启用 row-lineage / 含 variant 列 → fail-fast）
   ├─ schema 严格列对列校验（不做隐式转换/重排）
   ├─ 构造 IcebergCommitCollector(op_kind, base_snapshot_id, schema, partition_spec, staging_dir)
   └─ 把 SQL lower 成 ExecPlan
  │
  ▼
[src/lower/…]
   INSERT INTO      : SCAN(SELECT) → IcebergSink(DataFiles)
   INSERT OVERWRITE : SCAN(SELECT) → IcebergSink(DataFiles)         ─┐ 同样的 plan,
   DELETE           : SCAN(table, filter=WHERE) emit (_file,_pos)    │ collector 的
                      → IcebergSink(PositionDeletes)                ─┘ op_kind 区分
  │
  ▼
[src/exec/pipeline + src/connector/iceberg/sink.rs]              ←── 写入器
   IcebergSink 在 staging 路径写 Parquet：
     - 数据文件 (DataContentType::Data)
     - 位置删除文件 (DataContentType::PositionDeletes)
   每个文件 close → collector.record_file(WrittenFile{...})
  │
  ▼ pipeline 成功完成
[engine 层继续]                                                   ←── 提交协调
   IcebergCommitAction trait 的实现按 op_kind 分发：
     INSERT INTO       → FastAppendCommit (薄包装 Transaction::fast_append)
     INSERT OVERWRITE  → OverwriteCommit  (自实现，S4 抽象层)
     DELETE            → RowDeltaCommit   (自实现,  S4 抽象层)
  │
  ▼
[iceberg-rust 0.9 公共原语]                                      ←── catalog 提交
   ManifestWriter / ManifestListWriter / Snapshot / TableCommit
   updates = [AddSnapshot, SetSnapshotRef("main", …)]
   requirements = [AssertRefSnapshotId(base_snapshot_id), …]
   catalog.update_table(commit)
  │
  ▼ pipeline error / commit error / OCC 失败 / cancel
collector.abort():  通过 OpenDAL 删除已 staged 的数据/删除/manifest 文件
                    （manifest list 仅在 update_table 成功后才被 catalog 引用，未 commit 时不会被读到）
```

### 1.2 关键设计点

1. **engine 层是事务所有者**：staging 路径申请、lowering、sink 调用、commit、abort 都由它驱动；与 phase4a [mv_refresh_iceberg.rs](../../../src/connector/starrocks/managed/mv_refresh_iceberg.rs) 的层级一致，不打破现有骨架。
2. **IcebergSink 保持纯写入器角色**：复用 [sink.rs](../../../src/connector/iceberg/sink.rs)；只新增"每个文件 close 回调注入 collector"的 hook，不改变其内部 Parquet 写逻辑。
3. **IcebergCommitCollector 是有状态的中间层**：记录本次 query 所有 staged 文件，提供 `record_file / commit / abort` 入口。文件路径全部基于一个 query 级唯一的 staging UUID 前缀，便于 abort 时一键扫除。
4. **IcebergCommitAction trait 是 S4 的抽象边界**：今天三个实现（FastAppend / Overwrite / RowDelta），将来 iceberg-rust 上游补齐 OverwriteFiles / RowDelta API 时，对应实现内部替换即可，不影响 collector / engine 层。
5. **DELETE 计划与上游 StarRocks 对齐**：`SCAN(table, filter=WHERE) → emit (_file, _pos) → IcebergSink(PositionDeletes)`；`_file/_pos` 列由 scan 算子在已有 row-position 基础设施（[row_position.rs](../../../src/exec/row_position.rs)）上发射。
6. **OVERWRITE 与 DELETE 共享 commit 抽象**：因为 iceberg-rust 0.9 这两个动作都得自实现，把它们都收在 `IcebergCommitAction` 后面，避免 engine 层有 `match op_kind`-style 的 commit 分支。

### 1.3 不动的代码

- 现有 IcebergSink 的 Parquet 写入、分区分桶、文件 roll、位置删除写入（本来就支持的 `IcebergSinkMode::PositionDeletes`）
- phase4a 的 [mv_refresh_iceberg.rs](../../../src/connector/starrocks/managed/mv_refresh_iceberg.rs) commit 路径
- HDFS scan 路径的 append-only 限制（DELETE 写位置删除文件后读路径已支持）
- managed-lake / FE-driven 入口

### 1.4 新增 / 修改的代码

| 位置 | 改动 |
|---|---|
| [insert_flow.rs:65](../../../src/engine/insert_flow.rs:65) | 拆掉 iceberg 拒绝；接入 IcebergCommitCollector |
| `src/engine/delete_flow.rs`（新） | DELETE FROM 入口 |
| `src/sql/parser/ast.rs` | 在 `InsertStmt` 加 `overwrite: bool`；新增 `DeleteStmt` |
| `src/engine/statement.rs` | `convert_sqlparser_insert_to_custom` 透传 overwrite；新增 `convert_sqlparser_delete_to_custom` |
| `src/engine/mod.rs` | 新增 `Statement::Delete` 分支 |
| `src/connector/iceberg/commit/mod.rs`（新模块） | `IcebergCommitCollector` + `IcebergCommitAction` trait + `CommitOpKind` |
| `src/connector/iceberg/commit/abort.rs`（新） | `AbortLog` |
| `src/connector/iceberg/commit/validation.rs`（新） | 共享校验函数 |
| `src/connector/iceberg/commit/fast_append.rs`（新） | FastAppendCommit |
| `src/connector/iceberg/commit/overwrite.rs`（新） | OverwriteCommit |
| `src/connector/iceberg/commit/row_delta.rs`（新） | RowDeltaCommit |
| [sink.rs](../../../src/connector/iceberg/sink.rs) | 新增 commit_collector 回调 hook |
| `src/lower/...` | DELETE 节点 lowering（INSERT 复用现有路径） |

---

## 2. SQL 表面、分析与执行计划形态

### 2.1 INSERT INTO

**接受语法**：

```sql
INSERT INTO [<catalog>.]<db>.<tbl> [(col1, col2, ...)]
  SELECT ... | VALUES (...), ...
```

**AST 不变**：复用 `InsertStmt`；source 优先 `FromQuery`，VALUES 走 literal 快速路径。

**校验**（在 [insert_flow.rs](../../../src/engine/insert_flow.rs) 进入 lowering 之前）：

1. 表存在且 backend = iceberg
2. v3 兼容性：拒绝启用 row-lineage、含 variant 列的表
3. schema 严格列对列匹配：
   - 显式列列表 → 按列名匹配 + 类型严格相等（不做隐式 cast）
   - 无列列表 → SELECT 列数 = 表列数 + 类型严格相等
   - 不允许列重排，缺失列必须有 default 才允许（Phase 1 不支持 default → 必须显式列出所有列）
4. 拒绝 partition spec 已演化的表（多 partition spec），仅支持当前 spec
5. 既有的非 iceberg 限制保持不变

**ExecPlan 形态**：

```
[SELECT subplan]
   │ chunks 与表 schema 一致
   ▼
IcebergSink(DataFiles)
   │ 每文件 close → collector.record_file(content=Data)
   ▼
pipeline 完成
   │
   ▼
FastAppendCommit::commit
```

### 2.2 INSERT OVERWRITE

**接受语法**：

```sql
INSERT OVERWRITE [<catalog>.]<db>.<tbl> [(col1, col2, ...)]
  SELECT ... | VALUES (...), ...
```

**AST 改动**：`InsertStmt.overwrite: bool`；`convert_sqlparser_insert_to_custom` 透传 sqlparser `Insert.overwrite`；非 iceberg backend 的 INSERT OVERWRITE 暂时拒绝。

**语义**：整表覆盖 = 当前快照所有数据文件作为 deleted_data_files；从 SELECT 写出的新数据文件作为 added_data_files；同一个新 snapshot 同时完成"删旧 + 加新"。

**校验**：与 INSERT INTO 一致，外加：
- 拒绝静态分区子句（与 §0.3 范围一致）

**ExecPlan 形态**：与 INSERT INTO 完全一致。op_kind=Overwrite 仅在 collector 与 commit-action 层区分。

### 2.3 DELETE FROM

**接受语法**：

```sql
DELETE FROM [<catalog>.]<db>.<tbl> WHERE <predicate>;
```

**Phase 1 限制**：
- WHERE 子句必填；`DELETE FROM t`（无 WHERE）显式拒绝，错误提示建议用 `INSERT OVERWRITE t SELECT * FROM t WHERE FALSE`
- 不支持 USING / 子查询关联删除
- 不支持 LIMIT / ORDER BY

**AST 新增**：`DeleteStmt { table, where_clause }`；`mod.rs` 增加 `sqlast::Statement::Delete` 分支 → `convert_sqlparser_delete_to_custom` → `delete_flow::execute`。

**校验**（在 `delete_flow.rs`）：
1. 表存在且 backend = iceberg
2. v3 兼容性同 §2.1
3. WHERE 表达式：复用现有 SELECT 上 WHERE 的分析 / lowering
4. 拒绝 partition spec 演化的表
5. 拒绝当前快照已含 equality-delete 文件的表（reader 不支持，写完后无法读）

**ExecPlan 形态**：

```
IcebergScan(table, project=[_file, _pos], filter=WHERE)
   │ 输出 chunks 列 = (_file STRING, _pos BIGINT)
   ▼
IcebergSink(PositionDeletes)
   │ 内部按 _file 分组 → 每个 referenced data file 一组 → 写一个 v2/v3 删除文件
   │ 每文件 close → collector.record_file(content=PositionDeletes, referenced_data_file=…)
   ▼
pipeline 完成
   │
   ▼
RowDeltaCommit::commit
```

`_file/_pos` 发射：
- iceberg scan 内部已经维护 `(file_path, row_position)`（reader 用其做 position-delete 过滤）
- [hdfs_scan.rs](../../../src/lower/node/hdfs_scan.rs) lowering 时识别"投影里包含 `_file/_pos`"并指示 scan 算子 emit
- v2 spec 含义为文件内行号；v3 表 [row_position.rs:18](../../../src/exec/row_position.rs:18) 注释提到的 `scan_range_id + row_id` 不在 Phase 1 落地——继续走 v2 spec 的文件内行号
- 不引入"DELETE 专用 scan node 类型"

### 2.4 共享校验工具

`src/connector/iceberg/commit/validation.rs`：
- `ensure_v3_writable(table) -> Result<()>`
- `ensure_single_partition_spec(table) -> Result<()>`
- `ensure_no_equality_deletes(table) -> Result<()>`
- `match_select_schema_to_table(select_schema, table_schema, columns_clause) -> Result<…>`

INSERT 与 DELETE flow 都调用这些函数，错误信息一致。

### 2.5 三种语句的 plan 形态对比

| 语句 | scan 来源 | scan 投影 | sink 模式 | commit-action |
|---|---|---|---|---|
| INSERT INTO | SELECT subplan | 与表 schema 同 | DataFiles | FastAppendCommit |
| INSERT OVERWRITE | SELECT subplan | 与表 schema 同 | DataFiles | OverwriteCommit |
| DELETE FROM | 表自身 IcebergScan | (_file, _pos) | PositionDeletes | RowDeltaCommit |

---

## 3. IcebergCommitCollector 与 sink 集成

### 3.1 数据模型

```rust
// src/connector/iceberg/commit/mod.rs
pub struct IcebergCommitCollector {
    op_kind: CommitOpKind,            // FastAppend | Overwrite | RowDelta
    table_ident: TableIdent,
    base_snapshot_id: Option<i64>,    // None 表示空表（首次写入）
    base_sequence_number: i64,
    schema: SchemaRef,
    partition_spec: PartitionSpecRef,
    staging_dir: String,              // <table_location>/data/_staging/<query_uuid>/
    written_files: Mutex<Vec<WrittenFile>>,
    committed: AtomicBool,
}

pub enum CommitOpKind { FastAppend, Overwrite, RowDelta }

pub struct WrittenFile {
    pub path: String,
    pub format: DataFileFormat,                // Parquet
    pub content: DataContentType,              // Data | PositionDeletes
    pub partition_values: Struct,
    pub record_count: u64,
    pub file_size_in_bytes: u64,
    pub split_offsets: Vec<i64>,
    pub column_stats: Option<ColumnStats>,
    pub referenced_data_file: Option<String>,  // 仅 PositionDeletes 需要
    pub key_metadata: Option<Vec<u8>>,
}
```

要点：
- `Mutex<Vec<...>>` 保护并发 push（多个 sink driver 并行）
- `committed: AtomicBool` 单调标记，防止 commit/abort 后误用
- `base_snapshot_id` + `base_sequence_number` 在 collector 构造时一次性 snapshot，commit 阶段构造 `AssertRefSnapshotId(base_snapshot_id)` 用作 OCC
- `staging_dir` 作为 query 级唯一前缀；放在表的 `data/_staging/` 下避免 listing 数据时混入

### 3.2 lifecycle

```
engine.execute(stmt):
  1. validate(stmt, table) → 校验通过
  2. let collector = IcebergCommitCollector::new(op_kind, table, query_uuid)
  3. lower(stmt, &collector) → ExecPlan        // sink 算子持有 Arc<IcebergCommitCollector>
  4. pipeline.run(plan)
       on each file close: collector.record_file(WrittenFile)
       on pipeline error : break, jump to abort
  5. let action: Box<dyn IcebergCommitAction> = action_for(op_kind)
  6. action.commit(&collector, table, catalog)
       on commit error: jump to abort
  7. collector.mark_committed()
```

abort 路径：

```
collector.abort(catalog):
  if !committed:
      for f in written_files: opendal.delete(f.path)
      // manifest 文件由 commit-action 自己清理（见 §4 / §5）
      committed = true
```

### 3.3 sink 集成

```rust
pub struct IcebergSinkConfig {
    // …existing fields…
    pub commit_collector: Option<Arc<IcebergCommitCollector>>,  // None = phase4a 旧路径
}

// sink "file close" 回调里追加：
if let Some(c) = &config.commit_collector {
    c.record_file(WrittenFile::from_writer_close(...))?;
}
```

- phase4a MV refresh：`commit_collector = None`，行为不变
- INSERT/INSERT OVERWRITE/DELETE：`commit_collector = Some(...)` 走新路径
- sink 算子内部不区分 op_kind，统一"写文件 + 报告"

`WrittenFile::from_writer_close` 把 sink 已计算的 metadata 打包；`content_type` 由 sink 当前模式决定。

### 3.4 column_stats 取舍

iceberg-rust `DataFile` 支持 `lower_bounds / upper_bounds / null_value_counts / nan_value_counts / column_sizes / value_counts`。

Phase 1：
- **必带**：`record_count`、`file_size_in_bytes`、`split_offsets`、`partition_values`、`column_sizes`、`value_counts`、`null_value_counts`
- **暂不带**：`lower_bounds / upper_bounds`（需从 Parquet 列统计解码 + truncate；不是正确性必须）
- 后续按需扩展，不影响 commit 正确性

### 3.5 staging 路径与最终路径

| 场景 | 路径 |
|---|---|
| 写入期间 | `<table_location>/data/_staging/<query_uuid>/[part=…/]…parquet` |
| commit 成功后 | 不重命名，原地引用——Iceberg manifest 直接登记 staging 内绝对路径 |
| commit 失败 abort | 删除 `<table_location>/data/_staging/<query_uuid>/` 下所有文件 |

不 rename 的理由：
- Iceberg 以 manifest 而非目录布局定义"哪些文件属于 snapshot"
- 上游 StarRocks 也直接登记 staging 路径
- `_staging/<query_uuid>/` 这一层用于 abort 时整目录清理；commit 成功后与正常数据并存

抗孤儿：commit 失败但 abort 也失败（进程崩溃）的极端情况留运维 runbook 处理；不在 Phase 1 自动 GC。

### 3.6 公开接口

```rust
pub trait IcebergCommitAction: Send + Sync {
    fn commit(
        &self,
        collector: &IcebergCommitCollector,
        table: &Table,
        catalog: &dyn Catalog,
    ) -> Result<CommitOutcome>;
}

pub struct CommitOutcome {
    pub new_snapshot_id: i64,
    pub written_manifest_paths: Vec<String>,
}

pub async fn run_iceberg_write_or_delete(
    ctx: &EngineCtx,
    op_kind: CommitOpKind,
    plan: ExecPlan,
    collector: Arc<IcebergCommitCollector>,
    catalog: Arc<dyn Catalog>,
    table: Table,
) -> Result<CommitOutcome>;
```

`run_iceberg_write_or_delete` 负责"运行 plan → 失败时清理 → 否则 commit → commit 失败时清理"的完整流程。

---

## 4. Commit Action 层 — FastAppend / Overwrite / RowDelta

### 4.1 共享 trait 与上下文

```rust
pub trait IcebergCommitAction: Send + Sync {
    /// 必要时写入新 manifest，构造 TableCommit，通过 catalog.update_table 原子提交。
    /// 失败时不能留下"已被引用但不应存在"的状态——所有 manifest 引用都
    /// 在 catalog.update_table 成功之前才会被外部看见。
    async fn commit(&self, ctx: CommitCtx<'_>) -> Result<CommitOutcome>;
}

pub struct CommitCtx<'a> {
    pub collector: &'a IcebergCommitCollector,
    pub table: &'a Table,
    pub catalog: &'a dyn Catalog,
    pub commit_uuid: Uuid,
    pub abort_handle: Arc<AbortLog>,
}
```

`AbortLog` 见 §5.2；commit-action 每写一个 manifest 就 `abort_handle.record_manifest(path)`，commit 失败时由 §5 据此清理。

S4 抽象边界：commit-action 是唯一直接调用 iceberg-rust transaction API / manifest writer / catalog.update_table 的层。engine 层只调 trait 方法，不感知是 fast_append 还是自实现 RowDelta。将来 iceberg-rust 暴露 `Transaction::overwrite_files() / row_delta()` 时，对应实现内部替换即可。

**空输入 no-op 规约**（三个实现统一）：

| op_kind | written 为空 | base 也为空（首次写入空表） | 行为 |
|---|---|---|---|
| FastAppend | 是 | — | no-op：不构造 snapshot，不调 catalog.update_table，返回 `CommitOutcome { new_snapshot_id = base_snapshot_id, … }` |
| RowDelta | 是 | — | 同上 no-op |
| Overwrite | 是 | 是 | no-op |
| Overwrite | 是 | 否（base 含数据） | **不是** no-op——意味着"清空表"，按正常流程构造仅含 DELETED entries 的 snapshot |
| Overwrite | 否 | 是 | 正常流程（首次写入） |
| Overwrite | 否 | 否 | 正常流程 |

### 4.2 FastAppendCommit（INSERT INTO）

薄包装：

```rust
pub struct FastAppendCommit;

#[async_trait]
impl IcebergCommitAction for FastAppendCommit {
    async fn commit(&self, ctx: CommitCtx<'_>) -> Result<CommitOutcome> {
        let written = ctx.collector.take_written_files();
        debug_assert!(written.iter().all(|f| f.content == DataContentType::Data));

        let data_files = written.iter().map(WrittenFile::to_iceberg_data_file).collect();
        let mut tx = Transaction::new(ctx.table);
        let action = tx.fast_append()
            .add_data_files(data_files)
            .set_commit_uuid(ctx.commit_uuid)
            .set_snapshot_properties(default_summary_props(ctx, "append"));
        action.apply(&mut tx)?;
        let table_after = tx.commit(ctx.catalog).await?;
        Ok(CommitOutcome {
            new_snapshot_id: table_after.metadata().current_snapshot().unwrap().snapshot_id(),
            committed_manifest_paths: vec![],
        })
    }
}
```

- 只接受 `content=Data`；其它 content 由 `validate_added_data_files` 拒绝
- v3 表上 fast_append 已正确写 v3 manifest（[append.rs:253-256, snapshot.rs:443](file:///Users/harbor/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/iceberg-0.9.0/src/transaction/snapshot.rs:443) 已验证）

### 4.3 OverwriteCommit（INSERT OVERWRITE）

iceberg-rust 0.9 无 OverwriteFiles 动作，自实现。

**输入**：`written` = 新写的数据文件（`content=Data`）；`base` = 表的 base snapshot。

**步骤**：

1. **枚举 base 中所有现存 data files**：从 base snapshot 的 manifest list 读所有 data manifest，收集 live entries（status=ADDED 或 EXISTING）→ `existing_data_files: Vec<(DataFile, source_manifest_seq)>`。
   - 用 `ManifestList::parse_with_version(...)` + `ManifestFile.load(...)` 公共 API
   - base 有的 delete manifest 在 OVERWRITE 后变成"引用了已删除数据"的孤儿；Phase 1 不主动清理（与上游 StarRocks 一致），靠 Iceberg expire-snapshots / orphan-files 兜底

2. **写"DELETED" 数据 manifest**：用 `ManifestWriterBuilder.build_v{2,3}_data()` 新建 manifest 文件，对每个 `existing_data_files` 的元素写 `add_existing_file(file, seq=base_sequence_number, status=DELETED)`。
   - **Spike 风险**：iceberg-rust `ManifestWriter` 的 `add_*` 方法是否直接暴露 status=DELETED 入口需在实施第一阶段验证；如未暴露则回退到手工组装 `ManifestEntry { status: ManifestStatus::Deleted, … }` 直接走 avro encoding（见 §7）
   - 文件名：`<table_metadata_dir>/<commit_uuid>-overwrite-deletes-N.avro`，每写完调 `abort_handle.record_manifest(path)`

3. **写新数据 manifest**：用 `ManifestWriterBuilder.build_v{2,3}_data()` 新建，对每个 `written` data file 调 `add_file(file, sequence_number=new_seq)`。

4. **写 manifest list**：用 `ManifestListWriter::v{2,3}` 写：
   - 不继承 base manifest list（"overwrite-deletes" manifest 已把原数据全标 DELETED；继承反而冗余）
   - 等价做法：继承 base + 重写每个 base manifest 为 status=DELETED 的版本——更接近 iceberg-java，但 manifest 数量多
   - **方案选择**：Phase 1 用前者（不继承 + 集中删除 manifest），逻辑简单语义等价
   - 文件名：`<table_metadata_dir>/snap-<new_snap_id>-<commit_uuid>.avro`

5. **构造 Snapshot**：`Snapshot::builder()` 设 `snapshot_id`、`parent_snapshot_id=base_snapshot_id`、`sequence_number=base_sequence_number+1`、`timestamp_ms=now()`、`manifest_list=<manifest_list_path>`、`summary={ operation: "overwrite", … 计数 }`、`schema_id=current`。

6. **构造 TableCommit**：

```
updates = [
  TableUpdate::AddSnapshot { snapshot },
  TableUpdate::SetSnapshotRef { ref_name: "main", reference: SnapshotReference{snapshot_id: new_id, retention: Branch{…}} },
]
requirements = [
  TableRequirement::AssertRefSnapshotId { ref_: "main", snapshot_id: Some(base_snapshot_id) }, // OCC
  TableRequirement::AssertCurrentSchemaId { current_schema_id: ... },
  TableRequirement::AssertDefaultSpecId   { default_spec_id: ... },
]
```

7. **catalog.update_table(commit)**：原子提交。
   - 成功 → `CommitOutcome{ new_snapshot_id, committed_manifest_paths: [delete_manifest, data_manifest, manifest_list] }`；abort_handle 保留这些路径但 §5 在 commit 成功时**不删**它们
   - 失败 → 错误向上抛；§5 据 abort_handle 删除已写但未引用的 manifest + manifest list；数据文件由 collector 删除

### 4.4 RowDeltaCommit（DELETE）

比 OverwriteCommit 简单：base 数据文件全保留，只追加新的 position-delete 文件。

**输入**：`written` = 新写的 position-delete files（`content=PositionDeletes`，每个有 `referenced_data_file`）。

**步骤**：

1. **新增删除 manifest**：用 `ManifestWriterBuilder.build_v{2,3}_deletes()` 新建 delete manifest，对每个 `written` 文件调 `add_delete_file(file, sequence_number=new_seq)`。
2. **不写新的 data manifest**。
3. **写 manifest list**：
   - 继承 base manifest list 所有 entry（保持原 data manifests 与已有 delete manifests 不变）
   - 加新增的 delete manifest entry
   - 用 `ManifestListWriter::v{2,3}` 写
4. **构造 Snapshot**：`summary.operation = "delete"`，`parent_snapshot_id=base_snapshot_id`，`sequence_number=base_sequence_number+1`。
5. **TableCommit**：与 §4.3 同形（AddSnapshot + SetSnapshotRef + AssertRefSnapshotId + AssertCurrentSchemaId）。
6. **catalog.update_table(commit)**：与 §4.3 同样的成功 / 失败处理。

要点：
- `referenced_data_file` 在 sink 阶段就已写入 `WrittenFile`；`to_iceberg_data_file()` 把它放进 `DataFile.referenced_data_file`
- DELETE 不修改 base 的任何 manifest，每次 DELETE 增长 O(1) 个 manifest——长期需要 compaction（不是 Phase 1 目标）

### 4.5 三个实现的对比

| 维度 | FastAppend | Overwrite | RowDelta |
|---|---|---|---|
| 是否复用 iceberg-rust action | 是（`Transaction::fast_append`） | 否（自实现） | 否（自实现） |
| 写入新 manifest 数 | 由 iceberg-rust 决定 | 2（删除 + 新数据） | 1（新删除） |
| 是否继承 base manifest list | 是 | 否（用集中删除 manifest 代替） | 是 |
| OCC 需求 | iceberg-rust 内置 base-ref 校验 | AssertRefSnapshotId + AssertCurrentSchemaId + AssertDefaultSpecId（手填） | 同左 |
| 失败回滚责任 | iceberg-rust 自身（我们清理 collector 数据文件） | 我们自己（清理 2 manifest + manifest list） | 我们自己（清理 1 manifest + manifest list） |

### 4.6 S4 上游迁移路径

未来 iceberg-rust 暴露 `Transaction::overwrite_files() / row_delta()` 时：

1. `OverwriteCommit::commit` 内部直接换成 `tx.overwrite_files().add_data_files(...).delete_files(...)`，外部接口零变化
2. `RowDeltaCommit::commit` 内部换成 `tx.row_delta().add_delete_files(...)`
3. abort 路径里"清理 manifest"分支可缩短或移除（上游 action 自己接管 manifest 生命周期）
4. trait 与 collector 与 engine 层零改动

为兼容这条路径，§4.3 / §4.4 实现里"读 base manifests / 写新 manifests / 构造 Snapshot / 调 catalog.update_table"必须封装成**模块内部**辅助函数，不让细节泄漏到 trait 接口或 collector 数据结构上。

### 4.7 已知 Phase 1 简化

1. OVERWRITE 不主动清理被覆盖数据上挂着的 position-delete 文件（变成"指向已删除数据的孤儿"，靠 Iceberg expire-snapshots / orphan-files 兜底）
2. DELETE 不做 compaction
3. 没有 partition-aware 的 OVERWRITE WHERE 局部覆盖
4. 没有 sequence-number-based 的多版本 RowDelta 冲突解决（单写者假设，AssertRefSnapshotId 足以保护）

---

## 5. 失败处理与 abort 清理（T2 实现）

### 5.1 失败模式分类

| # | 阶段 | 触发例子 | 已写入磁盘的内容 | abort 清理责任 |
|---|---|---|---|---|
| F1 | sink 写入中途 | sink writer 报错 / IO 错误 / 算子 panic | 部分数据文件 + 部分位置删除文件 | collector 已 record_file 的全部物理文件 |
| F2 | pipeline cancel | 用户 Ctrl-C / 上层 cancel signal | 同 F1 | 同 F1 |
| F3 | pipeline 完成但未进入 commit | engine 在 collect→commit 之间 panic | 与 F1 相同 | 同 F1 |
| F4 | commit 中途构造 manifest 失败 | manifest writer IO 错误 / Snapshot 构造异常 | F1 集合 + 部分 manifest avro 文件 + 可能的 manifest list | F1 集合 + abort_handle 记录的 manifest 路径 |
| F5 | catalog.update_table 失败 | OCC 冲突 / 网络错误 / catalog 拒绝 | F1 集合 + 全部 manifest + manifest list（**未被任何 snapshot 引用**） | F1 集合 + abort_handle 记录的 manifest 路径 |
| F6 | catalog.update_table "commit unknown" | 网络中断后无法判定提交是否真生效 | 与 F5 相同，但**可能**已被引用 | **不删**（见 5.4） |
| F7 | 进程崩溃 / OOM | 任何阶段 | 子集 | 进程内 abort 不可达；磁盘留孤儿（见 5.5） |

### 5.2 AbortLog 数据结构

```rust
// src/connector/iceberg/commit/abort.rs
pub struct AbortLog {
    staged_data_files: Mutex<Vec<String>>,
    written_manifests:  Mutex<Vec<String>>,
    cleared: AtomicBool,
}

impl AbortLog {
    pub fn record_data_file(&self, path: String);
    pub fn record_manifest(&self, path: String);
    pub async fn cleanup(&self, fs: &Operator) -> Vec<CleanupError>;
}
```

- `Mutex<Vec<String>>` 保护并发 push
- 数据文件 / manifest 分两条链路（语义不同）
- `cleanup` 返回 `Vec<CleanupError>`：abort 是 best-effort，单文件失败不阻塞其它

### 5.3 abort 触发与清理算法

唯一入口（在 `run_iceberg_write_or_delete` 里）：

```rust
match outcome {
    Ok(commit_outcome) => collector.mark_committed(),
    Err(commit_err) if commit_err.is_commit_unknown() => {
        warn!("iceberg commit unknown — leaving all staged files for manual review: {commit_err}");
        return Err(commit_err.into());
    }
    Err(other) => {
        let cleanup_errors = run_abort(&abort_handle, &collector, &fs).await;
        for e in cleanup_errors { warn!("abort cleanup error: {e}"); }
        return Err(other.into());
    }
}
```

```rust
async fn run_abort(
    abort: &AbortLog,
    collector: &IcebergCommitCollector,
    fs: &Operator,
) -> Vec<CleanupError> {
    if abort.cleared.swap(true, Ordering::SeqCst) { return vec![]; }
    let mut errs = Vec::new();
    for path in collector.take_written_files_paths() {
        if let Err(e) = fs.delete(&path).await { errs.push((path, e).into()); }
    }
    for path in abort.drain_manifests() {
        if let Err(e) = fs.delete(&path).await { errs.push((path, e).into()); }
    }
    let _ = fs.delete_dir_if_empty(&collector.staging_dir()).await;
    errs
}
```

清理顺序无强约束。先数据文件后 manifest 是为了 abort 半途出错时残留更可观察。

### 5.4 commit-unknown 状态处理

`catalog.update_table` 失败分两类：

1. **明确失败**（HTTP 4xx / OCC AssertRefSnapshotId 不通过 / catalog 拒绝）：F5 路径，正常 abort
2. **状态不明**（网络中断 / 5xx / 超时）：F6 路径

iceberg-rust 0.9 的错误类型对二者区分有限。Phase 1 策略：

- 默认 fail-safe：除非能明确判定 OCC 冲突 / catalog 拒绝，否则按"状态不明"处理 → 不清理
- 错误信息附上所有已写文件路径
- `warn!` 级别 + metrics
- 等价于"宁可留孤儿、不要错删 catalog 已引用的文件"——孤儿可被 Iceberg orphan-files 流程清理，错删不可恢复

### 5.5 进程崩溃 / OOM 孤儿（F7）

进程内 abort 不可达 → staging 路径下留孤儿。

1. 不在 NovaRocks 内做自动 GC
2. staging 路径设计已使其易识别：`<table>/data/_staging/<query_uuid>/`
3. 运维 runbook：手工 / 标准 Iceberg orphan-files action / 周期任务清理

### 5.6 sink 内部分文件回滚

| 子情况 | 处理 |
|---|---|
| Parquet writer close 前失败 | 该文件不进 collector，但磁盘上可能有半截文件——sink 内部 close-on-error 路径必须 best-effort 删自己写一半的文件 |
| Parquet writer close 后但 record_file 之前失败 | 文件完整但未跟踪——理论上是孤儿。sink 必须在 close 成功后**第一时间**调 record_file，且二者之间无可失败操作 |

[sink.rs](../../../src/connector/iceberg/sink.rs) 这两条路径作为 Phase 1 实施的代码 review 检查项。

### 5.7 cancel 信号传播

- Pipeline cancel → sink driver 收到 cancel → 释放当前 writer（best-effort 删半截文件）→ pipeline 报 cancel error → engine 走 F1/F2 abort ✓
- Catalog 调用过程中 cancel：iceberg-rust `catalog.update_table` 不一定 cancel-safe → 退化为 F6（commit-unknown）→ 不清文件 ✓
- 不引入"在 commit 中途强制 abort"的机制

### 5.8 错误信息要求

`run_iceberg_write_or_delete` 抛出的错误必须包含：
- op_kind（INSERT / INSERT OVERWRITE / DELETE）
- table identifier（catalog.namespace.table）
- base_snapshot_id
- staging_dir
- 原始底层错误（`source` chain）

日志另打印：record 数据文件数、manifest 数、commit 阶段（pre-manifest / pre-list / pre-update_table）。

---

## 6. 测试策略

### 6.1 测试层级

| 层级 | 工具 | 覆盖目标 |
|---|---|---|
| 单元测试 | `cargo test` + `tokio::test` | commit-action 内部算法、manifest 构造、validation、AbortLog |
| 集成测试 | Rust integration test（`tests/iceberg_*.rs`） | 完整 INSERT / INSERT OVERWRITE / DELETE 流程，本地 FS catalog |
| SQL 回归 | [sql-test-runner](../../../tests/sql-test-runner) | 端到端 SQL 行为，作为 Phase 1 验收门 |
| 故障注入 | 集成测试 + 自定义 OpenDAL wrapper | abort、commit-unknown、OCC 冲突 |
| 兼容性 | Spark 3.5 + iceberg-spark-runtime | 跨引擎 round-trip（手工 / nightly） |

### 6.2 单元测试

`src/connector/iceberg/commit/`：

- `abort.rs::test_*`：AbortLog 幂等性、并发 record、清理失败汇总
- `validation.rs::test_*`：v3 兼容性、schema 不匹配、partition spec 演化、equality-deletes 表
- `overwrite.rs::test_*`：假 base snapshot + 假 written files，验证 TableCommit 内 updates / requirements 内容（不调真 catalog）
- `row_delta.rs::test_*`：验证 manifest list 继承、新 delete manifest entry 内容
- `fast_append.rs::test_*`：FastAppendCommit 字段映射

单元层用 in-memory mock catalog（实现 `iceberg::Catalog` trait 的最小内存版本）。

### 6.3 集成测试

文件：`tests/iceberg_insert_delete.rs`（新）。每用例使用 tempdir + 本地 FS catalog（无网络）。

**正向用例**：

| ID | 用例 | 验证 |
|---|---|---|
| IT-INS-1 | 创建 v2 表 → INSERT INTO → 重读 | 行数 / 内容相等 |
| IT-INS-2 | 创建 v3 表 → INSERT INTO → 重读 | 行数 / 内容相等；format_version=v3 |
| IT-INS-3 | 分区表 INSERT INTO（多分区值） | 每分区文件路径正确 |
| IT-INS-4 | 重复 INSERT INTO 三次 | snapshot 链长度 = 3 |
| IT-OW-1 | INSERT INTO 然后 INSERT OVERWRITE | 仅看到 OVERWRITE 后数据 |
| IT-OW-2 | 空表 INSERT OVERWRITE | base_snapshot_id=None 路径走通 |
| IT-OW-3 | 分区表 INSERT OVERWRITE | 全部分区被替换 |
| IT-DEL-1 | INSERT INTO → DELETE WHERE 部分 → 重读 | 剩余行 = 预期；position-delete 内容正确 |
| IT-DEL-2 | DELETE WHERE 命中 0 行 | 不写删除文件，commit 不发生 |
| IT-DEL-3 | DELETE WHERE 跨多 data file | 每个 referenced_data_file 一组删除文件 |
| IT-DEL-4 | DELETE 后再 INSERT | 删除与新增数据共存正确 |
| IT-RT-1 | 多次 INSERT + DELETE 交替 | 最终读出符合 SQL 语义；snapshot 链完整 |

**负向用例**：

| ID | 用例 | 期望错误 |
|---|---|---|
| NEG-1 | INSERT 列数不匹配 | schema validation error |
| NEG-2 | INSERT 类型不匹配 | type mismatch error（不做隐式 cast） |
| NEG-3 | DELETE FROM t（无 WHERE） | "WHERE 子句必填"错误 |
| NEG-4 | row-lineage 表 INSERT | v3 兼容性错误 |
| NEG-5 | 含 variant 列的表 INSERT | 同上 |
| NEG-6 | 当前快照含 equality-deletes 的表 DELETE | reader 不支持错误 |

### 6.4 SQL 回归

`tests/sql-test-runner/.../suites/iceberg-write/`：
- `insert.sql` / `insert.expected`
- `overwrite.sql` / `overwrite.expected`
- `delete.sql` / `delete.expected`
- `mixed.sql` / `mixed.expected`

```bash
NO_PROXY=127.0.0.1,localhost cargo run --release -- standalone-server --port 9030 &

cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg-write --mode verify
```

`--mode record` 用于首次生成 expected；CI 走 `verify`。

### 6.5 故障注入

OpenDAL fault wrapper（`tests/common/fault_fs.rs`）：

| ID | 用例 | 期望 |
|---|---|---|
| FI-1 | sink 写第二个数据文件 IO 错误 | 错误抛出；staging 全清；catalog 表状态 = base_snapshot_id |
| FI-2 | manifest 写入 IO 错误 | staging 数据文件清；已写 manifest 清 |
| FI-3 | catalog.update_table 返回 OCC 冲突 | F5：staging + manifest 全清 |
| FI-4 | catalog.update_table 模拟超时 | F6：**不清理**任何文件；错误信息含路径 |
| FI-5 | pipeline cancel 在写第一个文件途中 | 半截文件被 best-effort 删；其它清理一致 |

### 6.6 跨引擎兼容性（手工 / nightly，不卡门）

1. NovaRocks INSERT 写出的 v2 / v3 表 → Spark 3.5 + iceberg-spark-runtime 读，结果一致
2. NovaRocks DELETE 写出的 position-delete 文件 → Spark 正确应用
3. Spark INSERT → NovaRocks DELETE → Spark 再读，剩余行符合预期
4. Spark INSERT OVERWRITE → NovaRocks 读，最新数据正确

材料放 `tests/cross-engine/`，Phase 1 不要求每 PR 跑。

### 6.7 性能基线（不卡门）

- 1M 行 INSERT INTO 单分区 v3 表：耗时与 staged 文件数
- 100K 行 DELETE WHERE 命中：耗时与 position-delete 文件数

写入 commit 的 PR description 作为后续优化基线。

### 6.8 CI 集成

- 单元测试：`cargo test`（不变）
- 集成测试：`cargo test --test iceberg_insert_delete`
- SQL 回归：CI 启 standalone-server + 跑 `--suite iceberg-write --mode verify`
- 跨引擎与性能基线：**不进 CI**

### 6.9 验收门槛

Phase 1 完成时下列必须通过：

- §6.2 单测全绿
- §6.3 IT-* / NEG-* 全绿
- §6.4 SQL 回归 verify 全绿
- §6.5 FI-* 全绿
- §6.6 跨引擎手工 round-trip 通过

---

## 7. 实施风险与 Spike 项

### 7.1 Spike：iceberg-rust ManifestWriter 的 status=DELETED 入口

`ManifestWriter.add_existing_file(file, sequence_number)` 当前签名是否能直接生成 status=DELETED 的 entry 需要在 Phase 1 实施第一周内验证。

- 通过 → 按 §4.3 步骤 2 描述继续
- 不通过 → 升级为 Plan 风险，技术选择回退到"手工组装 ManifestEntry { status: ManifestStatus::Deleted, … } + 直接走 ManifestWriter 内部 avro encoding 公共 API"
- 仍不可行 → fork iceberg-rust 引入 patch（最坏情况，需在 Plan 阶段重新评估）

### 7.2 Spike：iceberg-rust 错误类型对 commit-unknown 的可识别性

§5.4 的 commit-unknown 区分依赖于能否从 `iceberg::Error` 链识别"明确失败 vs 状态不明"。如果能力不足：

- 通过 → 实现 `is_commit_unknown()` 谓词
- 不通过 → 简化为"所有 update_table 错误都按 commit-unknown 处理"（保守，孤儿稍多但安全）

### 7.3 风险：phase4a MV refresh 与 collector 路径并存

新引入 IcebergCommitCollector 时不动 phase4a，但需保证 sink 改动（新增 `commit_collector: Option<...>`）对 MV refresh 路径完全透明。回归测试覆盖：执行一次 MV refresh，确保行为字节级别相同。

### 7.4 风险：v3 manifest 写入 iceberg-rust 0.9 的覆盖度

iceberg-rust 0.9 已支持 v3 manifest 写入（`build_v3_data / build_v3_deletes / ManifestListWriter::v3`），但生产案例未必充分。Phase 1 实施时需在 v3 表上跑通端到端 round-trip（含 6.6 跨引擎读）作为信心检查。

---

## 8. 已知限制与 Phase 2 衔接

| 限制 | 说明 | Phase 2 处理 |
|---|---|---|
| 仅 standalone | FE-driven 路径不在 Phase 1 | 单独 spec，复用 commit-action / collector 抽象 |
| 仅 v2 兼容 deletes | 不支持 v3 deletion vectors | Phase 2 主体内容 |
| OVERWRITE 不清理孤儿 delete 文件 | 依赖 Iceberg expire-snapshots | 不计划在 NovaRocks 内自动清理 |
| 无 compaction | DELETE manifest 长期增长 | 不在 Phase 2 范围 |
| 无 partial OVERWRITE | 静态分区 OVERWRITE 排除 | 后续按需求加 I3 |
| schema 严格匹配 | 不做隐式 cast | 长期保留（与 NovaRocks 整体设计一致） |
| 单写者假设 | 多写者并发不保证 | 需要时引入 OCC 重试策略 |

Phase 2 的 deletion vectors 实施将复用：
- IcebergCommitCollector / WrittenFile（content 类型扩展为 `PuffinDV`）
- IcebergCommitAction trait（新增 PuffinRowDeltaCommit 实现）
- AbortLog（puffin blob 路径与 manifest 路径一同纳入）
- v3 兼容性校验（启用相反——支持 row-lineage / variant 时的处理）

---

## 9. 参考

- 上游 StarRocks Iceberg sink：`fe/fe-core/src/main/java/com/starrocks/planner/IcebergTableSink.java`、`be/src/connector/iceberg_chunk_sink.{h,cpp}`、`be/src/connector/iceberg_delete_sink.{h,cpp}`、`fe/fe-core/src/main/java/com/starrocks/connector/iceberg/IcebergMetadata.java`
- iceberg-rust 0.9 源码（本地 cargo registry）：`/Users/harbor/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/iceberg-0.9.0/`
- NovaRocks 现状代码：[sink.rs](../../../src/connector/iceberg/sink.rs)、[position_delete.rs](../../../src/connector/iceberg/position_delete.rs)、[hdfs_scan.rs](../../../src/lower/node/hdfs_scan.rs)、[insert_flow.rs](../../../src/engine/insert_flow.rs)、[mv_refresh_iceberg.rs](../../../src/connector/starrocks/managed/mv_refresh_iceberg.rs)
- Iceberg spec：v2 spec position deletes、v3 spec deletion vectors（Phase 2 参考）
