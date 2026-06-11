# IVM-A1 Delta Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> **Plan version:** v2（2026-05-15 改方案，原 leaf-swap 路线已废弃；参见 spec § 11 改动历史 / Task 12 替换说明）

**Goal:** 把 iceberg-backed MV 增量刷新从"两次独立 `execute_query` + temp parquet + 异步写后 commit"重构为"一次 `execute_query` + 新算子 `IcebergDeltaScan` 流式反向重建 + 新算子 `IcebergMergeSinkFactory` 按 `__change_op` 路由写入；A7 staging 与 A10 commit 仍由 refresh driver 调度"。

**Architecture (v2):** 把 `IcebergDeltaScan` 做成编译时一等节点：在 `idl/thrift/PlanNodes.thrift` 新增 `TPlanNodeType::ICEBERG_DELTA_SCAN_NODE = 1000` + `TIcebergDeltaScanNode` payload；refresh driver 在 sqlparser AST 层把 base 表的 `TableFactor::Table` 替换为内置 table function `__nr_ivm_delta('cat.ns.tbl', from_snap, to_snap)`；analyzer/planner/optimizer/codegen 整条链路原生识别此节点；lower_plan 现场调 `plan_changes` 计算 `change_files` 并加载 `IcebergRuntimeHandles`（含 base_first_row_ids + previous_delete_visibility）；pipeline 顶部由 refresh driver 通过 `execute_query` 的 sink 参数挂上 `IcebergMergeSinkFactory`；pipeline 跑完 refresh driver 调 `commit_iceberg_mv_with_populated_collector`。删除 `materialize_changes` / `write_mv_delete_temp_parquet` / `execute_query_for_mv_incremental_refresh` / `execute_query_for_mv_incremental_deletes` 四个旧路径。

**Tech Stack:** Rust 2024 / Arrow `RecordBatch` / Iceberg 0.9 (vendored) / opendal / NovaRocks `Operator`/`OperatorFactory`/`ProcessorOperator`/`ExecNodeKind` 体系 + Thrift IDL 扩展；测试用 `cargo test` + `tests/sql-test-runner` 跑 `iceberg-ivm` suite。

**Spec reference:** [`docs/design/specs/2026-05-14-ivm-a1-delta-pipeline-design.md`](../specs/2026-05-14-ivm-a1-delta-pipeline-design.md)

---

## File Structure Overview

### Create

| Path | Purpose |
|---|---|
| `src/exec/node/iceberg_delta_scan.rs` | `IcebergDeltaScanNode` + supporting structs (`DeltaSourceRole`, `DeltaSourceFile`, `ApplyKeySource`, `IcebergRuntimeHandles`, `DeltaScanDeleteSide`) |
| `src/exec/operators/iceberg_delta_scan.rs` | `IcebergDeltaScanFactory` + `IcebergDeltaScanOperator` (streaming pull operator) |
| `src/engine/mv/iceberg_merge_sink.rs` | `IcebergMergeSinkFactory` + `IcebergMergeSinkOperator` |
| `src/lower/thrift/iceberg_delta_scan.rs` | Thrift → ExecNode 转换：`lower_iceberg_delta_scan` (调 `plan_changes` + 加载 runtime) |
| `sql-tests/iceberg-ivm/sql/iceberg_ivm_a1_large_delta_mixed.sql` | New SQL test: 100MB+ delta + INSERT/DELETE 混合 |
| `sql-tests/iceberg-ivm/sql/iceberg_ivm_a1_update_only.sql` | New SQL test: UPDATE-only refresh |
| `sql-tests/iceberg-ivm/result/iceberg_ivm_a1_large_delta_mixed.result` | Recorded baseline |
| `sql-tests/iceberg-ivm/result/iceberg_ivm_a1_update_only.result` | Recorded baseline |

### Modify

| Path | What |
|---|---|
| `idl/thrift/PlanNodes.thrift` | 新增 `TPlanNodeType::ICEBERG_DELTA_SCAN_NODE = 1000` + `TIcebergDeltaScanNode` struct + `TPlanNode.iceberg_delta_scan_node` optional 字段 |
| `src/exec/node/mod.rs` | Add `IcebergDeltaScan(IcebergDeltaScanNode)` variant + extend `output_slots_for_node` / `push_down_local_runtime_filters_inner` / 其他 match dispatch sites |
| `src/exec/operators/mod.rs` | Export `IcebergDeltaScanFactory` |
| `src/exec/pipeline/builder.rs` | Add `ExecNodeKind::IcebergDeltaScan(...)` arm 构造 `IcebergDeltaScanFactory` |
| `src/lower/fragment.rs` / `src/lower/thrift/lower_plan.rs` | Dispatch `ICEBERG_DELTA_SCAN_NODE` → `lower_iceberg_delta_scan` |
| `src/sql/analyzer/resolve_from.rs` | `TableFactor::TableFunction` 分支新增 `__nr_ivm_delta` 识别 → `Relation::IcebergDeltaScan` |
| `src/sql/analyzer/mod.rs` | `Relation` 加 `IcebergDeltaScan(IcebergDeltaScanRelation)` 变体 |
| `src/sql/planner/...` | `LogicalPlan` 加 `IcebergDeltaScan(LogicalIcebergDeltaScan)` 变体，passthrough |
| `src/sql/optimizer/...` | `PhysicalPlan` 加 `IcebergDeltaScan(PhysicalIcebergDeltaScan)` 变体，passthrough |
| `src/sql/codegen/fragment_builder.rs` | `PhysicalIcebergDeltaScan` → emit `TPlanNode { node_type: ICEBERG_DELTA_SCAN_NODE, iceberg_delta_scan_node: Some(...) }` |
| `src/engine/mod.rs` | `execute_query` / `execute_plan` 增加可选 `terminal_sink: Option<Box<dyn OperatorFactory>>` 参数；其他 caller 传 None 用默认 ResultSink |
| `src/connector/iceberg/changes.rs` | 抽出 `materialize_changes` 内部反向重建为 per-file streaming-friendly helpers；`materialize_changes` 在 Task 14 才删 |
| `src/engine/mv/iceberg_refresh.rs` | `refresh_iceberg_mv` 增量分支重写：AST mutate + 自定义 sink 调 `execute_query` + `commit_iceberg_mv_with_populated_collector` |
| `src/engine/mv/mod.rs` | `pub mod iceberg_merge_sink;` |

### Delete

| Path | Why |
|---|---|
| `src/engine/mv_flow.rs` lines for `execute_query_for_mv_incremental_refresh` (230-280) | Replaced by single `execute_query` with custom sink |
| `src/engine/mv_flow.rs` lines for `write_mv_delete_temp_parquet` (282-329) | 不再走 temp parquet |
| `src/engine/mv_flow.rs` lines for `delete_temp_table_def_from_batch_schema` (331-378) | 一次性 catalog 魔术不需要 |
| `src/engine/mv_flow.rs` lines for `execute_query_for_mv_incremental_deletes` (387-431) | Replaced by single `execute_query` with custom sink |
| `src/connector/iceberg/changes.rs` `materialize_changes` (569-679) | 被新算子吞掉 |

### Reverted from v1 (废弃)

| Path | 状态 |
|---|---|
| `src/engine/mv/iceberg_delta_plan.rs` | v1 创建的 leaf-swap rewrite 文件，v2 不需要，**整体删除** |
| `src/engine/mod.rs` `compile_query_to_exec_plan` / `lower_single_fragment_to_exec_plan` 三函数抽取 | v1 的 Seam 1，v2 不需要 ExecPlan 切片，**回退**（只保留 `execute_query` 的 sink 参数化） |

### 不动（明示）

- `src/connector/hdfs.rs`（A4 已落地的 `ivm_change_op` 透明列保留；HDFS_SCAN 不引入 IVM 分支）
- `src/connector/iceberg/sink.rs`（FE-thrift-driven 现有 sink 不动；新 MV sink 独立模块）
- `src/connector/iceberg/commit/*`（A10 commit 框架不动）
- `src/connector/starrocks/managed/ivm_delta_source.rs`（managed-lake MV 仍走 temp parquet，A1 不动）
- `src/engine/mv/iceberg_target_apply.rs` 内部实现（A9 helper `load_target_apply_locator_inputs` / `locate_target_rows_by_apply_key` 函数签名不动，只是调用点搬家）

---

## Verification Strategy

每个 Phase 的验证手段：

| Phase | 验证命令 |
|---|---|
| Phase 0-3 (单元代码) | `cargo build --lib` + `cargo test --lib -- iceberg_delta_scan iceberg_merge_sink` |
| Phase 4 (IDL + analyzer + codegen + lower) | `cargo build --lib` + 编译期保证 Thrift 解码 + 单元测试 `__nr_ivm_delta` AST mutate 与 analyzer 分发 |
| Phase 5 (integration) | `cargo build` + 启动 standalone-server，手动 mysql client refresh 一次小 MV 烟测 |
| Phase 6 (deletion) | `cargo build` (确保删完没有死引用) + `cargo clippy --lib -- -D warnings` |
| Phase 7-8 (SQL tests) | `cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --config $NOVAROCKS_SQL_TEST_CONFIG --suite iceberg-ivm --mode verify` |

启动 standalone-server 的 Phase 5 / Phase 7 / Phase 8 需要 docker iceberg-rest 环境：

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
NO_PROXY=127.0.0.1,localhost target/debug/novarocks standalone-server \
  --config "$NOVAROCKS_STANDALONE_CONFIG" >/tmp/novarocks-server.log 2>&1 &
# wait for NOVAROCKS_READY marker per CLAUDE.md §7.3
```

---

# Phase 0 — Foundations: Data Types & ExecNodeKind Variant

### Task 1: 定义 `IcebergDeltaScanNode` 数据类型 + role 枚举

**Files:**
- Create: `src/exec/node/iceberg_delta_scan.rs`
- Modify: `src/exec/node/mod.rs:1-100`

- [ ] **Step 1: 写新文件，仅放数据类型骨架**

Create `src/exec/node/iceberg_delta_scan.rs`:

```rust
//! IVM `IcebergDeltaScan` ExecNode: snapshot-range delta source.
//!
//! Single source leaf that internally consumes Iceberg snapshot diff
//! products (data files / position-delete / equality-delete / deleted-data-file)
//! and emits a unified chunk stream tagged with the A4 transparent
//! `__change_op` column (+1 for INSERT, -1 for DELETE). Populated by
//! `lower_iceberg_delta_scan` (in `src/lower/thrift/iceberg_delta_scan.rs`)
//! when the Thrift plan carries `TPlanNodeType::ICEBERG_DELTA_SCAN_NODE`.

use std::sync::Arc;

use crate::connector::iceberg::changes::ChangedDataFile;
use crate::connector::iceberg::changes::DeletedDataFileRef;
use crate::connector::iceberg::changes::EqualityDeleteRef;
use crate::connector::iceberg::changes::PositionDeleteRef;
use crate::exec::chunk::ChunkSchemaRef;
use crate::fs::object_store::ObjectStoreConfig;
use crate::fs::opendal::OpendalRangeReaderFactory;

#[derive(Clone, Debug)]
pub struct IcebergDeltaScanNode {
    pub base_table_ident: BaseTableIdent,
    pub from_snapshot_id: i64,
    pub to_snapshot_id: i64,
    pub output_chunk_schema: ChunkSchemaRef,
    pub apply_key_source: ApplyKeySource,
    pub change_files: Vec<DeltaSourceFile>,
    pub object_store_config: Option<ObjectStoreConfig>,
    pub iceberg_runtime: Arc<IcebergRuntimeHandles>,
    pub node_id: i32,
}

#[derive(Clone, Debug)]
pub struct BaseTableIdent {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
}

#[derive(Clone, Debug)]
pub enum ApplyKeySource {
    /// A9 hidden apply key: base table's `_row_id` v3 row lineage column.
    BaseRowId,
}

#[derive(Clone, Debug)]
pub struct DeltaSourceFile {
    pub path: String,
    pub size: i64,
    pub role: DeltaSourceRole,
    pub partition_spec_id: Option<i32>,
    pub partition_key: Option<String>,
    pub first_row_id: Option<i64>,
    pub data_sequence_number: Option<i64>,
}

#[derive(Clone, Debug)]
pub enum DeltaSourceRole {
    DataFile,
    PositionDelete {
        targets: Vec<PositionDeleteTargetData>,
    },
    EqualityDelete {
        equality_field_ids: Vec<i32>,
        targets: Vec<EqualityDeleteTargetData>,
    },
    DeletedDataFile {
        previous_data_file_visibility: Option<DeletedFileVisibility>,
    },
}

#[derive(Clone, Debug)]
pub struct PositionDeleteTargetData {
    pub data_file_path: String,
    pub data_file_first_row_id: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct EqualityDeleteTargetData {
    pub data_file_path: String,
    pub data_file_first_row_id: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct DeletedFileVisibility {
    pub already_deleted_positions: Vec<i64>,
}

/// Iceberg per-table runtime handles required by `IcebergDeltaScanOperator`
/// to open delete files and re-read target data files. Constructed by
/// `lower_iceberg_delta_scan` when lowering `ICEBERG_DELTA_SCAN_NODE` —
/// `base_table` comes from `iceberg::Catalog::load_table`, `delete_side`
/// is populated via `base_data_file_first_row_id_index` +
/// `load_existing_delete_visibility_by_data_file_at` only when the change
/// batch has any DELETE-side roles.
#[derive(Debug)]
pub struct IcebergRuntimeHandles {
    pub base_table: iceberg::table::Table,
    pub object_store_factory: Arc<OpendalRangeReaderFactory>,
    pub delete_side: Option<DeltaScanDeleteSide>,
}

#[derive(Debug)]
pub struct DeltaScanDeleteSide {
    pub base_first_row_ids: std::collections::HashMap<String, i64>,
    pub previous_delete_visibility:
        crate::engine::delete_flow::ExistingDeleteVisibilityByDataFile,
}
```

- [ ] **Step 2: 在 `ExecNodeKind` 加 variant**

Modify `src/exec/node/mod.rs:50-89`:

```rust
// At top of file, add import (find existing use block for ValuesNode):
pub mod iceberg_delta_scan;
pub use iceberg_delta_scan::{
    ApplyKeySource, BaseTableIdent, DeltaScanDeleteSide, DeltaSourceFile, DeltaSourceRole,
    DeletedFileVisibility, EqualityDeleteTargetData, IcebergDeltaScanNode, IcebergRuntimeHandles,
    PositionDeleteTargetData,
};

// In `ExecNodeKind` enum (currently lines 70-89):
pub enum ExecNodeKind {
    AssertNumRows(AssertNumRowsNode),
    Values(ValuesNode),
    Project(ProjectNode),
    Filter(FilterNode),
    Repeat(RepeatNode),
    UnionAll(UnionAllNode),
    Limit(LimitNode),
    ExchangeSource(ExchangeSourceNode),
    Scan(ScanNode),
    IcebergDeltaScan(IcebergDeltaScanNode),  // ← new
    Fetch(FetchNode),
    LookUp(LookUpNode),
    Aggregate(AggregateNode),
    Join(JoinNode),
    NestedLoopJoin(NestedLoopJoinNode),
    Sort(SortNode),
    TableFunction(TableFunctionNode),
    Analytic(AnalyticNode),
    SetOp(SetOpNode),
}
```

- [ ] **Step 3: 验证编译失败（缺 match arms）**

Run: `cargo build --lib 2>&1 | head -40`
Expected: FAIL with multiple "non-exhaustive patterns: `IcebergDeltaScan(_)` not covered" errors in `mod.rs` and `builder.rs`.

- [ ] **Step 4: 添加 `output_slots_for_node` 与 `push_down_local_runtime_filters_inner` 的分支**

Modify `src/exec/node/mod.rs` — find `output_slots_for_node` (around line 193):

```rust
        ExecNodeKind::Scan(scan) => Some(
            scan.output_chunk_schema()
                .slot_ids()
                .iter()
                .copied()
                .collect(),
        ),
        ExecNodeKind::IcebergDeltaScan(scan) => Some(
            scan.output_chunk_schema
                .slot_ids()
                .iter()
                .copied()
                .collect(),
        ),
```

Find `push_down_local_runtime_filters_inner` (around line 312):

```rust
        ExecNodeKind::IcebergDeltaScan(_) => {
            // delta source is a leaf; runtime filters do not apply (A1)
        }
```

- [ ] **Step 5: 编译通过**

Run: `cargo build --lib 2>&1 | grep -E "(error\[|warning\[)" | head -20`
Expected: 仅有 `unused import` 类 warnings（`PositionDeleteRef` 等先 import 不用），无错误。

- [ ] **Step 6: Commit**

```bash
git add src/exec/node/iceberg_delta_scan.rs src/exec/node/mod.rs
git commit -m "feat(ivm-a1): add IcebergDeltaScan ExecNode skeleton"
```

---

### Task 2: 在 pipeline builder 与 lower/fragment 分发处加 stub

**Files:**
- Modify: `src/exec/pipeline/builder.rs:1220-1260`
- Modify: `src/lower/fragment.rs:105-140`

- [ ] **Step 1: 在 `builder.rs` 加临时 unimplemented stub**

Find the match `ExecNodeKind::Values(...)` arm (around line 1223):

```rust
        ExecNodeKind::IcebergDeltaScan(_) => {
            return Err(
                "IcebergDeltaScan pipeline build not yet implemented; expected in Phase 2"
                    .to_string(),
            );
        }
```

- [ ] **Step 2: 在 `lower/fragment.rs` preserve dispatch 加 no-op arm**

Find `ExecNodeKind::Values(_)` arm (around line 135):

```rust
        ExecNodeKind::IcebergDeltaScan(_) => {}
```

- [ ] **Step 3: 编译通过**

Run: `cargo build --lib 2>&1 | grep -E "error\[" | head -10`
Expected: 无错误（warnings 允许）。

- [ ] **Step 4: Commit**

```bash
git add src/exec/pipeline/builder.rs src/lower/fragment.rs
git commit -m "feat(ivm-a1): stub IcebergDeltaScan dispatch sites"
```

---

# Phase 1 — Helper Extraction: Streaming-Friendly Reverse Projection

### Task 3: 抽出 reusable streaming-friendly delete reverse-projection helpers

`materialize_changes` 当前内联了三类 helper 调用。Phase 2 的 `IcebergDeltaScanOperator` 要以 per-file 粒度按需调用，这里先把这些 helper 公开成"输入一个 file/refs，输出 `Vec<RecordBatch>`"的纯函数（先保留 eager 形态，Task 7 再让算子里把它们封装成 streaming 迭代器）。

**Files:**
- Modify: `src/connector/iceberg/changes.rs`（添加 `scan_position_delete_rows_for_targets` / `scan_equality_delete_rows_for_target` / `scan_deleted_data_file_rows_with_visibility` 三个新公开函数，从 `materialize_changes` body 抽出来）

- [ ] **Step 1: 在 `changes.rs` 新加纯函数 wrapper**

Add at file end (after `materialize_changes`):

```rust
/// Helper for `IcebergDeltaScanOperator`: scan one position-delete file
/// and reverse-project deleted rows from its target data file(s).
///
/// Returns rows with the same projection as a regular base-table scan
/// (including `_row_id` for A9 apply key). Each row has not yet had
/// `__change_op` injected — the operator will add it.
pub(crate) fn scan_position_delete_rows_for_targets(
    base_table: &iceberg::table::Table,
    delete: &PositionDeleteRef,
    base_first_row_ids: &std::collections::HashMap<String, i64>,
    factory: &crate::fs::opendal::OpendalRangeReaderFactory,
    object_store_config: Option<&crate::fs::object_store::ObjectStoreConfig>,
) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
    let size_lookup = |_path: &str| -> Option<u64> { None };
    crate::connector::iceberg::scan_deletes::scan_deletes_with_base_row_id_lookup_and_path_normalizer(
        std::slice::from_ref(delete),
        factory,
        base_table.file_io(),
        size_lookup,
        |path| base_first_row_ids.get(path).copied(),
        |path| normalize_delete_projection_path(path, object_store_config),
    )
    .map_err(|e| e.to_string())
}

/// Helper for `IcebergDeltaScanOperator`: scan one equality-delete file
/// and reverse-project the matching rows from its target data file(s).
pub(crate) fn scan_equality_delete_rows_for_one(
    base_table: &iceberg::table::Table,
    delete: &EqualityDeleteRef,
    factory: &crate::fs::opendal::OpendalRangeReaderFactory,
    object_store_config: Option<&crate::fs::object_store::ObjectStoreConfig>,
) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
    scan_equality_delete_rows_for_table(
        base_table,
        std::slice::from_ref(delete),
        factory,
        object_store_config,
    )
}

/// Helper for `IcebergDeltaScanOperator`: scan one deleted data file
/// (i.e., a file that was present at previous_snapshot and removed in
/// current snapshot). Returns the live rows from that file at the previous
/// snapshot, applying the previous-visibility delete mask.
pub(crate) fn scan_one_deleted_data_file(
    base_table: &iceberg::table::Table,
    deleted_file: &DeletedDataFileRef,
    object_store_config: Option<&crate::fs::object_store::ObjectStoreConfig>,
    previous_delete_visibility: &std::collections::HashMap<
        String,
        crate::engine::delete_flow::DeleteVisibilityForFile,
    >,
) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
    scan_deleted_data_file_rows_with_visibility(
        base_table,
        std::slice::from_ref(deleted_file),
        object_store_config,
        previous_delete_visibility,
    )
}
```

- [ ] **Step 2: 复用 changes.rs 内既有 materialize_changes 测试**

新增的三个 `scan_*` helper 只是把 `materialize_changes` body 已有的同名内层调用以"per-file 入参"形式重新公开，**语义未变**。已有的 `materialize_changes` 测试覆盖（同模块内 `#[cfg(test)]` 块）继续作为 helper 语义的回归屏障；不再为 helper 单独写 trivial smoke。Phase 7 SQL 测试覆盖端到端语义。

如果搬家过程中出现行为分歧（罕见），同模块既有 materialize_changes 测试会直接失败——这是合意的早期信号。

- [ ] **Step 3: 编译 + 运行现有 changes.rs 单元测试**

Run: `cargo test --lib --no-run 2>&1 | tail -5`
Expected: 成功 build；既有 tests 编译通过。

Run: `cargo test --lib --package novarocks changes:: 2>&1 | tail -10`
Expected: 既有测试全部 PASS（这次只是抽 helper，未改语义）。

- [ ] **Step 4: Commit**

```bash
git add src/connector/iceberg/changes.rs
git commit -m "refactor(iceberg-changes): extract per-file reverse-projection helpers for IVM-A1"
```

---

# Phase 2 — IcebergDeltaScan Operator (Streaming)

### Task 4: `IcebergDeltaScanFactory` + skeleton operator with empty-stream behavior

**Files:**
- Create: `src/exec/operators/iceberg_delta_scan.rs`
- Modify: `src/exec/operators/mod.rs` (add `pub mod iceberg_delta_scan; pub use iceberg_delta_scan::IcebergDeltaScanFactory;`)
- Modify: `src/exec/pipeline/builder.rs` (replace Phase-0 stub with real factory construction)

- [ ] **Step 1: 写失败的单元测试（empty change_files → 立即结束）**

Create `src/exec/operators/iceberg_delta_scan.rs`:

```rust
//! Streaming source operator for the `IcebergDeltaScan` ExecNode.
//!
//! Per-driver pull-driven scanner: at each `pull_chunk` call, advances
//! through `change_files`, opening a per-role scanner on demand and emitting
//! one chunk at a time. The `__change_op` column is added as a transparent
//! constant per-file (`+1` for `DataFile`, `-1` for delete roles).

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int8Array};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use crate::exec::change_op::{CHANGE_OP_COLUMN, CHANGE_OP_DELETE, CHANGE_OP_INSERT};
use crate::exec::chunk::Chunk;
use crate::exec::node::iceberg_delta_scan::{
    DeltaSourceFile, DeltaSourceRole, IcebergDeltaScanNode, IcebergRuntimeHandles,
};
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::runtime_state::RuntimeState;

#[derive(Clone)]
pub struct IcebergDeltaScanFactory {
    name: String,
    node: Arc<IcebergDeltaScanNode>,
}

impl IcebergDeltaScanFactory {
    pub fn new(node: IcebergDeltaScanNode) -> Self {
        let name = format!("IcebergDeltaScan (id={})", node.node_id);
        Self {
            name,
            node: Arc::new(node),
        }
    }
}

impl OperatorFactory for IcebergDeltaScanFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, driver_id: i32) -> Box<dyn Operator> {
        // Phase 1: single-driver only. driver_id != 0 returns an empty stream.
        let pending = if driver_id == 0 {
            self.node.change_files.clone().into_iter().collect()
        } else {
            std::collections::VecDeque::new()
        };
        Box::new(IcebergDeltaScanOperator {
            name: self.name.clone(),
            node: Arc::clone(&self.node),
            pending,
            current_scanner: None,
            finished: false,
        })
    }

    fn is_source(&self) -> bool {
        true
    }
}

struct IcebergDeltaScanOperator {
    name: String,
    node: Arc<IcebergDeltaScanNode>,
    pending: std::collections::VecDeque<DeltaSourceFile>,
    current_scanner: Option<Box<dyn DeltaFileScanner>>,
    finished: bool,
}

trait DeltaFileScanner: Send {
    /// Pull next batch from the underlying scan. Returns None when exhausted.
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, String>;
    /// Constant `__change_op` value to inject on every batch this scanner produces.
    fn change_op_value(&self) -> i8;
}

impl Operator for IcebergDeltaScanOperator {
    fn name(&self) -> &str {
        &self.name
    }

    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }

    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }

    fn is_finished(&self) -> bool {
        self.finished
    }
}

impl ProcessorOperator for IcebergDeltaScanOperator {
    fn need_input(&self) -> bool {
        false
    }

    fn has_output(&self) -> bool {
        !self.finished
    }

    fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> Result<(), String> {
        Err("IcebergDeltaScan does not accept input".to_string())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        loop {
            if self.current_scanner.is_none() {
                let Some(next) = self.pending.pop_front() else {
                    self.finished = true;
                    return Ok(None);
                };
                self.current_scanner = Some(open_scanner_for_role(&self.node, next)?);
            }
            match self.current_scanner.as_mut().unwrap().next_batch()? {
                Some(batch) => {
                    let op = self.current_scanner.as_ref().unwrap().change_op_value();
                    let tagged = inject_change_op_column(batch, op)?;
                    let chunk = Chunk::from_record_batch(tagged, &self.node.output_chunk_schema)?;
                    return Ok(Some(chunk));
                }
                None => {
                    self.current_scanner = None;
                }
            }
        }
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        Ok(())
    }
}

fn inject_change_op_column(batch: RecordBatch, value: i8) -> Result<RecordBatch, String> {
    let rows = batch.num_rows();
    let arr: ArrayRef = Arc::new(Int8Array::from(vec![value; rows]));
    let mut fields: Vec<arrow::datatypes::Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields.push(crate::exec::change_op::change_op_field());
    let new_schema = Arc::new(arrow::datatypes::Schema::new(fields));
    let mut columns = batch.columns().to_vec();
    columns.push(arr);
    RecordBatch::try_new(new_schema, columns)
        .map_err(|e| format!("inject __change_op column: {e}"))
}

fn open_scanner_for_role(
    node: &IcebergDeltaScanNode,
    file: DeltaSourceFile,
) -> Result<Box<dyn DeltaFileScanner>, String> {
    match file.role {
        DeltaSourceRole::DataFile => Err("data-file scanner: TODO Task 5".to_string()),
        DeltaSourceRole::PositionDelete { .. } => Err("position-delete scanner: TODO Task 6".to_string()),
        DeltaSourceRole::EqualityDelete { .. } => Err("equality-delete scanner: TODO Task 7".to_string()),
        DeltaSourceRole::DeletedDataFile { .. } => {
            Err("deleted-data-file scanner: TODO Task 8".to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::node::iceberg_delta_scan::{ApplyKeySource, BaseTableIdent};

    // Note: full IcebergDeltaScanNode fixtures require `iceberg::table::Table`
    // and `OpendalRangeReaderFactory` instances which are non-trivial to mock.
    // Operator semantic verification happens at the SQL test level (Phase 7).
    // The unit test below is compile-only — it asserts the factory type
    // implements the `OperatorFactory` trait.
    #[allow(dead_code)]
    fn _example_ident() -> BaseTableIdent {
        BaseTableIdent {
            catalog: "c".into(),
            namespace: "n".into(),
            table: "t".into(),
        }
    }

    #[test]
    fn iceberg_delta_scan_factory_compiles_as_operator_factory() {
        fn assert_is_factory<T: OperatorFactory + ?Sized>() {}
        assert_is_factory::<IcebergDeltaScanFactory>();
    }
}
```

(Note: The unit test above is necessarily limited because `iceberg::table::Table` cannot be cheaply constructed in a unit test. Real semantic verification of the operator happens at SQL test level in Phase 7. We keep the test file as a compile guard.)

- [ ] **Step 2: 注册 module + export factory**

Modify `src/exec/operators/mod.rs` (add at appropriate place, e.g., near other source operators):

```rust
pub mod iceberg_delta_scan;
pub use iceberg_delta_scan::IcebergDeltaScanFactory;
```

- [ ] **Step 3: 把 Phase-0 的 stub 替换为真实 factory 构造**

Modify `src/exec/pipeline/builder.rs` — find the `IcebergDeltaScan` arm added in Task 2:

```rust
        ExecNodeKind::IcebergDeltaScan(node) => {
            let factory = crate::exec::operators::IcebergDeltaScanFactory::new(node.clone());
            // Single-DOP for A1 phase 1 (multi-driver morsel allocation deferred)
            ctx.register_source_factory_for_node(node.node_id, std::sync::Arc::new(factory), 1)?;
        }
```

(Note: the exact `register_source_factory_for_node` signature may need a small adjustment per existing builder.rs conventions; verify by reading how `Values` arm at line 1223 registers itself, and follow the same pattern.)

- [ ] **Step 4: 编译通过**

Run: `cargo build --lib 2>&1 | grep -E "error\[" | head -10`
Expected: 无错误。

- [ ] **Step 5: 运行 operator 单元测试（编译验证）**

Run: `cargo test --lib --package novarocks iceberg_delta_scan 2>&1 | tail -10`
Expected: 测试运行（`empty_change_files_returns_none_immediately` 通过；构造 fixture 的代码因 `panic!` 不被执行）。

- [ ] **Step 6: Commit**

```bash
git add src/exec/operators/iceberg_delta_scan.rs src/exec/operators/mod.rs src/exec/pipeline/builder.rs
git commit -m "feat(ivm-a1): scaffold IcebergDeltaScanFactory + operator (no roles yet)"
```

---

### Task 5: `DataFile` role scanner (Iceberg parquet 流式读)

**Files:**
- Modify: `src/exec/operators/iceberg_delta_scan.rs`（增 `DataFileScanner` 子类）

- [ ] **Step 1: 添加 `DataFileScanner` 结构体 + 实现 `DeltaFileScanner`**

Add to `src/exec/operators/iceberg_delta_scan.rs` (after `inject_change_op_column`):

```rust
struct DataFileScanner {
    inner: crate::fs::scan_context::FileScanIter,
    schema: SchemaRef,
}

impl DeltaFileScanner for DataFileScanner {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, String> {
        match self.inner.next() {
            Some(Ok(batch)) => Ok(Some(batch)),
            Some(Err(e)) => Err(e.to_string()),
            None => Ok(None),
        }
    }

    fn change_op_value(&self) -> i8 {
        CHANGE_OP_INSERT
    }
}

fn open_data_file_scanner(
    node: &IcebergDeltaScanNode,
    file: &DeltaSourceFile,
) -> Result<Box<dyn DeltaFileScanner>, String> {
    let range = crate::fs::scan_context::FileScanRange {
        path: file.path.clone(),
        file_len: file.size,
        offset: 0,
        length: file.size,
        scan_range_id: 0,
        first_row_id: file.first_row_id,
        data_sequence_number: file.data_sequence_number,
        ivm_change_op: None, // injected by operator instead of via A4 scan-range path
        external_datacache: None,
        delete_files: Vec::new(),
    };
    let ctx = crate::fs::scan_context::FileScanContext::build(
        vec![range],
        None,
        node.object_store_config.as_ref(),
    )?;
    let inner = ctx.into_iter_for_a1_delta_scan()?;
    let schema = inner.schema();
    Ok(Box::new(DataFileScanner { inner, schema }))
}
```

- [ ] **Step 2: 在 `open_scanner_for_role` 中调用**

Modify the match arm:

```rust
        DeltaSourceRole::DataFile => open_data_file_scanner(node, &file),
```

(Note: `into_iter_for_a1_delta_scan` does not yet exist on `FileScanContext`. The scan_context module currently couples iteration to the morsel/driver path. If the existing public surface doesn't allow constructing a plain iterator, add a small adapter on `FileScanContext` in this task. Inspect `src/fs/scan_context.rs` to confirm before writing the call; if missing, add it as a thin wrapper around the existing parquet reader-builder used by `HdfsScanOp`.)

- [ ] **Step 3: 编译通过**

Run: `cargo build --lib 2>&1 | grep -E "error\[" | head`
Expected: 无错误。

- [ ] **Step 4: Commit**

```bash
git add src/exec/operators/iceberg_delta_scan.rs src/fs/scan_context.rs
git commit -m "feat(ivm-a1): IcebergDeltaScan supports DataFile role streaming"
```

---

### Task 6: `PositionDelete` role scanner

**Files:**
- Modify: `src/exec/operators/iceberg_delta_scan.rs`

- [ ] **Step 1: 添加 `PositionDeleteScanner` 实现**

Add (after `DataFileScanner`):

```rust
struct PositionDeleteScanner {
    batches: std::vec::IntoIter<RecordBatch>,
}

impl DeltaFileScanner for PositionDeleteScanner {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, String> {
        Ok(self.batches.next())
    }

    fn change_op_value(&self) -> i8 {
        CHANGE_OP_DELETE
    }
}

fn open_position_delete_scanner(
    node: &IcebergDeltaScanNode,
    file: &DeltaSourceFile,
    targets: &[crate::exec::node::iceberg_delta_scan::PositionDeleteTargetData],
) -> Result<Box<dyn DeltaFileScanner>, String> {
    let delete = crate::connector::iceberg::changes::PositionDeleteRef {
        path: file.path.clone(),
        size: file.size,
        partition_spec_id: file.partition_spec_id,
        partition_key: file.partition_key.clone(),
        data_sequence_number: file.data_sequence_number,
        targets: targets
            .iter()
            .map(|t| crate::connector::iceberg::changes::PositionDeleteTarget {
                data_file_path: t.data_file_path.clone(),
            })
            .collect(),
    };
    let base_first_row_ids = targets
        .iter()
        .filter_map(|t| t.data_file_first_row_id.map(|id| (t.data_file_path.clone(), id)))
        .collect::<std::collections::HashMap<_, _>>();
    let factory = crate::connector::iceberg::changes::build_factory_for_table(
        &node.iceberg_runtime.base_table,
        node.object_store_config.as_ref(),
    )?;
    let rows = crate::connector::iceberg::changes::scan_position_delete_rows_for_targets(
        &node.iceberg_runtime.base_table,
        &delete,
        &base_first_row_ids,
        &factory,
        node.object_store_config.as_ref(),
    )?;
    Ok(Box::new(PositionDeleteScanner {
        batches: rows.into_iter(),
    }))
}
```

(Note: this is a "batch-friendly streaming" — we pre-load all reverse-projected rows for ONE delete file before emitting, but each delete file produces an independent scanner. This is a deliberate compromise: scanning one position-delete file's targets typically yields a bounded number of rows, and going fully streaming inside this helper requires extending `scan_deletes_with_*` to return an iterator. A1 keeps the per-file granularity as the streaming boundary; if a single delete file proves too large, that's a follow-up.)

- [ ] **Step 2: dispatch 在 `open_scanner_for_role`**

```rust
        DeltaSourceRole::PositionDelete { targets } => {
            open_position_delete_scanner(node, &file, &targets)
        }
```

- [ ] **Step 3: 编译通过**

Run: `cargo build --lib 2>&1 | grep -E "error\[" | head`
Expected: 无错误。

- [ ] **Step 4: Commit**

```bash
git add src/exec/operators/iceberg_delta_scan.rs
git commit -m "feat(ivm-a1): IcebergDeltaScan supports PositionDelete role"
```

---

### Task 7: `EqualityDelete` role scanner

**Files:**
- Modify: `src/exec/operators/iceberg_delta_scan.rs`

- [ ] **Step 1: 添加 `EqualityDeleteScanner`**

```rust
struct EqualityDeleteScanner {
    batches: std::vec::IntoIter<RecordBatch>,
}

impl DeltaFileScanner for EqualityDeleteScanner {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, String> {
        Ok(self.batches.next())
    }

    fn change_op_value(&self) -> i8 {
        CHANGE_OP_DELETE
    }
}

fn open_equality_delete_scanner(
    node: &IcebergDeltaScanNode,
    file: &DeltaSourceFile,
    equality_field_ids: &[i32],
    targets: &[crate::exec::node::iceberg_delta_scan::EqualityDeleteTargetData],
) -> Result<Box<dyn DeltaFileScanner>, String> {
    let delete = crate::connector::iceberg::changes::EqualityDeleteRef {
        path: file.path.clone(),
        size: file.size,
        partition_spec_id: file.partition_spec_id,
        partition_key: file.partition_key.clone(),
        data_sequence_number: file.data_sequence_number,
        equality_field_ids: equality_field_ids.to_vec(),
        targets: targets
            .iter()
            .map(|t| crate::connector::iceberg::changes::EqualityDeleteTarget {
                data_file_path: t.data_file_path.clone(),
                data_file_first_row_id: t.data_file_first_row_id,
            })
            .collect(),
    };
    let factory = crate::connector::iceberg::changes::build_factory_for_table(
        &node.iceberg_runtime.base_table,
        node.object_store_config.as_ref(),
    )?;
    let rows = crate::connector::iceberg::changes::scan_equality_delete_rows_for_one(
        &node.iceberg_runtime.base_table,
        &delete,
        &factory,
        node.object_store_config.as_ref(),
    )?;
    Ok(Box::new(EqualityDeleteScanner {
        batches: rows.into_iter(),
    }))
}
```

- [ ] **Step 2: dispatch**

```rust
        DeltaSourceRole::EqualityDelete { equality_field_ids, targets } => {
            open_equality_delete_scanner(node, &file, &equality_field_ids, &targets)
        }
```

- [ ] **Step 3: 编译 + commit**

```bash
cargo build --lib && \
  git add src/exec/operators/iceberg_delta_scan.rs && \
  git commit -m "feat(ivm-a1): IcebergDeltaScan supports EqualityDelete role"
```

---

### Task 8: `DeletedDataFile` role scanner

**Files:**
- Modify: `src/exec/operators/iceberg_delta_scan.rs`

- [ ] **Step 1: 添加 `DeletedDataFileScanner`**

```rust
struct DeletedDataFileScanner {
    batches: std::vec::IntoIter<RecordBatch>,
}

impl DeltaFileScanner for DeletedDataFileScanner {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, String> {
        Ok(self.batches.next())
    }

    fn change_op_value(&self) -> i8 {
        CHANGE_OP_DELETE
    }
}

fn open_deleted_data_file_scanner(
    node: &IcebergDeltaScanNode,
    file: &DeltaSourceFile,
    visibility: &Option<crate::exec::node::iceberg_delta_scan::DeletedFileVisibility>,
) -> Result<Box<dyn DeltaFileScanner>, String> {
    let deleted_file = crate::connector::iceberg::changes::DeletedDataFileRef {
        path: file.path.clone(),
        size: file.size,
        record_count: 0,
        partition_spec_id: file.partition_spec_id,
        partition_key: file.partition_key.clone(),
        first_row_id: file.first_row_id,
        data_sequence_number: file.data_sequence_number,
    };
    let mut previous_delete_visibility = std::collections::HashMap::new();
    if let Some(vis) = visibility.as_ref()
        && !vis.already_deleted_positions.is_empty()
    {
        previous_delete_visibility.insert(
            file.path.clone(),
            crate::engine::delete_flow::DeleteVisibilityForFile {
                deleted_positions: vis.already_deleted_positions.iter().copied().collect(),
            },
        );
    }
    let rows = crate::connector::iceberg::changes::scan_one_deleted_data_file(
        &node.iceberg_runtime.base_table,
        &deleted_file,
        node.object_store_config.as_ref(),
        &previous_delete_visibility,
    )?;
    Ok(Box::new(DeletedDataFileScanner {
        batches: rows.into_iter(),
    }))
}
```

- [ ] **Step 2: dispatch**

```rust
        DeltaSourceRole::DeletedDataFile { previous_data_file_visibility } => {
            open_deleted_data_file_scanner(node, &file, &previous_data_file_visibility)
        }
```

- [ ] **Step 3: 编译 + commit**

```bash
cargo build --lib && \
  git add src/exec/operators/iceberg_delta_scan.rs && \
  git commit -m "feat(ivm-a1): IcebergDeltaScan supports DeletedDataFile role"
```

---

# Phase 3 — IcebergMergeSink Operator

### Task 9: 抽出 chunk → Iceberg DataFile 写入的 streaming helper

`write_chunks_as_iceberg_data_files` 当前是"一次性 take 所有 chunks 然后批量写"。Sink 需要"逐 chunk 流式 append + flush 时收 WrittenFile"的形态。

**Files:**
- Modify: `src/connector/iceberg/data_writer.rs`（新增 `IcebergStreamingDataFileWriter` struct + `write_record_batch` / `finish` 方法）

- [ ] **Step 1: 在 `data_writer.rs` 添加 streaming writer 结构**

具体步骤：

1. 读 `src/connector/iceberg/data_writer.rs::write_record_batches_as_data_files` 当前 body（约 40-150 行），识别这几样：
   - Iceberg `ParquetWriterBuilder` 初始化（表 metadata、写入位置、压缩参数）
   - 内层 `DataFileWriter` 句柄
   - rolling 阈值与已 close 的 `DataFile` 列表
   - 输入是 `impl Iterator<Item = RecordBatch>`，按 batch 调 `write` 然后最终 `close()`
2. 把这些以 `pub(crate) struct IcebergStreamingDataFileWriter` 封装：
   - `new(table) -> Result<Self, String>`：构造 builder + 打开 first writer，但不消费任何 batch
   - `async fn write_record_batch(&mut self, batch)`：转发到当前 writer，触发 rolling 时把 close 出来的 DataFile push 到内部 vec
   - `async fn finish(self) -> Result<Vec<DataFile>, String>`：关闭当前 writer，合并内部 vec，返回
3. 把原 `write_record_batches_as_data_files` 改成：

```rust
pub(crate) async fn write_record_batches_as_data_files(
    table: &iceberg::table::Table,
    batches: impl IntoIterator<Item = arrow::record_batch::RecordBatch>,
) -> Result<Vec<iceberg::spec::DataFile>, String> {
    let mut writer = IcebergStreamingDataFileWriter::new(table.clone())?;
    for b in batches {
        writer.write_record_batch(b).await?;
    }
    writer.finish().await
}
```

——其他 8+ caller 完全不动。

- [ ] **Step 2: 编译 + 跑 `write_record_batches_as_data_files` 既有 caller 测试**

Run: `cargo test --lib --package novarocks data_writer 2>&1 | tail -10`
Expected: 既有 tests 全部 PASS（refactor 不改语义）。

- [ ] **Step 3: Commit**

```bash
git add src/connector/iceberg/data_writer.rs
git commit -m "refactor(iceberg-data-writer): extract IcebergStreamingDataFileWriter for IVM-A1 sink"
```

---

### Task 10: `IcebergMergeSinkFactory` scaffold + INSERT-only routing

**Files:**
- Create: `src/engine/mv/iceberg_merge_sink.rs`
- Modify: `src/engine/mv/mod.rs`（`pub mod iceberg_merge_sink;`）

- [ ] **Step 1: 创建文件，写 factory + operator skeleton（只支持 INSERT 路由）**

Create `src/engine/mv/iceberg_merge_sink.rs`:

```rust
//! IVM-A1 merge sink: routes mixed +/- chunks to data-file writer or
//! A9 target locator, accumulating `WrittenFile`s and `PositionDeleteGroup`s
//! into a shared `IcebergCommitCollector`. Commit dispatch is owned by the
//! refresh driver (not this sink) per design §3 / §5.

use std::sync::Arc;

use arrow::array::Int8Array;
use arrow::record_batch::RecordBatch;
use iceberg::spec::DataFile;

use crate::connector::iceberg::commit::{IcebergCommitCollector, WrittenFile};
use crate::connector::iceberg::data_writer::IcebergStreamingDataFileWriter;
use crate::exec::change_op::{CHANGE_OP_COLUMN, CHANGE_OP_INSERT, CHANGE_OP_DELETE};
use crate::exec::chunk::Chunk;
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::global_async_runtime::data_block_on;
use crate::runtime::runtime_state::RuntimeState;

#[derive(Clone)]
pub struct IcebergMergeSinkFactory {
    name: String,
    plan: Arc<IcebergMergeSinkPlan>,
}

pub struct IcebergMergeSinkPlan {
    pub target_table: iceberg::table::Table,
    pub collector: Arc<IcebergCommitCollector>,
    pub locator_state: Option<TargetLocatorState>,
    pub apply_key_column: String,
    pub apply_key_field_id: i32,
}

pub struct TargetLocatorState {
    pub existing_deletes_by_file:
        std::collections::HashMap<String, crate::engine::mv::iceberg_target_apply::ExistingDeleteState>,
    pub referenced_data_file_partitions:
        crate::engine::mv::iceberg_target_apply::ReferencedDataFilePartitions,
}

impl IcebergMergeSinkFactory {
    pub fn try_new(plan: IcebergMergeSinkPlan) -> Result<Self, String> {
        Ok(Self {
            name: format!(
                "IcebergMergeSink ({}.{}.{})",
                plan.target_table.identifier().namespace().to_url_string(),
                plan.target_table.identifier().name(),
                "target"
            ),
            plan: Arc::new(plan),
        })
    }
}

impl OperatorFactory for IcebergMergeSinkFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, driver_id: i32) -> Box<dyn Operator> {
        // Phase 1: single-driver. driver_id != 0 produces a no-op sink (only driver 0 writes).
        let writer = if driver_id == 0 {
            Some(
                IcebergStreamingDataFileWriter::new(self.plan.target_table.clone())
                    .expect("init streaming writer"),
            )
        } else {
            None
        };
        Box::new(IcebergMergeSinkOperator {
            name: self.name.clone(),
            plan: Arc::clone(&self.plan),
            writer,
            driver_id,
            finished: false,
        })
    }

    fn is_sink(&self) -> bool {
        true
    }
}

struct IcebergMergeSinkOperator {
    name: String,
    plan: Arc<IcebergMergeSinkPlan>,
    writer: Option<IcebergStreamingDataFileWriter>,
    driver_id: i32,
    finished: bool,
}

impl Operator for IcebergMergeSinkOperator {
    fn name(&self) -> &str {
        &self.name
    }
    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }
    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }
    fn is_finished(&self) -> bool {
        self.finished
    }
}

fn partition_chunk_by_change_op(
    chunk: &Chunk,
) -> Result<(Option<RecordBatch>, Option<RecordBatch>), String> {
    let batch = &chunk.batch;
    let col_idx = batch
        .schema()
        .index_of(CHANGE_OP_COLUMN)
        .map_err(|_| format!("merge sink: chunk missing column {CHANGE_OP_COLUMN}"))?;
    let arr = batch
        .column(col_idx)
        .as_any()
        .downcast_ref::<Int8Array>()
        .ok_or_else(|| format!("merge sink: column {CHANGE_OP_COLUMN} must be Int8"))?;

    let mut insert_indices = Vec::new();
    let mut delete_indices = Vec::new();
    for (i, value) in arr.iter().enumerate() {
        match value {
            Some(CHANGE_OP_INSERT) => insert_indices.push(i),
            Some(CHANGE_OP_DELETE) => delete_indices.push(i),
            Some(other) => {
                return Err(format!(
                    "merge sink: unexpected {CHANGE_OP_COLUMN} value {other}"
                ));
            }
            None => return Err(format!("merge sink: null {CHANGE_OP_COLUMN}")),
        }
    }

    let take = |indices: &[usize]| -> Result<Option<RecordBatch>, String> {
        if indices.is_empty() {
            return Ok(None);
        }
        let index_arr = arrow::array::UInt32Array::from_iter_values(
            indices.iter().map(|&i| i as u32),
        );
        let mut taken_columns = Vec::with_capacity(batch.num_columns());
        for col in batch.columns() {
            let taken = arrow::compute::take(col.as_ref(), &index_arr, None)
                .map_err(|e| format!("merge sink take: {e}"))?;
            taken_columns.push(taken);
        }
        let new_batch = RecordBatch::try_new(batch.schema(), taken_columns)
            .map_err(|e| format!("merge sink rebuild batch: {e}"))?;
        Ok(Some(new_batch))
    };

    Ok((take(&insert_indices)?, take(&delete_indices)?))
}

impl ProcessorOperator for IcebergMergeSinkOperator {
    fn need_input(&self) -> bool {
        !self.finished
    }
    fn has_output(&self) -> bool {
        false
    }
    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        if self.driver_id != 0 {
            return Ok(());
        }
        let (insert_batch, delete_batch) = partition_chunk_by_change_op(&chunk)?;
        if let Some(batch) = insert_batch
            && let Some(writer) = self.writer.as_mut()
        {
            data_block_on(writer.write_record_batch(strip_change_op(batch)?))?;
        }
        if delete_batch.is_some() {
            return Err("merge sink: DELETE routing not implemented yet (Task 11)".to_string());
        }
        Ok(())
    }
    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        Err("merge sink does not produce output".to_string())
    }
    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        if let Some(writer) = self.writer.take() {
            let data_files: Vec<DataFile> = data_block_on(writer.finish())?;
            for df in data_files {
                self.plan.collector.inject_written_file(WrittenFile::from_data_file(df));
            }
        }
        self.finished = true;
        Ok(())
    }
    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        Err("merge sink does not produce output".to_string())
    }
}

fn strip_change_op(batch: RecordBatch) -> Result<RecordBatch, String> {
    let schema = batch.schema();
    let Some(idx) = schema
        .fields()
        .iter()
        .position(|f| f.name() == CHANGE_OP_COLUMN)
    else {
        return Ok(batch);
    };
    let mut fields: Vec<arrow::datatypes::Field> = schema
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields.remove(idx);
    let mut columns: Vec<arrow::array::ArrayRef> = batch.columns().to_vec();
    columns.remove(idx);
    let new_schema = Arc::new(arrow::datatypes::Schema::new(fields));
    RecordBatch::try_new(new_schema, columns)
        .map_err(|e| format!("merge sink strip __change_op: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int32Array, Int8Array};
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn partition_pure_insert_chunk() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int32, false),
            crate::exec::change_op::change_op_field(),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(Int8Array::from(vec![CHANGE_OP_INSERT; 3])) as ArrayRef,
            ],
        )
        .unwrap();
        let chunk = Chunk::from_record_batch(batch, &crate::exec::chunk::ChunkSchema::empty())
            .unwrap();
        let (ins, del) = partition_chunk_by_change_op(&chunk).unwrap();
        assert!(ins.is_some());
        assert!(del.is_none());
        assert_eq!(ins.unwrap().num_rows(), 3);
    }

    #[test]
    fn partition_mixed_chunk() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int32, false),
            crate::exec::change_op::change_op_field(),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef,
                Arc::new(Int8Array::from(vec![1, -1, 1, -1])) as ArrayRef,
            ],
        )
        .unwrap();
        let chunk = Chunk::from_record_batch(batch, &crate::exec::chunk::ChunkSchema::empty())
            .unwrap();
        let (ins, del) = partition_chunk_by_change_op(&chunk).unwrap();
        assert_eq!(ins.unwrap().num_rows(), 2);
        assert_eq!(del.unwrap().num_rows(), 2);
    }

    #[test]
    fn partition_rejects_null_change_op() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int32, false),
            crate::exec::change_op::change_op_field(),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1])) as ArrayRef,
                Arc::new(Int8Array::from(vec![None] as Vec<Option<i8>>)) as ArrayRef,
            ],
        )
        .unwrap();
        let chunk = Chunk::from_record_batch(batch, &crate::exec::chunk::ChunkSchema::empty())
            .unwrap();
        let err = partition_chunk_by_change_op(&chunk).unwrap_err();
        assert!(err.contains("null"));
    }
}
```

- [ ] **Step 2: 注册 module**

Modify `src/engine/mv/mod.rs`:

```rust
pub(crate) mod iceberg_merge_sink;
```

- [ ] **Step 3: 编译 + 跑新测试**

Run: `cargo test --lib --package novarocks iceberg_merge_sink 2>&1 | tail -10`
Expected:
```
test partition_pure_insert_chunk ... ok
test partition_mixed_chunk ... ok
test partition_rejects_null_change_op ... ok
```

- [ ] **Step 4: Commit**

```bash
git add src/engine/mv/iceberg_merge_sink.rs src/engine/mv/mod.rs
git commit -m "feat(ivm-a1): scaffold IcebergMergeSinkFactory + INSERT routing"
```

---

### Task 11: `IcebergMergeSink` DELETE routing with A9 locator

**Files:**
- Modify: `src/engine/mv/iceberg_merge_sink.rs`

- [ ] **Step 1: 把 DELETE 分支实现**

Replace the `delete_batch.is_some()` error branch with:

```rust
        if let Some(batch) = delete_batch {
            let locator_state = self.plan.locator_state.as_ref().ok_or_else(|| {
                format!(
                    "merge sink: DELETE chunk arrived but no locator preloaded (refresh driver \
                     must call load_target_apply_locator_inputs when has_deletes)"
                )
            })?;
            let apply_keys = extract_apply_key_values_from_record_batch(
                &batch,
                &self.plan.apply_key_column,
            )?;
            if apply_keys.is_empty() {
                // pure-INSERT chunk wrongly classified; skip.
                return Ok(());
            }
            let groups = data_block_on(
                crate::engine::mv::iceberg_target_apply::locate_target_rows_by_apply_key(
                    &self.plan.target_table,
                    &apply_keys,
                    &locator_state.existing_deletes_by_file,
                    &locator_state.referenced_data_file_partitions,
                ),
            )?;
            for group in groups {
                self.plan.collector.inject_delete_group(group);
            }
        }
```

- [ ] **Step 2: 添加 helper `extract_apply_key_values_from_record_batch`**

```rust
fn extract_apply_key_values_from_record_batch(
    batch: &RecordBatch,
    apply_key_column: &str,
) -> Result<Vec<i64>, String> {
    let idx = batch
        .schema()
        .index_of(apply_key_column)
        .map_err(|_| format!("merge sink: DELETE batch missing apply-key column {apply_key_column}"))?;
    let arr = batch
        .column(idx)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .ok_or_else(|| {
            format!("merge sink: apply-key column {apply_key_column} must be Int64")
        })?;
    arr.iter()
        .map(|v| v.ok_or_else(|| format!("merge sink: null value in apply-key column {apply_key_column}")))
        .collect()
}
```

- [ ] **Step 3: 编译通过**

Run: `cargo build --lib 2>&1 | grep -E "error\[" | head`
Expected: 无错误。

- [ ] **Step 4: 添加 sink "缺 locator 时 fail-fast" 单元测试**

Add to `mod tests`:

```rust
    #[test]
    fn extract_apply_key_values_rejects_missing_column() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![1])) as ArrayRef],
        )
        .unwrap();
        let err = extract_apply_key_values_from_record_batch(&batch, "__nova_base_row_id").unwrap_err();
        assert!(err.contains("missing apply-key column"));
    }
```

- [ ] **Step 5: Commit**

```bash
cargo test --lib iceberg_merge_sink && \
  git add src/engine/mv/iceberg_merge_sink.rs && \
  git commit -m "feat(ivm-a1): IcebergMergeSink DELETE routing via A9 locator"
```

---

# Phase 4 — Plan-Time `IcebergDeltaScan` Dispatch（IDL + Analyzer + Codegen + Lower）

Task 12 是把 `IcebergDeltaScan` 做成编译时一等节点 —— 在 SQL → AST → 各阶段 plan → Thrift → ExecPlan 整条链路上原生出现，不依赖 ExecPlan 后置 mutation。**v1 的 leaf-swap 路线已废弃**，原 `src/engine/mv/iceberg_delta_plan.rs` 整体不创建。

### Task 12: Plan-time `IcebergDeltaScan` 全链路接入

**Files:**

- Modify: `idl/thrift/PlanNodes.thrift`（加 enum 变体 + struct + TPlanNode 字段）
- Modify: `src/sql/analyzer/resolve_from.rs`（识别 `__nr_ivm_delta` table function）
- Modify: `src/sql/analyzer/mod.rs`（`Relation` 加变体 `IcebergDeltaScan`）
- Modify: `src/sql/planner/...`（`LogicalPlan` 加变体）
- Modify: `src/sql/optimizer/...`（`PhysicalPlan` 加变体 + passthrough）
- Modify: `src/sql/codegen/fragment_builder.rs`（emit `ICEBERG_DELTA_SCAN_NODE`）
- Create: `src/lower/thrift/iceberg_delta_scan.rs`（`lower_iceberg_delta_scan`）
- Modify: `src/lower/thrift/lower_plan.rs`（dispatch ICEBERG_DELTA_SCAN_NODE）
- Modify: `src/exec/node/iceberg_delta_scan.rs`（`IcebergRuntimeHandles` 加 `object_store_factory` + `delete_side`）

- [ ] **Step 1: 扩展 Thrift IDL**

Edit `idl/thrift/PlanNodes.thrift`，在 `TPlanNodeType` 末尾、`}` 之前加：

```thrift
enum TPlanNodeType {
    OLAP_SCAN_NODE,
    ...
    LAKE_CACHE_STATS_SCAN_NODE,

    // NovaRocks-only nodes start from 1000 to avoid colliding with upstream
    // starrocks additions (which occupy 0..999 by sequential ordering).
    ICEBERG_DELTA_SCAN_NODE = 1000,
}
```

在文件合适位置（与其他 `T*ScanNode` 一起）加 `TIcebergDeltaScanNode`：

```thrift
// IVM-A1 Iceberg incremental delta scan source.
//
// Only carries lightweight descriptors (catalog/namespace/table strings +
// from/to snapshot ids); the actual `Vec<DeltaSourceFile>` and runtime
// state (visibility / first_row_id index) are computed at lower_plan time
// by `plan_changes` so they never traverse the wire format.
struct TIcebergDeltaScanNode {
    1: required string catalog,
    2: required string namespace,
    3: required string table,
    4: required i64 from_snapshot_id,
    5: required i64 to_snapshot_id,
}
```

在 `TPlanNode` 结构体加 optional 字段挂这个 payload。

`cargo build --lib` 编译以触发 Thrift 代码再生成。

- [ ] **Step 2: 扩展 `IcebergRuntimeHandles`（加 `object_store_factory` + `delete_side`）**

Edit `src/exec/node/iceberg_delta_scan.rs`：

```rust
pub struct IcebergRuntimeHandles {
    pub base_table: iceberg::table::Table,
    pub object_store_factory: Arc<crate::fs::opendal::OpendalRangeReaderFactory>,
    pub delete_side: Option<DeltaScanDeleteSide>,
}

pub struct DeltaScanDeleteSide {
    pub base_first_row_ids: std::collections::HashMap<String, i64>,
    pub previous_delete_visibility:
        crate::engine::delete_flow::ExistingDeleteVisibilityByDataFile,
}
```

Tasks 6/7/8 的 operator scanner 跟着改 —— 从 `delete_side` 借用而不是每次自己 build。

- [ ] **Step 3: analyzer 识别 `__nr_ivm_delta` table function**

Edit `src/sql/analyzer/resolve_from.rs`，在 [`TableFactor::TableFunction`](../../../src/sql/analyzer/resolve_from.rs:424) 分支起首加：

```rust
sqlast::TableFactor::TableFunction { expr, alias } => {
    let func_name = expr.name.0.last()
        .and_then(|p| match p {
            sqlast::ObjectNamePart::Identifier(i) => Some(i.value.to_ascii_lowercase()),
            _ => None,
        })
        .unwrap_or_default();
    if func_name == "__nr_ivm_delta" {
        return self.analyze_iceberg_delta_table_function(expr, alias.as_ref());
    }
    // ... 原有 user-defined table function 逻辑
}
```

新增方法 `analyze_iceberg_delta_table_function`：

```rust
fn analyze_iceberg_delta_table_function(
    &self,
    expr: &sqlast::Function,
    alias: Option<&sqlast::TableAlias>,
) -> Result<(Relation, AnalyzerScope), String> {
    let args = expr.args.as_ref()
        .ok_or_else(|| "__nr_ivm_delta requires args".to_string())?;
    if args.args.len() != 3 {
        return Err(format!("__nr_ivm_delta expects 3 args (catalog.namespace.table, from_snap, to_snap), got {}", args.args.len()));
    }
    let fqn = parse_string_arg(&args.args[0], "table fqn")?;
    let from_snap = parse_i64_arg(&args.args[1], "from_snapshot_id")?;
    let to_snap = parse_i64_arg(&args.args[2], "to_snapshot_id")?;
    if from_snap < 0 || to_snap < 0 {
        return Err(format!("__nr_ivm_delta snapshot id must be non-negative, got from={from_snap} to={to_snap}"));
    }
    let (catalog, namespace, table) = split_catalog_namespace_table(&fqn)?;

    // 查 base 表的 TableDef，拿 schema + 强制 advertise row-lineage 虚拟列
    let table_def = self.catalog.get_table(&namespace, &table)?;
    require_iceberg_row_lineage_advertised(&table_def)?;

    let qualifier = alias
        .map(|a| a.name.value.clone())
        .unwrap_or_else(|| table_def.name.clone());

    let mut scope = AnalyzerScope::new();
    for col in &table_def.columns {
        scope.add_column(Some(&qualifier), &col.name, col.data_type.clone(), col.nullable);
    }
    for col in &table_def.iceberg_row_lineage_metadata_columns {
        scope.add_column(Some(&qualifier), &col.name, col.data_type.clone(), col.nullable);
    }

    let relation = Relation::IcebergDeltaScan(IcebergDeltaScanRelation {
        catalog, namespace, table,
        from_snapshot_id: from_snap,
        to_snapshot_id: to_snap,
        qualifier,
        columns: table_def.columns.clone(),
        row_lineage_columns: table_def.iceberg_row_lineage_metadata_columns.clone(),
    });
    Ok((relation, scope))
}
```

`Relation` enum 加变体 `IcebergDeltaScan(IcebergDeltaScanRelation)`；其他 match 站点同步加 arm。

`parse_string_arg` / `parse_i64_arg` / `split_catalog_namespace_table` / `require_iceberg_row_lineage_advertised` 都是 helper，写在 `resolve_from.rs` 底下或 `mod.rs`。

- [ ] **Step 4: planner / optimizer passthrough**

LogicalPlan 加变体 `IcebergDeltaScan(LogicalIcebergDeltaScan)`；planner 把 `Relation::IcebergDeltaScan` 1:1 转过去。

PhysicalPlan 加变体 `IcebergDeltaScan(PhysicalIcebergDeltaScan)`；optimizer 现阶段不做专用规则，passthrough。

- [ ] **Step 5: fragment_builder emit `ICEBERG_DELTA_SCAN_NODE`**

Edit `src/sql/codegen/fragment_builder.rs`，在物理节点 emit 的 match 加分支：

```rust
PhysicalPlan::IcebergDeltaScan(ds) => {
    let tnode = TPlanNode {
        node_id: ...,
        node_type: TPlanNodeType::ICEBERG_DELTA_SCAN_NODE,
        iceberg_delta_scan_node: Some(TIcebergDeltaScanNode {
            catalog: ds.catalog.clone(),
            namespace: ds.namespace.clone(),
            table: ds.table.clone(),
            from_snapshot_id: ds.from_snapshot_id,
            to_snapshot_id: ds.to_snapshot_id,
        }),
        ...
    };
    // 输出 schema 走标准 desc_tbl 流程
    ...
}
```

- [ ] **Step 6: lower_plan 现场构造 `IcebergDeltaScanNode`**

Create `src/lower/thrift/iceberg_delta_scan.rs`：

```rust
pub(crate) fn lower_iceberg_delta_scan(
    thrift_node: &TIcebergDeltaScanNode,
    node_id: i32,
    output_chunk_schema: ChunkSchemaRef,
    iceberg_catalogs: &crate::connector::iceberg::catalog::IcebergCatalogRegistry,
) -> Result<ExecNode, String> {
    let entry = iceberg_catalogs.get(&thrift_node.catalog)?;
    let loaded = crate::connector::iceberg::catalog::load_table(
        &entry, &thrift_node.namespace, &thrift_node.table,
    )?;

    let batch = crate::connector::iceberg::changes::plan_changes(
        &loaded.table,
        thrift_node.from_snapshot_id,
        &[],
    ).map_err(|e| format!("ivm-a1 lower delta-scan: plan_changes failed: {e}"))?;

    let change_files = build_delta_source_files_from_batch(&batch);

    let object_store_factory = Arc::new(
        crate::connector::iceberg::changes::build_factory_for_table(
            &loaded.table, entry.object_store_config(),
        )?
    );

    let delete_side = if !batch.deletes.is_empty()
        || !batch.equality_deletes.is_empty()
        || !batch.deleted_data_files.is_empty()
    {
        Some(DeltaScanDeleteSide {
            base_first_row_ids: crate::connector::iceberg::changes::base_data_file_first_row_id_index(&loaded.table)?,
            previous_delete_visibility: crate::engine::delete_flow::load_existing_delete_visibility_by_data_file_at(
                &loaded.table,
                Some(batch.previous_snapshot_id),
                entry.object_store_config(),
            )?,
        })
    } else {
        None
    };

    Ok(ExecNode { kind: ExecNodeKind::IcebergDeltaScan(IcebergDeltaScanNode {
        base_table_ident: BaseTableIdent {
            catalog: thrift_node.catalog.clone(),
            namespace: thrift_node.namespace.clone(),
            table: thrift_node.table.clone(),
        },
        from_snapshot_id: thrift_node.from_snapshot_id,
        to_snapshot_id: thrift_node.to_snapshot_id,
        output_chunk_schema,
        apply_key_source: ApplyKeySource::BaseRowId,
        change_files,
        object_store_config: entry.object_store_config().cloned(),
        iceberg_runtime: Arc::new(IcebergRuntimeHandles {
            base_table: loaded.table,
            object_store_factory,
            delete_side,
        }),
        node_id,
    })})
}
```

Edit `src/lower/thrift/lower_plan.rs` 的 main dispatch match：

```rust
match plan_node.node_type {
    TPlanNodeType::OLAP_SCAN_NODE => ...,
    ...
    TPlanNodeType::ICEBERG_DELTA_SCAN_NODE => {
        let payload = plan_node.iceberg_delta_scan_node.as_ref()
            .ok_or_else(|| "ICEBERG_DELTA_SCAN_NODE missing iceberg_delta_scan_node payload".to_string())?;
        lower_iceberg_delta_scan(payload, plan_node.node_id, output_chunk_schema, iceberg_catalogs)?
    }
}
```

`iceberg_catalogs` 是 lower_plan 新加的参数（见下一 Step）。

- [ ] **Step 7: lower_plan 接 IcebergCatalogRegistry 引用**

Edit `src/lower/thrift/lower_plan.rs` 的 `lower_plan` 函数签名，加一个 `iceberg_catalogs: Option<&IcebergCatalogRegistry>` 参数；所有现有 caller 默认传 None；MV refresh 路径传 `Some(...)`。

`execute_plan` / `execute_query` 调 `lower_plan` 时从 `state.iceberg_catalogs` 拿出来传进。

- [ ] **Step 8: 编译通过**

Run: `cargo build --lib 2>&1 | grep -E "^error" | head`
Expected: 无错误。

- [ ] **Step 9: 加单测 —— `__nr_ivm_delta` analyzer 分发**

Create test in `src/sql/analyzer/resolve_from.rs` (in existing `#[cfg(test)] mod tests`)：

```rust
#[test]
fn analyzer_recognizes_nr_ivm_delta_table_function() {
    // 构造 InMemoryCatalog 注册一个 v3 row-lineage iceberg 表，
    // 解析 `SELECT * FROM __nr_ivm_delta('cat.ns.t', 100, 200) AS t`，
    // 断言 ResolvedQuery 顶层 Relation 是 IcebergDeltaScan 变体。
}

#[test]
fn analyzer_rejects_nr_ivm_delta_with_negative_snapshot() {
    // `__nr_ivm_delta('cat.ns.t', -1, 200)` → 报错含 "non-negative"
}

#[test]
fn analyzer_rejects_nr_ivm_delta_on_non_v3_table() {
    // 注册一个 v2 表，预期报错含 "row-lineage" 提示
}
```

- [ ] **Step 10: Commit**

```bash
git add idl/thrift/PlanNodes.thrift \
        src/sql/analyzer/resolve_from.rs src/sql/analyzer/mod.rs \
        src/sql/planner/ src/sql/optimizer/ src/sql/codegen/fragment_builder.rs \
        src/lower/thrift/iceberg_delta_scan.rs src/lower/thrift/lower_plan.rs \
        src/exec/node/iceberg_delta_scan.rs
git commit -m "feat(ivm-a1): plan-time IcebergDeltaScan dispatch via __nr_ivm_delta table function + ICEBERG_DELTA_SCAN_NODE Thrift type"
```

---

# Phase 5 — Refresh Driver Integration

### Task 13: 把 `refresh_iceberg_mv` 增量分支重写为 AST mutate + custom-sink execute_query

新方案下 refresh driver 不再 mutate ExecPlan，而是在 sqlparser AST 层把 base 表的 `TableFactor::Table` 替换为 `__nr_ivm_delta` table function，然后调 `execute_query` 时传入自定义 sink（`IcebergMergeSinkFactory`）。Plan-time TPlanNode 由 Phase 4 提供，本 Task 不重复。

**Files:**

- Modify: `src/engine/mv/iceberg_refresh.rs`（line 2009-2253 那段 —— `IcebergChangePolicySignal::Incremental` 实际处理路径）
- Use: `build_iceberg_table_def_for_delta_scan`（Seam 2 commit `4e0b6d4a` 已落地）
- Use: `new_iceberg_mv_commit_collector` + `commit_iceberg_mv_with_populated_collector`（Seam 3 commit `c9b07f01` 已落地）
- Use: `execute_query` 的 sink 参数（Seam 1 v2 形态，待新 commit 落地）

- [ ] **Step 1: 在 `refresh_iceberg_mv` 模块上方加 helper：AST mutate**

Add to `iceberg_refresh.rs`：

```rust
/// Mutate a parsed MV SELECT query in-place: find the unique TableFactor::Table
/// referencing `base_ref`, replace it with a TableFunction call to
/// `__nr_ivm_delta('cat.ns.tbl', from_snap, to_snap) AS <original_alias_or_table>`.
fn mutate_query_for_ivm_delta_scan(
    query: &mut sqlparser::ast::Query,
    base_ref: &crate::connector::starrocks::managed::model::IcebergTableRef,
    from_snapshot_id: i64,
    to_snapshot_id: i64,
) -> Result<(), String> {
    use sqlparser::ast::{Function, FunctionArg, FunctionArgExpr, ObjectName,
                          ObjectNamePart, TableFactor, Value};

    // walk Query.body.from[*] and joins, replace matching TableFactor::Table.
    // For A1 single-base-table contract there must be exactly one match; fail fast
    // when none or multiple.
    let mut matches: usize = 0;
    visit_table_factors_mut(query, &mut |tf| {
        if let TableFactor::Table { name, alias, .. } = tf {
            if matches_base_ref(name, base_ref) {
                matches += 1;
                let alias_for_replacement = alias.clone().or_else(|| {
                    // 默认 alias 用 base 表名，保持外面 SELECT column refs 能 resolve
                    Some(make_table_alias(&base_ref.table))
                });
                *tf = TableFactor::TableFunction {
                    expr: Function {
                        name: ObjectName(vec![ObjectNamePart::Identifier(
                            sqlparser::ast::Ident::new("__nr_ivm_delta"),
                        )]),
                        args: Some(sqlparser::ast::FunctionArguments::List(
                            sqlparser::ast::FunctionArgumentList {
                                duplicate_treatment: None,
                                args: vec![
                                    FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                        sqlparser::ast::Expr::Value(
                                            Value::SingleQuotedString(base_ref.fqn()).into()
                                        )
                                    )),
                                    FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                        sqlparser::ast::Expr::Value(
                                            Value::Number(from_snapshot_id.to_string(), false).into()
                                        )
                                    )),
                                    FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                        sqlparser::ast::Expr::Value(
                                            Value::Number(to_snapshot_id.to_string(), false).into()
                                        )
                                    )),
                                ],
                                clauses: vec![],
                            }
                        )),
                        ...  // 其他 Function 字段照填 default
                    },
                    alias: alias_for_replacement,
                };
            }
        }
    });
    match matches {
        0 => Err(format!(
            "ivm-a1 ast mutate: no FROM {} found in MV SELECT",
            base_ref.fqn()
        )),
        1 => Ok(()),
        n => Err(format!(
            "ivm-a1 ast mutate: expected exactly one FROM {} in MV SELECT, found {n}",
            base_ref.fqn()
        )),
    }
}
```

`visit_table_factors_mut` / `matches_base_ref` / `make_table_alias` 都是新加的小 helper。

- [ ] **Step 2: 重写 `IcebergChangePolicySignal::Incremental` 的处理段**

替换原 line 2009-2253 那段 `let (chunks, delete_base_row_ids) = if has_delete_changes { ... } else { ... };` + 下游 commit。新流程：

```rust
        // 1. 提前 plan_changes 做空 delta 早返回判断（lower_plan 之后还会再调一次，可接受）
        let batch = plan_changes(base_table, previous_snapshot_id, &[])?;
        let has_delete_changes = !batch.deletes.is_empty()
            || !batch.equality_deletes.is_empty()
            || !batch.deleted_data_files.is_empty();
        let is_empty_delta = batch.inserts.is_empty() && !has_delete_changes;
        if is_empty_delta {
            // existing lineage-advance code 不变，提前 return
            ...
            return Ok(StatementResult::Ok);
        }

        // 2. begin A7 staged refresh intent → staging branch（不变）
        let staging_branch = format!("__nova_mv_refresh_{}_{}", ...);
        let refresh_id = begin_staged_iceberg_mv_refresh_intent(...)?;
        ensure_iceberg_mv_staging_branch(...)?;
        let target_table = reload_iceberg_mv_target_table(...)?;

        // 3. 构建 catalog：注册 base 表（empty storage + row-lineage 虚拟列）
        let base_table_def = crate::engine::query_prep::build_iceberg_table_def_for_delta_scan(
            state, &base_ref.catalog, &base_ref.namespace, &base_ref.table,
        )?;
        let mut catalog = crate::sql::analyzer::InMemoryCatalog::default();
        catalog.create_database(&base_ref.namespace)?;
        catalog.register(&base_ref.namespace, base_table_def)
            .map_err(|e| format!("register base table: {e}"))?;
        // target MV 表的 TableDef（如果 MV SELECT 引用 —— A1 简单 MV 不会，但保险注册）
        // 略

        // 4. 解析 MV physical_select_sql → sqlparser AST，mutate base table → __nr_ivm_delta
        let physical_sql = iceberg_mv_physical_select_sql(&mv_definition.select_sql)?;
        let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(&physical_sql)?;
        let statement = crate::sql::parser::parse_normalized_sql_raw(&normalized)
            .map_err(|e| format!("sql parser error: {e}"))?;
        let mut query = match statement {
            sqlparser::ast::Statement::Query(q) => *q,
            _ => return Err("MV SELECT must parse to a Query".to_string()),
        };
        mutate_query_for_ivm_delta_scan(
            &mut query,
            base_ref,
            previous_snapshot_id,
            current_snapshot_id,
        )?;
        // 三部分名转两部分（与 execute_query_for_mv_incremental_refresh 现有处理一致）
        crate::sql::parser::query_refs::strip_catalog_from_three_part_names(&mut query);

        // 5. 构造 commit collector 与 merge sink
        let ident = iceberg_mv_table_ident(target)?;
        let op_kind = if has_delete_changes {
            CommitOpKind::RowDeltaDv
        } else {
            CommitOpKind::FastAppend
        };
        let collector = new_iceberg_mv_commit_collector(
            &target_table, &ident, &staging_branch, op_kind,
        );
        let merge_sink_plan = crate::engine::mv::iceberg_merge_sink::IcebergMergeSinkPlan {
            target_table: target_table.clone(),
            collector: Arc::clone(&collector),
            apply_key_column:
                crate::engine::mv::iceberg_target_apply::ICEBERG_MV_APPLY_KEY_COLUMN.to_string(),
        };
        let merge_sink = crate::engine::mv::iceberg_merge_sink::IcebergMergeSinkFactory::new(
            merge_sink_plan,
        );

        // 6. 单次 execute_query：编译链路全程 plan-time IcebergDeltaScan，pipeline 跑 sink
        crate::engine::execute_query_with_sink(
            &query,
            &catalog,
            current_database,
            state.exchange_port,
            None,                                // query_opts
            Some(Box::new(merge_sink)),          // 自定义 sink 参数
        )?;

        // 7. driver 端 commit
        let marker = load_iceberg_mv_refresh_marker(state, refresh_id, mv_definition.mv_id)?
            .to_summary_properties();
        let new_snapshot_id = match data_block_on(
            commit_iceberg_mv_with_populated_collector(
                &target_table, iceberg_catalog, target_entry, &ident,
                collector, &staging_branch, marker,
            ),
        ) {
            Ok(Ok(outcome)) => outcome.new_snapshot_id,
            Ok(Err(err)) | Err(err) => {
                return Err(handle_iceberg_mv_commit_error(
                    state, target, target_entry, &staging_branch, refresh_id, err,
                ));
            }
        };

        // 8. publish + record + finalize（不变）
        ...
```

注意：`execute_query_with_sink` 是 Seam 1 v2 新增的入口（接受 `terminal_sink: Option<Box<dyn OperatorFactory>>` 参数）。`execute_query` 也可以直接改签名加可选参数，调用方根据需要传 None / Some。

- [ ] **Step 3: 早返回 + 提前 plan_changes 的位置调整**

`is_empty_delta` 检查从原 line 2086-2107 上移到本 Task Step 2 的开头（在 staging branch 创建之前），避免空 delta 时白创 staging。如果担心重复，把 plan_changes 结果作为参数传给后续 lower_plan —— 但这又把 change_files 拉回上层，违背 plan-time TPlanNode 设计；保留"lower_plan 重新调一次"的轻微 redundancy。

- [ ] **Step 4: 编译通过**

Run: `cargo build 2>&1 | grep -E "error\[" | head -20`
Expected: 无错误（warnings 允许）。

- [ ] **Step 5: 手动烟测**

Start docker iceberg-rest + standalone server (per CLAUDE.md §7.3):

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
LOG=/tmp/novarocks-server.log
NO_PROXY=127.0.0.1,localhost target/debug/novarocks standalone-server \
  --config "$NOVAROCKS_STANDALONE_CONFIG" >"$LOG" 2>&1 &
SRV_PID=$!
for i in $(seq 1 60); do
  grep -q '^NOVAROCKS_READY ' "$LOG" && break
  kill -0 "$SRV_PID" 2>/dev/null || { tail -20 "$LOG"; exit 1; }
  sleep 1
done
```

Run an existing iceberg-ivm SQL fixture manually via `mysql` client at port `$NOVA_ENV_MYSQL_PORT`:

```sql
USE iceberg.test_db;
-- create + refresh a small MV (mimic iceberg_backed_mv_projection_filter.sql semantics)
-- assert REFRESH MATERIALIZED VIEW succeeds
```

Expected: no error from `REFRESH MATERIALIZED VIEW`. If analyzer / lower / `IcebergDeltaScan` / `IcebergMergeSink` panics, the error surfaces here.

```bash
kill -9 "$SRV_PID"
```

- [ ] **Step 6: Commit**

```bash
git add src/engine/mv/iceberg_refresh.rs
git commit -m "feat(ivm-a1): rewrite incremental refresh as AST mutate + execute_query with custom sink"
```

---

# Phase 6 — Old Code Removal

### Task 14: 删 `materialize_changes` + 三个 mv_flow.rs 旧函数

**Files:**
- Modify: `src/connector/iceberg/changes.rs`（删 `materialize_changes` 569-679）
- Modify: `src/engine/mv_flow.rs`（删 230-431 与相关 imports）

- [ ] **Step 1: 删 `materialize_changes`**

Delete lines 569-679 from `src/connector/iceberg/changes.rs`. Confirm via `cargo build` that no caller remains.

```bash
cargo build --lib 2>&1 | grep -E "materialize_changes" 
```
Expected: empty (no references).

- [ ] **Step 2: 删 `mv_flow.rs` 中四个函数**

Delete from `src/engine/mv_flow.rs`:
- `execute_query_for_mv_incremental_refresh` (lines 230-280)
- `write_mv_delete_temp_parquet` (lines 282-329)
- `delete_temp_table_def_from_batch_schema` (lines 331-378)
- `execute_query_for_mv_incremental_deletes` (lines 387-431)

Also remove any imports that are now unused (e.g., `use crate::engine::query_prep::IcebergFileForQuery;` if only used by deleted funcs).

- [ ] **Step 3: 删 tests 块中相关测试**

Delete tests in `mv_flow.rs` `#[cfg(test)] mod tests` that reference the deleted functions (likely the `mv_delete_temp_parquet_*` tests at the bottom of the file).

- [ ] **Step 4: 编译 + clippy 验证**

```bash
cargo build --lib 2>&1 | grep -E "error\[" | head
cargo clippy --lib -- -D warnings 2>&1 | head -20
```
Expected: 无错误，无 warning（warning 允许仅 `unused_variables` / `dead_code` 类——但这次不应该剩余）。

- [ ] **Step 5: 跑全部已有单元测试**

```bash
cargo test --lib 2>&1 | tail -20
```
Expected: 全 PASS。

- [ ] **Step 6: Commit**

```bash
git add src/connector/iceberg/changes.rs src/engine/mv_flow.rs
git commit -m "chore(ivm-a1): remove materialize_changes + temp parquet path"
```

---

# Phase 7 — SQL Test Additions

### Task 15: 添加 `iceberg_ivm_a1_large_delta_mixed.sql`

**Files:**
- Create: `sql-tests/iceberg-ivm/sql/iceberg_ivm_a1_large_delta_mixed.sql`
- Create: `sql-tests/iceberg-ivm/result/iceberg_ivm_a1_large_delta_mixed.result`

- [ ] **Step 1: 写 SQL 测试**

Create `sql-tests/iceberg-ivm/sql/iceberg_ivm_a1_large_delta_mixed.sql`. 参考现有的 `iceberg_ivm_base_delete_row_lineage.sql` 的结构与 `CREATE TABLE` / `CREATE MATERIALIZED VIEW` / `INSERT INTO` / `DELETE FROM` / `REFRESH MATERIALIZED VIEW` 的 pattern。

测试 case 要点：
1. 创建一个 base 表（v3 + row-lineage）含 5k+ 行
2. 创建一个 projection+filter MV
3. 首次 refresh full
4. 执行 INSERT 1k 行 + DELETE 1k 行（DELETE 触发 position-delete）
5. REFRESH MATERIALIZED VIEW（A1 路径）
6. SELECT FROM MV，验证结果与 full refresh 等价

```sql
-- iceberg-ivm SQL test: large mixed INSERT+DELETE delta via A1 pipeline
-- Verifies the new IcebergDeltaScan + IcebergMergeSink pipeline handles
-- delta containing both new data files and position-delete files in a
-- single execute_plan invocation (no temp parquet, no double-execute_query).

USE iceberg.test_db;

DROP TABLE IF EXISTS a1_orders;
CREATE TABLE a1_orders (
    id BIGINT,
    region VARCHAR(32),
    amount DECIMAL(10, 2)
)
TBLPROPERTIES ('format-version' = '3', 'write.row-lineage' = 'true');

INSERT INTO a1_orders VALUES (1, 'APAC', 100.00), (2, 'EMEA', 200.00) /* + ~5000 more rows generated via series */;
-- (Use existing test-runner row-generation helper if available; otherwise
-- insert in batches of 200 rows so the .sql file stays readable.)

DROP MATERIALIZED VIEW IF EXISTS a1_mv_high_value;
CREATE MATERIALIZED VIEW a1_mv_high_value
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT region, amount FROM iceberg.test_db.a1_orders WHERE amount > 150;

REFRESH MATERIALIZED VIEW a1_mv_high_value;

SELECT region, count(*), sum(amount) FROM a1_mv_high_value GROUP BY region ORDER BY region;

-- Phase 2: mixed delta — INSERT 1k + DELETE 1k
INSERT INTO a1_orders VALUES (5001, 'APAC', 999.00) /* + ~1000 more rows */;
DELETE FROM a1_orders WHERE id BETWEEN 1 AND 1000;

REFRESH MATERIALIZED VIEW a1_mv_high_value;

SELECT region, count(*), sum(amount) FROM a1_mv_high_value GROUP BY region ORDER BY region;

-- Verify equivalence with full refresh
REFRESH MATERIALIZED VIEW a1_mv_high_value FULL;
SELECT region, count(*), sum(amount) FROM a1_mv_high_value GROUP BY region ORDER BY region;

DROP MATERIALIZED VIEW a1_mv_high_value;
DROP TABLE a1_orders;
```

(Note: actual row-count adjustments and `FULL` syntax depend on the current state of `REFRESH MATERIALIZED VIEW ... FULL` — per #132 this is currently disabled. The test author should check whether to verify-via-direct-SELECT-on-base instead of `FULL`. Adjust accordingly.)

- [ ] **Step 2: 用 sql-test-runner 录制基线**

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
# start standalone-server (per Phase 5 task 13 step 5)
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm --only iceberg_ivm_a1_large_delta_mixed --mode record
```

Expected: 生成 `sql-tests/iceberg-ivm/result/iceberg_ivm_a1_large_delta_mixed.result`，内容是上述 SQL 的实际输出。

- [ ] **Step 3: verify 一次确认稳定**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm --only iceberg_ivm_a1_large_delta_mixed --mode verify
```

Expected: PASS。

- [ ] **Step 4: Commit**

```bash
git add sql-tests/iceberg-ivm/sql/iceberg_ivm_a1_large_delta_mixed.sql \
        sql-tests/iceberg-ivm/result/iceberg_ivm_a1_large_delta_mixed.result
git commit -m "test(ivm-a1): add large mixed INSERT+DELETE delta SQL case"
```

---

### Task 16: 添加 `iceberg_ivm_a1_update_only.sql`

**Files:**
- Create: `sql-tests/iceberg-ivm/sql/iceberg_ivm_a1_update_only.sql`
- Create: `sql-tests/iceberg-ivm/result/iceberg_ivm_a1_update_only.result`

- [ ] **Step 1: 写 UPDATE-only 测试**

Create with same structure as Task 15 but:
- Use `UPDATE iceberg.test_db.a1_orders SET amount = amount * 2 WHERE region = 'APAC';` (which Iceberg materializes as new data file + position-delete)
- Verify target MV reflects the update

- [ ] **Step 2: 录制 + verify + commit**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm --only iceberg_ivm_a1_update_only --mode record
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm --only iceberg_ivm_a1_update_only --mode verify

git add sql-tests/iceberg-ivm/sql/iceberg_ivm_a1_update_only.sql \
        sql-tests/iceberg-ivm/result/iceberg_ivm_a1_update_only.result
git commit -m "test(ivm-a1): add UPDATE-only refresh SQL case"
```

Expected: 两次都 PASS。

---

# Phase 8 — Full Suite Validation

### Task 17: 跑全部 iceberg-ivm + mv-on-iceberg suite

- [ ] **Step 1: 跑 iceberg-ivm suite（必须全过）**

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
# server already running
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm --mode verify
```

Expected: 16/16 PASS（已有 4 + 既有 11 个 A11 + 1 个 base_delete + A1 新增 2 = 18，但若 A11 case 数有变请按实际显示）。

如果有 case fail，需要按 baseline 对比逐一调查。常见可能：
- A4 `__change_op` 透明列在新 sink 边界丢失 → 检查 Project codegen 是否保留未引用列
- A9 locator 调用边界变化（locator_state 预加载时机晚于 staging branch 创建）→ 检查顺序

- [ ] **Step 2: 跑 mv-on-iceberg suite（baseline 12/17 不允许下降）**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite mv-on-iceberg --mode verify
```

Expected: 至少 12/17 PASS（与 origin/main baseline 一致）。新失败必须调查。

- [ ] **Step 3: 跑全部 Rust 单元测试**

```bash
cargo test --lib 2>&1 | tail -30
```

Expected: 全 PASS。

- [ ] **Step 4: 跑 cargo clippy（不增 warnings）**

```bash
cargo clippy --lib -- -D warnings 2>&1 | head -20
```

Expected: 无 error。

- [ ] **Step 5: 写一个 squashed commit message 或保持多 commits**

如果选择不 squash，确认 git log 一致：

```bash
git log --oneline origin/main..HEAD
```

Expected: 看到 Task 1-17 的逐步 commits。

如果选择最终交付为单一 PR，准备 PR 描述（用现有 PR 命名习惯，例如 `feat(ivm-a1): replace temp parquet + double-query with single delta pipeline`）。

---

## Self-Review Checklist (Plan Author 已做)

- [x] 每 Task 的 Files 部分给出绝对路径
- [x] 每 step 都有可运行命令或具体代码片段
- [x] 不留 TODO/TBD 占位（少数 `Note:` 标记的是实现期需要决定的微调点，已说明决策依据）
- [x] Phase 顺序按依赖：foundations → helpers → operators → plan rewrite → integration → deletion → tests → validation
- [x] Phase 4/5/6/7 都有"任何阶段可停下来 review"的天然 commit 边界
- [x] 验收测试 (Phase 7) 在删 temp parquet 代码 (Phase 6) 之后跑——硬保证

---

## Risks Summary（与 spec §7 对应）

实施过程中如果遇到以下情况，停下来 review：

- Streaming state machine 在 multi-driver 下不正确 → A1 第一版坚持 single-driver（factory 已限制 driver_id == 0 才工作），multi-driver 留后续
- 反向重建 helper 搬家后 changes.rs 既有单元测试 fail → 不要继续推进；保留 helper 的 batch 接口为内层调用，新 streaming helper 是外层包装
- Leaf-swap 找不到唯一 base scan → fail-fast 报错（不要 silently 走旧路径）
- `commit_iceberg_mv_apply_with_ref` 参数兼容性：A1 改造把 written/delete_groups 从参数移到 collector 时，要确保其他 8+ 个 `write_chunks_as_iceberg_data_files` caller 不受影响

---

**Plan complete and saved to `docs/design/plans/2026-05-14-ivm-a1-delta-pipeline-plan.md`.**
