# Iceberg V3 Row-Lineage Metadata Columns Read Path Design

**日期**：2026-04-30
**状态**：Accepted（与用户 brainstorming 中逐节确认）
**范围**：让 NovaRocks SQL 在 Iceberg V3 row-lineage 表上正确投影 `_row_id` / `_last_updated_sequence_number` 两个 reserved metadata columns，严格按 V3 spec 的 stored/fallback 双路径语义实现。本工作对应 IVM-on-Iceberg roadmap 里程碑 C 的 §2.4 + §2.3 + §3.2 子项；不含 IVM 适配器侧的 §5.1。

**关联文档**：
- IVM-on-Iceberg roadmap：`~/Documents/Obsidian/NovaRocks IVM on Iceberg Roadmap.md`
- Phase 2a 设计（写侧 row-lineage 基础）：`docs/superpowers/specs/2026-04-29-iceberg-v3-row-lineage-dv-phase2-design.md`
- IVM Phase 2 changelog scan：`docs/superpowers/specs/2026-04-29-ivm-iceberg-v3-changelog-scan-design.md`
- Vendor patch 索引：`vendor/iceberg-0.9.0/PATCH.md`
- Iceberg spec — Row Lineage：https://iceberg.apache.org/spec/#row-lineage

---

## 0. 目标与非目标

### 目标

1. `SELECT _row_id, _last_updated_sequence_number FROM ice.ns.t` 在 V3 row-lineage 表（`format-version=3` + `write.row-lineage=true`）上返回 spec 兼容的 BIGINT 值。
2. Vendor `iceberg-rust` 0.9 的 `_row_id` 读路径从「无条件 `first_row_id + _pos`」升级到 spec 兼容的「stored 列存在且非 NULL → 用 stored；否则 fallback」。
3. Vendor 新增 `_last_updated_sequence_number` 读路径，对称语义：「stored → 用 stored；否则 fallback 到 manifest entry 的 `data_sequence_number`」。
4. NovaRocks SQL analyzer / lowering / connector / scan operator 把这两个名字端到端接到 vendor scan。
5. 在不支持的表（V2 Iceberg / V3 但未启 row-lineage / 非 Iceberg）上 SELECT 这两个名字时 fail fast，错误信息明确指出要求。

### 非目标

- 写侧 partial-overwrite 保留旧 `_row_id` / `_last_updated_sequence_number`（roadmap §3.1 长期方案）。
- IVM 适配器消费 `_row_id` 替换现有 position 反查（roadmap §5.1）。
- Spark/Trino 写出的 stored-column 非 NULL 文件的 cross-engine 集成测试（vendor 单测构造 RecordBatch 已覆盖逻辑路径）。
- 其它 V3 reserved 元数据列（`_change_type` / `_change_ordinal` / `_commit_snapshot_id` 等）。
- 写侧改动：`OverwriteCommit` / `RowDeltaDvCommit` / `FastAppendCommit` 不动。当前不写 stored 列 = 全 NULL，spec 允许，读时 fallback 路径正确。

---

## 1. Iceberg V3 Spec 行为摘要

### 1.1 `_row_id`

- field id：`2147483540` (`i32::MAX - 107`)
- 写：新增/修改行写 NULL（读侧推导）；rewrite 时保留未变行的非 NULL stored 值
- 读：
  - parquet 列存在且行值非 NULL → 用 stored
  - 行值 NULL / 列缺失 → fallback = `first_row_id + parquet_row_index`，其中 `first_row_id` 来自 manifest entry

### 1.2 `_last_updated_sequence_number`

- field id：`2147483539` (`i32::MAX - 108`)
- 写：新增/修改行 NULL；rewrite 时保留未变行的非 NULL stored 值
- 读：
  - parquet 列存在且行值非 NULL → 用 stored
  - 行值 NULL / 列缺失 → fallback = manifest entry 的 `data_sequence_number`

### 1.3 NovaRocks 当前生态下的实际触发面

- NovaRocks 写侧从未写这两个 stored 列（FastAppend / Overwrite / RowDeltaDv 都没有这一步）。所以 NovaRocks 自己写出的 v3 row-lineage parquet 文件里这两列**永远缺失**。
- 这意味着「stored 非 NULL」分支只在跨引擎读（Spark/Trino 写、NovaRocks 读）时被触发。
- vendor 单测必须覆盖 stored 分支（手工构造 RecordBatch）；NovaRocks 集成测试可以只覆盖 fallback 分支。

---

## 2. 架构

```
SQL                                        analyzer
  SELECT _row_id, _last_updated_sequence_number  ──►  在 V3 row-lineage 表的 column 解析路径
  FROM ice_v3_lineage_t                                上接受这两个 reserved 名字；
                                                              其它表 fail fast
                                                              │
                                                              ▼
                                          lowering (hdfs_scan.rs)
                                                  把两个 slot 标记下来，扩展 IcebergVirtualSpec
                                                  跟现有 _pos / _file 路径并行
                                                              │
                                                              ▼
                                          connector / scan range
                                              ScanRange 透传 first_row_id (existing) 和
                                              data_sequence_number (new)
                                                              │
                                                              ▼
                                          file_scan operator
                                              把 reserved field id 加进 vendor TableScanBuilder.select；
                                              data_sequence_number 填到 FileScanTask
                                                              │
                                                              ▼
                                          vendor iceberg-0.9.0
                                              record_batch_transformer 按 spec：
                                                  stored 列存在     → 逐行 stored / fallback
                                                  stored 列缺失     → 全 fallback
                                                       _row_id      = first_row_id + _pos
                                                       _last_updated = data_sequence_number
```

**架构原则**：所有跨 file 推导逻辑收敛在 vendor 层（spec 兼容性的天然层级）；NovaRocks 层只负责 SQL 名字解析和 slot 路由。这样上游 iceberg-rust 将来补完同款逻辑时，NovaRocks 改动量为零（vendor patch 退役即可）。

---

## 3. 数据模型变更

### 3.1 Vendor — `FileScanTask`

`vendor/iceberg-0.9.0/src/scan/task.rs`：

```rust
pub struct FileScanTask {
    // existing
    pub first_row_id: Option<i64>,
    // NEW
    pub data_sequence_number: Option<i64>,
}
```

`data_sequence_number` 来源：manifest entry 的 `data_sequence_number()`（iceberg-rust 已自动处理 inherited vs explicit）。

### 3.2 Vendor — `ColumnSource`

`vendor/iceberg-0.9.0/src/arrow/record_batch_transformer.rs`：

```rust
pub(crate) enum ColumnSource {
    PassThrough { source_index: usize },
    Promote { target_type: DataType, source_index: usize },
    Add { target_type: DataType, value: Option<PrimitiveLiteral> },

    // EXTENDED — patch 3 修订
    RowId {
        first_row_id: i64,
        pos_source_index: usize,
        stored_source_index: Option<usize>,  // Some 时 parquet 文件含物理 _row_id 列
    },

    // NEW — patch 5
    LastUpdatedSeqNum {
        fallback_value: i64,                 // = task.data_sequence_number
        stored_source_index: Option<usize>,
    },
}
```

`generate_transform_operations` 在见到 `RESERVED_FIELD_ID_ROW_ID` / `RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER` 时：

1. 在 `field_id_to_source_schema_map` 中按 reserved field id 查找 stored 列的 `source_index`（沿用 patch 3 处理 `_pos` 的同 pattern：parquet 文件层用 field-id metadata 把列 ID 关联到 reserved id）
2. 找到 → `stored_source_index = Some(idx)`，找不到 → `None`

`process_record_batch` 实际计算（per-row）：

- `RowId { first_row_id, pos_source_index, stored_source_index }`：
  - `stored_source_index = Some(idx)`：对每行，stored 非 NULL → 用 stored；NULL → `first_row_id + pos[i]`
  - `stored_source_index = None`：所有行 = `first_row_id + pos[i]`
- `LastUpdatedSeqNum { fallback_value, stored_source_index }`：
  - `stored_source_index = Some(idx)`：对每行，stored 非 NULL → 用 stored；NULL → `fallback_value`
  - `stored_source_index = None`：所有行 = `fallback_value`

### 3.3 NovaRocks — `IcebergVirtualSpec`

`src/exec/row_position.rs`：

```rust
pub const ICEBERG_FILE_PATH_COL: &str = "_file";
pub const ICEBERG_ROW_POS_COL: &str = "_pos";
pub const ICEBERG_ROW_ID_COL: &str = "_row_id";                                // NEW
pub const ICEBERG_LAST_UPDATED_SEQ_COL: &str = "_last_updated_sequence_number"; // NEW

pub fn is_iceberg_file_path(name: &str) -> bool { ... }
pub fn is_iceberg_row_pos(name: &str) -> bool { ... }
pub fn is_iceberg_row_id(name: &str) -> bool { name.eq_ignore_ascii_case(ICEBERG_ROW_ID_COL) }
pub fn is_iceberg_last_updated_sequence_number(name: &str) -> bool {
    name.eq_ignore_ascii_case(ICEBERG_LAST_UPDATED_SEQ_COL)
}

pub struct IcebergVirtualSpec {
    pub file_path_slot: Option<SlotId>,
    pub row_pos_slot: Option<SlotId>,
    pub row_id_slot: Option<SlotId>,                       // NEW
    pub last_updated_seq_slot: Option<SlotId>,             // NEW
    pub file_path_field: Option<Field>,
    pub row_pos_field: Option<Field>,
    pub row_id_field: Option<Field>,                       // NEW
    pub last_updated_seq_field: Option<Field>,             // NEW
}
```

### 3.4 NovaRocks — Scan Range

`src/connector/hdfs.rs`：scan range 已携带 `first_row_id: Option<i64>`，新增 `data_sequence_number: Option<i64>`，从 iceberg manifest entry 取，跟 first_row_id 同 pattern 透传。

---

## 4. 改动点列表

### 4.1 Vendor

| 文件 | 改动 |
|---|---|
| `vendor/iceberg-0.9.0/src/scan/task.rs` | `FileScanTask` 加 `data_sequence_number: Option<i64>` |
| `vendor/iceberg-0.9.0/src/scan/mod.rs` 或 plan_files 路径 | manifest entry → FileScanTask 时填入 `data_sequence_number` |
| `vendor/iceberg-0.9.0/src/arrow/reader.rs` | (a) 投影 `_row_id` 时同时探测 parquet 文件是否含 stored `_row_id` 列；(b) 投影 `_last_updated_sequence_number` 时同上；(c) 把 `task.data_sequence_number` 透传给 transformer |
| `vendor/iceberg-0.9.0/src/arrow/record_batch_transformer.rs` | (a) `ColumnSource::RowId` 加 `stored_source_index` 字段；(b) 新增 `ColumnSource::LastUpdatedSeqNum`；(c) `generate_transform_operations` 处理两个 reserved field id；(d) `process_record_batch` 实现 stored / fallback 双路径 |
| `vendor/iceberg-0.9.0/PATCH.md` | patch 3 描述更新 + 新增 patch 5（`_last_updated_sequence_number`）|

### 4.2 NovaRocks

| 文件 | 改动 |
|---|---|
| `src/exec/row_position.rs` | 加两个名字常量 + `is_iceberg_row_id` / `is_iceberg_last_updated_sequence_number` + `IcebergVirtualSpec` 扩 4 字段 |
| `src/lower/node/hdfs_scan.rs` | 在现有 `iceberg_virtual_pos_slot` 识别循环中加两个新分支，类型校验（BIGINT），把 slot id + reserved field id 填到 `IcebergVirtualSpec` |
| `src/lower/node/file_scan.rs` | `data_sequence_number` 从 metadata 一路传到 vendor scan task |
| `src/connector/hdfs.rs` | scan range 透传 `data_sequence_number`，跟 `first_row_id` 同 pattern |
| `src/exec/operators/scan/runner.rs` 或 `source.rs` | 当 `IcebergVirtualSpec` 的 `row_id_slot` / `last_updated_seq_slot` 是 Some 时，把 `RESERVED_COL_NAME_ROW_ID` / `RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER` 加进 vendor `TableScanBuilder.select(...)` |
| `src/sql/analyzer/`（具体函数位置：处理 SELECT 列解析、把列名映射到 base table schema 字段的主路径——plan 阶段在该目录下定位现有 column resolution 入口） | 在 Iceberg 表的 column 解析路径中：当 base table 为 `format-version=3` 且 `write.row-lineage=true` 时把 `_row_id` / `_last_updated_sequence_number` 作为 BIGINT pseudo-column 加入可选投影列表；其它情况下返回 §5.1 的明确错误 |

### 4.3 显式不动

- 任何写侧 commit 路径（`src/connector/iceberg/commit/*`）
- IVM 路径（`src/connector/iceberg/changes.rs`、`scan_deletes.rs`、`src/connector/starrocks/managed/ivm_change_stream.rs`、`mv_refresh_iceberg.rs`）
- v2 `_pos` / `_file` 在 DELETE flow 中的现有使用

---

## 5. 错误处理（fail-fast 边界）

### 5.1 SQL Analyzer 层

| 情况 | 行为 |
|---|---|
| `SELECT _row_id FROM v2_iceberg_t` | analyzer error: `column "_row_id" is only available on Iceberg V3 row-lineage tables (table is format-version=2)` |
| `SELECT _row_id FROM v3_no_lineage_t` | analyzer error: `column "_row_id" is only available on Iceberg V3 row-lineage tables (write.row-lineage is not enabled)` |
| `SELECT _row_id FROM internal_olap_t` | analyzer error: `column "_row_id" is only available on Iceberg V3 row-lineage tables (table is not an Iceberg table)` |
| `_last_updated_sequence_number` | 同样三类错误，文案对称 |

错误用现有 NovaRocks SQL error type 表达；无需新增 error variant。

### 5.2 Vendor 层

| 情况 | 行为 |
|---|---|
| 投影 `_row_id` 但 `FileScanTask::first_row_id` 是 None | `Error::Unexpected("_row_id projected but task is missing first_row_id; row-lineage table required")` |
| 投影 `_last_updated_sequence_number` 但 `FileScanTask::data_sequence_number` 是 None | 对称 fail |
| stored 列存在但 type 不是 Int64 | type mismatch fail |
| stored 列存在，`first_row_id < 0`（patch 3 当前行为） | 保留 fail |
| stored 列存在，单行 stored = NULL | 用 fallback 值（**不报错**，spec 允许）|

### 5.3 NovaRocks Lowering 层

如果 `_row_id` slot 出现在非 Iceberg V3 row-lineage 表的 scan 上（理论上 analyzer 已挡住），lowering 返回 `internal error`；这是 invariant 校验，不是用户错误。

---

## 6. 数据流（端到端 SELECT 路径）

以 `SELECT id, _row_id, _last_updated_sequence_number FROM ice.ns.t` 为例（t 是 V3 row-lineage 表）：

1. **Analyzer**：从 catalog 加载 t 的 metadata，发现 `format-version=3` + `write.row-lineage=true`。在解析 SELECT 列时把 `_row_id` / `_last_updated_sequence_number` 作为 BIGINT pseudo-column 加入；否则报 §5.1 中相应错误。
2. **Lowering**：scan node 的 slot 列表里这两个名字进入 `hdfs_scan.rs`，被新分支识别成 row-lineage slot；写到 `IcebergVirtualSpec` 的 `row_id_slot` / `last_updated_seq_slot`。
3. **Connector → ScanRange**：`hdfs.rs` 的 ScanRange 已携带 `first_row_id`，新增 `data_sequence_number`，从 iceberg manifest entry 取。
4. **Operator**：scan operator 根据 `IcebergVirtualSpec` 的两个新 slot 决定在 vendor `TableScanBuilder.select(...)` 里加上 `RESERVED_COL_NAME_ROW_ID` / `RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER`；把 `data_sequence_number` 传给 vendor。
5. **Vendor**：`reader.rs` 构造 `FileScanTask` 时填入 `first_row_id` 和 `data_sequence_number`；扫描时检测 parquet 文件 schema 中是否有对应 reserved field id 的列（按 iceberg-rust 既有的 field-id metadata 路径），有就把它的 source_index 一起传给 transformer；transformer 按第 §3.2 节的 stored / fallback 双路径输出。
6. **Output**：`RecordBatch` 多两列 `_row_id: Int64` / `_last_updated_sequence_number: Int64`，沿现有 chunk 路径回到 SQL。

---

## 7. 测试策略

### 7.1 Vendor 单元测试

`vendor/iceberg-0.9.0/src/arrow/record_batch_transformer.rs::tests`：

| 测试 | 覆盖 |
|---|---|
| `row_id_uses_stored_column_when_present` | RecordBatch 含 stored `_row_id` 全非 NULL → 全部用 stored，不算 fallback |
| `row_id_falls_back_when_stored_is_null_per_row` | mixed RecordBatch（部分行 stored=NULL，部分行有值）→ NULL 行用 `first_row_id+_pos`，非 NULL 行用 stored |
| `row_id_falls_back_when_stored_column_missing` | 不含 stored 列 → 全部 fallback（保留 patch 3 当前行为） |
| `row_id_fails_when_first_row_id_missing` | first_row_id=None 时投影 → fail |
| `last_updated_seq_uses_stored_when_present` | 对称 |
| `last_updated_seq_falls_back_per_row_null` | 对称 |
| `last_updated_seq_falls_back_when_column_missing` | 对称 |
| `last_updated_seq_fails_when_data_sequence_number_missing` | 对称 |

`vendor/iceberg-0.9.0/src/scan/task.rs::tests` 或 plan_files 测试位置：

| 测试 | 覆盖 |
|---|---|
| `task_carries_data_sequence_number_from_manifest` | 构造 manifest entry → 验证 `FileScanTask::data_sequence_number` 正确填入 |

### 7.2 NovaRocks 单元测试

| 文件 / 测试 | 覆盖 |
|---|---|
| `src/exec/row_position.rs::tests::is_iceberg_row_id_recognizes_name` | 名字判定大小写不敏感 |
| `src/exec/row_position.rs::tests::is_iceberg_last_updated_sequence_number_recognizes_name` | 同上 |
| `src/lower/node/hdfs_scan.rs::tests::lowering_propagates_row_id_slot` | 验证 IcebergVirtualSpec 新字段被填 |
| `src/lower/node/hdfs_scan.rs::tests::lowering_propagates_last_updated_seq_slot` | 同上 |
| `src/sql/analyzer/...::tests::rejects_row_id_on_v2_iceberg_table` | 错误信息 |
| `src/sql/analyzer/...::tests::rejects_row_id_on_v3_table_without_row_lineage` | 错误信息 |
| `src/sql/analyzer/...::tests::rejects_row_id_on_non_iceberg_table` | 错误信息 |
| `src/sql/analyzer/...::tests::accepts_row_id_on_v3_row_lineage_table` | resolve 成功 |

### 7.3 NovaRocks 集成测试

放在 `src/engine/mod.rs::tests`（沿用现有 iceberg 端到端测试 pattern，跟 `iceberg_v3_row_lineage_*` 系列同位置；plan 阶段如发现该模块过大，可拆出 `src/engine/iceberg_row_lineage_select_tests.rs` 独立模块）：

```rust
#[test]
fn select_row_id_and_last_updated_seq_on_v3_row_lineage_table() {
    // 1. 创建 V3 row-lineage iceberg 表 ice.ns.t (id BIGINT, name STRING)
    //    tblproperties("format-version"="3","write.row-lineage"="true")
    // 2. INSERT (1,'A'), (2,'B'), (3,'C')   -> snapshot S1, sequence_number seq_S1
    // 3. SELECT id, _row_id, _last_updated_sequence_number FROM ice.ns.t ORDER BY id
    //    断言:
    //      _row_id = [first_row_id_S1, first_row_id_S1+1, first_row_id_S1+2]
    //      _last_updated_sequence_number = [seq_S1, seq_S1, seq_S1]
    // 4. INSERT (4,'D'), (5,'E')           -> snapshot S2, sequence_number seq_S2
    // 5. SELECT 全部行
    //    断言:
    //      新行 _row_id 紧接旧行（连续,不重叠)
    //      新行 _last_updated = seq_S2，旧行仍 = seq_S1
    // 6. DELETE WHERE id=2 (Phase 2a Puffin DV)
    //    SELECT 余下行
    //    断言:
    //      id=1 的 _row_id 不变, _last_updated_sequence_number 不变
    //      (DV 不重写 data file → fallback 值不变 → spec 兼容)
}

#[test]
fn select_row_id_fails_on_v2_iceberg_table() {
    // 错误信息匹配 §5.1
}

#[test]
fn select_row_id_fails_on_v3_no_lineage_table() {
    // 错误信息匹配 §5.1
}

#[test]
fn select_last_updated_sequence_number_fails_on_v2_iceberg_table() {
    // 错误信息匹配 §5.1
}
```

minio 不可达时按既有 `is_unavailable_object_store_error` 模式跳过。

### 7.4 不做的测试

- Cross-engine fixture（Spark/Trino 写 stored 非 NULL）：vendor 单测的 RecordBatch 已覆盖。
- Partial-overwrite 后行身份保留：写侧不在本 PR 范围。
- IVM 用 `_row_id` 替代 position 反查：roadmap §5.1。
- Schema evolution / partition evolution 下的 row-lineage：roadmap 范围外。

---

## 8. 风险与坑点

### 8.1 Vendor source schema 的 reserved field id 映射

vendor `field_id_to_source_schema_map` 当前对 `_pos` 的处理依赖 reader.rs 注入 RowNumber 虚拟列时携带的 field-id metadata。新加的 stored `_row_id` / `_last_updated_sequence_number` 是否在 source schema map 中按 reserved id 找得到？需要在实施阶段读 `reader.rs` 的 source schema 构造路径确认：parquet 文件层 field-id metadata 是否会自动关联到 reserved id。如果不会，需要 reader.rs 在 select 阶段就把 reserved id 显式注入到 source schema map。

### 8.2 `data_sequence_number` 的 inherited vs explicit

iceberg-rust `ManifestEntry::data_sequence_number()` 的 API 已自动处理 inherited（snapshot 继承）vs explicit。本 PR 直接调用此 API，无需自行处理 inherit 逻辑。

### 8.3 NovaRocks SQL 暴露新名字的位置

NovaRocks analyzer 当前对未声明列名按表 schema 拒绝（user-level `SELECT _pos FROM ice_t` 不工作；`_pos` 仅 DELETE flow 内部 SQL 重写时使用）。本 PR 必须新增「Iceberg V3 row-lineage 表上下文中两个 reserved 名字作为可投影 pseudo-column」机制。具体接入位置（catalog schema 加载阶段 / column resolution 阶段 / SELECT 列展开阶段）由 plan 阶段确定，需要先读 NovaRocks analyzer 的 column resolution 主路径。

### 8.4 IVM 路径不受影响

IVM Phase 2 的 `plan_changes` / `materialize_changes` / `scan_deletes` 当前基于 position 反查工作良好，本 PR 不动。`_row_id` 在 IVM 中的应用由后续 PR（roadmap §5.1）处理。本 PR 完成判定中包含 IVM 既有测试无回归。

### 8.5 集成测试的 `_row_id` 起始值断言

NovaRocks 自己写的第一个 INSERT snapshot 的 `first_row_id` 应该是 0（按 V3 spec，table metadata 的 `next-row-id` 从 0 开始）。第二个 INSERT snapshot 的 `first_row_id` = 第一次的 `record_count`。集成测试断言不要硬编码具体值，而是从 INSERT 后的 metadata 取出 `first_row_id` 跟 SELECT 结果对照——避免引擎调度顺序细节导致 flaky。

---

## 9. 提交边界

按 Brainstorming approach A：单一 PR，4 个 commit，每个独立可编译可测试。

1. **`feat(vendor): add stored-column override for _row_id read path`**
   `record_batch_transformer.rs` 的 `RowId` 变体扩 `stored_source_index` + 检测逻辑 + 4 个新单测 + PATCH.md 更新 patch 3 描述
2. **`feat(vendor): add _last_updated_sequence_number read path`**
   `FileScanTask::data_sequence_number` + manifest 填入 + reader.rs 透传 + 新 `LastUpdatedSeqNum` 变体 + 4 个新单测 + PATCH.md 加 patch 5
3. **`feat(novarocks): expose _row_id / _last_updated_sequence_number in iceberg scan`**
   `exec/row_position.rs` 新名字判定 + `IcebergVirtualSpec` 扩字段 + `hdfs_scan.rs` lowering 分支 + ScanRange `data_sequence_number` 透传 + scan operator 把两个 reserved field id 加入 vendor select + analyzer 在 V3 row-lineage 表上接受新名字 + fail-fast 错误 + 单元测试
4. **`test(novarocks): cover row_id / last_updated_sequence_number end-to-end on v3 row-lineage table`**
   集成测试 4 个

每个 commit 独立 review、独立 bisect 友好。

---

## 10. 完成判定

- `cargo fmt --check` 通过
- `cargo build -p novarocks` 通过
- `cargo build` （vendor + 全 workspace）通过
- Vendor 单测：8 个新增（4 row_id + 4 last_updated_seq）+ 1 个 task carry test 全过
- NovaRocks 单测：8 个新增全过
- NovaRocks 集成测试：4 个新增全过（minio 不可达时按既有模式跳过）
- IVM Phase 2 既有测试：无回归（`cargo test -p novarocks --lib connector::iceberg::changes`、`scan_deletes`、`commit::puffin_dv`、`starrocks::managed::mv_refresh`）
- Phase 2a 既有测试：无回归（`cargo test -p novarocks --lib engine::tests::iceberg_`、`connector::iceberg::commit`）
- `cargo clippy -p novarocks --all-targets`：无新增 warning

---

## 11. 变更记录

- 2026-04-30 — 初版。Brainstorming 经命名（直接用 `_row_id`）/ 行为（fail fast）/ scope（approach A）/ 5 节设计逐节确认通过。基于发现 vendor patch 3 的 `_row_id` 实现不符合 V3 spec 而要求修订，比 IVM-on-Iceberg roadmap §2.3 原始设想多一项 vendor 端 spec compliance 工作。
