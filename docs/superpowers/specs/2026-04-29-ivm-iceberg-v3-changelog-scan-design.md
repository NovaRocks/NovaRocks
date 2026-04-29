# IVM-on-Iceberg-v3 Changelog Scan Design

**日期**：2026-04-29
**状态**：Accepted（与用户在 brainstorming 中逐节确认）
**范围**：让 IVM Phase 2 的 `plan_changes` + `scan_deletes` 接受 Iceberg v3 row-lineage 表与 Puffin deletion-vector deletes，端到端打通「内表 MV + Iceberg v3 base 的 INSERT + DELETE 增量刷新」

**关联文档**：
- 通用 v3 roadmap：`Obsidian/NovaRocks Iceberg v3 Roadmap.md`
- IVM-on-Iceberg roadmap（本工作隶属里程碑 A + B）：`Obsidian/NovaRocks IVM on Iceberg Roadmap.md`
- Phase 2a 设计：`docs/superpowers/specs/2026-04-29-iceberg-v3-row-lineage-dv-phase2-design.md`

---

## 0. 目标与非目标

### 目标

1. CREATE-time 验证接受 `format-version=3` 的 base 表（不限是否 row-lineage）
2. `plan_changes` / `collect_files` 在 `Operation::Delete` snapshot 下识别 `DataFileFormat::Puffin` deletion-vector 文件
3. `PositionDeleteRef` 数据模型扩展承载 DV 元数据（`content_offset` / `content_size_in_bytes` / `file_format`）
4. `scan_deletes` 新增 Puffin DV 反向投影路径，与现有 v2 Parquet PD 路径并存
5. 端到端集成测试：v3 row-lineage 表 + INSERT + DELETE → MV 增量刷新结果正确

### 非目标（明确不做）

- `_row_id` / `_last_updated_sequence_number` 虚拟列读路径（roadmap §2.3 / §3.2，里程碑 C）
- Patch 3 `_pos` 根因修复（roadmap §2.4，里程碑 C；本 PR 不依赖）
- INSERT OVERWRITE 在 row-lineage 表上的「fall-back full refresh」处理（roadmap §3.1，本 PR 维持 ERROR + 改进信息）
- 多 partition / partition evolution 验证（roadmap §3.3，里程碑 E）
- Snapshot summary `total-*` carry-forward
- DV 文件 compaction、多 blob Puffin 写

---

## 1. 数据模型变更

### 1.1 `PositionDeleteRef` 新字段

```rust
// src/connector/iceberg/changes.rs
pub(crate) struct PositionDeleteRef {
    pub delete_file_path: String,
    pub delete_file_size: i64,
    pub record_count: Option<i64>,
    pub referenced_data_file: Option<String>,

    // 新增：
    /// Parquet 表示 v2 position-delete；Puffin 表示 v3 deletion-vector。
    pub file_format: iceberg::spec::DataFileFormat,
    /// Puffin DV 必填，Parquet 必空：deletion-vector blob 在 Puffin 文件内的字节偏移。
    pub content_offset: Option<i64>,
    /// Puffin DV 必填，Parquet 必空：deletion-vector blob 在 Puffin 文件内的字节长度。
    pub content_size_in_bytes: Option<i64>,
}
```

**Invariant**（在构造函数 / 校验函数里 assert，不依赖类型）：

```
file_format == Puffin
  ⇔ content_offset.is_some() ∧ content_size_in_bytes.is_some() ∧ referenced_data_file.is_some()
file_format == Parquet
  ⇔ content_offset.is_none() ∧ content_size_in_bytes.is_none()
```

违反 invariant 一律返回 `ChangeError::InternalInconsistency`。

### 1.2 `ChangeError` 变更

- `DeletionVectorUnsupported` 变体**保留枚举位置但不再被构造**（向后兼容；删除会触动测试和外部 dead_code lint）
- `IcebergFormatUnsupported` 文案更新：从 "requires v2" 改为 "requires v2 or v3"，校验逻辑接受 `2` 与 `3`
- `UnsupportedOperation { op: "overwrite" }` 错误信息追加：「base 表上做了 INSERT OVERWRITE，IVM 当前不能跨过这个 snapshot；请改用 DELETE+INSERT，或者 DROP MATERIALIZED VIEW + CREATE 重新初始化」

### 1.3 `DeletionVector` 新增方法

```rust
// src/connector/iceberg/commit/puffin_dv.rs
impl DeletionVector {
    /// 把内部 BTreeMap<u32, RoaringBitmap> 转成 RoaringTreemap，便于跟
    /// scan_deletes 现有的 `RoaringTreemap` 数据流对齐。
    pub fn to_roaring_treemap(&self) -> roaring::RoaringTreemap;
}
```

实现：迭代 `bitmaps` 把每个 `(high32, RoaringBitmap)` 展开成 `RoaringTreemap` 项。

---

## 2. `collect_files::CollectDeletes` 改动

当前实现把 `DataContentType::PositionDeletes` 一律按 v2 Parquet 处理。改为按 `df.file_format()` 分发：

```rust
DataContentType::PositionDeletes => {
    match df.file_format() {
        DataFileFormat::Parquet => {
            // 现有 v2 路径：
            deletes.push(PositionDeleteRef {
                delete_file_path: df.file_path().to_string(),
                delete_file_size: ...,
                record_count: ...,
                referenced_data_file: df.referenced_data_file(),
                file_format: DataFileFormat::Parquet,
                content_offset: None,
                content_size_in_bytes: None,
            });
        }
        DataFileFormat::Puffin => {
            // v3 DV 新路径：
            let referenced = df.referenced_data_file().ok_or_else(|| ChangeError::InternalInconsistency(
                format!("Puffin DV {} missing referenced_data_file", df.file_path())
            ))?;
            let offset = df.content_offset().ok_or_else(|| ...)?;
            let length = df.content_size_in_bytes().ok_or_else(|| ...)?;
            deletes.push(PositionDeleteRef {
                delete_file_path: df.file_path().to_string(),
                delete_file_size: ...,
                record_count: ...,
                referenced_data_file: Some(referenced),
                file_format: DataFileFormat::Puffin,
                content_offset: Some(offset),
                content_size_in_bytes: Some(length),
            });
        }
        other => return Err(ChangeError::InternalInconsistency(
            format!("delete manifest contains unsupported file_format {:?}: {}", other, df.file_path())
        )),
    }
}
DataContentType::EqualityDeletes => Err(ChangeError::EqualityDeleteUnsupported { snapshot_id }),  // 不变
DataContentType::Data => Err(...),  // 不变
```

`CollectInserts` 路径完全不动——row-lineage 写出的 data file 是 Parquet，跟 v2 走同一条逻辑。

---

## 3. CREATE-time format 验证放宽

`engine/mv_flow.rs`（或当前 IVM CREATE 验证位置）的 format-version 校验：

```rust
// 旧：
if metadata.format_version() != FormatVersion::V2 {
    return Err(ChangeError::IcebergFormatUnsupported { format_version: ... });
}
// 新：
match metadata.format_version() {
    FormatVersion::V2 | FormatVersion::V3 => {}
    other => return Err(ChangeError::IcebergFormatUnsupported { format_version: other as i32 }),
}
```

错误文案同步更新。

---

## 4. `scan_deletes` 改动

### 4.1 新增 `read_dv_positions_per_data_file`

```rust
async fn read_dv_positions_per_data_file(
    delete_files: &[PositionDeleteRef],   // 仅 file_format=Puffin 子集
    file_io: &iceberg::io::FileIO,
) -> Result<HashMap<String, RoaringTreemap>, ChangeError> {
    let mut out = HashMap::new();
    for r in delete_files {
        debug_assert_eq!(r.file_format, DataFileFormat::Puffin);
        let referenced = r.referenced_data_file.clone().ok_or(...)?;
        let offset = r.content_offset.ok_or(...)?;
        let length = r.content_size_in_bytes.ok_or(...)?;
        let dv = puffin_dv::read_deletion_vector_puffin(file_io, &r.delete_file_path, offset, length)
            .await
            .map_err(|e| ChangeError::InternalInconsistency(format!("read Puffin DV {}: {e}", r.delete_file_path)))?;
        let treemap = dv.to_roaring_treemap();
        out.entry(referenced).or_insert_with(RoaringTreemap::new).extend(treemap);
    }
    Ok(out)
}
```

注意：调用 `read_deletion_vector_puffin` 需要 `FileIO`，但 v2 路径用的是 `OpendalRangeReaderFactory`。两者并存即可——v3 路径走 FileIO，v2 路径走 factory。

### 4.2 `scan_deletes` 顶层分发

```rust
pub(crate) fn scan_deletes<F>(
    delete_files: &[PositionDeleteRef],
    factory: &OpendalRangeReaderFactory,
    file_io: &iceberg::io::FileIO,        // 新增参数：Puffin 路径需要
    data_file_size_lookup: F,
) -> Result<Vec<RecordBatch>, ChangeError>
where F: Fn(&str) -> Option<u64>
{
    if delete_files.is_empty() { return Ok(Vec::new()); }

    // 按 file_format 分桶
    let (parquet_dels, puffin_dels): (Vec<_>, Vec<_>) = delete_files
        .iter()
        .partition(|r| r.file_format == DataFileFormat::Parquet);

    let mut positions_per_file = read_delete_positions_per_data_file(&parquet_dels, factory)?;
    if !puffin_dels.is_empty() {
        let dv_positions = block_on_iceberg(read_dv_positions_per_data_file(&puffin_dels, file_io))??;
        for (path, treemap) in dv_positions {
            positions_per_file.entry(path).or_insert_with(RoaringTreemap::new).extend(treemap);
        }
    }

    // 后续 read_data_file_at_positions 完全不变（已经按 file_format 无关方式工作）
    ...
}
```

`read_data_file_at_positions` 不动——它读 raw parquet + running row_offset 做 boolean mask 过滤，对 v2/v3 来源的 position 都正确。

### 4.3 `materialize_changes` 调用点

`changes.rs::materialize_changes` 已经调用 `scan_deletes`，新增 `file_io` 参数从 `base_table.file_io()` 取即可。

---

## 5. 测试策略

### 5.1 单元测试（必加）

**`changes.rs::tests`**：
- `collect_deletes_parses_puffin_dv_entries`：构造 v3 delete manifest with Puffin DV entry → `PositionDeleteRef` 字段正确填充
- `collect_deletes_rejects_puffin_dv_missing_offset`：缺 `content_offset` → `InternalInconsistency`
- `format_v3_accepted`：v3 base 不再触发 `IcebergFormatUnsupported`
- `overwrite_error_message_explains_full_refresh`：错误信息包含 "DELETE+INSERT" / "DROP MATERIALIZED VIEW" 提示

**`scan_deletes::tests`**：
- `dv_path_reads_positions_from_puffin_file`：用 `commit::puffin_dv::write_single_deletion_vector_puffin` 写一个真实 Puffin 文件 → `read_dv_positions_per_data_file` 解出正确 positions
- `mixed_v2_and_v3_deletes_merge_into_same_position_set`：同一个 data file 被 v2 PD 和 v3 DV 各删几行 → merge 后位置正确

**`puffin_dv::tests`**：
- `to_roaring_treemap_round_trips`：DV insert 一组 positions → `to_roaring_treemap` → 包含全部 positions

### 5.2 集成测试（必加）

`src/engine/mod.rs` 新增：

```rust
#[test]
fn iceberg_v3_row_lineage_mv_incremental_refresh_picks_up_deletes() {
    // 1. 创建 v3 row-lineage iceberg base 表
    // 2. INSERT 4 行
    // 3. CREATE MATERIALIZED VIEW mv ON base SQL=...
    // 4. 第一次 REFRESH（full）→ MV 内容正确
    // 5. base 表 DELETE WHERE id = 2（写 Puffin DV）
    // 6. 第二次 REFRESH（增量）→ MV 行被对应删除
    // 7. base 表 INSERT 一行
    // 8. 第三次 REFRESH（增量）→ MV 多一行
    // 9. assert MV 内容 = base 表 INSERT/DELETE 后的逻辑结果
}
```

复用 Phase 2a 已有的 `open_row_lineage_iceberg_session_with_table` 辅助函数 + IVM Phase 2 的 `CREATE MATERIALIZED VIEW` 测试模式。

### 5.3 不加的测试

- 多 partition：roadmap §3.3，本 PR 范围外
- OVERWRITE 后 IVM 行为：本 PR 维持 ERROR，错误信息测试已在 5.1
- `_row_id` 跨 snapshot 跟踪：里程碑 C 范围

---

## 6. 风险与坑点

### 6.1 `read_data_file_at_positions` 的 running row_offset 是否对 v3 安全？

`read_data_file_at_positions` 读 raw parquet（不通过 iceberg-rust scan，不应用 row_selection），running row_offset = 真实 parquet row index。v3 row-lineage data file 也是 Parquet，写侧（`OverwriteCommit` / `FastAppendCommit`）跟 v2 共用 IcebergSink，物理布局一致。**安全**。

### 6.2 同 data file 多次 DELETE 的 DV 已被 `RowDeltaDvCommit` 合并

`RowDeltaDvCommit` 在 commit time 已经合并旧 DV → 一个 data file 在任意时刻最多有一个 live Puffin DV entry。但**多 snapshot 区间内**（snap_S1 写了 DV_a，snap_S2 写了 merge 后的 DV_b 并把 DV_a 标 Deleted），`plan_changes` 的 `CollectDeletes` 只会看到当前 snapshot 的 added entries——不会重复读到 DV_a。所以**重复读不是风险**。

### 6.3 `read_deletion_vector_puffin` 是 async，`scan_deletes` 现在是 sync

需要在 `scan_deletes` 内部用 `block_on_iceberg` 包一下；或者把 `scan_deletes` 改成 async。前者改动小，后者更纯。**选前者**——`scan_deletes` 顶层签名保持 sync，对调用方零侵入；只在 Puffin 分支内部 block。

### 6.4 DV cardinality=0 的 Puffin 文件

`RowDeltaDvCommit::commit` 在 vectors 全空时早返回（不写文件），所以**理论上不会出现**。但防御性：`read_dv_positions_per_data_file` 遇到空 DV → 跳过该条目（不污染 `positions_per_file`）。

---

## 7. 提交边界

单一 PR，commit 拆分建议：

1. `feat: extend PositionDeleteRef with v3 deletion-vector fields` — 数据模型 + invariant 校验 + DeletionVector::to_roaring_treemap + 单测
2. `feat: classify v3 deletion-vector entries in plan_changes` — `collect_files::CollectDeletes` Puffin 分发 + format-version 放宽 + OVERWRITE 错误信息 + 单测
3. `feat: read positions from puffin deletion vectors in scan_deletes` — `read_dv_positions_per_data_file` + `scan_deletes` 顶层分发 + `materialize_changes` 接 file_io + 单测
4. `test: cover v3 row-lineage mv incremental refresh end-to-end` — 集成测试

每个 commit 独立可编译可测试，便于 review / bisect。

---

## 8. 完成判定

- `cargo fmt --check` 通过
- `cargo build -p novarocks` 通过
- `cargo test connector::iceberg::changes` / `connector::iceberg::scan_deletes` / `connector::iceberg::commit::puffin_dv` 全过
- 新增集成测试通过
- 既有 IVM Phase 2 测试（v2 路径）回归全过
- vendor `iceberg-rust` 不再受 v3 接受触发新失败（vendor patch 4 已经接好 Puffin DV 读路径）

---

## 9. 变更记录

- 2026-04-29 — 初版。Brainstorming 三个问题逐条确认（option B 范围、option A OVERWRITE 处理、option A 数据模型扩展），用户跳过逐节确认直接进入 plan 阶段。
