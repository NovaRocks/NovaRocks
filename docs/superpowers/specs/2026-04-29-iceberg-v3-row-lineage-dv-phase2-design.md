# Iceberg v3 Row Lineage + Puffin DV Phase 2 Design

**日期**：2026-04-29
**状态**：Accepted for Phase 2a planning
**范围**：standalone 模式下，对 `write.row-lineage=true` 的 Iceberg v3 表支持 `INSERT INTO` / `INSERT OVERWRITE` / `DELETE`

---

## 0. 决策摘要与文档范围

### 0.1 目标

解锁 NovaRocks 对 `write.row-lineage=true` Iceberg v3 表的 `INSERT INTO` / `INSERT OVERWRITE` / `DELETE`，写出协议层符合 Iceberg v3 row-lineage 与 Puffin Deletion Vector 规范的表，使外部 Spark / Trino 可正确读取 row lineage 与删除结果。

### 0.2 关键决策

| 维度 | 选择 |
|---|---|
| 范围 | row lineage 写侧 + Puffin Deletion Vector 写侧；不含 SELECT 投影 / equality deletes / variant 列 |
| DELETE 支持 | **本期支持 v3 DELETE**：`write.row-lineage=true` 表的 DELETE 写 Puffin DV，不再写 v2 position-delete |
| 读侧能力 | NovaRocks SQL 不暴露 `_row_id` / `_last_updated_sequence_number`；正确性依赖 Spark / Trino 读取与 manifest / Puffin inspect 验证 |
| 路径关系 | 双路径并存：`row-lineage=false` 表保持 Phase 1 position-delete 行为；`row-lineage=true` / v3 row-lineage 表走新 v3 path |
| 分发位置 | 在 `IcebergCommitAction` trait 后分发：FastAppend / Overwrite 增强 row lineage，DELETE 使用 RowDelta-with-DV action |
| v2 pos-delete 混合 | 拒绝：row-lineage 表执行 DELETE 时，如果 current snapshot 有 v2 position-delete 文件，fail-fast，要求先 compaction |
| 既有 DV | 支持读取、合并、替换；同一个 referenced data file 在新 snapshot 中保持最多一个 live DV |
| 事务模型 | 单写者假设 + OCC requirement + 失败时主动清理 staged 文件；commit-unknown 仍沿用 Phase 1 保守处理 |
| 上游对齐 | 自实现 RowDeltaCommit-with-DV，扩展 FastAppendCommit / OverwriteCommit row-lineage 元数据；trait 边界保持，便于未来替换为上游 API |
| 拆分 | 本 spec 覆盖 Phase 2 整体设计；plan 阶段拆 2a / 2b 两个独立 plan / PR |

### 0.3 2a / 2b 边界

Phase 2a 是协议闭环，不是仅 INSERT：

- `INSERT INTO`：解除 row-lineage 表拒绝，依赖 iceberg-rust FastAppend 写 v3 row-range。
- `INSERT OVERWRITE`：自实现 OverwriteCommit 写 v3 row-range、manifest first-row-id、table next-row-id。
- `DELETE`：对 row-lineage 表写 Puffin DV；拒绝 base snapshot 中已有 v2 position-delete；读取并合并已有 DV；替换被触达 data file 的旧 DV。
- 最小验证：NovaRocks 写入后，manifest / metadata / Puffin inspect 可证明 row-lineage 与 DV 字段正确；Spark 或 Trino 可正确读取删除后的数据。

Phase 2b 做工程加固：

- 完整 manifest rewrite 工具化，降低 touched delete manifest 过滤逻辑的维护风险。
- 更系统的跨引擎验证与失败注入。
- S3 / local FS 行为统一、staged 文件清理与 commit-unknown 文档化。
- 性能优化：多个 data file 的 Puffin 文件布局、manifest 合并、批量 DV 编码。

### 0.4 非目标

- SELECT `_row_id` / `_last_updated_sequence_number` 元数据列投影。
- equality deletes 读 / 写。
- variant 类型列写入。
- IVM 改造为基于 row lineage；`plan_changes` 仍走 `(file, pos)` 模型。
- v2 到 v3 自动迁移 / compaction。
- DELETE 时跨多个 referenced data file 合并成单个 Puffin 文件的优化；2a 默认每个 data file 一个 Puffin 文件。
- FE-driven 模式。

---

## 1. 当前基础与缺口

Phase 1 已经落地：

- `IcebergCommitAction` trait 和 `run_iceberg_commit` 分发。
- `FastAppendCommit`：包一层 `Transaction::fast_append`。
- `OverwriteCommit`：自实现 snapshot / manifest / manifest-list。
- `RowDeltaCommit`：自实现 v2 position-delete manifest。
- `AbortLog`：staged data / delete / manifest 文件清理。
- `ensure_v3_writable`：目前仍拒绝 `write.row-lineage=true`。

Phase 2 需要补齐三类缺口：

1. `ensure_v3_writable` 从“统一拒绝 row-lineage”改为“按表能力分流”。
2. 自实现 commit action 必须和 iceberg-rust FastAppend 一样维护 v3 row-lineage 元数据。
3. DELETE 对 row-lineage 表不能再产出 position-delete Parquet 文件，必须写 Puffin DV 并在 delete manifest 中引用具体 blob。

---

## 2. 总体架构

```
SQL INSERT / INSERT OVERWRITE / DELETE
  │
  ▼
engine/iceberg_writer.rs | engine/delete_flow.rs
  ├─ load Table + current snapshot
  ├─ classify_write_mode(table)
  │    ├─ LegacyPositionDeletePath  (row-lineage=false)
  │    └─ V3RowLineagePath          (row-lineage=true / v3 row-lineage)
  ├─ validate schema / partition spec / unsupported deletes
  ├─ build IcebergCommitCollector
  └─ run writer or delete scanner
        │
        ├─ INSERT/OVERWRITE writes Parquet data files
        └─ DELETE emits grouped (data_file_path, pos)
  │
  ▼
IcebergCommitAction
  ├─ FastAppendCommit          row lineage via iceberg-rust Transaction
  ├─ OverwriteCommit           custom v3 row-range + manifest-list first_row_id
  ├─ RowDeltaCommit            legacy position-delete path
  └─ RowDeltaDvCommit          Puffin DV path for row-lineage=true
  │
  ▼
Catalog::update_table(TableCommit)
```

核心边界：

- engine 层只决定写入模式，不直接拼 Iceberg metadata。
- Puffin 文件、DV manifest entries、old DV 替换都收在 `RowDeltaDvCommit` 内。
- Phase 1 `RowDeltaCommit` 不删除，继续服务 `row-lineage=false` 表。

---

## 3. Row Lineage 写入语义

### 3.1 INSERT INTO

`FastAppendCommit` 继续使用 iceberg-rust `Transaction::fast_append`。iceberg-rust 0.9 已在 v3 path 中：

- 用 table metadata 的 `next-row-id` 作为 snapshot `first-row-id`。
- 在 manifest-list 写入 data manifest 的 `first_row_id`。
- 用新增 data files 的 `record_count` 推进 table metadata `next-row-id`。

Phase 2a 需要做的是解除 NovaRocks 当前 validator 对 row-lineage 表的拒绝，并增加验证用例证明 FastAppend 输出的 snapshot / manifest-list / table metadata 字段符合预期。

### 3.2 INSERT OVERWRITE

`OverwriteCommit` 是自实现 action，必须补齐和 FastAppend 一致的 row-lineage 元数据：

1. 读取 base table metadata 的 `next-row-id`，记为 `base_next_row_id`。
2. 新增 data manifest 记录本次写入的数据文件；每个文件仍不物理写 hidden columns。
3. v3 manifest-list writer 以 `Some(base_next_row_id)` 初始化，使新增 data manifest 获得 `first_row_id`。
4. 新 snapshot 写入 `first-row-id = base_next_row_id`，`added-rows = sum(new_data_files.record_count)`。
5. table metadata 通过 `TableUpdate::AddSnapshot` 推进 `next-row-id`。

被 overwrite 删除的旧 data files 不重新分配 row id；它们只以 DELETED entries 进入 overwrite delete manifest。清空表的 `INSERT OVERWRITE ... SELECT empty` 仍创建 overwrite snapshot，`added-rows=0`，`next-row-id` 不变。

### 3.3 DELETE

DELETE 不新增 data rows，但 v3 snapshot 仍必须带 row-range：

- `first-row-id = table.metadata.next-row-id`
- `added-rows = 0`
- table metadata `next-row-id` 不变

DELETE 的 row lineage 正确性由 base data files 的 row id 继承与 DV 删除位置共同决定；NovaRocks 不需要写 `_row_id` 列。

---

## 4. Puffin Deletion Vector 写入语义

### 4.1 DELETE 输入

Phase 1 DELETE 已经可以通过 iceberg-rust scan 得到 `_file` / `_pos`。Phase 2a 复用这条扫描路径，但输出不再进入 position-delete Parquet writer，而是形成：

```text
HashMap<referenced_data_file, DeletionVector>
```

规则：

- `row_position` 必须非负。
- `DeletionVector` 支持正 64-bit position：高 32 bit 作为 key，低 32 bit 存入对应的 32-bit Roaring Bitmap。
- 同一个 data file 内的位置去重、按 unsigned key 升序编码。
- 一个 touched data file 默认写一个新的 Puffin 文件，便于 abort 清理和 manifest inspect。

### 4.2 Puffin blob

每个 touched data file 生成一个 `deletion-vector-v1` blob：

- blob type：`deletion-vector-v1`
- fields：空数组
- snapshot_id：`-1`（Puffin v1 写入时 snapshot 尚未可知）
- sequence_number：`-1`（Puffin v1 写入时 sequence 尚未可知）
- data：Iceberg DV payload：big-endian length、magic `D1 D3 39 64`、分段 Roaring portable vector、big-endian CRC-32
- properties：必须包含 `referenced-data-file` 与 `cardinality`
- compression：必须省略，`deletion-vector-v1` 不压缩

写出的 Puffin 文件路径位于 query staging 目录。commit 成功后由 manifest 引用；commit 失败或 definite-fail 时由 `AbortLog` 清理。

### 4.3 Delete manifest entry

每个新 DV blob 对应一个 delete manifest entry：

- `content = PositionDeletes`
- `file_format = Puffin`
- `file_path = <puffin-file-path>`
- `referenced_data_file = <data-file-path>`
- `content_offset = <blob offset in puffin>`
- `content_size_in_bytes = <blob compressed length>`
- `record_count = bitmap.cardinality()`
- `partition` / `partition_spec_id` 从 referenced data file 的 manifest entry 继承

如果一个 Puffin 文件只存一个 blob，`content_offset` 通常是 Puffin magic header 后的第一个 blob offset；实现不能写死该值，必须从 Puffin metadata 或 writer 返回值取真实 offset / length。

### 4.4 既有 DV 合并与替换

row-lineage 表执行 DELETE 时，先遍历 current snapshot 的 delete manifests：

- 发现任何 v2 position-delete Parquet entry：fail-fast。
- 发现 equality-delete entry：fail-fast。
- 发现 referenced data file 属于本次 touched set 的 DV：读取 Puffin blob，解码 bitmap，与本次 bitmap 做 OR 合并。
- 发现 referenced data file 不属于 touched set 的 DV：保持 live。

替换语义：

1. 对包含 old DV 的 delete manifest，不能简单继承整个 manifest，否则旧 DV 会继续 live。
2. 对 touched manifest 执行 rewrite：把未触达的 live delete entries 作为 EXISTING 写入新 delete manifest；old DV 不再作为 live entry 继承。
3. 对每个 touched data file 写入合并后的 new DV，作为 ADDED entry。
4. 未触达的 delete manifests 可以直接继承。

这保证新 snapshot 中同一个 referenced data file 最多只有一个 live DV。

---

## 5. 校验与 Fail-Fast

### 5.1 写入模式分类

新增 `IcebergWriteMode`：

```rust
enum IcebergWriteMode {
    LegacyPositionDeletes,
    RowLineageV3,
}
```

分类建议：

- `format_version < V3`：`LegacyPositionDeletes`
- `format_version == V3` 且 `write.row-lineage=true`：`RowLineageV3`
- `format_version == V3` 但 property 缺失：按 Iceberg v3 row-lineage 要求保守进入 `RowLineageV3`，同时在 spec / tests 中固定行为

### 5.2 RowLineageV3 统一拒绝项

- variant 类型列。
- equality deletes。
- current snapshot 中有 v2 position-delete 文件。
- DELETE without WHERE 仍保持 Phase 1 行为：拒绝，建议用户用 overwrite 清表。
- 无法定位 referenced data file manifest entry：拒绝，避免写出 partition metadata 不完整的 DV entry。

### 5.3 Legacy 路径保持不变

`row-lineage=false` 表继续走 Phase 1：

- INSERT INTO：FastAppend。
- INSERT OVERWRITE：OverwriteCommit。
- DELETE：position-delete Parquet + RowDeltaCommit。

本 spec 不改变 legacy 表行为；测试必须覆盖 legacy negative / positive case，防止 Phase 2 改动误伤 Phase 1。

---

## 6. Commit Action 设计

### 6.1 FastAppendCommit

改动很小：

- 允许 row-lineage 表进入。
- 增加 row-lineage metadata 验证测试。
- 继续由 iceberg-rust 生成 v3 manifest-list 和 snapshot row-range。

### 6.2 OverwriteCommit

需要扩展：

- `write_manifest_list` helper 支持传入 `first_row_id: Option<u64>`，并返回 writer 最终 `next_row_id` 或 assigned row count。
- snapshot builder 在 v3 path 写 `with_row_range(base_next_row_id, added_rows)`.
- DELETED data manifest 不消耗新 row id；ADDED data manifest 消耗 `record_count`。

### 6.3 RowDeltaDvCommit

新增 action，职责：

1. 接收 DELETE scan 产出的 grouped positions。
2. 读取 base manifest list，构建 data file metadata index 与 delete file index。
3. 验证没有 v2 position-delete / equality-delete。
4. 对 touched data files 合并已有 DV。
5. 写 Puffin DV files，记录到 AbortLog。
6. 写 delete manifest：rewritten existing DV entries + newly added DV entries。
7. 写 manifest-list：继承未触达 manifests，替换 touched delete manifests，追加 new delete manifest。
8. 写 snapshot：`operation=delete`，v3 row-range 为 `(next-row-id, 0)`。
9. 提交 `AddSnapshot + SetSnapshotRef`，requirements 保持 `CurrentSchemaIdMatch` / `DefaultSpecIdMatch` / `RefSnapshotIdMatch`。

---

## 7. 数据结构与文件范围

| 文件 | 改动 |
|---|---|
| `src/connector/iceberg/commit/validation.rs` | 拆分 row-lineage validator；新增 write mode 分类 |
| `src/connector/iceberg/commit/types.rs` | `WrittenFile` 扩展 Puffin DV 需要的 offset / size，或新增 `WrittenDvFile` |
| `src/connector/iceberg/commit/run.rs` | `CommitOpKind` 增加 `RowDeltaDv` 分发 |
| `src/connector/iceberg/commit/overwrite.rs` | 写 v3 snapshot row-range 与 manifest-list first-row-id |
| `src/connector/iceberg/commit/row_delta_dv.rs` | 新增 Puffin DV RowDelta action |
| `src/connector/iceberg/commit/puffin_dv.rs` | Puffin DV encode/decode、existing DV merge、metadata inspect helper |
| `src/engine/delete_flow.rs` | row-lineage 表 DELETE 分发到 DV path |
| `src/engine/iceberg_writer.rs` | INSERT/OVERWRITE 解除 row-lineage 拒绝，走 row-lineage validator |
| `vendor/iceberg-0.9.0/PATCH.md` | 如需暴露 Puffin writer metadata 或 DataFileBuilder setter，记录最小 patch |

优先尝试不新增 vendor patch：PuffinWriter close 后用 PuffinReader 读回 metadata，或在 NovaRocks 自实现最小 Puffin writer 以直接掌握 offset / length。只有公共 API 无法构造 `Blob` / `DataFile` 所需字段时，再扩大 vendor patch。

---

## 8. 测试与验收

### 8.1 Unit tests

- write mode 分类：v2 / v3 property missing / v3 row-lineage true。
- OverwriteCommit v3 row-range：`first-row-id`、`added-rows`、`next-row-id`。
- Puffin DV encode/decode：bitmap round-trip、offset / length 对齐 Puffin footer。
- existing DV merge：old bitmap OR new bitmap。
- touched delete manifest rewrite：old DV 不再 live，untouched DV 保持 live。
- fail-fast：existing position-delete、equality-delete、variant column。

### 8.2 Integration tests

| Case | 验证点 |
|---|---|
| INSERT INTO row-lineage table | snapshot `first-row-id` 正确，table `next-row-id` 推进 |
| INSERT OVERWRITE row-lineage table | old data deleted，新 data row ids 从旧 `next-row-id` 开始 |
| DELETE first time | 写出 Puffin DV，delete manifest entry 字段完整 |
| DELETE same data file second time | 读取旧 DV 并合并，新 snapshot 只有一个 live DV |
| DELETE with existing position-delete | fail-fast，错误提示要求 compaction |
| Legacy row-lineage=false DELETE | 仍写 position-delete Parquet |

### 8.3 Cross-engine validation

至少保留一条手工或自动化验证路径：

1. NovaRocks 创建 / 写入 row-lineage v3 表。
2. NovaRocks DELETE 写 Puffin DV。
3. Spark 或 Trino 读取表，返回删除后的结果。
4. manifest inspect 验证：
   - table metadata `next-row-id`
   - snapshot `first-row-id` / `added-rows`
   - data manifest `first_row_id`
   - delete manifest `file_format=Puffin`
   - DV entry `referenced_data_file/content_offset/content_size_in_bytes`

---

## 9. 风险与 Open Items

1. **Puffin writer metadata 可见性**：iceberg-rust 0.9 PuffinWriter 不直接返回 blob metadata。优先 close 后读回 footer；如果构造 Blob 或 DataFile setter 受限，再做最小 vendor patch。
2. **DELETE manifest rewrite 复杂度**：必须避免继承包含 old DV 的 manifest。2a 先用直接 rewrite touched delete manifests，2b 再优化。
3. **v3 property 判定**：Iceberg v3 spec 要求 row lineage；如果实际创建出的 v3 表缺 `write.row-lineage=true`，NovaRocks 仍按 row-lineage path 写，避免写出 v3 但缺 row-range 的 snapshot。
4. **跨引擎差异**：Spark / Trino 对 Puffin DV 的兼容性需要用真实 engine 验证，不只依赖 metadata inspect。
5. **commit-unknown 清理**：Puffin 文件和 rewritten manifests 必须全部进 AbortLog；commit-unknown 时仍保留 staged 文件，避免误删已提交文件。

---

## 10. 参考

- Apache Iceberg spec: Row Lineage, Delete Formats, Deletion Vectors。
- Apache Iceberg Puffin spec: `deletion-vector-v1` blob type。
- Phase 1 spec: `docs/superpowers/specs/2026-04-27-iceberg-v3-insert-delete-phase1-design.md`。
- Current code: `src/connector/iceberg/commit/*`, `src/engine/iceberg_writer.rs`, `src/engine/delete_flow.rs`。
