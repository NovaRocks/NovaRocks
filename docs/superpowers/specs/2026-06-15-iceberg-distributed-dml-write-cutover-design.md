# Iceberg 分布式 DML 写入统一 + 移除「进程内本地写后注入」模式 — 设计稿

- 日期：2026-06-15（2026-06-15 修订：补 MERGE 原子性目标、调整 phase 顺序、MERGE 尊重 `write.update.mode`）
- 状态：Phase 1 已实现并合入（#323）；其余 phase 待实现计划（writing-plans）
- 范围：standalone SQL engine 的 Iceberg 写路径（DELETE / UPDATE / MERGE / ADD EQUALITY DELETE）
- 关联：[Iceberg Distributed Write Pipeline Roadmap]（NovaRocks Roadmap 索引）；上游已完成 IW-1~IW-8（INSERT/OVERWRITE 已分布式）

---

## 1. 背景

NovaRocks 的 Iceberg 写当前有两套执行模型：

- **分布式 async-sink（目标模型）**：行在 BE 上产出，经 `ICEBERG_*_SINK` fragment 在各 BE 写 staged 文件，coordinator 收齐 writer 结果后由 **FE 做唯一一次 metadata commit**。今天 `INSERT INTO`、`INSERT OVERWRITE`、v2 position-delete `DELETE`、v3 DV `DELETE`（Phase 1，#323）、merge-on-read(MOR) `UPDATE` 的数据侧、`MERGE` 的 unmatched-INSERT 走此路径。
- **本地「写后注入」（待移除）**：coordinator 进程内自己 scan/compute 并写数据/删除文件，再把文件清单 `local_writer_commit_input(...)` 注入共享的 commit runner。今天仍走此路径：**copy-on-write(COW) UPDATE**、**MOR UPDATE 的 DV 侧**（FE-inject 中心化写 DV）、**MERGE matched-DELETE / matched-UPDATE**、**ADD EQUALITY DELETE**。

两套模型共享后半段：`IcebergWriteTransactionRunner` → `IcebergWriteCommitExecutor::commit_write_input` → `IcebergCommitCollector` → `run_iceberg_commit_typed`。差异**只在前半段** `IcebergWriteTransactionExecutor::run_coordinated_write`（`src/engine/write_transaction.rs:88-108`）。

本设计把剩余路径全部切到分布式模型、**彻底删除本地写后注入相关代码**，并**修复 MERGE 的原子性缺陷**（见 §2）。

### 1.1 参考调研结论（StarRocks）

- StarRocks **不写 v3 Puffin deletion vector**：它写**可叠加的 MOR position-delete Parquet 文件**，按**分区列** shuffle，FE 仅 `RowDelta` 提交 metadata；默认建 v2 表，**无 equality-delete 写**，**UPDATE 仅 MOR（无 COW）**，**MERGE 一律 MOR**。
- 结论：NovaRocks 的 v3 DV 写、equality-delete 写、COW update、以及「MERGE 尊重 `write.update.mode`」**均超出 StarRocks，无参考可抄**。v3 表必须写 DV，故 Phase 1 用 **per-`_file` shuffle**（每个 data file 由唯一 BE 独占、本地读旧 DV+合并+写 Puffin）解决了分布式 DV 写——这是后续 MERGE/UPDATE 复用的基石。

### 1.2 Phase 1 已落地实现要点（as-merged #323，后续复用）

- 新 `IcebergWriteSinkMode::DeletionVectors` → `TDataSinkType::ICEBERG_DV_SINK`；BE sink `push_chunk_deletion_vector` + `finish_deletion_vectors`。
- BE 读旧 DV 是**自己 walk snapshot**（`read_existing_dv_positions` → `scan_deletes::previously_deleted_positions_at_snapshot`，按 owned-file 过滤），**不**经 thrift 下推描述符。
- per-`_file` shuffle：`execute_query_as_iceberg_write(..., Some(iceberg_write_shuffle_by_output_index(0)))` → optimizer `optimize_with_root_distribution(DistributionSpec::shuffle_agg([_file 的 ColumnId]))`。
- thrift `TIcebergDataFile` 已加 `content_offset` / `content_size_in_bytes` / `cardinality`（字段 16/17/18）；`WrittenFile` 同步；`collector.convert` 按 `format` 还原。
- metadata-only commit：`CommitOpKind::RowDeltaDvFromFiles` → `RowDeltaDvFromFilesCommit`（登记 BE 写好的 Puffin DV，不再中心化构建/合并/写）。
- executor 模板：`DistributedDvDeleteWriteExecutor` + `run_delete_dv_write_transaction`。

---

## 2. 目标与非目标

### 目标

1. `DELETE`（v3 DV，已完成）、`UPDATE`（COW + MOR）、`MERGE`、`ADD EQUALITY DELETE` 全部经分布式 async-sink 在 **BE** 写文件。
2. 删除本地写后注入的全部代码（见 §6 删除面）。
3. **逻辑结果等价**（行集相同）；仅改变**执行位置**（进程内 → 分布式）。标准 standalone UPDATE/DELETE 的**落盘形态**也保持等价（按 `write.update.mode`/delete 策略）。
4. **MERGE 原子性**：一条 `MERGE` 语句 = **一次 Iceberg 提交、一个 snapshot**。修复当前「一条 MERGE 产生最多 2 个独立 snapshot（not-matched-insert 与 matched 分支各自 commit）」的**既有缺陷**——它违反 MERGE 原子语义（非原子可见性、崩溃留半成品、冲突检测错位）。
5. 在 1FE+NBE 与 all-in-one 下均正确；`all-in-one` 行为/性能不回退。

### 非目标

- 不重写 INSERT/OVERWRITE/v2-delete（已分布式，仅作模板复用）。
- 不改 CTAS、compaction（它们仍用 `run_select_to_chunks*` 本地写，**不在本范围**，见 §7）。
- 不在本次迁移 IMV refresh / IV3-4 / IV3-5 到 BE DV writer（基建已由 Phase 1 建出，迁移留作后续，见 §11）。
- 不改变 `write.update.mode` 对 **standalone UPDATE/DELETE** 的语义（COW 表仍 COW、MOR 表仍 MOR）。**MERGE 也尊重 `write.update.mode`**（见 §5.3）——不强制 MERGE 一律 MOR。

---

## 3. 核心原则（硬不变量）

> **FE 只提交 metadata；所有数据文件、position-delete 文件、deletion vector、equality-delete 文件都由 BE 写。**

这条不变量是本设计一切取舍的根。在 1FE+NBE 下 coordinator = **FE**，让 FE 写数据/删除文件属于数据面 I/O，跨过了 INSERT/OVERWRITE 坚持的边界。FE 在 commit 时写 manifest/metadata.json（元数据）是固有且允许的；写 data/DV/delete/equality 文件不允许。

---

## 4. 统一架构

所有 DML 收敛为同一形状：

```text
FE: 解析/规划 → 构造写 query + 选择 sink mode → 按需施加分布式 shuffle
  → 派发 fragment 到 BE（RemoteDispatcher；all-in-one 下 InProcessDispatcher）
BE: 执行 fragment（scan/compute/produce rows）→ 对应 *_SINK 写 staged 文件 → 上报 writer 结果
FE: ExecutionCoordinator 收齐 writer 结果 → 汇入一个 IcebergCommitCollector
  → 唯一一次 metadata commit（run_iceberg_commit_typed）→ finalize（失效缓存等）
```

**关键机制（已由 Phase 1 调研/实现验证）——写与提交可分离**：分布式写产出的 `TSinkCommitInfo` 经 `collector.inject_written_files` / `inject_delete_group` **纯累加**进 collector，提交只发生在 `run_iceberg_commit_typed`。因此**多个分支的分布式写可以喂入同一个 collector、最后只提交一次**——这是 MERGE 原子提交的基础（§5.3）。

---

## 5. 分项设计

> Phase 顺序见 §10。依赖：§5.3 原子 MERGE 依赖 §5.1（DV 写，已完成）与 §5.2（分布式 UPDATE）。

### 5.1 v3 DV-delete（Phase 1，已完成 #323）

per-`_file` shuffle + BE 写合并后的 Puffin DV + `RowDeltaDvFromFiles` metadata-only commit。要点见 §1.2。后续 MERGE/UPDATE 的删除侧直接复用这套 DV sink。

### 5.2 分布式 UPDATE（Phase 2；standalone UPDATE，且是 MERGE 的依赖）

standalone `UPDATE` 按 `write.update.mode` 分两条；本 phase 把两条都做成**分布式 + BE 写 + 一次提交**，并产出 MERGE 复用的两块基建。

- **COW（`write.update.mode=copy-on-write`）— 分布式整文件重写**：
  1. 阶段 A（metadata）：分布式 query 算出命中行的 distinct `_file` = 被替换文件集合，收集回 FE。
  2. 阶段 B（BE 数据面）：对被命中文件**整体扫描**（scope 到 A 的文件集合），命中行套更新、未命中行透传，写新文件（`RowLineageData` sink + row-lineage 保号）。
  3. FE commit：**Overwrite** 提交（replaced = 被命中文件，added = 新文件），`CowUpdateRewriteSet` 由 A 的文件集合 + B 的产出在 coordinator 侧组装为**元数据**。
  - 删除现有进程内 `MutationWriteExecutor::run_cow_update_write` + 全部 COW 助手。这是最难、无 StarRocks 参考的一块。
- **MOR（`write.update.mode=merge-on-read`）— DV 侧移到 BE 写**：数据侧已分布式（`RowLineageData` sink）；当前删除侧仍 **FE-inject 中心化写 DV**（`build_position_delete_groups_from_matched` + `collector.inject_delete_group` → `RowDeltaDvCommit` 在 FE 构建/合并/写 Puffin），**违反硬不变量**。本 phase 把 MOR-update 的旧行删除位置改为走 §5.1 的 `DeletionVectors` sink（BE 写 DV）+ `RowDeltaDvFromFiles`。

**`RowDeltaDvFromFiles` 扩展（本 phase 必做）**：使其除 Puffin DV 文件外，也接受 `content==Data` 的 BE 写数据文件，在同一 snapshot 里 `write_added_data_manifest`（机械移植 `row_delta_dv.rs:290-320`）。这样「新数据 + DV」可在**一次** RowDelta 提交内落地——MOR-update（数据 + DV）与原子 MERGE 都依赖它。

### 5.3 原子分布式 MERGE（Phase 3）

**一条 MERGE = 一次提交、一个 snapshot。** `matched` 子句是 UPDATE **xor** DELETE，外加可选 not-matched INSERT。**MERGE 尊重 `write.update.mode`**（与 standalone UPDATE 一致）。

**装配方法（基于 §4 的写/提交分离）**：建**一个**共享 `IcebergCommitCollector`；逐分支跑分布式写、把各自 `TSinkCommitInfo` **inject 进同一 collector 而不提交**；最后只调用一次 `run_iceberg_commit_typed`。需要一个新的「多分支 MERGE executor」：其 `run_coordinated_write` 跑齐所有分支并把结果汇入共享 collector，其 `commit` 只做一次提交（runner 当前是 1 写:1 提交，需要这层多分支封装；collector 与各 commit action 不改）。

**两种提交形态（按 `matched` 动作 × update mode）**：

| MERGE 组合 | 提交形态 | 复用 |
|---|---|---|
| matched-DELETE + INSERT | **RowDelta**：BE 写 DV（删除位置）+ BE 写 data（插入行） | §5.1 DV sink + §5.2 扩展后的 `RowDeltaDvFromFiles`（带 data） |
| matched-UPDATE + INSERT（**MOR 表**） | **RowDelta**：BE 写 DV（旧行位置）+ BE 写 data（更新后新行 + 插入行） | §5.2 MOR-update + DV sink |
| matched-UPDATE + INSERT（**COW 表**） | **Overwrite**：BE 重写命中文件 + 追加（更新后文件 + 插入文件） | §5.2 COW 分布式重写 |

- DELETE 侧（matched-DELETE 位置、MOR matched-UPDATE 旧行位置）：分布式 query（target ⋈ source 的命中行投影 `_file,_pos,<partition>`，按 `_file` shuffle）→ DV sink（BE 写）。多 source 命中同一 target 行产生的重复 `(_file,_pos)` 由 BE 的 `DeletionVector` 去重；MERGE cardinality 仍由 orchestrator 既有 `validate_unique_target_row_ids` 在 coordinator 侧校验。
- DATA 侧（INSERT 行、matched-UPDATE 新行）：分布式 query → data sink（BE 写）。
- 全部 inject 进**一个** collector → 一次提交。COW 组合走 Overwrite action（命中文件重写 + 追加）。

**单分支 MERGE（只有 matched 或只有 not-matched）今天已是一次提交、本就原子**，本 phase 主要修「matched + not-matched 同时出现」时的 2-commit 缺陷。

### 5.4 equality-delete（Phase 4，新分布式 sink）

`execute_add_equality_delete_statement` 当前从字面量行进程内写 equality-delete 文件（仅 unpartitioned）。改造：字面量行 → 内存 VALUES 源 query → 新 `EqualityDeletes` sink mode → BE 写 → FE commit。逻辑复用现有 `build_equality_delete_batch` + `write_equality_delete_file`，迁入 sink。无并行收益，但统一进分布式模型以满足硬不变量、删本地写。

### 5.5 已是模板（不改，仅复用）

- INSERT/OVERWRITE：`DistributedInsertWriteExecutor` + `ICEBERG_TABLE_SINK`。
- v2 position-delete DELETE：`DistributedDeleteWriteExecutor` + `ICEBERG_DELETE_SINK`。
- v3 DV-delete（Phase 1）：`DistributedDvDeleteWriteExecutor` + `DeletionVectors` sink + `RowDeltaDvFromFiles`。
- MERGE match 物化：`materialize_merge_match` 已是分布式 query（产 `_file,_pos,_row_id` + op-code 投影），原子 MERGE 复用其 join 投影构造各分支 query。

---

## 6. 删除面（移除本地写后注入；按 phase 推进）

- Phase 1（已完成）：v3-DV 本地 scan/inject（`scan_for_position_deletes_at`、`InjectedDeleteGroupExecutor` 等）已删。
- Phase 2：`MutationWriteExecutor::run_cow_update_write` + 全部 COW 助手（`write_cow_update_files`/`build_cow_rewrite_batches`/`build_cow_rewrite_set`/`load_data_file_lineage` 等）；MOR-update 的 FE-inject DV 路径。
- Phase 3：MERGE matched 分支的 `MutationWritePlan::MergeMatchedDelete` 本地 arm 等。
- Phase 4：`equality_delete_flow.rs` 本地写路径。
- 收口（Phase 5）：`local_writer_commit_input` / `new_local_writer_write_id`（`write_transaction.rs:205-242`）、`has_preloaded_commit_output` trait 方法（`:105-107`）及 runner 门控分支（`:302-306`）——所有 override 者删完后移除。

---

## 7. 明确保留 / 范围外

- **保留**（与 MV/IMV refresh + compaction 共用，禁删）：`IcebergCommitCollector::inject_delete_group` / `inject_written_file`（注入 API 本身保留；只让 DML executor 停止以**本地**方式调用）；`run_select_to_chunks*`、`data_file_to_written_file`、`data_writer` 本地写函数（CTAS/compaction 用）。
- **范围外**：CTAS、compaction 仍本地写；IMV refresh 仍中心化 DV apply（其迁移见 §11）。
- 注：MOR-update 的 DV 侧此前列为「已分布式」，实为 **FE-inject 中心化写 DV、违反不变量**——本设计已将其纳入 §5.2 Phase 2 修复，不再属范围外。

---

## 8. 正确性与错误处理

- **MERGE 原子性**：一条 MERGE 一个 snapshot；整体对**同一** base snapshot 做 OCC（`RefSnapshotIdMatch`/`SchemaIdMatch`/`SpecIdMatch`）；崩溃不留半成品。`validate_unique_target_row_ids` 仍在 coordinator 侧保证 at-most-one-match。
- **snapshot pin**：base snapshot 规划期冻结；BE 读旧 DV / 重写文件均基于此；commit 期冲突检测。
- **写时读快照一致性**：BE 写 DV/重写文件时读旧状态，必须读 pinned snapshot 视图，不受并发 commit 影响。
- **统一状态机**：timeout / cancel / writer failure / commit unknown 沿用 `IcebergWriteTransactionRunner` 现有终态，不新增。
- **abort/cleanup**：BE 写出的 staged 文件失败时按 `build_abort_cleanup_for_catalog_entry` 清理。
- **per-file 独占失效保护**：commit 期检测同一 data file 多个 DV 输入并 fail-fast。

---

## 9. 测试与验收（每 phase 的合并门）

1. **逻辑等价**：切换前/后同一输入产出等价行集；standalone UPDATE/DELETE 落盘形态亦等价。覆盖 `sql-tests/iceberg-dml/`。
2. **多 BE 正确**：`--cluster-mode cross-process --cluster-size 2` 与 all-in-one 一致。
3. **MERGE 原子性**：一条带 matched + not-matched 的 MERGE 只产生**一个**新 snapshot（断言 commit 后 snapshot 计数 +1）；并新增首个 `WHEN MATCHED THEN DELETE` 用例。
4. **守卫测试**：源自省/plan-shape 断言该 op 不再走本地路径（对照 `overwrite_path_uses_distributed_writer_not_local_collect`）。
5. **FE 零数据 I/O**：审查/断言 FE 不写 data/DV/delete/equality 文件。
6. **跨引擎 compat**：Spark / FE 可读（`iceberg-compatibility`、`iceberg-rest`）。

---

## 10. 实施阶段（rollout）

按 op 分阶段、每阶段过 §9 验证门、**按阶段独立合并**（Phase 1 已如此合入 #323）。顺序（已据「MERGE 尊重 `write.update.mode`」调整——分布式 COW 是 MERGE 的依赖，故 UPDATE 在 MERGE 之前）：

1. **DV-delete**（DV sink + per-`_file` shuffle + `RowDeltaDvFromFiles`）。✅ 已完成（#323）。
2. **分布式 UPDATE**：COW 整文件重写分布式化（删本地 COW 写）+ MOR-update 的 DV 侧移到 BE 写 + 扩展 `RowDeltaDvFromFiles` 接受 data 文件。是 standalone UPDATE 与 MERGE 的共同依赖。
3. **原子分布式 MERGE**：复用 Phase 1 DV + Phase 2 COW/MOR + data sink；多分支汇入一个 collector、一次提交；尊重 `write.update.mode`（两种提交形态）；含 atomicity 与 matched-delete 回归用例。
4. **equality-delete**：新 `EqualityDeletes` sink + VALUES 源。
5. **删除面收口**：移除 §6 收口项 + `has_preloaded_commit_output` + 门控；补守卫测试。

---

## 11. 后续复用

Phase 1 的「按 data-file 分片的 BE 端 DV writer」+ Phase 2 扩展后的 `RowDeltaDvFromFiles`（带 data）是让以下也满足硬不变量的同一块基建（后续单独迁移）：

- IMV refresh 删除侧 apply（当前中心化写 DV）。
- IV3-4：v2 position-delete → DV 迁移。
- IV3-5：DV compaction（天然 per-file）。

---

## 12. 风险与待解问题

1. **`RowDeltaDvFromFiles` 带 data 扩展**：需把 written 按 `content` 拆成 DV 子集 + data 子集，data 走 `write_added_data_manifest` 并正确计 `added_data_records` / row-lineage 保号（移植自 `RowDeltaDvCommit`）。
2. **多分支 MERGE executor**：runner/`commit_write_input` 当前 1 写:1 提交；需一层多分支封装把「各分支 inject」与「一次提交」拆开。collector、commit action、`run_iceberg_commit_typed` 不改。
3. **COW 阶段 A→B 文件集合 scope**：阶段 B 扫描需精确 scope 到命中文件，不退化为全表扫。
4. **MERGE-update COW 表 = Overwrite，matched-delete = RowDelta**：同一 MERGE 入口需按 `(matched 动作, update mode)` 选对 commit action；二者不在同一条 MERGE 内混用（matched 互斥），但入口分派要清晰。
5. **写时读旧状态**：BE 读 pinned snapshot 的 DV / 命中文件，需确认 sink operator 上下文可构造 read-view（Phase 1 `read_existing_dv_positions` 已验证可行，COW 重写读侧同理）。
6. **all-in-one 新依赖**：分布式路径依赖 `ensure_standalone_exchange_server`，确认 all-in-one 启动已就绪（Phase 1 已验证）。

---

## 13. PR 自检清单

每个阶段 PR 开始前：

1. 本 PR 改的是 sink mode / shuffle / BE writer / commit-from-files / COW 重写 / MERGE 装配 / 删除面 哪一层？
2. 是否有任何 BE 直接提交 Iceberg metadata？是否有 **FE 写 data/DV/delete/equality 文件**（违反硬不变量）？
3. （MERGE）一条带 matched + not-matched 的语句是否只产生**一个** snapshot？
4. writer 输出是否足够让 FE 唯一 commit（files、referenced_data_file、content_offset/size、stats、snapshot guard）？
5. 逻辑等价 + 1FE+2BE + 守卫测试 是否齐？
6. 是否仅让本 op 停用本地写，而未误删共享 inject API / CTAS-compaction 用的本地写函数？
7. timeout/cancel/writer failure/commit unknown 是否仍由统一状态机覆盖？
