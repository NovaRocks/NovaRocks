# Iceberg 分布式 DML 写入统一 + 移除「进程内本地写后注入」模式 — 设计稿

- 日期：2026-06-15
- 状态：设计已定稿，待写实现计划（writing-plans）
- 范围：standalone SQL engine 的 Iceberg 写路径（DELETE / UPDATE / MERGE / ADD EQUALITY DELETE）
- 关联：[Iceberg Distributed Write Pipeline Roadmap]（NovaRocks Roadmap 索引）；上游已完成 IW-1~IW-8（INSERT/OVERWRITE 已分布式）

---

## 1. 背景

NovaRocks 的 Iceberg 写当前有两套执行模型：

- **分布式 async-sink（目标模型）**：行在 BE 上产出，经 `ICEBERG_*_SINK` fragment 在各 BE 写 staged 文件，coordinator 收齐 writer 结果后由 **FE 做唯一一次 metadata commit**。今天 `INSERT INTO`、`INSERT OVERWRITE`、v2 position-delete `DELETE`、merge-on-read(MOR) `UPDATE`、`MERGE` 的 unmatched-INSERT 走此路径。
- **本地「写后注入」（待移除）**：coordinator 进程内自己 scan/compute 并写数据/删除文件，再把文件清单 `local_writer_commit_input(...)` 注入共享的 commit runner。今天仍走此路径的有 4 条：**v3 deletion-vector DELETE**、**copy-on-write(COW) UPDATE（默认 UPDATE 模式）**、**MERGE matched-DELETE**、**ADD EQUALITY DELETE**。

两套模型共享后半段：`IcebergWriteTransactionRunner` → `IcebergWriteCommitExecutor::commit_write_input` → `IcebergCommitCollector` → `run_iceberg_commit_typed`。差异**只在前半段** `IcebergWriteTransactionExecutor::run_coordinated_write`（`src/engine/write_transaction.rs:88-108`）。

本设计把这 4 条全部切到分布式模型，并**彻底删除本地写后注入相关代码**。

### 1.1 参考调研结论（StarRocks）

- StarRocks **不写 v3 Puffin deletion vector**：它写**可叠加的 MOR position-delete Parquet 文件**（读时由引擎并集），按**分区列** shuffle（非按 data-file），FE 仅 `RowDelta` 提交 metadata。因此它没有「合并已有 DV」问题，分布式天然成立；StarRocks 默认建 v2 表，**无 equality-delete 写**，**UPDATE 仅 MOR（无 COW）**。
- 结论：NovaRocks 的 v3 DV 写、equality-delete 写、COW update 三项**均超出 StarRocks，无参考可抄**。对 v3 表必须写 DV（position-delete Parquet 不符合 v3 spec，且 `RowDeltaDvCommit` 已强制 Puffin-only），所以 StarRocks 的「叠加 position-delete」捷径不适用，需要 NovaRocks 自己解决分布式 DV 写。

---

## 2. 目标与非目标

### 目标

1. `DELETE`（含 v3 DV）、`UPDATE`（含 COW）、`MERGE`（含 matched-delete）、`ADD EQUALITY DELETE` 全部经分布式 async-sink 在 **BE** 写文件。
2. 删除本地写后注入的全部代码（见 §6 删除面）。
3. 行为与落盘形态保持等价（byte-identical 结果），仅改变**执行位置**（进程内 → 分布式）。
4. 在 1FE+NBE 与 all-in-one 下均正确；`all-in-one` 行为/性能不回退。

### 非目标

- 不重写 INSERT/OVERWRITE/v2-delete/MOR-update（已分布式，仅作模板复用）。
- 不改 CTAS、compaction（它们仍用 `run_select_to_chunks*` 本地写，**不在本范围**，见 §7）。
- 不在本次迁移 IMV refresh / IV3-4 / IV3-5 到新 DV writer（本次只把基建建出来，迁移留作后续，见 §11）。
- 不改变 COW vs MOR 的用户语义（保留 `write.update.mode` 双模式）。

---

## 3. 核心原则（硬不变量）

> **FE 只提交 metadata；所有数据文件、position-delete 文件、deletion vector、equality-delete 文件都由 BE 写。**

这条不变量是本设计一切取舍的根。它直接淘汰了「中心化（FE）写 DV」的方案（Option 3）——因为在 1FE+NBE 下 coordinator = **FE**，让 FE 读旧 DV + 写 Puffin 属于数据面 I/O，跨过了 INSERT/OVERWRITE 坚持的边界。FE 在 commit 时写 manifest/metadata.json（元数据）是固有且允许的；写 delete/DV/数据文件不允许。

---

## 4. 统一架构

所有 DML 收敛为同一形状：

```text
FE: 解析/规划 → 构造写 query + 选择 sink mode → 按需施加分布式 shuffle
  → 派发 fragment 到 BE（RemoteDispatcher；all-in-one 下 InProcessDispatcher）
BE: 执行 fragment（scan/compute/produce rows）→ 对应 *_SINK 写 staged 文件 → 上报 writer 结果
FE: ExecutionCoordinator 收齐 writer 结果 → IcebergWriteTransactionRunner
  → 唯一一次 metadata commit（run_iceberg_commit_typed）→ finalize（失效缓存等）
```

切换点（每条 op 要改的唯一方法）：`IcebergWriteTransactionExecutor::run_coordinated_write`，让它调用 `execute_query_as_iceberg_write(...)`（`src/engine/mod.rs:3403`）并带上正确的 `IcebergWriteSinkSpec`，与 `DistributedInsertWriteExecutor` / `DistributedDeleteWriteExecutor` / `DistributedMorUpdateExecutor` 完全一致。runner / commit executor / collector 不动。

`coordinated_execution_services`（`mod.rs:3670-3716`）按 `ClusterRole` 选 dispatcher：`Fe → RemoteDispatcher`，`AllInOne → InProcessDispatcher`（all-in-one 在进程内跑同一条分布式路径，故删本地模式不破坏单机）。

---

## 5. 分项设计

### 5.1 v3 DV-delete + MERGE matched-delete（核心，Option 2：per-file shuffle + BE 写 DV）

**问题**：写一个 data file 的 v3 DV 需要把「新删除位置」与该 data file「已有 DV」合并；若同一 data file 的删除位置散落在多个 BE fragment 上，谁都无法独立写出正确的合并 DV。

**解法（按 data-file 路径 shuffle，使每个 data file 由唯一 BE 独占）**：

1. **FE 规划**：把删除条件表达为分布式 query `SELECT _file, _pos[, <partition cols>] FROM <target> [FOR VERSION AS OF <pinned>] WHERE <pred>`（复用 v2 路径已有的 `build_delete_position_sink_query`，`src/engine/delete_flow.rs:466-487`）。在该 query 与 DV sink 之间**施加按 `_file`（data-file 路径）的 hash-distribution**（新的 distribution 要求），保证一个 data file 的所有删除位置进入同一个 BE/driver。
   - 对照 StarRocks `IcebergPlannerUtils.createShuffleProperty`（它按分区列）；这里 key 是 `_file`，是 NovaRocks 超出 StarRocks 的部分。
2. **新 sink mode `DeletionVectors`**：在 `IcebergWriteSinkMode`（`src/sql/codegen/iceberg_write_sink.rs:27-32`）与 `IcebergSinkMode`（`src/connector/iceberg/sink.rs:87-90`）各加一个变体，映射到一个新的 `TDataSinkType`（`ICEBERG_DV_SINK`，或给 `ICEBERG_DELETE_SINK` 加 format 标志，二选一，倾向独立类型以免污染 v2 路径）。
3. **BE DV writer**（新增 `push_chunk_deletion_vector`，类比 `push_chunk_position_delete` `sink.rs:765-887`）：本 BE 已独占若干 data file 的全部删除位置；对每个 `referenced_data_file`：
   - 用 pinned base snapshot 经 caching delete loader（`src/connector/iceberg/caching_delete_file_loader.rs`）读该文件**已有 DV**；
   - 由新位置构建 roaring bitmap，与已有 DV 合并；
   - 写 Puffin DV blob（`write_single_deletion_vector_puffin`，`src/connector/iceberg/commit/puffin_dv.rs:215-280`）；
   - 上报 `TIcebergDataFile { file_content = POSITION_DELETES, format = "puffin", referenced_data_file, content_offset, content_size_in_bytes, cardinality, file_size_in_bytes }`。
4. **Thrift / collector**：`TIcebergDataFile` 需带 `content_offset` / `content_size_in_bytes`（`RowDeltaDvCommit` 已用这些字段，见 `row_delta_dv.rs:789-803`）；`IcebergCommitCollector::convert` / `take_written_files` 需对 Puffin DV 文件做无损往返（当前对 data + Parquet delete 无损，需扩展）。
5. **FE commit（metadata-only）**：新增「从 BE 写好的 Puffin DV 文件直接登记」的提交路径——FE **不**再构建 bitmap、**不**读旧 DV、**不**写 Puffin（这些已在 BE 完成）。可由现有 `RowDeltaDvCommit` 派生一个 from-files 变体：跳过 `groups_to_vectors` 与中心化合并，直接以 BE 上报的 DV 文件构造 `RowDeltaDv` snapshot，沿用其 carry-forward / 校验（`row_delta_dv.rs:552-630`）中与「写」无关的部分。
6. **MERGE matched-delete**：matched 行的 `(_file,_pos)` 来自分布式 match query；同样按 `_file` shuffle 进 DV sink。MERGE 的 matched-delete / matched-update / unmatched-insert 三支需在**同一 RowDelta 内原子提交**——采用 op-code 路由的 row-delta sink（NO_OP/DELETE/UPDATE/INSERT，对照 StarRocks `IcebergRowDeltaSink`），matched-delete 行 → DV writer，insert/update 行 → data writer。

**正确性关键**：base snapshot 在规划期 pin 定，BE 据此读旧 DV，commit 期 `validateFromSnapshot(pinned)` 做冲突检测（对照 StarRocks `commitDeleteOperation`）。per-file 独占保证「一个文件的 DV 合并只发生在一个 BE 上」，无多 fragment 同文件 partial-DV 问题。

### 5.2 equality-delete（新分布式 sink）

当前 `execute_add_equality_delete_statement`（`equality_delete_flow.rs:52-178`）从字面量行在进程内写一个 equality-delete 文件（仅 unpartitioned）。

**改造**：把字面量行做成内存 VALUES 源 query → 新增 `EqualityDeletes` sink mode → BE 写 equality-delete 文件 → FE commit 登记。BE 写逻辑复用现有 `build_equality_delete_batch`（`equality_delete_flow.rs:244-310`）+ `write_equality_delete_file` 的核心，迁到 sink operator 内。无 scan、无并行收益，但统一进分布式模型以满足硬不变量并彻底删本地写。

### 5.3 COW-update（两阶段分布式重写）

COW 语义：对每个被命中的 data file，整体重写（保留未命中行 + 应用更新行 → 新文件），旧文件被 overwrite 替换。无 StarRocks 参考。

**两阶段**：

1. **阶段 A（识别被命中文件，metadata）**：分布式 query 算出命中行的 distinct `_file` = 被替换文件集合，收集路径回 FE（仅元数据，符合不变量）。
2. **阶段 B（BE 重写，数据面在 BE）**：对被命中文件**整体扫描**，对命中行套更新、未命中行透传，写新文件（`RowLineageData` sink，复用 MOR 的 `build_update_mor_data_sink_query` `mutation_flow.rs:344-389` 形态 + row-lineage 保号）。扫描需 scope 到阶段 A 的文件集合。
3. **FE commit**：overwrite 提交（replaced = 被命中文件，added = 新文件），沿用现有 commit op kind（`CommitOpKind::CowUpdate` 语义），但 `CowUpdateRewriteSet`（旧→新映射）由阶段 A 的命中文件集合 + 阶段 B 的产出在 coordinator 侧组装为**元数据**，不再来自本地写。

COW 是最复杂、无参考的一块，作为**独立验证阶段**实现（见 §10）。

### 5.4 已是模板（不改，仅复用）

- INSERT/OVERWRITE：`DistributedInsertWriteExecutor`（`iceberg_writer.rs:260-294`）+ `ICEBERG_TABLE_SINK`。
- v2 position-delete DELETE：`DistributedDeleteWriteExecutor` + `build_delete_position_sink_query` + `ICEBERG_DELETE_SINK`。
- MOR-UPDATE：`DistributedMorUpdateExecutor`（`mutation_flow.rs:619`），数据侧分布式 + 删除侧 coordinator 物化 match 后 inject（match 物化是 query 结果收集，属元数据收集，符合不变量）。

---

## 6. 删除面（移除本地写后注入）

切换完成后删除（约 600+ 行，集中在 4 个文件）：

- `local_writer_commit_input`（`write_transaction.rs:209-242`）、`new_local_writer_write_id`（`:205-207`）。
- `InjectedDeleteGroupExecutor`（`delete_flow.rs:302-336`）+ 手写进程内 scan `scan_for_position_deletes_at` / `scan_for_position_deletes`（`delete_flow.rs:1076-1166`）+ 进程内可见性扫描 `load_existing_delete_visibility_by_data_file_at`（DV 路径专用部分）。
- `MutationWriteExecutor` + 全部 COW 助手：`run_cow_update_write`、`write_cow_update_files`、`build_cow_rewrite_batches`、`CowRewriteFile/Accumulator`、`build_cow_rewrite_set`、`load_data_file_lineage` 等（`mutation_flow.rs:776-905, 1205-1473+`）。
- `equality_delete_flow.rs` 的本地写路径（`EqualityDeleteWriteExecutor` 等；逻辑迁入 sink）。
- `has_preloaded_commit_output` trait 方法（`write_transaction.rs:105-107`）及 runner 门控分支（`:302-306`）——所有 override 者删除后即可移除。

---

## 7. 明确保留 / 范围外

- **保留**（与 MV/IMV refresh + compaction 共用，禁删）：`IcebergCommitCollector::inject_delete_group` / `inject_written_file`（`collector.rs:142,217`）；`run_select_to_chunks*`（CTAS/compaction 用，`iceberg_writer.rs:1092-1149`）；`data_file_to_written_file`、`written_file_to_sink_commit_info`、`data_writer` 本地写函数（compaction/CTAS 用）。
- **范围外**：CTAS、compaction 仍本地写；IMV refresh 仍中心化 DV apply（其迁移见 §11）。
- 删除时只让**本 4 条 DML executor** 停止调用 inject_* / 本地写，不动这些共享 API 本身。

---

## 8. 正确性与错误处理

- **snapshot pin**：base snapshot 规划期冻结；BE 读旧 DV / 重写文件均基于此；commit 期 `validateFromSnapshot` + `validateDataFilesExist` + 冲突过滤（对照 StarRocks `commitDeleteOperation`）。
- **写时读快照一致性**（Option 2 新引入）：BE 在写 DV 时读旧 DV，必须读 pinned snapshot 视图，不受并发 commit 影响。
- **timeout / cancel / writer failure / commit unknown**：沿用现有 `IcebergWriteTransactionRunner` 状态机（Preparing→Committing→Finalizing→Finalized + abort/failure 分支），不新增终态。
- **abort/cleanup**：BE 写出的 staged DV/equality/数据文件在失败时按现有 abort cleanup 清理（`build_abort_cleanup_for_catalog_entry`）。
- **per-file 独占失效保护**：若 shuffle 未能保证独占（理论上不应发生），commit 期需检测同一 data file 多个 DV 输入并 fail-fast（防止静默错误）。

---

## 9. 测试与验收

每条 op 必须满足（作为该 op 阶段的合并门）：

1. **byte-identical 等价**：切换前/后对同一输入产出等价结果（行集 + 落盘 content type/format），覆盖 `sql-tests/iceberg-dml/`（DV delete、cow update、merge matched-delete、equality-delete schema-evolution 等既有用例）。
2. **多 BE 正确**：`--cluster-mode cross-process --cluster-size 2`（1FE+2BE）下结果与 all-in-one 一致；DV per-file shuffle 在跨 BE 下产出单一正确合并 DV。
3. **守卫测试**：新增 plan-shape/执行守卫，断言该 op **不再**走本地路径（对照现有 `overwrite_path_uses_distributed_writer_not_local_collect`、`append_executor_does_not_use_synthetic_commit_input`，`iceberg_writer.rs:1252-1300`）。
4. **跨引擎 compat**：DV/equality 结果可被 Spark / FE 读（`sql-tests/iceberg-compatibility`、`iceberg-rest`）。
5. **FE 零数据 I/O 断言**：测试/审查确认 FE 角色在这些 op 中不写 DV/数据/删除文件（只写 metadata）。

---

## 10. 实施阶段（rollout）

用户选定「big-bang」（最终一份统一交付），但写路径正确性敏感，故**实现按 op 分阶段、每阶段过 §9 验证门**；建议**按阶段合并**而非单个巨型 diff（最终合并策略在 writing-plans 决定）。推荐顺序（难度递增）：

1. **DV sink 基建 + DV-delete**：新 `DeletionVectors` sink mode + per-file shuffle + BE DV writer + thrift/collector 往返 + from-files commit。（核心、最高风险）
2. **MERGE matched-delete**：复用 DV sink + op-code 路由 row-delta sink 的原子提交。
3. **equality-delete**：新 sink mode + VALUES 源。
4. **COW-update**：两阶段分布式重写（独立验证）。
5. **删除面收口**：移除 §6 全部本地代码 + `has_preloaded_commit_output` + 门控；补守卫测试。

---

## 11. 后续复用（验证 Option 2 的长期价值）

本次建出的「按 data-file 分片的 BE 端 DV writer」是让以下三者也满足硬不变量的同一块基建（均后续单独迁移）：

- IMV refresh 删除侧 apply（当前中心化写 DV）。
- IV3-4：v2 position-delete → DV 迁移。
- IV3-5：DV compaction（天然 per-file）。

---

## 12. 风险与待解问题

1. **DV from-files commit 的 carry-forward 校验**：`RowDeltaDvCommit` 现含「未触及的 live delete 条目 forward 为 Existing」与「拒绝非 Puffin」的逻辑（`row_delta_dv.rs:589-630`）；from-files 变体需保留这些与「写」无关的不变量，仅去掉中心化「构建+合并+写」。需逐行确认拆分边界。
2. **caching delete loader 上写路径可用性**：当前 `caching_delete_file_loader.rs` 为读侧；BE 写 DV 时复用它读旧 DV 需确认其在 sink operator 上下文可用、且绑定 pinned snapshot。
3. **MERGE 原子多 sink**：op-code 路由的 row-delta sink（DV + data 两子 sink，一次 RowDelta 提交）是新机制，需确认 collector 能同时收集两类 writer 结果并原子提交。
4. **COW 阶段 A→B 的文件集合 scope**：阶段 B 扫描需精确 scope 到被命中文件；需确认 scan 能按 `_file ∈ set` 裁剪而不退化为全表扫。
5. **all-in-one 新依赖**：切到分布式后每条 DML 依赖 `ensure_standalone_exchange_server`（`mod.rs:3411-3415`），本地路径原先不需要——需确认 all-in-one 启动路径已就绪。

---

## 13. PR 自检清单

每个阶段 PR 开始前：

1. 本 PR 改的是 sink mode / shuffle / BE writer / commit-from-files / 删除面 哪一层？
2. 是否有任何 BE 直接提交 Iceberg metadata？是否有 **FE 写 DV/数据/删除文件**（违反硬不变量）？
3. writer 输出是否足够让 FE 唯一 commit（files、referenced_data_file、content_offset/size、stats、snapshot guard）？
4. byte-identical 等价 + 1FE+2BE + 守卫测试 是否齐？
5. 是否仅让本 op 停用 inject_*/本地写，而未误删共享 API？
6. timeout/cancel/writer failure/commit unknown 是否仍由统一状态机覆盖？
