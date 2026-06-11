# Iceberg V3 Compact Data Files 设计

**日期**：2026-05-04
**状态**：Accepted（brainstorming 中已确认）
**范围**：支持 NovaRocks standalone 对 Iceberg v3 表的 data-file compaction：先兼容外部 compaction，再提供持久化异步 `ALTER TABLE ... OPTIMIZE` 整表 rewrite。

## 0. 背景

NovaRocks 当前已经具备 Iceberg v3 row-lineage、Puffin deletion vector、position delete、equality delete、MV delete apply、schema evolution 和 partition evolution 的基础能力。当前缺口是 data-file compaction：

- 外部引擎可能提交 Iceberg `replace` snapshot 来 compact data files，并同时消除 delete files。
- NovaRocks 的 IVM `plan_changes` 已能把经过校验的 `replace` snapshot 当作 compaction 跳过，但还缺少更完整的外部 compaction 验收。
- NovaRocks 还不能主动发起 Iceberg data-file compaction。

Iceberg spec 将 `replace` operation 定义为添加和删除 data/delete files 但不改变表逻辑数据，典型场景包括 compaction、文件格式转换或文件搬迁。Iceberg v3 还要求新 snapshot 正确维护 row lineage：manifest list 的 `first_row_id`、snapshot row range、table metadata 的 `next-row-id` 必须一致。

本设计选择先做语义最清楚的整表 rewrite：读取 current snapshot 的可见行，写出新的 data files，提交 `replace` snapshot，退役旧 data/delete files。后续再在同一框架上扩展分区级 compaction 和小文件 bin-pack。

参考：

- [Apache Iceberg Spec - snapshot operation](https://iceberg.apache.org/spec/)
- [Apache Iceberg Spec - first row ID assignment](https://iceberg.apache.org/spec/)

## 1. 已确认决策

| 维度 | 决策 |
|---|---|
| 总体顺序 | 先支持外部 compaction 兼容性，再支持 NovaRocks 主动 compaction |
| 外部 compaction 验收形态 | rewrite 后消除 deletes：旧 data/delete files 退役，新 snapshot 只保留 compact 后 data files |
| 主动 compaction 入口 | StarRocks 风格异步 `ALTER TABLE ... OPTIMIZE` |
| job 持久化 | 写入 metadata store；历史状态重启后可见，RUNNING job 重启后标 FAILED，不做自动恢复 |
| 首版执行粒度 | 整表 rewrite，不做分区级、不做小文件选择策略 |
| 首版目标 | 协议正确性和可观测性优先，不追求生产级调度和 compaction 策略 |

## 2. 目标

1. 兼容外部引擎提交的 Iceberg v3 `replace` compaction snapshot，特别是 rewrite 后消除 Puffin DV、position delete、equality delete 的形态。
2. 让 MV refresh 遇到 validated compaction `replace` snapshot 时继续按 no-op lineage step 处理，不产生错误或重复增量。
3. 提供 `ALTER TABLE <iceberg_table> OPTIMIZE` 入口，创建持久化异步 OPTIMIZE job。
4. 提供 `SHOW ALTER TABLE OPTIMIZE ...` 查询 job 状态，让现有 `sql-tests` 的 `@wait_alter_optimize` 可直接轮询。
5. 后台 job 做整表 visible-row rewrite，提交 Iceberg `Operation::Replace` snapshot。
6. 对 Iceberg v3 row-lineage 表正确分配新 row ID 范围，并推进 table metadata `next-row-id`。
7. compaction 后新 snapshot 不继承旧 delete files，达到 delete 消除。
8. 对并发写入、catalog OCC、IO、delete 读取、commit-unknown 做明确 fail-fast 或保守处理。

## 3. 非目标

- 分区级 `OPTIMIZE PARTITION (...)`。
- 小文件 bin-pack、target file size 选择、按文件大小/数量筛选输入。
- 多 job 并发、队列调度、优先级、取消、自动重试。
- 重启恢复 RUNNING job。
- expire snapshots、orphan files GC、旧物理文件删除。
- FE-driven 模式或 StarRocks FE 侧语义改动。
- 文件格式转换、sort order rewrite、Z-order 或排序 compaction。
- REST/HMS/JVM catalog 的额外专用逻辑；首版复用现有 standalone local-FS / S3 Hadoop-style catalog 能力。

## 4. SQL 与用户可见语义

### 4.1 创建 OPTIMIZE job

首版接受：

```sql
ALTER TABLE <iceberg_table> OPTIMIZE;
```

规则：

- 目标必须解析为 Iceberg backend。
- 只支持普通 Iceberg table，不支持 materialized view 名称或 managed-lake 内表。
- 语句返回时只保证 job 已创建，不保证 compaction 已完成。
- 同一张 Iceberg 表如果已有 `PENDING` 或 `RUNNING` OPTIMIZE job，新请求直接拒绝。
- 创建 job 时记录 `base_snapshot_id`。如果表为空且没有 current snapshot，job 允许创建并最终 no-op FINISHED。

首版不接受：

```sql
ALTER TABLE t OPTIMIZE PARTITION (...);
ALTER TABLE t OPTIMIZE WITH (...);
ALTER TABLE t OPTIMIZE WHERE ...;
```

这些语法应返回明确 unsupported 错误。

### 4.2 查询 OPTIMIZE job

支持最小 StarRocks 风格查询：

```sql
SHOW ALTER TABLE OPTIMIZE FROM <db>
  WHERE TableName = '<table>'
  ORDER BY CreateTime DESC
  LIMIT 1;
```

返回列至少包括：

| 列 | 含义 |
|---|---|
| `JobId` | metadata store 分配的 job id |
| `TableName` | Iceberg table name |
| `State` | `PENDING` / `RUNNING` / `FINISHED` / `FAILED` |
| `CreateTime` | job 创建时间 |
| `FinishTime` | job 完成或失败时间，未结束为空 |
| `Msg` | 错误或摘要信息 |

为了便于调试，可额外返回：

- `BaseSnapshotId`
- `TargetSnapshotId`
- `InputDataFiles`
- `OutputDataFiles`
- `InputDeleteFiles`
- `OutputDeleteFiles`

`tests/sql-test-runner` 已有 `@wait_alter_optimize`，它会轮询 `SHOW ALTER TABLE OPTIMIZE` 并等待输出包含 `FINISHED`。因此 `SHOW` 输出必须稳定包含状态字符串。

## 5. 持久化 Job 模型

在 standalone metadata store 中新增 Iceberg OPTIMIZE job 表，不复用 managed-lake `erase_jobs`。原因：

- `erase_jobs` 只表达物理删除和 metadata purge，不适合记录 Iceberg catalog/table/snapshot/file-count 语义。
- Iceberg table 持久化信息目前和 managed snapshot 并列存在于 `MetadataSnapshot`，OPTIMIZE job 也应属于 Iceberg catalog 侧元数据。

建议新增：

```text
iceberg_optimize_jobs (
  job_id INTEGER PRIMARY KEY,
  catalog_name TEXT NOT NULL,
  namespace_name TEXT NOT NULL,
  table_name TEXT NOT NULL,
  base_snapshot_id INTEGER,
  target_snapshot_id INTEGER,
  state TEXT NOT NULL,
  input_data_files INTEGER NOT NULL DEFAULT 0,
  output_data_files INTEGER NOT NULL DEFAULT 0,
  input_delete_files INTEGER NOT NULL DEFAULT 0,
  output_delete_files INTEGER NOT NULL DEFAULT 0,
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL,
  finished_at_ms INTEGER,
  last_error TEXT
)
```

状态转换：

```text
PENDING -> RUNNING -> FINISHED
PENDING -> RUNNING -> FAILED
RUNNING --server restart--> FAILED
```

首版 API：

- `create_iceberg_optimize_job(...) -> job_id`
- `list_pending_iceberg_optimize_jobs()`
- `claim_iceberg_optimize_job(job_id)`
- `finish_iceberg_optimize_job(job_id, outcome)`
- `fail_iceberg_optimize_job(job_id, error)`
- `fail_running_iceberg_optimize_jobs_on_startup()`
- `show_iceberg_optimize_jobs(db/table/filter/order/limit)`

metadata schema 需要 bump `PRAGMA user_version`，并加迁移函数保证已有本地 metadata store 可以打开。

## 6. 后台执行模型

`ALTER TABLE ... OPTIMIZE` 创建 job 后唤醒一个 standalone 后台 worker。首版 worker 可串行执行：

1. 启动时调用 `fail_running_iceberg_optimize_jobs_on_startup()`。
2. 定期或被通知后扫描 `PENDING` jobs。
3. 对每个 job 执行 `claim`，成功后进入 compaction。
4. 成功则 `FINISHED`，失败则 `FAILED`。

首版不做自动 retry。用户可以再次执行 `ALTER TABLE ... OPTIMIZE` 创建新 job。`FAILED` 历史不阻塞新 job；`PENDING/RUNNING` 阻塞同表新 job。

worker 需要持有足够少的锁：

- metadata store 只在 claim / finish / fail 时短事务更新。
- Iceberg catalog load、scan、write、commit 不持有 metadata store 写锁。
- 同一表 active job 约束由创建 job 时的 metadata transaction 保证。

## 7. Rewrite 数据流

后台 job 核心流程：

```text
load job
  -> resolve Iceberg catalog/table
  -> load table metadata
  -> verify current snapshot == base_snapshot_id
  -> plan visible-row full-table scan
  -> write compacted data files to staging
  -> reload table and verify current snapshot still == base_snapshot_id
  -> RewriteDataFilesCommit
  -> finish job with target snapshot + file counts
```

### 7.1 规划输入

输入是 current snapshot 的整表可见行：

- 枚举所有 live data files。
- 枚举所有 live delete files，包括 Puffin DV、position deletes、equality deletes。
- 复用现有 Iceberg scan/delete apply 能力，确保读出的 rows 等价于 `SELECT * FROM table`。
- 不做 partition pruning。
- 不做文件大小选择。

如果 current snapshot 为空，job no-op FINISHED，`target_snapshot_id = base_snapshot_id`。

### 7.2 重写数据

写出新 data files：

- 复用现有 Iceberg Parquet writer / sink 能力。
- 输出 schema 使用 current Iceberg schema。
- 输出 partition spec 使用 current default partition spec。
- 对 evolved partition spec 的历史文件，重写后统一落到 current default spec。
- 首版不保证 target file size，只保证 row contents 正确。

可见行数 guard：

- compaction 前统计 visible row count。
- compaction 后统计 new data files `record_count` 总和。
- 两者不一致则 fail job，不提交 replace snapshot。

### 7.3 提交 replace snapshot

新增 `RewriteDataFilesCommit`，内部自实现 Iceberg replace commit action：

- 写 deleted-data manifest：旧 live data files 作为 `DELETED` entries。
- 写 deleted-delete manifest：旧 live delete files 作为 `DELETED` entries。
- 写 added-data manifest：新 data files 作为 `ADDED` entries。
- 写 manifest list。
- 构造 `Snapshot`，`summary.operation = Operation::Replace`。
- `TableUpdate::AddSnapshot` + `SetSnapshotRef(main)`。
- `TableRequirement::RefSnapshotIdMatch(main, base_snapshot_id)`。
- 同时校验 schema id、default spec id、sort order 等现有 commit guard。

提交成功后：

- current snapshot 指向新 replace snapshot。
- 旧 data/delete files 不再 live。
- 旧物理文件保留，等待后续 expire/orphan cleanup。

## 8. Iceberg V3 Row Lineage

整表 compaction 会把当前可见行写成新的 data files，因此新文件获得新的 row ID 范围，不继承旧 data files 的 row ID。

对 `format-version=3` 且 `write.row-lineage=true` 的表：

- `first_row_id = table.metadata.next_row_id()`。
- manifest list 写入时给 added data manifest 分配 `first_row_id`。
- snapshot 使用 `with_row_range(first_row_id, added_rows)`。
- `added_rows = sum(new_data_files.record_count)`。
- table metadata `next-row-id` 前进 `added_rows`。
- delete manifests 的 `first_row_id` 始终为 null。

旧 data/delete manifests 作为 `DELETED` entries 退役，不参与新 row ID 范围。compaction 后 `_row_id` 变化是预期行为，因为 rewrite 产生了全新的 rows lineage。

对 v2 表或 v3 但未启用 row-lineage 的表：

- 不写 snapshot row range。
- 继续保持现有 position/equality delete 兼容路径。

## 9. Delete 消除语义

首版主动 compaction 的语义是“rewrite after applying deletes”：

- Puffin DV 在读取旧 snapshot 时 apply。
- v2 position deletes 在读取旧 snapshot 时 apply。
- equality deletes 在读取旧 snapshot 时 apply。
- 新 snapshot 不继承旧 delete files。
- 新 snapshot 的 live delete file count 应为 0。

这也服务外部 compaction 兼容性：如果外部引擎提交同样形态的 `replace` snapshot，NovaRocks 应能：

- 正确读取 compact 后 data files。
- 不再把旧 delete files 视为 live。
- MV refresh 遇到这个 replace snapshot 时通过 compaction validator 并跳过增量。

`plan_changes` 现有 replace validator 需要继续要求：

- `total-records` 不变。
- `added-data-files > 0` 且 `deleted-data-files > 0`，除非空表 no-op。
- schema id 不变。
- 不把 `overwrite` 当成 compaction。

若外部 replace snapshot 缺少足够 summary 信息，继续 fail fast，而不是猜测它是安全 compaction。

## 10. 错误处理

### 10.1 并发变化

job 创建时记录 `base_snapshot_id`。执行时：

- 开始 rewrite 前校验 current snapshot 等于 `base_snapshot_id`。
- commit 前重新 load table，再校验一次。
- commit requirement 再用 catalog OCC 校验 main ref。

任一校验失败：

- 不提交 replace snapshot。
- staged files definite-fail 情况下清理。
- job 标为 `FAILED`，`Msg` 写明 snapshot changed。

### 10.2 Commit 失败分类

沿用现有 Iceberg commit abort policy：

- catalog requirement mismatch、validation failure、metadata serialization failure 等 definite-fail：清理 staged data/manifest files。
- 网络/IO 导致 commit-unknown：不删除 staged files，job 标为 `FAILED`，提示人工检查 catalog 和 staging path。

### 10.3 文件 IO 和 delete apply

以下都标记 job `FAILED`：

- data file read 失败。
- Puffin DV decode 失败。
- position delete / equality delete 读取失败。
- output data file 写入失败。
- manifest / manifest-list 写入失败。
- visible row count 与 output record count 不一致。

错误信息应保留具体文件路径或 snapshot id，便于定位。

## 11. 代码边界

建议新增或修改：

| 位置 | 责任 |
|---|---|
| `src/engine/statement.rs` | 识别并解析 `ALTER TABLE ... OPTIMIZE` |
| `src/engine/mod.rs` | dispatch `ALTER TABLE ... OPTIMIZE`，创建 job，支持 `SHOW ALTER TABLE OPTIMIZE` |
| `src/connector/starrocks/managed/store.rs` | 新增 `iceberg_optimize_jobs` schema、CRUD、show 查询、startup RUNNING fail |
| `src/connector/iceberg/compact.rs` | Iceberg OPTIMIZE job 执行核心：load/validate/scan/write/commit |
| `src/connector/iceberg/commit/rewrite_data_files.rs` | `RewriteDataFilesCommit` |
| `src/connector/iceberg/commit/types.rs` | 增加 `CommitOpKind::RewriteDataFiles`；file counts 由 compact executor 写入 job outcome，`CommitOutcome` 不承载统计字段 |
| `src/connector/iceberg/commit/helpers.rs` | 共享 manifest list、manifest rewrite helpers |
| `src/connector/iceberg/changes.rs` | 加强 replace compaction validator 测试，确保外部 compaction 被跳过 |
| `tests/sql-test-runner` | 如现有 `SHOW ALTER TABLE OPTIMIZE` 轮询已满足，不需要修改；否则补稳定输出适配 |

`RewriteDataFilesCommit` 不应复用 `OverwriteCommit`。两者语义不同：

- `OverwriteCommit` 是逻辑 overwrite，会改变表数据。
- `RewriteDataFilesCommit` 是 compaction replace，必须证明逻辑数据不变，并退役 delete files。

## 12. 测试计划

### 12.1 Rust 单测

- parser:
  - `ALTER TABLE ice.db.t OPTIMIZE`
  - 拒绝 `OPTIMIZE PARTITION`
  - 拒绝 `OPTIMIZE WITH (...)`
- metadata store:
  - create / claim / finish / fail optimize job
  - 同表 active job 去重
  - startup 将 RUNNING job 标为 FAILED
  - show rows 可按 db/table/order/limit 查询
- commit action:
  - 生成 `Operation::Replace`
  - old data manifest entries 被标 `DELETED`
  - old delete manifest entries 被标 `DELETED`
  - new data manifest entries 被标 `ADDED`
  - v3 row lineage `first-row-id` / `next-row-id` 推进正确
- change planning:
  - 外部 compaction `replace` 被 validated skip
  - 缺 summary 的 replace fail fast
  - schema id 改变的 replace fail fast

### 12.2 Standalone 集成测试

- v3 row-lineage + Puffin DV:
  - insert 多批数据制造多个 data files
  - `DELETE` 产生 Puffin DV
  - `ALTER TABLE ... OPTIMIZE`
  - `SHOW ALTER TABLE OPTIMIZE` 变为 `FINISHED`
  - SELECT 结果不变，live delete file count 为 0
  - `_row_id` 为新 row range
- equality delete:
  - `ALTER TABLE ... ADD EQUALITY DELETE`
  - OPTIMIZE 后 SELECT 正确
  - live equality delete file count 为 0
- concurrent snapshot change:
  - job 创建后插入新数据
  - job 执行应 FAILED，Msg 包含 snapshot changed

### 12.3 SQL Tests

新增：

```text
sql-tests/iceberg/sql/iceberg_v3_optimize_compact_data_files.sql
```

覆盖：

- 创建 v3 row-lineage Iceberg 表。
- 多批 insert。
- delete 产生 DV。
- `ALTER TABLE ... OPTIMIZE`。
- `-- @wait_alter_optimize=<table>` 等待完成。
- SELECT 结果正确。
- `SHOW ALTER TABLE OPTIMIZE` 包含 `FINISHED`。
- 基于该 base table 的 MV refresh 在 compaction 后稳定。

外部 compaction 兼容性可以增加 Rust integration helper 或 SQL-test 辅助入口来生成 replace snapshot。重点不是外部命令本身，而是 NovaRocks 面对已存在的 replace snapshot 时读和 MV refresh 正确。

### 12.4 验证命令

Focused 验证：

```bash
cargo test --lib iceberg_optimize -- --exact
cargo test --lib rewrite_data_files -- --exact
cargo test --lib plan_changes_replace -- --exact
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite iceberg --only iceberg_v3_optimize_compact_data_files --mode verify
cargo fmt --check
git diff --check
```

若 exact module names 在实施时不同，plan 中应替换为实际 test path。

## 13. 分阶段落地建议

### PR 1：外部 compaction 兼容性和 validator 加固

- 加强 `plan_changes` replace validator 测试。
- 添加外部 replace snapshot fixture/helper。
- 验证 MV refresh 对 delete-eliminating replace snapshot 是 no-op lineage step。

### PR 2：OPTIMIZE job metadata 和 SHOW

- metadata store 新增 `iceberg_optimize_jobs`。
- `ALTER TABLE ... OPTIMIZE` 创建 job。
- `SHOW ALTER TABLE OPTIMIZE` 查询状态。
- startup RUNNING -> FAILED。
- 后台 worker 可先 claim 后立即 FAILED/unsupported，用于闭合 SQL/job 骨架。

### PR 3：整表 rewrite executor 和 commit action

- visible-row scan。
- output data file writer。
- `RewriteDataFilesCommit`。
- v3 row-lineage metadata。
- delete file retire。

### PR 4：SQL-test 和 end-to-end 验收

- v3 DV delete 消除。
- equality delete 消除。
- MV refresh after compaction。
- concurrent snapshot change failure。

## 14. 延后决策

以下问题不影响首版实施，首版按本节给出的固定选择执行：

1. `SHOW ALTER TABLE OPTIMIZE` 首版不追求完全匹配 StarRocks FE 的列名和列顺序，只保证 sql-test 轮询和人工诊断需要的列稳定。
2. output file roll 策略首版不暴露配置，使用现有 writer 策略；后续再用 `WITH` 参数扩展。
3. 旧物理文件首版不清理；expire snapshots / orphan cleanup 后续单独设计。
