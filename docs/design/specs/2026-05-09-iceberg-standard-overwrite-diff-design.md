# Iceberg Standard Overwrite Diff for MV Incremental Refresh

## 背景

NovaRocks 当前 Iceberg v3 COW UPDATE 会写 NovaRocks 私有 sidecar：

- snapshot summary:
  - `novarocks.row-level-op = update`
  - `novarocks.update.mode = copy-on-write`
  - `novarocks.update.sidecar = metadata/<uuid>-update-sidecar.json`
- 持久化结构：`MutationSidecar` / `MutationSidecarFile`
- 写入路径：`src/engine/mutation_flow.rs` 和 `src/connector/iceberg/commit/update_cow.rs`
- 消费路径：`src/connector/iceberg/changes.rs`

这个 sidecar 不是 Iceberg v3 spec 标准内容。Spark、Flink、Trino、Athena 等外部 writer 不会写它，因此 MV 增量正确性不能依赖这个私有扩展。

## 目标

以 Iceberg 标准 metadata 作为 MV 增量 correctness 的唯一来源：

- `Append` snapshot：added data files 表示 insert delta。
- `Delete` snapshot：added delete files、Puffin DV、equality delete 表示 delete delta。
- `Overwrite` snapshot：deleted data files 表示 delete delta，added data files 表示 insert delta。
- `Replace` snapshot：只有能证明逻辑等价的 compaction 才 no-op；无法证明时保守 full refresh 或 unsupported。

NovaRocks 自己写的 COW UPDATE 对外表现为标准 `operation=overwrite` snapshot。新代码不再写 sidecar，也不再读取 sidecar。

## 非目标

- 不在本设计里实现任意 `Replace` snapshot 的业务语义推断。
- 不根据 snapshot summary 猜测 UPDATE、MERGE、INSERT OVERWRITE 的业务意图。
- 不为 Iceberg-backed MV 强行实现 delete-bearing 增量；如果其写入模型暂时不能表达 delete branch，应 fallback full refresh。
- 不保证清理历史 sidecar orphan files。历史文件可以留在 metadata 目录中，但新刷新逻辑不再读取它们。

## 当前代码结论

调研确认当前行为如下：

- `src/connector/iceberg/commit/types.rs` 定义 sidecar constants、`MutationSidecar`、`MutationSidecarFile`。
- `src/engine/mutation_flow.rs` 的 COW UPDATE 流程构造 sidecar，并通过 `RunInput.cow_update_sidecar` 传给 commit action。
- `src/connector/iceberg/commit/update_cow.rs` 写 `*-update-sidecar.json`，并在 snapshot summary 记录 NovaRocks 私有 marker。
- `src/connector/iceberg/changes.rs` 对 `Operation::Overwrite` 的处理依赖 marker：有 `copy-on-write` marker 时走 `CollectCowUpdate`，否则返回 `UnsupportedOperation(overwrite)`。
- `changes.rs` 已有标准 manifest-list 收集能力：added data files、position delete、Puffin DV、equality delete。
- `src/connector/starrocks/managed/mv_refresh.rs` 已有 insert/delete 双流和 apply policy，可以承接 overwrite diff 产生的 delete + insert delta。

## 设计概览

采用激进方案：删除持久 sidecar 作为 Iceberg metadata 的一部分。

COW UPDATE 写入路径仍然需要内部校验 old files、replacement files、base snapshot、table uuid，但这些信息只在进程内传递，不序列化，不写入 snapshot summary，不作为消费端协议。

MV change planning 改成标准 snapshot diff：

- 按 parent chain 顺序遍历 snapshots。
- 每个 snapshot 只根据 Iceberg operation 和 manifest entry status 产出 delta。
- `Overwrite` 不再 special-case NovaRocks marker，而是收集同一 snapshot 的 `ADDED` data entries 和 `DELETED` data entries。
- materialize 阶段对 deleted data files 读取整文件旧行，再运行 MV SELECT 的 delete branch。

## 写入路径改造

### `mutation_flow.rs`

保留当前 COW UPDATE 的行级执行语义：

1. matched-row 查询产出 old rows、new rows、`_file`、`_pos`、`_row_id`。
2. 对 touched data files 执行 copy-on-write rewrite。
3. replacement rows 继续保留原 `_row_id`，更新行设置新的 `_last_updated_sequence_number`。
4. 将 replacement data files 注入 `IcebergCommitCollector`。

删除持久 sidecar 构造：

- 删除 `build_cow_sidecar`。
- 删除 `empty_sidecar`。
- 不再生成 `MutationSidecar` / `MutationSidecarFile`。

如 commit 前仍需要校验信息，改用私有进程内结构，例如：

```rust
struct CowUpdateRewriteSet {
    base_snapshot_id: i64,
    target_table_uuid: String,
    touched_data_files: Vec<CowUpdateTouchedFile>,
}

struct CowUpdateTouchedFile {
    old_file: String,
    new_files: Vec<String>,
    row_ids: Vec<i64>,
}
```

这个结构不实现 `Serialize` / `Deserialize`，不出现在 Iceberg snapshot summary，不作为外部协议。

### `update_cow.rs`

`CowUpdateCommit` 改成只发布标准 Iceberg overwrite snapshot：

- touched old data files 写成 data manifest 中的 `ManifestStatus::Deleted` entries。
- replacement data files 写成 data manifest 中的 `ManifestStatus::Added` entries。
- snapshot summary 使用标准 overwrite counters：
  - `added-data-files`
  - `added-records`
  - `added-files-size`
  - `deleted-data-files`
  - `deleted-records`
- 不写 `metadata/*-update-sidecar.json`。
- 不写 `novarocks.row-level-op`、`novarocks.update.mode`、`novarocks.update.sidecar`。

COW UPDATE 对外与外部 writer 的 COW UPDATE / MERGE / overwrite 保持同一种 Iceberg metadata 形态：`operation=overwrite` + manifest diff。

## Change Planning 改造

### 数据结构

`IcebergChangeBatch` 增加标准 deleted data files：

```rust
pub(crate) struct DeletedDataFileRef {
    pub path: String,
    pub size: i64,
    pub record_count: Option<i64>,
    pub partition_spec_id: Option<i32>,
    pub partition_key: Option<String>,
    pub first_row_id: Option<i64>,
    pub data_sequence_number: Option<i64>,
}
```

`IcebergChangeBatch` 保留：

- `inserts`
- `deletes`
- `equality_deletes`

新增：

- `deleted_data_files`

删除：

- `cow_updates`
- `CowUpdateRef`
- sidecar read helpers
- `mor_updates`

MOR 正确性来自 added data files + added delete files，不再保留依赖 NovaRocks marker 的 `mor_updates` 向量。

### Snapshot 分类

`classify_snapshot` 改为：

- `Append` -> `CollectInserts`
- `Delete` -> `CollectDeletes`
- `Overwrite` -> `CollectOverwriteDiff`
- `Replace` -> 通过 `validate_replace_snapshot` 后 no-op；失败时返回可映射到 full refresh 的 error

不再读取 `novarocks.row-level-op` 或 `novarocks.update.mode`。

### Manifest 收集

新增或扩展 collector：

- `collect_added_data_files_for_manifest_list`
  - 只收集 `ManifestStatus::Added`
  - `entry.snapshot_id() == Some(snapshot_id)`
- `collect_deleted_data_files_for_manifest_list`
  - 只收集 `ManifestStatus::Deleted`
  - `entry.snapshot_id() == Some(snapshot_id)`
  - `df.content_type() == DataContentType::Data`
- `collect_added_delete_files_for_manifest_list`
  - 保持现有 position delete / Puffin DV / equality delete 逻辑

`CollectOverwriteDiff` 对同一个 manifest-list 同时收集：

- added data files -> insert delta
- deleted data files -> delete delta

不根据 operation summary 的 counters 推导文件列表。summary counters 只能作为日志或校验辅助。

## Materialize 改造

`materialize_changes` 的 insert branch 不变：用 added data files 构造 one-shot table，运行 MV SELECT。

delete branch 扩展为三类来源：

1. position delete / Puffin DV：现有 `scan_deletes`。
2. equality delete：现有 equality delete reverse projection。
3. deleted data files：直接读取这些旧 data files 的完整行内容。

deleted data files 的读取可以复用当前 `read_full_data_file` 逻辑，但函数名和错误信息应泛化为 overwrite delete projection，而不是 COW sidecar。

然后将三类 deleted rows 合并，交给 `execute_query_for_mv_incremental_deletes` 跑同一份 MV SELECT。这样 projection/filter 和 aggregate 的语义仍由 SQL 自身保证。

## MV 策略

### Managed-lake MV

`src/connector/starrocks/managed/mv_refresh.rs` 已经有：

- `IvmChangeStream { inserts, deletes }`
- projection MV 的 upsert/delete op
- aggregate MV 的 insert delta + delete delta 负向合并
- MIN/MAX delete retract fallback full refresh

这里主要调整 `has_deletes`：

- `batch.deletes`
- `batch.equality_deletes`
- `batch.deleted_data_files`

都算 delete branch。

### Iceberg-backed MV

`src/connector/starrocks/managed/mv_refresh_iceberg.rs` 当前是 insert-only 增量路径。若 change batch 包含 delete branch，应保守 full refresh，不应只应用 inserts。

## 错误策略

### Append

added data files 可读时走 insert delta。manifest I/O 或 metadata 不可解释时返回 planner error，由上层策略决定 full refresh 或 unsupported。

### Delete

added position delete、Puffin DV、equality delete 走 delete delta。遇到不支持的 delete file format、缺失 DV offset/size、缺失 equality ids 时，不生成错误增量。

### Overwrite

以 manifest diff 为准：

- added + deleted：delete old rows + insert new rows。
- only added：等价 insert delta。
- only deleted：等价 delete/truncate delta。
- manifest diff 读取失败或 deleted data file 无法扫描：fallback full refresh，而不是依赖 sidecar 或跳过。

### Replace

只有通过 compaction no-op 校验时跳过。不能证明逻辑等价时 fallback full refresh 或 unsupported。绝不把 replace 的 added/deleted files 当业务 update。

### Lineage / schema / identity

- previous snapshot 不可达：full refresh。
- base table uuid 变化：full refresh。
- schema evolution：除非现有逻辑已经证明安全，否则 unsupported 或 full refresh。
- 不猜测缺失字段、不降级类型、不使用私有 marker 修正标准 metadata。

## 外部 Writer 覆盖

设计对外部 writer 的解释方式：

- Spark COW UPDATE / MERGE：通常表现为 overwrite snapshot 中 deleted data files + added data files，按标准 diff 增量。
- Spark MOR：通常表现为 delete snapshot 或 row delta 的 added delete files，可能伴随 added data files，按 delete files + inserts 增量。
- Flink equality delete / upsert：equality delete 通过现有 equality delete reverse projection 进入 delete branch。
- Trino / Athena position delete 或 overwrite：position delete 走 delete-file branch；overwrite 走 deleted data + added data branch。
- Compaction / rewrite：`Replace` 只在证明 no-op 时跳过，否则保守 fallback。

## 测试计划

### `changes.rs`

- 普通 `Operation::Overwrite` 分类为 `CollectOverwriteDiff`，不再 rejected。
- 无 NovaRocks sidecar 的 overwrite snapshot 可以收集 added data files 和 deleted data files。
- 旧 NovaRocks marker 即使存在也不被读取；结果仍由 manifest entries 决定。
- `Replace` compaction 仍 no-op。
- 无法证明安全的 `Replace` 不被误判为业务 update。

### COW UPDATE 写入

- COW UPDATE commit 后 snapshot summary 不包含：
  - `novarocks.row-level-op`
  - `novarocks.update.mode`
  - `novarocks.update.sidecar`
- COW UPDATE commit 仍产生标准 overwrite snapshot。
- `plan_changes` 能从该 snapshot 的 manifest diff 看到 old files delete + replacement files insert。

### MV 端到端

- managed-lake aggregate MV：COW UPDATE 后 refresh，old row 被 retract，new row 被 apply。
- 同一测试断言 COW UPDATE snapshot 无 sidecar marker。
- compaction/replace 不被当成业务 update；不能证明 no-op 时走 full refresh 或 unsupported。

## 验证命令

实现完成后运行：

```bash
cargo fmt
cargo test -p novarocks connector::iceberg::changes
cargo test -p novarocks connector::iceberg::commit::update_cow
cargo test -p novarocks connector::starrocks::managed::mv_refresh
```

涉及 Docker/Iceberg REST 或 SQL/MV 环境时，必须先发现当前 worktree 环境：

```bash
source docker/iceberg-rest/runtime/current/env.sh
```

不硬编码 NovaRocks MySQL port 或对象存储端口。

## 实施顺序

1. 删除持久 sidecar 类型和 summary constants 的 public API。
2. 将 COW UPDATE 写入改成标准 overwrite summary 和 manifest diff。
3. 在 `changes.rs` 引入 `DeletedDataFileRef` 和 `CollectOverwriteDiff`。
4. 用标准 deleted data files 替换 sidecar old-row scan。
5. 更新 managed-lake MV delete detection。
6. 让 Iceberg-backed MV 在 delete-bearing change batch 上 full refresh。
7. 添加 focused tests 并运行验证。

## 成功标准

- 新 COW UPDATE snapshot 不含 NovaRocks sidecar marker。
- 无 sidecar 的 overwrite snapshot 能通过 manifest diff 产生 delete old rows + insert new rows。
- NovaRocks 自己写的 COW UPDATE 在没有 sidecar 的情况下仍能正确刷新 MV。
- Replace/compaction 不被误判成业务 update。
- MV 增量刷新在不能安全增量时保守 fallback，而不是返回错误结果。
