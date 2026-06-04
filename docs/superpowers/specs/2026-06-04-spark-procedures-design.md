# NovaRocks 对齐 Iceberg Spark Procedures 设计

日期：2026-06-04

## 背景

IV3-5 原本关注 deletion vector compaction 和 multi-blob Puffin。调研 Iceberg Spark 后，任务方向调整为对齐 Spark procedures：NovaRocks 不新增专属 DV compaction SQL，而是提供 Spark-style `CALL <catalog>.system.<procedure>(...)` 入口，并把 DV rewrite 收敛到 `rewrite_position_delete_files` 语义。

本设计采用方案 2：先建立统一 Iceberg maintenance/procedure action 框架，再让 Spark `CALL` 和现有 legacy `ALTER TABLE ...` 维护命令共用这套执行层。

## 上游基准

参考来源：

- 官方文档：[Apache Iceberg Spark Procedures](https://apache.github.io/iceberg/docs/latest/spark-procedures/)
- 上游源码：`apache/iceberg` main，commit `4b4c5b5`，重点文件：
  - `spark/v4.0/spark/src/main/java/org/apache/iceberg/spark/procedures/SparkProcedures.java`
  - `spark/v4.0/spark/src/main/java/org/apache/iceberg/spark/procedures/RewritePositionDeleteFilesProcedure.java`
  - `core/src/main/java/org/apache/iceberg/DVUtil.java`
  - `core/src/main/java/org/apache/iceberg/deletes/BaseDVFileWriter.java`

Spark 当前注册的 procedures：

| 分组 | procedures |
| --- | --- |
| 快照 / ref 管理 | `rollback_to_snapshot`, `rollback_to_timestamp`, `set_current_snapshot`, `cherrypick_snapshot`, `fast_forward`, `ancestors_of` |
| 表维护 | `rewrite_data_files`, `rewrite_manifests`, `rewrite_position_delete_files`, `expire_snapshots`, `remove_orphan_files`, `rewrite_table_path` |
| 迁移 / 注册 | `migrate`, `snapshot`, `add_files`, `register_table` |
| WAP / changelog / stats | `publish_changes`, `create_changelog_view`, `compute_table_stats`, `compute_partition_stats` |

本任务第一版只实现维护类优先集合：`rewrite_data_files`、`rewrite_manifests`、`expire_snapshots`、`remove_orphan_files`、`rewrite_position_delete_files`。

## 当前 NovaRocks 差距

- 没有 `CALL <catalog>.system.<procedure>` parser 和 dispatcher。
- 已有维护能力使用 NovaRocks 自有 `ALTER TABLE ...` 入口。
- `ALTER TABLE ... OPTIMIZE` 是异步 job 语义，和 Spark `CALL system.rewrite_data_files` 的同步结果语义不同。
- `rewrite_manifests`、`expire_snapshots`、`remove_orphan_files` 已有执行函数，但参数和结果 schema 未按 Spark procedure 暴露。
- 当前 V3 DELETE 写侧按 touched data file 写单 blob Puffin DV，缺少 procedure 触发的 multi-blob DV repack。

## 目标

1. 增加 Spark-style procedure SQL surface。
2. 建立统一 Iceberg maintenance action 层。
3. 让 Spark `CALL` 和 legacy `ALTER TABLE ...` 都路由到统一 action。
4. 对齐已有维护能力的 Spark 参数和结果 schema。
5. 实现 `rewrite_position_delete_files` 的 V3 Puffin DV repack。
6. 保留 legacy `ALTER TABLE ...` 行为，不引入用户可见回退。

## 非目标

- 不实现 V2 Parquet position delete rewrite。遇到 V2 position delete file 时直接报错。
- 不新增 NovaRocks 专属 `ALTER TABLE ... COMPACT DELETION VECTORS` 主入口。
- 不一次性实现全部 20 个 Spark procedures。
- 不支持 `rewrite_position_delete_files` 的 row predicate `where`。第一版传入 `where` 直接报错。
- 不照搬 Spark 分布式 execution 模型。
- 不把 legacy `ALTER TABLE ... OPTIMIZE` 强制改成同步命令。

## 总体架构

```text
SQL surface
  |- CALL <catalog>.system.<procedure>(...)
  `- ALTER TABLE ... legacy maintenance syntax

        |
        v

Iceberg procedure / maintenance dispatcher
  |- procedure name or legacy command kind
  |- normalized catalog and table identifier
  |- named or positional args normalized to named args
  |- options
  `- source: SparkProcedure or LegacyAlter

        |
        v

Unified maintenance actions
  |- RewriteDataFiles
  |- RewriteManifests
  |- ExpireSnapshots
  |- RemoveOrphanFiles
  `- RewritePositionDeleteFiles

        |
        v

Iceberg commit/action implementation
  |- existing rewrite_manifests / expire_snapshots / remove_orphan_files
  |- existing OPTIMIZE rewrite_data_files execution core
  `- new V3 Puffin DV repack path

        |
        v

QueryResult
  |- Spark-compatible schema for CALL
  `- legacy-compatible status/result for ALTER when required
```

统一的是 action 层，不强迫 SQL surface 完全一致。`CALL` 返回 Spark procedure schema；legacy `ALTER` 保留现有用户可见行为，但底层执行核心与 Spark procedure 共享。

## CALL 语法

第一版只支持 Iceberg catalog 的 `system` namespace。

示例：

```sql
CALL ice.system.rewrite_manifests(table => 'db.t');

CALL ice.system.expire_snapshots(
  table => 'db.t',
  older_than => TIMESTAMP '2026-01-01 00:00:00'
);

CALL ice.system.remove_orphan_files(
  table => 'db.t',
  older_than => TIMESTAMP '2026-01-01 00:00:00'
);

CALL ice.system.rewrite_data_files(
  table => 'db.t',
  options => map('rewrite-all', 'true')
);

CALL ice.system.rewrite_position_delete_files(
  table => 'db.t',
  options => map('rewrite-all', 'true')
);
```

解析规则：

- procedure 名称大小写不敏感。
- 参数名大小写不敏感，内部统一为 Spark 小写参数名。
- named arguments 是推荐形式。
- positional arguments 允许作为兼容形式，但解析后立即规范化成 named args。
- 不允许混用 named 和 positional arguments。
- `table` 使用 string 值，支持 `'db.t'` 和 `'catalog.db.t'`。如果未写 catalog，绑定到 `CALL` 的 catalog。
- `options` 使用 `map('k','v',...)`，key/value 第一版按字符串处理。
- 非 Iceberg catalog、非 `system` namespace、未知 procedure、重复参数、未知参数都报错。

## Action 请求和结果模型

新增统一请求类型，概念字段如下：

```text
MaintenanceActionRequest
  - source: SparkProcedure | LegacyAlter
  - catalog_name
  - table_name
  - action_kind
  - args
  - options
  - branch
  - filter
```

新增统一结果类型，概念字段如下：

```text
MaintenanceActionOutcome
  - action_kind
  - metrics
  - previous_snapshot_id
  - current_snapshot_id
  - warnings
```

`MaintenanceActionOutcome` 不直接等于最终 `QueryResult`。它先保存 action 语义结果，再由 surface 转换：

- `SparkProcedure` 转成 Spark-compatible schema。
- `LegacyAlter` 转成已有 legacy 行为需要的状态、job 或结果。

## Procedures 第一版模型

| Action | Spark procedure | 参数 | CALL 输出 |
| --- | --- | --- | --- |
| `RewriteManifests` | `rewrite_manifests` | `table`, `use_caching`, `spec_id` | `rewritten_manifests_count`, `added_manifests_count` |
| `ExpireSnapshots` | `expire_snapshots` | `table`, `older_than`, `retain_last`, `max_concurrent_deletes`, `stream_results`, `snapshot_ids`, `clean_expired_metadata` | `deleted_data_files_count`, `deleted_position_delete_files_count`, `deleted_equality_delete_files_count`, `deleted_manifest_files_count`, `deleted_manifest_lists_count`, `deleted_statistics_files_count` |
| `RemoveOrphanFiles` | `remove_orphan_files` | `table`, `older_than`, `location`, `dry_run`, `max_concurrent_deletes`, `file_list_view`, `equal_schemes`, `equal_authorities`, `prefix_mismatch_mode`, `prefix_listing`, `stream_results` | `orphan_file_location` |
| `RewriteDataFiles` | `rewrite_data_files` | `table`, `strategy`, `sort_order`, `options`, `where`, `branch` | `rewritten_data_files_count`, `added_data_files_count`, `rewritten_bytes_count`, `failed_data_files_count`, `removed_delete_files_count` |
| `RewritePositionDeleteFiles` | `rewrite_position_delete_files` | `table`, `options`, `where` | `rewritten_delete_files_count`, `added_delete_files_count`, `rewritten_bytes_count`, `added_bytes_count` |

参数可以先解析但分阶段支持。无法正确执行的参数必须报错，不能静默忽略。

## `rewrite_data_files` 执行模式

NovaRocks 当前 `ALTER TABLE ... OPTIMIZE` 是异步 job，Spark `CALL system.rewrite_data_files` 是同步 action。统一 action 层要支持两种 source：

```text
LegacyAlter + RewriteDataFiles
  -> 保留异步 job 和 SHOW ALTER TABLE OPTIMIZE 行为

SparkProcedure + RewriteDataFiles
  -> 同步执行，返回 Spark procedure result schema
```

两种入口共享参数规范化、表解析、核心 rewrite commit 能力和 metrics 汇总，但不强行统一用户可见执行模式。

## `rewrite_position_delete_files` V3 语义

第一版实现 V3 Puffin deletion vector repack，不实现 V2 Parquet position delete rewrite。

执行流程：

1. 加载当前 snapshot 和 delete manifests。
2. 找出当前 live metadata 中的 Puffin DV delete entries。
3. 如果发现 V2 Parquet position delete file，直接报 unsupported。
4. 如果传入 `where`，直接报 unsupported。
5. 按 `referenced-data-file` 聚合 DV entries。
6. 对需要 repack 的 DV，使用 `file_path + content_offset + content_size_in_bytes` 精确读取旧 blob。
7. 合并同一个 referenced data file 的 bitmap。
8. 写出一个或多个 multi-blob Puffin。每个 blob 对应一个 referenced data file。
9. 生成新的 delete manifest entries，更新 `file_path`、`content_offset`、`content_size_in_bytes`、`file_size_in_bytes`、`record_count` 等元数据。
10. 提交只替换 delete files / delete manifests，不重写 data files。
11. 返回 Spark-compatible metrics。

no-op 规则：

- 如果没有 position delete files 或 DV delete files，返回 0 指标。
- 如果只有一个无需合并的 DV 文件，返回 0 指标。
- 不制造空维护 snapshot。

option 支持：

- `rewrite-all`
- `min-input-files`
- `target-file-size-bytes`

选择规则：

- `rewrite-all=true` 时，所有 V3 Puffin DV entries 都进入 repack 候选集。
- `rewrite-all=false` 或未设置时，只有满足 `min-input-files` 的输入组进入 repack。输入组按待合并 Puffin DV entries 计数，默认 `min-input-files=2`。
- `target-file-size-bytes` 控制单个输出 Puffin 文件的目标大小；未设置时沿用 NovaRocks Iceberg writer 的默认目标大小。
- 如果候选集为空，返回 0 指标，不提交 snapshot。

其他 Spark option 第一版报 unsupported。后续可以逐步增加 `max-file-group-size-bytes`、`rewrite-job-order`、partial progress 等。

## V3 DV 正确性要求

- 读取 Puffin 必须尊重 `content_offset` 和 `content_size_in_bytes`。
- 写出的 Puffin footer 必须可被 Iceberg reader 和 NovaRocks reader 识别。
- 每个新 DV entry 的 `referenced-data-file` 必须指向原 data file。
- `record_count` 必须等于 DV cardinality。
- `data_sequence_number`、partition spec id、partition tuple 必须与被合并 DV 兼容；不兼容时 fail fast。
- 提交后查询结果必须保持不变。
- 并发提交冲突沿用 Iceberg optimistic commit/retry 语义。

## 错误处理

必须明确报错的场景：

- 非 Iceberg catalog 调用 system procedure。
- namespace 不是 `system`。
- procedure 未注册。
- 参数缺失、重复、未知或 named/positional 混用。
- option 未支持。
- `rewrite_position_delete_files` 遇到 V2 Parquet position delete file。
- `rewrite_position_delete_files` 传入 `where`。
- DV blob metadata 与 manifest entry 不一致。
- 合并 DV 的 sequence number、partition spec 或 partition tuple 不兼容。

所有错误在 parser、dispatcher 或 action planning 阶段尽早暴露；不得用 best-effort 行为掩盖语义差异。

## 测试计划

### Parser / dispatcher 单测

- `CALL ice.system.rewrite_manifests(table => 'db.t')`
- `CALL ice.system.rewrite_position_delete_files(table => 'db.t', options => map('rewrite-all', 'true'))`
- positional arguments 规范化。
- 非 `system` namespace 报错。
- 未知 procedure 报错。
- 重复参数、未知参数、named/positional 混用报错。

### Action 层单测

- legacy `ALTER` 和 Spark `CALL` 都能生成统一 `MaintenanceActionRequest`。
- 每个 action outcome 都能转换成 Spark schema。
- `rewrite_data_files` 支持 `SparkProcedure` 同步执行和 `LegacyAlter` 异步 job。
- `rewrite_position_delete_files` 对 V2 position delete 报错。
- `rewrite_position_delete_files` 对无 DV 场景返回 0。
- `rewrite_position_delete_files` 对 V3 多 DV 场景进入 repack。

### SQL tests

- 新增 `sql-tests/iceberg` procedure 入口 cases。
- 覆盖 named arguments、positional arguments、结果列名和错误用例。
- 覆盖 legacy `ALTER TABLE ...` 仍可用。
- V3 场景：多次 DELETE 产生多个 Puffin DV 后，`CALL ... rewrite_position_delete_files` 重写为 multi-blob Puffin，查询结果不变。
- 负例：V2 position delete、`where`、unsupported option 都返回明确错误。

## 分阶段落地

1. 新增 `CALL` 识别、解析和 procedure dispatcher。
2. 新增统一 `MaintenanceActionRequest` / `MaintenanceActionOutcome`。
3. 将 `rewrite_manifests`、`expire_snapshots`、`remove_orphan_files` 接入统一 action。
4. 将 `rewrite_data_files` 的执行核心接入统一 action，同时保留 legacy async job surface。
5. 新增 `rewrite_position_delete_files` action 和 V3 Puffin DV repack。
6. 补齐 SQL tests 和 Rust 单测。

## 代码入口

- `src/engine/statement.rs`
- `src/engine/mod.rs`
- `src/engine/iceberg_rewrite_manifests.rs`
- `src/engine/iceberg_expire_snapshots.rs`
- `src/engine/iceberg_remove_orphan_files.rs`
- `src/connector/iceberg/compact.rs`
- `src/connector/iceberg/commit/rewrite_data_files.rs`
- `src/connector/iceberg/commit/rewrite_manifests.rs`
- `src/connector/iceberg/commit/expire_snapshots.rs`
- `src/connector/iceberg/commit/remove_orphan_files.rs`
- `src/connector/iceberg/commit/row_delta_dv.rs`
- `src/connector/iceberg/commit/puffin_dv.rs`
- `src/connector/iceberg/stats_assembler.rs`
- `vendor/iceberg-0.9.0/src/puffin/writer.rs`
- `vendor/iceberg-0.9.0/src/puffin/reader.rs`

## 设计决策记录

- 选择方案 2：先统一 maintenance action 层，再接 Spark `CALL` 和 legacy `ALTER`。
- 第一版范围是 P0 + P1 + P2。
- P2 只实现 V3 Puffin DV repack。
- V2 Parquet position delete rewrite 明确不实现，遇到即报错。
- `where` 第一版不支持。
- existing legacy `ALTER TABLE ...` 入口继续保留。
