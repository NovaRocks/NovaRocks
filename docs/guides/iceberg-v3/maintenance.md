# 维护 / 治理

> Iceberg 的运维操作：压缩小文件、清理历史 snapshot、回收孤儿文件、重写 manifest 等。NovaRocks 支持 OPTIMIZE TABLE（whole-table 压缩）、EXPIRE SNAPSHOTS、REMOVE ORPHAN FILES、REWRITE MANIFESTS；增量压缩、自动调度等高级治理能力仍待补。

| 能力 | 状态 | 备注 |
| --- | --- | --- |
| `OPTIMIZE TABLE`（whole-table 文件压缩） | ✅ | `src/connector/iceberg/compact.rs` |
| `OPTIMIZE TABLE` 增量（仅小文件 / 仅 partition） | ❌ | |
| `EXPIRE SNAPSHOTS` | ✅ | `src/connector/iceberg/commit/expire_snapshots.rs` |
| `REMOVE ORPHAN FILES` | ✅ | `src/connector/iceberg/commit/remove_orphan_files.rs` |
| `REWRITE MANIFESTS` | ✅ | `src/connector/iceberg/commit/rewrite_manifests.rs` |
| `REWRITE POSITION DELETES`（v2→DV） | ❌ | |
| `REWRITE DATA FILES BY SORT ORDER` | ❌ | |
| 自动 maintenance 调度器 | ❌ | |

---

## ✅ OPTIMIZE TABLE

```sql
ALTER TABLE orders OPTIMIZE;
```

行为：

- 把当前 snapshot 的所有 data file 重写到一组新 file（按当前 partition spec）
- 同时合并 V2 position-delete / V3 DV：被删行不出现在新 file 中，DV blob 在重写完成后失效
- 跨历史 partition spec：所有老文件按其 spec 解释，重写到当前 spec
- 写出新 manifest，老文件由 EXPIRE SNAPSHOTS 路径回收

> ⚠️ 当前 OPTIMIZE 重写后**会重新分配 `_row_id`**（详见 [row-lineage](row-lineage.md)）。spec 允许保留 / 重新分配，NovaRocks 还没决定最终策略。

## ❌ OPTIMIZE 增量

Spec：

```sql
-- 暂未实现
ALTER TABLE orders OPTIMIZE WHERE size_in_bytes < 16777216;     -- 仅压小文件
ALTER TABLE orders OPTIMIZE WHERE country = 'CN';               -- 仅压指定 partition
```

**TODO**：未实现。当前只能 whole-table 压缩，对大表代价高。

## ✅ EXPIRE SNAPSHOTS

### 行为

删除 `metadata.json` 中不被任何 ref 祖先链覆盖的快照，物理删除其 orphan 文件。OLDER THAN 和 RETAIN LAST 可单独或组合使用，两个条件取交集：只有同时满足"早于时间戳"且"不属于最近 N 个"的 snapshot 才会被删。

### 示例

```sql
ALTER TABLE orders EXPIRE SNAPSHOTS OLDER THAN '2026-04-01 00:00:00';
ALTER TABLE orders EXPIRE SNAPSHOTS RETAIN LAST 5;
ALTER TABLE orders EXPIRE SNAPSHOTS OLDER THAN '2026-04-01 00:00:00' RETAIN LAST 5;
```

### 支持的子集

- Branch / Tag 当前指向的 snapshot 永不过期（所有 ref 头保护）
- RETAIN LAST 仅对 main ancestor chain 生效
- per-branch retention 属性（`branch.<n>.min-snapshots-to-keep` 等）**未读取**
- 至少要给一个 OLDER THAN 或 RETAIN LAST，否则拒绝（防止误清全部历史）
- 不支持 `t.branch_<x>` 后缀（parse-time reject）

### 入口

`src/connector/iceberg/commit/expire_snapshots.rs`

## ✅ REMOVE ORPHAN FILES

### 行为

扫描 warehouse 下 `data/` + `metadata/` 路径，找到不被 `metadata.json` 中任何 snapshot 引用的文件，按 OLDER THAN 阈值过滤后物理删除。不提交新 snapshot，不更新 metadata.json。

### 示例

```sql
ALTER TABLE orders REMOVE ORPHAN FILES OLDER THAN '2026-04-01 00:00:00';
```

### 支持的子集

- OLDER THAN **强制**（建议 ≥ 3 天，防御 in-flight 写入误删）
- 保护当前 `metadata.json` + metadata-log 中所有历史 `metadata.json`
- DV puffin 半引用保护：任一 blob 关联 live data file → 整个 puffin 文件保留
- 支持 `file://`、`s3://`、`oss://` scheme；`hdfs://` **暂未实现**
- 不支持 `t.branch_<x>` 后缀（parse-time reject）

### 入口

`src/connector/iceberg/commit/remove_orphan_files.rs`

## ✅ REWRITE MANIFESTS

### 行为

按 `(partition_spec_id, content_type)` 分组将多个 manifest 合并为单个 manifest，发出 `operation=replace` 快照。不移动或重写 data file，仅重建 manifest 层。

### 示例

```sql
ALTER TABLE orders REWRITE MANIFESTS;
```

### 支持的子集

- 单 manifest / 空表 / 全 singleton 组 → noop（不写新快照）
- V3 row-lineage 字段（`first_row_id`、`referenced_data_file` 等）保留 round-trip
- DELETED entry 在合并时丢弃；ADDED + EXISTING 都改成 EXISTING
- `snapshot.sequence_number` 严格 +1（catalog 不变量），但 entry-level `data_sequence_number` / `file_sequence_number` 保留原值
- 不支持 `t.branch_<x>` 后缀（parse-time reject）

### 入口

`src/connector/iceberg/commit/rewrite_manifests.rs`

## ❌ REWRITE POSITION DELETES

Spec：把 V2 position-delete 重写成 V3 deletion vector（升级路径）。

**TODO**：未实现。如果你要从 V2 迁移到 V3，目前需要 DELETE 全部数据再重写，或借助外部工具。

## ❌ REWRITE DATA FILES BY SORT ORDER

Spec：按 Iceberg sort order 物理重排数据文件，让后续 sort-merge join / range scan 更高效。

```sql
-- 暂未实现
ALTER TABLE orders REWRITE DATA FILES BY SORT ORDER (user_id, ts DESC);
```

**TODO**：未实现。当前 OPTIMIZE 不感知 sort order。

## ❌ 自动 maintenance 调度器

Spec / 工程实践：基于 schedule 自动跑 OPTIMIZE / EXPIRE / ORPHAN，类似 Snowflake auto-clustering 或 Databricks `OPTIMIZE` cron。

**TODO**：未实现。运维方需要自己用外部 cron / Airflow 调度。
