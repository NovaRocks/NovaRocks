# Metadata Tables

> Spec 定义的虚拟表，用来查表内部状态（snapshot 历史、refs、文件统计、partition 统计 ...）。PR #81 落地了 BE 端的 `snapshots / history / refs / partitions` 四张，PR #85 接通了 standalone parser 的 `<tbl>$<metatype>` 路由 —— **首批四张端到端 SQL 现在可用**。其余 metadata table（`$manifests` / `$files` / `$delete_files` / `$entries` / `$metadata_log_entries` / `$position_deletes` / `$all_files` 等）仍在路线图上。

| 能力 | 状态 | 备注 |
| --- | --- | --- |
| `<tbl>$snapshots` | ✅ | BE PR #81 + SQL 路由 PR #85 |
| `<tbl>$history` | ✅ | 同上 |
| `<tbl>$refs`（branches + tags） | ✅ | 同上 |
| `<tbl>$partitions` | ✅ | 同上 |
| `<tbl>$manifests` | ❌ | |
| `<tbl>$files` / `<tbl>$all_data_files` | ❌ | |
| `<tbl>$delete_files` / `<tbl>$all_delete_files` | ❌ | |
| `<tbl>$entries`（manifest entries） | ❌ | |
| `<tbl>$metadata_log_entries` | ❌ | |
| `<tbl>$position_deletes` | ❌ | |
| `<tbl>$all_files`（V3 统一视图） | ❌ | |

实现入口：

- 读路径 op：`src/connector/iceberg/metadata.rs::IcebergMetadataScanOp`（native Rust，基于 vendored iceberg-rust 0.9 的 `iceberg::spec::TableMetadata`，不再走 JNI/Java SDK）
- Parser 路由：`src/sql/parser/dialect/mod.rs`（`<tbl>$<metatype>` 重写为 `<tbl>.__nr_meta_<metatype>__`）
- Analyzer：`src/sql/analyzer/iceberg_metadata.rs`（`split_metadata_suffix`）+ `Relation::IcebergMetadataScan` variant
- 当前覆盖：snapshots / history / refs。Files / Manifests / Partitions / LogicalIcebergMetadata 在 op 构造时 fail-fast 报 `not yet implemented in the native-Rust scan path`，留给后续 PR 用 iceberg-rust 的 manifest/scan API 补

参考 plan：[2026-05-06-iceberg-v3-row-lineage-completion.md](../../design/plans/2026-05-06-iceberg-v3-row-lineage-completion.md)（Phase A）

---

## ✅ `<tbl>$snapshots`

列出表的所有 snapshot 与基本元信息。

```sql
SELECT * FROM orders$snapshots;
```

| 列 | 类型 | 说明 |
| --- | --- | --- |
| `committed_at` | TIMESTAMP | snapshot 提交时刻 |
| `snapshot_id` | BIGINT | snapshot ID |
| `parent_id` | BIGINT | 父 snapshot ID（root 为 NULL） |
| `operation` | STRING | `append` / `overwrite` / `delete` / `replace` 等 |
| `manifest_list` | STRING | manifest list 文件路径 |
| `summary` | MAP&lt;STRING, STRING&gt; | snapshot summary（写入行数 / 字节数 / 删除行数等） |

## ✅ `<tbl>$history`

列出"什么时候哪个 snapshot 变成 current"的时间线。

```sql
SELECT * FROM orders$history;
```

| 列 | 类型 |
| --- | --- |
| `made_current_at` | TIMESTAMP |
| `snapshot_id` | BIGINT |
| `parent_id` | BIGINT |
| `is_current_ancestor` | BOOLEAN |

`is_current_ancestor=true` 表示该 snapshot 是当前 current snapshot 的祖先（直接 / 间接），方便区分主线和已经被覆盖 / rollback 掉的旁支。

## ✅ `<tbl>$refs`

列出表的所有 branch / tag。

```sql
SELECT * FROM orders$refs;
```

| 列 | 类型 |
| --- | --- |
| `name` | STRING |
| `type` | STRING（`BRANCH` / `TAG`） |
| `snapshot_id` | BIGINT |
| `min_snapshots_to_keep` | INT |
| `max_snapshot_age_ms` | BIGINT |
| `max_ref_age_ms` | BIGINT |

## ✅ `<tbl>$partitions`

列出表当前的 partition 一览，含每个分区的文件 / 行数。

```sql
SELECT * FROM orders$partitions;
```

| 列 | 类型 | 说明 |
| --- | --- | --- |
| 分区列（按表当前 partition spec） | varies | `country='CN'` / `bucket(8, id)` 等都按 transform 后的值出现 |
| `record_count` | BIGINT | live data record 数 |
| `file_count` | BIGINT | live data file 数 |
| `position_delete_file_count` | BIGINT | V2 position-delete 文件数 |
| `equality_delete_file_count` | BIGINT | equality-delete 文件数 |

> **注意**：当前不暴露 `dv_count`（V3 deletion vector），未来扩展。`$partitions` 只反映 current snapshot 的 partition 一览；要查历史 partition spec 的文件用 time travel + `$partitions`。

---

## 与 Spark / Trino 兼容性

NovaRocks 的语法用 `<tbl>$<metatype>`（`$` 分隔），与 Spark 一致：

- ✅ NovaRocks: `SELECT * FROM orders$snapshots`
- ✅ Spark: `SELECT * FROM orders$snapshots` 或 `SELECT * FROM orders.snapshots`

> Spark 也支持 `<tbl>.<metatype>` 形式（点号），NovaRocks 当前**只接受 `$` 形式**，因为点号会与 schema/database 限定名歧义。如果你从 Spark 搬 SQL，把 `.snapshots` 换成 `$snapshots` 即可。

---

## ❌ 其他 metadata table

下列 spec 要求的 metadata table 在 BE 和 SQL 两端都未实现：

- `$manifests`：列出 current snapshot 的所有 manifest 文件（path / length / partition spec / added/existing/deleted file counts ...）
- `$files`：current snapshot 的 live data file 列表（path / file_format / record_count / file_size_in_bytes / column stats / partition）
- `$all_data_files`：所有 snapshot 的并集
- `$delete_files`：current snapshot 的 delete file 列表（V2 position-delete + equality-delete + V3 DV）
- `$all_delete_files`：所有 snapshot 的并集
- `$entries`：manifest entries（包含 added / existing / deleted 状态、sequence number ...）
- `$metadata_log_entries`：每次 metadata.json 切换的日志
- `$position_deletes`：所有 V2 position-delete 行展开
- `$all_files`（V3）：data file + delete file 的统一视图

**TODO**：路线图按需推进，BE bridge 可以参考 PR #81 的 4 张实现。

---

## 时间旅行 + metadata table

可以组合 time travel 与 metadata table 查"某个历史 snapshot 当时的状态"：

```sql
-- 历史 partition 一览
SELECT * FROM orders$partitions FOR VERSION AS OF 1234567890;

-- 历史 ref 列表
SELECT * FROM orders$refs FOR TIMESTAMP AS OF '2026-05-01 00:00:00';
```
