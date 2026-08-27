<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# 维护 / 治理

> Iceberg 的运维操作包括压缩小文件、清理历史 snapshot、回收孤儿文件、重写 manifest 与重写 deletion vector。NovaRocks 支持 whole-table OPTIMIZE、EXPIRE SNAPSHOTS、REMOVE ORPHAN FILES、REWRITE MANIFESTS 与 V3 Puffin deletion-vector repack；增量压缩、sort-order rewrite 和通用定时调度仍待补。

| 能力 | 状态 | 备注 |
| --- | --- | --- |
| `ALTER TABLE ... OPTIMIZE`（whole-table 文件压缩） | ✅ | 异步 job |
| `OPTIMIZE` 增量（仅小文件 / 仅 partition） | ❌ | |
| `EXPIRE SNAPSHOTS` | ✅ | 同步 action |
| `REMOVE ORPHAN FILES` | ✅ | 同步 action |
| `REWRITE MANIFESTS` | ✅ | 同步 action |
| `rewrite_position_delete_files`（V3 Puffin DV repack） | ✅ | 同步 Spark-style `CALL` |
| V2 Parquet position delete → V3 DV 升级 | ❌ | |
| `REWRITE DATA FILES BY SORT ORDER` | ❌ | |
| 自动 maintenance 调度器 | ❌ | |

---

## Owner、执行边界与 StateStore

maintenance 的三层职责是固定的：

- `novarocks-frontend` 的 `TableMaintenanceService` 是 application owner，负责 SQL
  路由、异步 optimize job repository/worker 与生命周期。
- core 通过 consumer-owned `TableMaintenanceEngine` port 提供 target resolve 与执行能力，
  不再拥有第二套 maintenance job service。
- Iceberg connector 仍是 catalog、snapshot/file、rewrite/expire/orphan、commit、cache
  invalidation 与 MV target snapshot adopt 的唯一执行 truth。

异步 optimize job 只持久化到 frontend StateStore。单 FE 部署的最小配置如下：

```toml
[state_store]
provider = "sqlite"
path = "/absolute/path/frontend-state.sqlite"
cluster_id = "cluster-a"
```

重启 FE 时复用同一绝对 `path` 与 `cluster_id`，已完成的
`SHOW ALTER TABLE OPTIMIZE` 历史仍可见，新 job 也可继续提交。未配置 StateStore 时，
`ALTER TABLE ... OPTIMIZE`、`SHOW ALTER TABLE OPTIMIZE` 与 automatic optimize 会返回明确的
unavailable error；`REWRITE MANIFESTS`、`EXPIRE SNAPSHOTS`、`REMOVE ORPHAN FILES` 和
Spark-style maintenance `CALL` 等同步 action 仍可直接执行。

当前 lifecycle 只支持单 FE owner。关闭 FE 时会先停止并 join maintenance worker，再释放
StateStore；它不提供多 FE lease、takeover 或 fencing。多 FE active/standby 的 claim 与
destructive-action fencing 属于后续 CP-4。

## ✅ Whole-table OPTIMIZE

```sql
ALTER TABLE orders OPTIMIZE;
SHOW ALTER TABLE OPTIMIZE
  FROM iceberg_catalog.database
  WHERE TableName = 'orders'
  ORDER BY CreateTime DESC
  LIMIT 10;
```

`ALTER TABLE ... OPTIMIZE` 在 job 成功写入 StateStore 后返回，rewrite 在 frontend worker
中异步执行。调用方必须轮询 `SHOW ALTER TABLE OPTIMIZE`，并以 `FINISHED` 或 `FAILED`
作为终态。Spark-style `CALL ...rewrite_data_files(...)` 则保持同步，并直接返回 action
结果。

行为：

- 把当前 snapshot 的所有 data file 重写到一组新 file（按当前 partition spec）
- 同时合并 V2 position-delete / V3 DV：被删行不出现在新 file 中，DV blob 在重写完成后失效
- 跨历史 partition spec：所有老文件按其 spec 解释，重写到当前 spec
- 写出新 manifest，老文件由 EXPIRE SNAPSHOTS 路径回收
- V3 row-lineage 表保留现有 `_row_id` 与 `_last_updated_sequence_number`，rewrite 不分配新 row ID

实现入口：`novarocks/core/src/connector/iceberg/compact.rs`。

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

`novarocks/core/src/connector/iceberg/commit/expire_snapshots.rs`

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

`novarocks/core/src/connector/iceberg/commit/remove_orphan_files.rs`

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

`novarocks/core/src/connector/iceberg/commit/rewrite_manifests.rs`

## ✅ REWRITE POSITION DELETE FILES

```sql
CALL iceberg_catalog.system.rewrite_position_delete_files(
  table => 'database.orders',
  options => map('rewrite-all', 'true')
);
```

该同步 action 重写 Iceberg V3 表中现有 Puffin deletion-vector 文件，保持可见 row set、
`_row_id` 与 live data files 不变，并返回 rewritten/added file 与 byte counts。当前不支持
`where`、`target-file-size-bytes` 或 V2 Parquet position delete → V3 DV 的格式升级。

实现入口：
`novarocks/core/src/connector/iceberg/commit/rewrite_position_delete_files.rs`。

## ❌ REWRITE DATA FILES BY SORT ORDER

Spec：按 Iceberg sort order 物理重排数据文件，让后续 sort-merge join / range scan 更高效。

```sql
-- 暂未实现
ALTER TABLE orders REWRITE DATA FILES BY SORT ORDER (user_id, ts DESC);
```

**TODO**：未实现。当前 OPTIMIZE 不感知 sort order。

## ❌ 自动 maintenance 调度器

Spec / 工程实践：基于 schedule 自动跑 OPTIMIZE / EXPIRE / ORPHAN，类似 Snowflake auto-clustering 或 Databricks `OPTIMIZE` cron。

frontend service 已承接 MV policy 发出的 typed automatic action/optimize submission，但尚未提供
面向普通表的通用 schedule。运维方仍需用外部 cron / Airflow 调度；其中异步
`ALTER TABLE ... OPTIMIZE` 必须继续轮询 `SHOW ALTER TABLE OPTIMIZE` 到终态。
