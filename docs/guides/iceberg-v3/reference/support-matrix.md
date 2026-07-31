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

# Iceberg v3 完整支持矩阵

> 按 Iceberg v3 spec 的能力域逐条列出。本表与 [`docs/guides/iceberg-v3/`](../) 各章节互相对应。
> 标记：✅ 已支持 / 🚧 部分支持 / ❌ 未实现。
>
> 内部更详细的实现状态（含代码入口、PR 编号）见 `NovaRocks Iceberg v3 完成度清单`（Obsidian / 项目内部）。

---

## 1. Catalog

| 能力 | 状态 | 备注 |
| --- | --- | --- |
| Hadoop catalog（`metadata.json` + commit lock） | ✅ | `src/connector/iceberg/catalog/hadoop_catalog.rs` |
| In-memory catalog（仅测试） | ✅ | |
| REST catalog（spec 主推） | 🚧 | 客户端基础已落（PR #82）：属性解析 + config handshake + dispatcher；engine flow 路由 / OAuth2 / SigV4 / Bearer 鉴权 / 端到端 fixture 待补 |
| AWS Glue catalog | ❌ | |
| Hive Metastore（HMS） | ❌ | |
| Nessie catalog | ❌ | |
| JDBC catalog | ❌ | |
| Catalog credential vending（REST 透传 FileIO 临时凭据） | ❌ | |

## 2. 对象存储 / FileIO

| 能力 | 状态 | 备注 |
| --- | --- | --- |
| 本地文件系统 | ✅ | |
| HDFS | ✅ | |
| S3 / S3-compatible（含 MinIO） | ✅ | |
| 阿里云 OSS | ✅ | |
| Azure Blob / ADLS Gen2 | ❌ | |
| Google Cloud Storage（GCS） | ❌ | |
| 腾讯 COS | ❌ | |
| 华为云 OBS | ❌ | |
| FileIO 加密（S3 SSE-KMS / SSE-C） | ❌ | |
| FileIO checksum（v3 `content-checksum`） | ❌ | |

## 3. Format Version

| 能力 | 状态 | 备注 |
| --- | --- | --- |
| V2 表读 | ✅ | |
| V2 表写（INSERT / OVERWRITE / DELETE / UPDATE） | ✅ | |
| V3 表读（含 row-lineage / DV / equality-delete） | ✅ | |
| V3 表写（含 row-lineage 全链路） | ✅ | |
| V1 表（read-only legacy 兼容） | ❌ | |
| V1 → V2 / V2 → V3 表升级 | ❌ | |

## 4. 数据类型

### 4.1 原始类型

| 能力 | 状态 | 备注 |
| --- | --- | --- |
| boolean / int / long / float / double | ✅ | |
| string / binary / fixed | ✅ | |
| date / time / timestamp（毫秒/微秒） | ✅ | |
| timestamptz | ✅ | |
| decimal | ✅ | 含写入 + 比较 + cast |
| uuid | ✅ | 基础支持 |
| timestamp_ns / timestamptz_ns（V3 纳秒） | ❌ | |
| unknown（V3 placeholder） | ❌ | |

### 4.2 嵌套类型

| 能力 | 状态 | 备注 |
| --- | --- | --- |
| struct（含嵌套） | ✅ | |
| list | ✅ | |
| map | ✅ | |
| 复合类型 schema evolution（按 field-id） | ✅ | |

### 4.3 V3 新类型

| 能力 | 状态 | 备注 |
| --- | --- | --- |
| variant（读） | ✅ | `src/exec/variant.rs` |
| variant（INSERT 写） | ✅ | PR #87；`INSERT INTO ... VALUES (parse_json(...))` / `INSERT INTO ... SELECT` 均 OK；PR6 支持显式 shredding 表属性 |
| variant（OVERWRITE / DELETE / UPDATE / MERGE / equality-delete 写） | ❌ | PR #87 fail-fast，错误信息明确指向非目标边界 |
| variant shredding（`typed_value` 子树） | ✅ | 读已支持；INSERT 写通过 `write.parquet.variant-shredding.<col>` 显式开启 |
| variant default value（`initial-default` / `write-default`） | ❌ | |
| variant 在 partition spec / sort order / equality_ids | ❌ | spec 禁止；NovaRocks reject |
| variant predicate pushdown 到 parquet | ❌ | 当前在 BE 层 evaluate |
| geometry / geography（V3 空间类型） | ❌ | 读端 parquet `geometry` 物理类型解码 / 写端 WKB-EWKB 序列化 / `ST_*` 函数族都未做 |
| default value（`initial-default` / `write-default`） | ✅ | PR #79；DDL `DEFAULT <literal>` + 读端 backfill + INSERT 写端 fill |

## 5. Schema Evolution（DDL）

> Phase 2 收官（PR #86 / #88 / #89）：10 项全部 ✅。详见 [schema-evolution](../schema-evolution.md)。

| 能力 | 状态 | 备注 |
| --- | --- | --- |
| ADD COLUMN（含嵌套 STRUCT 路径） | ✅ | PR #86；`src/connector/iceberg/catalog/schema_update.rs` |
| DROP COLUMN（含嵌套） | ✅ | PR #86 |
| RENAME COLUMN（含嵌套） | ✅ | PR #86 |
| 类型 widening（int→long / float→double / decimal precision↑ / date→timestamp） | ✅ | PR #86 |
| ARRAY / MAP 元素类型 widening | ✅ | PR #86；`<list>.element` / `<map>.key` / `<map>.value` |
| Column reorder（`ALTER COLUMN ... FIRST / AFTER / BEFORE`，含嵌套） | ✅ | PR #86 |
| `SET / DROP NOT NULL`（含 identifier-field 保护 + `novarocks.nullability.attested.*` 留痕） | ✅ | PR #86 |
| DDL 失败原子回滚（commit 冲突 3 次指数退避 10/100/500ms 重试） | ✅ | PR #88；retry 间隙 invalidate cache 强制重读 metadata |
| `ALTER TABLE … SET / UNSET TBLPROPERTIES`（含 denylist + IF EXISTS） | ✅ | PR #89；`novarocks.*` / `format-version` / Iceberg 内部键全部 reject |

## 6. 分区与分区演进

| 能力 | 状态 | 备注 |
| --- | --- | --- |
| 分区写入：identity / bucket / truncate / year / month / day / hour | ✅ | |
| Partition evolution DDL（`ADD/DROP PARTITION COLUMN`） | ✅ | |
| 跨历史 partition spec 的扫描 | ✅ | |
| DELETE 跨历史 partition spec | ✅ | |
| OPTIMIZE 跨历史 spec（compact 到当前 spec） | ✅ | |
| partition transform `void`（V3） | ❌ | |
| dynamic partition overwrite（仅替换被写到的分区） | ❌ | |
| partition stats puffin（V3）写入与消费 | ❌ | |
| partition spec 修复 / 等价合并 | ❌ | |

## 7. 读路径

### 7.1 文件格式

| 能力 | 状态 | 备注 |
| --- | --- | --- |
| Parquet 读 | ✅ | |
| ORC 读 | ✅ | |
| Avro 数据文件读 | ❌ | spec 允许，主流不用 |
| Parquet 写 | ✅ | |
| ORC 写 | ❌ | |
| Parquet bloom filter index | ❌ | |
| Parquet page index | ❌ | |
| Parquet column index 跳过空 page | ❌ | |

### 7.2 Iceberg 扫描

| 能力 | 状态 | 备注 |
| --- | --- | --- |
| 列裁剪 / 谓词下推（min/max + null counts） | ✅ | |
| Runtime filter（HashJoin → Scan） | ✅ | |
| V2 position-delete 扫描合并 | ✅ | |
| V3 deletion vector（Puffin）扫描合并 | ✅ | |
| Equality delete 扫描合并 | ✅ | |
| 隐藏分区扫描（partition transform 自动展开） | ✅ | |
| V3 row-lineage 元数据列读 | ✅ | |
| V3 row-lineage manifest first-row-id 继承 | ✅ | |
| double-stat 裁剪审计（manifest + parquet） | ❌ | |
| V3 partition stats puffin 入 planner 决策 | ❌ | |
| Iceberg sort order 入 planner（合并 sort） | ❌ | |

## 8. 时间旅行 / Branch / Tag / Snapshot

### 8.1 SQL 时间旅行

| 能力 | 状态 | 备注 |
| --- | --- | --- |
| `FOR VERSION AS OF <snapshot_id>`（数字） | ✅ | PR #80 |
| `FOR TIMESTAMP AS OF '<ts>'`（epoch-ms 或字符串字面量） | ✅ | 表达式形式仍 reject |
| `FOR VERSION AS OF '<branch_or_tag>'` | ✅ | normalizer 重写绕开 sqlparser 限制 |
| 快照保留期读侧拒绝（拿过期 snapshot 报错） | ❌ | |

### 8.2 Branch / Tag DDL

| 能力 | 状态 | 备注 |
| --- | --- | --- |
| `CREATE BRANCH [AS OF VERSION <id>]`（含 `IF NOT EXISTS` / `OR REPLACE` / `WITH SNAPSHOT RETENTION`） | ✅ | PR #80 |
| `DROP BRANCH [IF EXISTS]` | ✅ | |
| `CREATE TAG [AS OF VERSION <id>]`（含 `IF NOT EXISTS` / `OR REPLACE`） | ✅ | |
| `DROP TAG [IF EXISTS]` | ✅ | |
| `REPLACE BRANCH … AS OF VERSION <id>` | ✅ | 通过 `CREATE OR REPLACE BRANCH` 覆盖 |
| `FAST FORWARD <branch> TO <branch>` | ❌ | |
| `CHERRYPICK SNAPSHOT <id>` | ❌ | |
| `ROLLBACK TO VERSION <id>` / `ROLLBACK TO TIMESTAMP '<ts>'` | ❌ | |
| 写指定 branch（`t.branch_<x>` 形式：INSERT / UPDATE / DELETE） | ✅ | 要求 v3 row-lineage |
| Spark 风格 `t@<branch>` 写入语法 | ❌ | |
| MERGE INTO 写指定 branch | ❌ | |

### 8.3 Snapshot 生命周期

| 能力 | 状态 | 备注 |
| --- | --- | --- |
| `parent-snapshot-id` / `summary` 正确记录 | ✅ | |
| `EXPIRE SNAPSHOTS [OLDER THAN] [RETAIN LAST]` | ✅ | OLDER THAN / RETAIN LAST 子句，per-branch retention 属性未读取 |
| `REMOVE ORPHAN FILES` | ✅ | OLDER THAN 强制；hdfs:// 暂不支持 |
| `REWRITE MANIFESTS` | ✅ | 按 (spec_id, content_type) 分组合并；V3 row-lineage round-trip |

## 9. Metadata Tables

| 能力 | 状态 | 备注 |
| --- | --- | --- |
| `$snapshots` | ✅ | BE PR #81 + SQL 路由 PR #85 |
| `$history` | ✅ | BE PR #81 + SQL 路由 PR #85 |
| `$refs`（branches + tags） | ✅ | BE PR #81 + SQL 路由 PR #85 |
| `$partitions` | ✅ | BE PR #81 + SQL 路由 PR #85 |
| `$manifests` | ❌ | |
| `$files` / `$all_data_files` | ❌ | |
| `$delete_files` / `$all_delete_files` | ❌ | |
| `$entries`（manifest entries） | ❌ | |
| `$metadata_log_entries` | ❌ | |
| `$position_deletes` | ❌ | |
| `$all_files`（V3 统一视图） | ❌ | |

## 10. 写路径 / DML

| 能力 | 状态 | 备注 |
| --- | --- | --- |
| INSERT INTO（VALUES / SELECT） | ✅ | |
| INSERT OVERWRITE（静态分区 + 全表） | ✅ | |
| DELETE FROM（V2 position-delete + V3 DV 双路径） | ✅ | |
| UPDATE（COW + MOR + UPDATE FROM source） | ✅ | PR #76 |
| OPTIMIZE TABLE（whole-table 重写） | ✅ | |
| MERGE INTO（matched UPDATE / matched DELETE / not matched INSERT） | ✅ | PR #78 |
| INSERT OVERWRITE 动态分区（`OVERWRITE PARTITIONS`） | ❌ | |
| CTAS（写 Iceberg） | ❌ | |
| CTAS 默认 V3 row-lineage | ❌ | |
| TRUNCATE TABLE | ❌ | |
| CDC sink（Flink-style 持续写入） | ❌ | |

## 11. 维护 / 治理

| 能力 | 状态 | 备注 |
| --- | --- | --- |
| OPTIMIZE TABLE（whole-table） | ✅ | |
| OPTIMIZE 增量（仅小文件 / 仅 partition） | ❌ | |
| EXPIRE SNAPSHOTS | ✅ | OLDER THAN / RETAIN LAST 子句，per-branch retention 属性未读取 |
| REMOVE ORPHAN FILES | ✅ | OLDER THAN 强制；hdfs:// 暂不支持 |
| REWRITE MANIFESTS | ✅ | 按 (spec_id, content_type) 分组合并；V3 row-lineage round-trip |
| REWRITE POSITION DELETES（v2→DV） | ❌ | |
| REWRITE DATA FILES BY SORT ORDER | ❌ | |
| 自动 maintenance 调度器 | ❌ | |

## 12. Row Lineage（V3）

| 能力 | 状态 | 备注 |
| --- | --- | --- |
| `_row_id` / `_last_updated_sequence_number` 元数据列读 | ✅ | |
| INSERT / OVERWRITE 写出 `first_row_id` + `row_range` | ✅ | |
| DELETE 不分配新 `_row_id`（DV 合并保留语义） | ✅ | |
| COW UPDATE 保留 `_row_id` + 写 `novarocks.update.sidecar` JSON | ✅ | |
| MOR UPDATE 复用 `_row_id` + 显式赋 `DataFile.first_row_id` | ✅ | |
| OPTIMIZE 重写后保留每行 `_row_id`（写到 reserved field id `i32::MAX-107` / `-108`） | ✅ | PR #85 |
| `_row_id` 跨 snapshot 唯一性 invariant 测试（含 OPTIMIZE 后） | ✅ | PR #85（`iceberg_v3_row_lineage_uniqueness.sql`） |
| Branch / tag 切换 `_row_id` 一致性回归 | ❌ | |
| Cross-engine `_row_id` 一致性测试 | ❌ | 待 §17 cross-engine fixture |

## 13. Deletion Vector / Puffin

| 能力 | 状态 | 备注 |
| --- | --- | --- |
| `deletion-vector-v1` blob 编解码 | ✅ | `src/connector/iceberg/commit/puffin_dv.rs` |
| DV 写入（DELETE / MOR UPDATE） | ✅ | |
| DV 读取并应用到 scan | ✅ | |
| 多次 DELETE 合并到同一 DV blob | ✅ | |
| 跨 partition spec 的 DV 写入 | ✅ | |
| Puffin `apache-datasketches-theta-v1`（NDV） | ❌ | |
| Puffin `partition-stats-blob`（V3） | ❌ | |
| Puffin `bloom-filter-v1` | ❌ | |

## 14. 物化视图（NovaRocks 差异化）

### 14.1 已落地

| 能力 | 状态 | 备注 |
| --- | --- | --- |
| Iceberg-backed MV 定义 | ✅ | |
| MV 全量刷新 | ✅ | |
| IVM —— Insert | ✅ | |
| IVM —— V2 position-delete | ✅ | |
| IVM —— V3 deletion vector | ✅ | |
| IVM —— Equality delete | ✅ | |
| IVM —— Insert Overwrite（fallback 到全刷） | ✅ | |
| IVM —— V3 row-lineage 行级 delete 复用 `_row_id` | ✅ | |
| IVM —— Schema evolution 安全 fallback | ✅ | |
| IVM —— Partition evolution | ✅ | |
| 投影 / 过滤 MV | ✅ | |
| 聚合 MV：SUM / COUNT / AVG / MIN / MAX | ✅ | |
| IVM —— COW UPDATE | ✅ | PR #76 |
| IVM —— MOR UPDATE | ✅ | PR #76 |
| IVM —— MERGE INTO（COW + MOR） | ✅ | PR #78 |

### 14.2 缺口

| 能力 | 状态 | 备注 |
| --- | --- | --- |
| **MV 自动 query rewrite**（用户 SQL 不引用 MV，optimizer 自动命中） | ❌ | NovaRocks 差异化最关键的单点 |
| MV freshness contract / staleness budget | ❌ | |
| 多基表 JOIN 的 IVM | ❌ | |
| Window 函数 MV | ❌ | |
| 含 DISTINCT / 子查询的 MV | ❌ | |
| 跨 catalog 的 MV | ❌ | |
| MV 物化结果存到 Iceberg | ✅ | MV target 是 external Iceberg catalog 中的 Iceberg 表 |

## 15. SQL 语言特性（与 Iceberg 协同的部分）

| 能力 | 状态 | 备注 |
| --- | --- | --- |
| SELECT / FROM / WHERE / GROUP BY / HAVING / ORDER BY / LIMIT | ✅ | |
| JOIN（INNER / LEFT / RIGHT / FULL OUTER / CROSS / SEMI / ANTI） | ✅ | |
| CTE（含 multicast） | ✅ | |
| Set operations（UNION / INTERSECT / EXCEPT） | ✅ | |
| 标量 / EXISTS / IN 子查询 | ✅ | |
| Window functions（基础） | ✅ | |
| GROUPING SETS / CUBE / ROLLUP | ✅ | |
| Recursive CTE | ❌ | |
| Lateral join | ❌ | |
| TABLESAMPLE | ❌ | |
| Stored procedure（Iceberg 系统过程） | ❌ | |
| UDF / UDAF / UDTF | ❌ | |
| PIVOT / UNPIVOT | ❌ | |
| QUALIFY | ❌ | |
| MATCH_RECOGNIZE | ❌ | |

---

> 此矩阵每次大版本合并后会同步更新，与各章节保持一致。如发现矩阵和章节正文冲突，**以章节正文为准**并提 issue。
