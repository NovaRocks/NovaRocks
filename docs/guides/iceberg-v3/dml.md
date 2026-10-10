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

# DML（INSERT / DELETE / UPDATE / MERGE / OVERWRITE / TRUNCATE）

> NovaRocks 支持 INSERT、DELETE、UPDATE、MERGE、OVERWRITE 与 TRUNCATE；CTAS 使用标准 REST staged create。Catalog 写入准入和 v3 row-lineage 要求见各节，CDC sink 尚未实现。

| 能力 | 状态 | 备注 |
| --- | --- | --- |
| INSERT INTO（VALUES / SELECT） | ✅ | |
| INSERT OVERWRITE（静态分区 + 全表） | ✅ | |
| DELETE FROM（V2 position-delete + V3 DV 双路径） | ✅ | |
| UPDATE（COW + MOR + UPDATE FROM source） | ✅ | PR #76 |
| MERGE INTO（matched UPDATE / matched DELETE / not matched INSERT） | ✅ | PR #78 |
| OPTIMIZE TABLE（whole-table 重写） | ✅ | 见 [maintenance](maintenance.md) |
| INSERT OVERWRITE 动态分区（`INSERT OVERWRITE PARTITIONS`） | ✅ | v3、分区表，见下文 |
| CTAS（`CREATE TABLE AS SELECT`） | ✅ | 标准 REST staged-create，显式 warehouse；Hadoop/HMS 在副作用前拒绝 |
| CTAS 默认 V3 row-lineage | ❌ | 默认格式为 v2，v3 需显式声明 |
| TRUNCATE TABLE | ✅ | 清空目标 ref，保留 schema 和历史 |
| CDC sink（Flink-style 持续写入） | ❌ | |

---

## 并发提交政策

源输出计算完成后，每次 attempt 仍需验证操作依赖。当前政策如下；更细的重写/行级冲突域尚未开放：

| 操作 | 提交时验证 |
|---|---|
| INSERT、INSERT SELECT、纯 MV 追加 | 无读依赖，可在新 head 上重新准备 |
| 静态全表 INSERT OVERWRITE | 无读依赖，覆盖提交时的状态 |
| ADD FILES | 源路径不在目标 ref 刷新后的存活文件集中 |
| DELETE / UPDATE / MERGE | 目标 ref 与源读取基线相同 |
| 动态分区覆盖、OPTIMIZE、TRUNCATE | 目标 ref 与源读取基线相同 |
| 绑定基线的文档或分区演进提交 | 目标 ref 与输出基线相同 |
| ANALYZE | 测得统计的快照在准备时存在；发布时存在性不受原子条件保护 |

读依赖语句遇到 head 变化会以确定未提交的冲突结束，需要重新执行来读取新事实。它不会将旧输出
盲目应用到新 head。只有 catalog 已确定拒绝且依赖仍成立，才允许重新准备；CommitUnknown 不能重试
或清理。重试表属性、清理结果与人工核对见 [lake-publication](lake-publication.md)。

## ✅ INSERT INTO

```sql
-- VALUES
INSERT INTO orders VALUES (1, 1001, 19.90, '2026-05-01');

-- SELECT
INSERT INTO orders SELECT id, user_id, amount * 1.1, ts FROM orders_staging;

-- 显式列表（配合 default value 自动 fill）
INSERT INTO orders (id, user_id, amount) VALUES (2, 1002, 30.00);
```

v3 新逻辑行在提交时分配 `first_row_id`，snapshot 的 row range 取实际 manifest-list 分配量，见 [row-lineage](row-lineage.md)。

## ✅ INSERT OVERWRITE

```sql
-- 全表覆盖
INSERT OVERWRITE orders SELECT * FROM orders_staging;

-- 静态分区覆盖
INSERT OVERWRITE orders PARTITION (country = 'CN') SELECT * FROM orders_cn;
```

实现：写出新 data file，由 canonical preparer 将目标 ref 的旧条目标为 DELETED 并发布完整请求。

### ✅ 动态分区 OVERWRITE

仅替换本次输出命中的分区，未命中的分区保留；空输出不删除已有分区。
当前要求 v3 分区表，并拒绝跨历史 partition spec 的覆盖范围。

```sql
INSERT OVERWRITE PARTITIONS orders SELECT * FROM orders_changes;
```

移除 data file 时同步移除它适用的精确 DV；共享 Puffin 中其他 data file 的 DV 继续保留。

## ✅ DELETE FROM

```sql
DELETE FROM orders WHERE amount < 10.0;
```

V2 / V3 双路径：

- V2 表：写 position-delete 文件
- V3 表：写 Puffin deletion vector blob，**不分配新 `_row_id`**（DV 合并保留语义）

入口：`novarocks/frontend-application/src/query_execution/dml/delete/`。

## ✅ UPDATE（COW + MOR + UPDATE FROM source）

```sql
-- 简单 UPDATE
UPDATE orders SET amount = amount * 1.1 WHERE user_id = 1001;

-- UPDATE FROM source（关联其他表）
UPDATE orders t
   SET t.amount = s.new_amount
  FROM (SELECT id, new_amount FROM repricing_staging) s
 WHERE t.id = s.id;
```

NovaRocks 默认走 COW（写新文件 + 替换），可通过表 property `write.update.mode = 'merge-on-read'` 切到 MOR（写 DV + 新文件保留 `_row_id`）。

MoR 更新行的 `_last_updated_sequence_number` 写 NULL，读时继承实际提交的 data sequence；
其他 branch 先提交也不会使它落到预测的旧版本。入口：`novarocks/frontend-application/src/query_execution/dml/mutation_flow.rs`。

## ✅ MERGE INTO

```sql
MERGE INTO orders t
USING (SELECT id, user_id, amount, ts FROM orders_changes) s
   ON t.id = s.id
 WHEN MATCHED AND s.amount IS NULL THEN DELETE
 WHEN MATCHED THEN UPDATE SET amount = s.amount, ts = s.ts
 WHEN NOT MATCHED THEN INSERT (id, user_id, amount, ts) VALUES (s.id, s.user_id, s.amount, s.ts);
```

实现概要（PR #78）：

- 单次 LEFT JOIN 用 `__nr_match_kind` 字段区分 matched / not-matched，再叠加每子句的 `AND` apply flag，一次性物化所有批次
- matched 走 v3 row-lineage UPDATE executor（COW / MOR 双路径同 UPDATE）
- not matched 走 FastAppend
- matched DELETE 走 position-delete / DV 路径

要求基表 v3 + row-lineage。

### ❌ MERGE INTO 写指定 branch

phase 1 仅覆盖 INSERT / UPDATE / DELETE 写指定 branch，MERGE INTO 暂未支持。

## ✅ CTAS（CREATE TABLE AS SELECT）

```sql
CREATE TABLE t_new AS SELECT * FROM t_old;
```

要求支持标准 staged create 的 REST catalog 和显式 warehouse，以便在执行 source 前确定 staging
namespace。完整初始化、属性和数据请求使用 assert-create 一次发布；零行结果创建无快照空表。
目标被并发创建时按 `IF NOT EXISTS` 语义结束，结果未知时按 lake-publication 核对。

## ❌ CTAS 默认 V3 row-lineage

CTAS 默认格式仍为 v2。需要 v3 写入时，在 CREATE 中显式设置 `format-version=3`；需要依赖已存储
行血缘的 UPDATE/MERGE 等能力时，同时显式设置 `write.row-lineage=true`。

## ✅ TRUNCATE TABLE

发布目标 ref 的清空 snapshot，将所有 data / delete / DV 逻辑条目标为 DELETED，保留 schema 和历史
snapshot。它不启动 BE 数据 writer，也不立即删除历史仍引用的物理文件。

```sql
TRUNCATE TABLE orders;
```

支持 v2 与 v3；`TRUNCATE ... PARTITION (...)` 和 `TRUNCATE ... WHERE ...` 在解析阶段拒绝。

## ❌ CDC sink

Spec：Flink-style 持续从 source 流（Kafka / 上游 CDC）写入 Iceberg，按 sequence number 切 snapshot。

**TODO**：未实现。如果你需要 CDC，目前只能让 Flink / Spark 直接写 Iceberg 表，再让 NovaRocks 读。
