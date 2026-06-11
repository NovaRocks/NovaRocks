# DML（INSERT / DELETE / UPDATE / MERGE / OVERWRITE / TRUNCATE）

> NovaRocks 在 v3 row-lineage 表上实现了 INSERT / DELETE / UPDATE / MERGE INTO / OVERWRITE 全套；CTAS / TRUNCATE / 动态分区 OVERWRITE / CDC sink 仍待补。

| 能力 | 状态 | 备注 |
| --- | --- | --- |
| INSERT INTO（VALUES / SELECT） | ✅ | |
| INSERT OVERWRITE（静态分区 + 全表） | ✅ | |
| DELETE FROM（V2 position-delete + V3 DV 双路径） | ✅ | |
| UPDATE（COW + MOR + UPDATE FROM source） | ✅ | PR #76 |
| MERGE INTO（matched UPDATE / matched DELETE / not matched INSERT） | ✅ | PR #78 |
| OPTIMIZE TABLE（whole-table 重写） | ✅ | 见 [maintenance](maintenance.md) |
| INSERT OVERWRITE 动态分区（`OVERWRITE PARTITIONS`） | ❌ | |
| CTAS（`CREATE TABLE AS SELECT`） | ❌ | |
| CTAS 默认 V3 row-lineage | ❌ | |
| TRUNCATE TABLE | ❌ | |
| CDC sink（Flink-style 持续写入） | ❌ | |

---

## ✅ INSERT INTO

```sql
-- VALUES
INSERT INTO orders VALUES (1, 1001, 19.90, '2026-05-01');

-- SELECT
INSERT INTO orders SELECT id, user_id, amount * 1.1, ts FROM orders_staging;

-- 显式列表（配合 default value 自动 fill）
INSERT INTO orders (id, user_id, amount) VALUES (2, 1002, 30.00);
```

写出 v3 row-lineage 元数据列（`first_row_id` + `row_range`），见 [row-lineage](row-lineage.md)。

## ✅ INSERT OVERWRITE

```sql
-- 全表覆盖
INSERT OVERWRITE orders SELECT * FROM orders_staging;

-- 静态分区覆盖
INSERT OVERWRITE orders PARTITION (country = 'CN') SELECT * FROM orders_cn;
```

实现：写出新 data file，再用 `OverwriteFiles` commit 替换旧文件集。

### ❌ 动态分区 OVERWRITE

Spec / Spark：`INSERT OVERWRITE` 在 dynamic 模式下只替换被本次写入命中的分区。

```sql
-- 暂未实现
INSERT OVERWRITE orders OVERWRITE PARTITIONS SELECT ...;
```

替代方案：先 `DELETE FROM t WHERE <partition condition>`，再 `INSERT INTO t SELECT ...`。

## ✅ DELETE FROM

```sql
DELETE FROM orders WHERE amount < 10.0;
```

V2 / V3 双路径：

- V2 表：写 position-delete 文件
- V3 表：写 Puffin deletion vector blob，**不分配新 `_row_id`**（DV 合并保留语义）

入口：`src/engine/delete_flow.rs`

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

入口：`src/engine/mutation_flow.rs`

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

## ❌ CTAS（CREATE TABLE AS SELECT）

```sql
-- 暂未实现
CREATE TABLE t_new AS SELECT * FROM t_old;
```

替代方案：先 `CREATE TABLE` 显式声明 schema，再 `INSERT INTO t_new SELECT ...`。

## ❌ CTAS 默认 V3 row-lineage

CTAS 上线后，路线图希望默认 `format-version=3` + `write.row-lineage=true`，让新表直接享受 v3 全套能力。当前需要在 CREATE 时显式指定 TBLPROPERTIES。

## ❌ TRUNCATE TABLE

Spec：写一个清空 snapshot（删除所有 data files / delete files，但保留 schema 和历史 snapshot）。

```sql
-- 暂未实现
TRUNCATE TABLE orders;
```

替代方案：`DELETE FROM orders;`（语义等价但会写出 DV / position-delete，不如 TRUNCATE 干净）。

## ❌ CDC sink

Spec：Flink-style 持续从 source 流（Kafka / 上游 CDC）写入 Iceberg，按 sequence number 切 snapshot。

**TODO**：未实现。如果你需要 CDC，目前只能让 Flink / Spark 直接写 Iceberg 表，再让 NovaRocks 读。
