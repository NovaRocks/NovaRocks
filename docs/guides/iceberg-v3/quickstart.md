# 快速上手

> 5 分钟跑通一个 Iceberg v3 表：起 standalone-server → 建表 → 写入 → 查询 → 时间旅行。

---

## 前置

- Rust toolchain（参考 `CLAUDE.md` 的"Build Mode"段）
- 一个 MySQL CLI（用于连接 standalone-server）

> 当前版本仅推荐 **Hadoop catalog**（写本地 / HDFS / S3 / OSS）。REST catalog 客户端基础已经就位但 engine 流程未切换，端到端 fixture 待补，详见 [Catalog 接入](catalog.md)。

---

## 1. 启动 standalone-server

```bash
# Debug build（编译快，查询慢，适合学习与功能验证）
NO_PROXY=127.0.0.1,localhost cargo run -- standalone-server --port 9030

# Release build（编译慢，查询快，适合性能测试）
NO_PROXY=127.0.0.1,localhost cargo run --release -- standalone-server --port 9030
```

服务监听 `127.0.0.1:9030`（MySQL wire 协议）。

## 2. 连接

```bash
mysql -h 127.0.0.1 -P 9030 -u root
```

## 3. 创建一个 Iceberg v3 表

```sql
CREATE EXTERNAL CATALOG ice
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "warehouse" = "file:///tmp/iceberg-warehouse"
);

CREATE DATABASE ice.demo;

USE ice.demo;

CREATE TABLE orders (
  id      BIGINT,
  user_id BIGINT,
  amount  DECIMAL(18, 2),
  ts      TIMESTAMP
)
TBLPROPERTIES (
  "format-version"   = "3",
  "write.row-lineage" = "true"
);
```

> v3 + row-lineage 是后面 branch DML / MERGE INTO 的前置条件。

## 4. 写入与查询

```sql
INSERT INTO orders VALUES
  (1, 1001, 19.90, '2026-05-01 10:00:00'),
  (2, 1002, 39.50, '2026-05-01 10:05:00'),
  (3, 1001, 12.30, '2026-05-02 09:30:00');

SELECT user_id, SUM(amount) AS total
  FROM orders
  GROUP BY user_id
  ORDER BY user_id;
```

## 5. UPDATE / MERGE INTO

```sql
-- COW UPDATE 默认行为
UPDATE orders SET amount = amount * 1.1 WHERE user_id = 1001;

-- MERGE INTO upsert
MERGE INTO orders t
USING (SELECT 4 AS id, 1003 AS user_id, 99.00 AS amount, TIMESTAMP '2026-05-03 12:00:00' AS ts) s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET amount = s.amount
WHEN NOT MATCHED THEN INSERT (id, user_id, amount, ts) VALUES (s.id, s.user_id, s.amount, s.ts);
```

## 6. 时间旅行

```sql
-- 当前 main 分支
SELECT COUNT(*) FROM orders;

-- 指定 snapshot id（数字字面量）
SELECT COUNT(*) FROM orders FOR VERSION AS OF 1234567890123;

-- 指定时间戳
SELECT COUNT(*) FROM orders FOR TIMESTAMP AS OF '2026-05-01 11:00:00';

-- 指定 branch / tag 名
SELECT COUNT(*) FROM orders FOR VERSION AS OF 'main';
```

## 7. Branch / Tag DDL

```sql
ALTER TABLE orders CREATE BRANCH dev;
ALTER TABLE orders CREATE TAG release_v1;

INSERT INTO orders.branch_dev VALUES (5, 1004, 8.80, '2026-05-04 09:00:00');

-- 验证 main 没有被影响
SELECT COUNT(*) FROM orders FOR VERSION AS OF 'main';
SELECT COUNT(*) FROM orders FOR VERSION AS OF 'dev';

ALTER TABLE orders DROP BRANCH dev;
ALTER TABLE orders DROP TAG release_v1;
```

> 写指定 branch 的语法是 `<table>.branch_<name>`，**不是 Spark 的 `<table>@<branch>`**。

---

## 接下来读什么

- 学习 NovaRocks 的语义边界：[Format Version](format-versions.md) / [数据类型](data-types.md)
- 用更高级的能力：[Branch / Tag](branches-and-tags.md) / [MERGE INTO](dml.md#merge-into) / [物化视图](materialized-views.md)
- 看哪些 spec 能力还没实现：[支持矩阵](reference/support-matrix.md)
