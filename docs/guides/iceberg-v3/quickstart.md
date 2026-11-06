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

# 快速上手

> 5 分钟跑通一个 Iceberg v3 表：起 standalone → 建表 → 写入 → 查询 → 时间旅行。

---

## 前置

- Rust toolchain（参考 `CLAUDE.md` 的"Build Mode"段）
- Trino CLI（用于连接 FE 的 HTTP statement 入口）

Catalog 的当前能力与 fixture 准备方式见 [Catalog 接入](catalog.md)；Hadoop catalog 仅用于单写者测试，生产写入使用 REST catalog。

---

## 1. 启动 standalone

先按 [本地部署](../deployment/standalone.md) 准备普通 FE/BE 配置，并将两份配置路径分别导出为 `NOVAROCKS_FE_CONFIG` 和 `NOVAROCKS_BE_CONFIG`。

```bash
# Debug build（编译快，查询慢，适合学习与功能验证）
NO_PROXY=127.0.0.1,localhost cargo run -p novarocks-server -- standalone --role all-in-one \
  --fe-config "$NOVAROCKS_FE_CONFIG" --be-config "$NOVAROCKS_BE_CONFIG"

# Release build（编译慢，查询快，适合性能测试）
NO_PROXY=127.0.0.1,localhost cargo run --release -p novarocks-server -- standalone --role all-in-one \
  --fe-config "$NOVAROCKS_FE_CONFIG" --be-config "$NOVAROCKS_BE_CONFIG"
```

FE 监听 `127.0.0.1` 的 Trino HTTP 端口（`trino_port` 默认 `8080`）。等待 `NOVAROCKS_READY role=fe trino_port=<actual-port> pid=<pid>`，连接本次实际端口。

## 2. 连接

```bash
trino --server http://127.0.0.1:8080 --user root
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
  "format-version" = "3"
);
```

> 后面的 branch DML / MERGE INTO 要求 v3；无需声明 `write.row-lineage`。

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
