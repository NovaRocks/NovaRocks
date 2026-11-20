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

# Branch / Tag DDL 与分支写入

Iceberg branch 和 tag 是命名的 snapshot ref。可以在分支内隔离写入，也可以用标签保留一个快照。

| 能力 | 状态 | 备注 |
| --- | --- | --- |
| `CREATE [OR REPLACE] BRANCH [IF NOT EXISTS] … IN TABLE t` | 支持 | 可用 `WITH (snapshot_id = …)` 指定已有快照 |
| `DROP BRANCH [IF EXISTS] … IN TABLE t` | 支持 | |
| `ALTER TABLE t EXECUTE create_tag(...)` | 支持 | 可指定 `snapshot_id`、`if_not_exists` 或 `replace` |
| `ALTER TABLE t EXECUTE drop_tag(...)` | 支持 | 可指定 `if_exists` |
| `INSERT INTO t @dev`、`UPDATE t @dev`、`DELETE FROM t @dev`、`MERGE INTO t @dev` | 支持 | 分支写入要求 Iceberg v3 |
| 自定义 ref 保留策略、`CREATE BRANCH … FROM …`、`FAST FORWARD` | 暂不支持 | |
| Cherry-pick 或回滚快照 | 暂不支持 | |

实现入口：

- 分支语句：`novarocks/parser/src/parser/branch_statement.rs` 和 `novarocks/query-application/src/sql/trino_table_command.rs`。
- 标签过程：`novarocks/query-application/src/sql/table_procedure.rs` 和 `novarocks/frontend-application/src/mv/domain/ref_flow.rs`。
- 引用提交：`novarocks/connector/iceberg/src/commit/ref_action.rs`。
- 行变更准备：`novarocks/frontend-application/src/dml/mutation/mod.rs` 和 `novarocks/connector/iceberg/src/commit/row_mutation_preparation.rs`。

## 创建 branch 或 tag

下列语句分别示范不同选项。将示例中的 snapshot ID 换成表内已有的 ID。省略 `snapshot_id` 时使用当前快照。

```sql
CREATE BRANCH dev IN TABLE t;
CREATE BRANCH dev WITH (snapshot_id = 1234567890123) IN TABLE t;
CREATE BRANCH IF NOT EXISTS dev IN TABLE t;

ALTER TABLE t EXECUTE create_tag(name => 'release_v1');
ALTER TABLE t EXECUTE create_tag(name => 'release_v1', snapshot_id => 1234567890123);
ALTER TABLE t EXECUTE create_tag(name => 'release_v1', if_not_exists => true);
ALTER TABLE t EXECUTE create_tag(name => 'release_v1', snapshot_id => 1234567890123, replace => true);
```

`OR REPLACE` 和 `IF NOT EXISTS` 是两种互斥的分支创建模式。`main` 是保留引用。替换引用时，原引用与新引用必须同为 branch 或同为 tag。NovaRocks 会拒绝自定义保留策略，旧式 `WITH SNAPSHOT RETENTION` 也会被拒绝。

## 删除 branch 或 tag

```sql
DROP BRANCH dev IN TABLE t;
DROP BRANCH IF EXISTS dev IN TABLE t;

ALTER TABLE t EXECUTE drop_tag(name => 'release_v1', if_exists => false);
ALTER TABLE t EXECUTE drop_tag(name => 'release_v1', if_exists => true);
```

## 将已有 branch 指向另一个快照

```sql
CREATE OR REPLACE BRANCH dev WITH (snapshot_id = 1234567890123) IN TABLE t;
```

## 写入指定 branch

在目标表名后用 `@dev` 指定分支。Row lineage 由 Iceberg v3 格式版本提供。

```sql
INSERT INTO orders @dev VALUES (3, 1003, 99.00, '2026-05-04');
UPDATE orders @dev SET amount = 0 WHERE id = 3;
DELETE FROM orders @dev WHERE id = 3;
```

每次分支提交只推进该分支的引用，OCC 按引用隔离。

## 用 MERGE 写入指定 branch

```sql
MERGE INTO orders @dev AS t
USING (SELECT 3 AS id) AS s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET amount = 0;
```

当前 MERGE 形状最多包含一条 `WHEN MATCHED` 与一条 `WHEN NOT MATCHED`。

## 对照 main 与 dev

```sql
SELECT COUNT(*) FROM orders FOR VERSION AS OF 'main';
SELECT COUNT(*) FROM orders FOR VERSION AS OF 'dev';
```

## 尚未支持的快照操作

下面的语句可解析，NovaRocks 在语义准入阶段返回 `NOT_SUPPORTED`。

```sql
ALTER BRANCH main IN TABLE t FAST FORWARD TO dev;
```

NovaRocks 暂未提供快照 cherry-pick 或回滚操作。`CREATE OR REPLACE BRANCH main` 会因 `main` 为保留引用而被拒绝。
