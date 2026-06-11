# Branch / Tag DDL 与分支写入

> Iceberg branch / tag 是命名的 snapshot ref，让你在表内做隔离写、保留版本、回滚。NovaRocks phase 1 实现了 CREATE / DROP 全套以及 INSERT / UPDATE / DELETE 写指定 branch；FAST FORWARD / CHERRYPICK / ROLLBACK / MERGE-on-branch 仍待补。

| 能力 | 状态 | 备注 |
| --- | --- | --- |
| `CREATE BRANCH [AS OF VERSION <id>]`（含 `IF NOT EXISTS` / `OR REPLACE` / `WITH SNAPSHOT RETENTION`） | ✅ | |
| `DROP BRANCH [IF EXISTS]` | ✅ | |
| `CREATE TAG [AS OF VERSION <id>]`（含 `IF NOT EXISTS` / `OR REPLACE`） | ✅ | |
| `DROP TAG [IF EXISTS]` | ✅ | |
| `REPLACE BRANCH … AS OF VERSION <id>` | ✅ | 通过 `CREATE OR REPLACE BRANCH` |
| `INSERT INTO t.branch_<x>` | ✅ | 要求 v3 row-lineage |
| `UPDATE t.branch_<x> ...` | ✅ | |
| `DELETE FROM t.branch_<x> WHERE ...` | ✅ | |
| `MERGE INTO t.branch_<x>` | ❌ | |
| Spark 风格 `t@<branch>` 写入语法 | ❌ | 用 `t.branch_<x>` 替代 |
| `FAST FORWARD <branch> TO <branch>` | ❌ | |
| `CHERRYPICK SNAPSHOT <id>` | ❌ | |
| `ROLLBACK TO VERSION <id>` / `ROLLBACK TO TIMESTAMP '<ts>'` | ❌ | |

实现入口：
- 解析 + AST：`src/sql/parser/dialect/alter_iceberg_ref.rs`、`src/sql/parser/ast/iceberg_ref.rs`
- Analyzer：`src/sql/analyzer/alter_iceberg_ref.rs`、`src/sql/analyzer/iceberg_ref.rs`
- Commit：`src/connector/iceberg/commit/ref_action.rs`
- Engine：`src/engine/iceberg_ref_flow.rs`、`src/engine/mutation_flow.rs`、`src/engine/insert_flow.rs`、`src/engine/delete_flow.rs`

---

## ✅ ALTER TABLE … CREATE BRANCH / TAG

```sql
ALTER TABLE t CREATE BRANCH dev;
ALTER TABLE t CREATE BRANCH dev AS OF VERSION 1234567890123;
ALTER TABLE t CREATE BRANCH IF NOT EXISTS dev;
ALTER TABLE t CREATE OR REPLACE BRANCH dev AS OF VERSION 1234567890123;
ALTER TABLE t CREATE BRANCH dev WITH SNAPSHOT RETENTION 5 SNAPSHOTS;

ALTER TABLE t CREATE TAG release_v1;
ALTER TABLE t CREATE TAG release_v1 AS OF VERSION 1234567890123;
```

校验：
- ref 名不能与保留名冲突
- `AS OF VERSION <id>` 必须存在于 `metadata.snapshots[]`
- BRANCH / TAG kind 不能互换（现有 BRANCH 不能被 `CREATE TAG` 覆盖，反之亦然，除非 `OR REPLACE` 显式指定）

## ✅ ALTER TABLE … DROP BRANCH / TAG

```sql
ALTER TABLE t DROP BRANCH dev;
ALTER TABLE t DROP BRANCH IF EXISTS dev;

ALTER TABLE t DROP TAG release_v1;
ALTER TABLE t DROP TAG IF EXISTS release_v1;
```

## ✅ REPLACE BRANCH

Spec 写法 `REPLACE BRANCH <name> AS OF VERSION <id>` 在 NovaRocks 中通过 `CREATE OR REPLACE BRANCH` 等价表达：

```sql
ALTER TABLE t CREATE OR REPLACE BRANCH dev AS OF VERSION 1234567890123;
```

## ✅ 写入指定 branch

> 要求基表是 v3 + `write.row-lineage = true`。

NovaRocks 用 `<table>.branch_<name>` 后缀语法限定写入 ref——**注意这与 Spark 的 `<table>@<branch>` 不同**：

```sql
-- INSERT 到 dev branch
INSERT INTO orders.branch_dev VALUES (3, 1003, 99.00, '2026-05-04');

-- UPDATE dev branch
UPDATE orders.branch_dev SET amount = 0 WHERE id = 3;

-- DELETE from dev branch
DELETE FROM orders.branch_dev WHERE id = 3;

-- 验证 main 不受影响
SELECT COUNT(*) FROM orders FOR VERSION AS OF 'main';
SELECT COUNT(*) FROM orders FOR VERSION AS OF 'dev';
```

每次 branch 上的 commit 只推进该 branch 的 ref，不动 main。OCC 隔离按 ref 级别生效。

## ❌ MERGE INTO 写指定 branch

```sql
-- 暂未实现
MERGE INTO orders.branch_dev t USING ... ON ... WHEN MATCHED ...;
```

phase 1 的 branch DML 只覆盖 INSERT / UPDATE / DELETE。MERGE INTO 路径需要补 branch-aware 的 commit hooks，路线图上跟踪。

## ❌ Spark 风格 `t@<branch>` 写入语法

```sql
-- 暂未实现
INSERT INTO orders@dev VALUES (...);
```

NovaRocks 用 `t.branch_<x>` 替代。后续若上线 cross-engine 兼容，会在 parser 层补 `@<ref>` 别名。

## ❌ FAST FORWARD / CHERRYPICK / ROLLBACK

Spec：

```sql
-- 暂未实现
ALTER TABLE t FAST FORWARD main TO dev;
ALTER TABLE t CHERRYPICK SNAPSHOT 1234567890123;
ALTER TABLE t ROLLBACK TO VERSION 1234567890123;
ALTER TABLE t ROLLBACK TO TIMESTAMP '2026-05-01 12:00:00';
```

**TODO**：未实现。

替代方案：

- 想"回滚 main 到某个老 snapshot"：用 `CREATE OR REPLACE BRANCH main AS OF VERSION <id>`（**注意**：这会把 main 直接改写为指向旧 snapshot，效果与 ROLLBACK 类似但绕开了 spec 的 "rollback log" 语义）。
- CHERRYPICK：暂无替代，需要等实现。
