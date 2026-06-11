# 时间旅行

> 时间旅行让你以"过去某个 snapshot 的状态"读表。NovaRocks 在 phase 1 中实现了 SELECT 端的全套：snapshot id / timestamp / branch & tag 名都可以作为 anchor。

| 能力 | 状态 | 备注 |
| --- | --- | --- |
| `SELECT … FOR VERSION AS OF <snapshot_id>` | ✅ | 数字字面量 |
| `SELECT … FOR TIMESTAMP AS OF '<ts>'` | ✅ | epoch-ms 整数或字符串字面量（RFC 3339 / `YYYY-MM-DD HH:MM:SS` UTC） |
| `SELECT … FOR VERSION AS OF '<branch_or_tag>'` | ✅ | 字符串 ref 名 |
| 跨 ref 自连接 / 多 ref join | ✅ | 同一张表两次扫不同 ref 也可以 |
| 表达式形式时间戳（`CURRENT_TIMESTAMP() - INTERVAL ...`） | ❌ | phase 1 只接受字面量 |
| 快照保留期读侧拒绝（拿过期 snapshot 报错） | ❌ | |

实现入口：
- `src/sql/analyzer/iceberg_ref.rs`（IcebergRefBinding 解析与校验）
- `src/sql/parser/dialect/mod.rs`（`FOR VERSION AS OF '<string>'` 的 normalizer 重写）
- `src/connector/iceberg/read.rs`（`build_read_snapshot_at` 按 snapshot_id 加载历史 schema）

---

## ✅ 按 snapshot id 时间旅行

```sql
-- 数字字面量
SELECT * FROM t FOR VERSION AS OF 1234567890123;
```

NovaRocks 会：
1. 在 metadata.json 的 `snapshots[]` 中查找 `snapshot_id=1234567890123`；找不到报错
2. 用该 snapshot 当时绑定的 schema-id + partition-spec-id 重建读 plan
3. 数据文件按该 snapshot 的 manifest 集合扫描

跨 snapshot schema evolution 是被尊重的——老 snapshot 不会看到后加的列。

## ✅ 按时间戳时间旅行

```sql
-- 字符串：RFC 3339 或 'YYYY-MM-DD HH:MM:SS'（UTC）
SELECT * FROM t FOR TIMESTAMP AS OF '2026-05-01 12:34:56';
SELECT * FROM t FOR TIMESTAMP AS OF '2026-05-01T12:34:56Z';

-- 整数 epoch-ms
SELECT * FROM t FOR TIMESTAMP AS OF 1746101696000;
```

resolver 逻辑：在 `metadata.history()` 中找 `timestamp_ms ≤ requested_ms` 的最新条目；找不到报"no snapshot at or before timestamp ..."。

### ❌ 表达式形式时间戳

```sql
-- 暂不支持
SELECT * FROM t FOR TIMESTAMP AS OF (CURRENT_TIMESTAMP() - INTERVAL 1 HOUR);
```

phase 1 只接受字面量。如果你需要"1 小时前"，目前必须在客户端先算好再传字符串字面量。

**TODO**：phase 2 让 analyzer 在 plan 阶段对常量表达式做 fold（包括 `CURRENT_TIMESTAMP() - INTERVAL`）。

## ✅ 按 branch / tag 名时间旅行

```sql
SELECT * FROM t FOR VERSION AS OF 'main';
SELECT * FROM t FOR VERSION AS OF 'dev';        -- 一个 branch
SELECT * FROM t FOR VERSION AS OF 'release_v1'; -- 一个 tag
```

> sqlparser 0.61 的 `FOR VERSION AS OF` 子句默认要求数字字面量，NovaRocks 通过 dialect normalizer 把 `FOR VERSION AS OF '<ref>'` 重写成 `FOR SYSTEM_TIME AS OF '__nr_ref:<ref>'`，再在 analyzer 阶段还原。

## ✅ 跨 ref 自连接（多版本对比）

```sql
SELECT m.id, m.v AS main_v, b.v AS bak_v
  FROM t FOR VERSION AS OF 'main' m
  LEFT JOIN t FOR VERSION AS OF 'backup' b
    ON m.id = b.id;
```

每个 FROM 子句独立绑定 ref，互不干扰。

## ❌ 快照保留期读侧拒绝

Spec：表的 `history.expire.max-snapshot-age-ms` 定义快照保留窗口；读端如果收到一个超出窗口的 snapshot id，应该报错而不是默默读旧文件。

**TODO**：未实现。当前只要 metadata.json 里还能查到 snapshot id，就允许读，不校验保留期。

---

## 与 [Branch / Tag DDL](branches-and-tags.md) 配合

时间旅行 read 路径和 branch / tag DDL 是配套的：

- 用 [`ALTER TABLE ... CREATE TAG release_v1`](branches-and-tags.md#alter-table--create-tag) 给当前 main 打个永久 ref
- 之后用 `FOR VERSION AS OF 'release_v1'` 反复回到那个版本
- 也可以 `CREATE BRANCH dev`，在 `dev` 上做隔离写，再用 `FOR VERSION AS OF 'dev'` 校验

写指定 branch 的 DML 见 [Branch / Tag DDL § 写入指定 branch](branches-and-tags.md#写入指定-branch)。
