# 分区与分区演进

> Iceberg 用 partition transform 而不是物理目录绑定分区，支持隐藏分区 + 演进。NovaRocks 覆盖了主要 transform 与 evolution DDL，但 V3 新增的 `void` transform 与 partition stats puffin 仍未实现。

| 能力 | 状态 | 备注 |
| --- | --- | --- |
| 分区写入：identity / bucket / truncate / year / month / day / hour | ✅ | |
| Partition evolution DDL（`ADD/DROP PARTITION COLUMN`） | ✅ | |
| 跨历史 partition spec 的扫描 | ✅ | |
| DELETE 跨历史 partition spec | ✅ | |
| OPTIMIZE 跨历史 spec（compact 到当前 spec） | ✅ | |
| Partition transform `void`（V3） | ❌ | |
| Dynamic partition overwrite | ❌ | |
| Partition stats puffin（V3 `partition-stats-blob`） | ❌ | 写入 + 消费均未实现 |
| Partition spec 修复 / 等价合并 | ❌ | |

---

## ✅ 分区写入（identity / bucket / truncate / year / month / day / hour）

```sql
CREATE TABLE events (
  id BIGINT,
  ts TIMESTAMP,
  country STRING,
  user_id BIGINT
)
PARTITION BY (
  day(ts),                 -- year(ts) / month(ts) / hour(ts) 同理
  country,                 -- identity
  bucket(64, user_id),
  truncate(10, country)
);
```

读 / 写 / DELETE / UPDATE 都会自动按 partition transform 路由。

## ✅ Partition evolution DDL

```sql
ALTER TABLE events ADD PARTITION COLUMN month(ts);
ALTER TABLE events DROP PARTITION COLUMN day(ts);
```

NovaRocks 实现：

- ✅ 跨历史 partition spec 的扫描：老 data file 仍按其当时的 spec_id 解释，新写入按当前 spec
- ✅ DELETE 跨历史 spec：DELETE 计划会同时考虑历史 spec 文件
- ✅ OPTIMIZE 跨历史 spec：whole-table compact 会把所有 spec 的文件重写到当前 spec

## ❌ Partition transform `void`（V3）

Spec：v3 引入 `void(col)` transform，作用是"声明这一列不再用作分区，但仍保留 column 在 schema 里"。等价于隐藏旧分区的快捷写法。

**TODO**：未实现。当前不接受 `void(col)` 语法，要"隐藏旧分区"只能 `DROP PARTITION COLUMN`。

## ❌ Dynamic partition overwrite

Spec / Spark 行为：`INSERT OVERWRITE` 默认替换整张表；带 `OVERWRITE PARTITIONS` 子句或在 dynamic 模式下，仅替换被本次写入命中的分区。

**TODO**：未实现。当前 `INSERT OVERWRITE` 总是替换全表（或显式指定的静态分区），动态分区覆盖不可用。

替代方案：先 `DELETE FROM t WHERE <partition condition>`，再 `INSERT INTO t SELECT ...`。

## ❌ Partition stats puffin（V3 `partition-stats-blob`）

Spec：v3 在 Puffin 文件中记录每个 partition 的统计（行数 / 文件数 / 列 min-max），让 planner 在 partition pruning 之外多一层粗粒度跳过。

**TODO**：未实现。

- 写端：写出的 Puffin 不包含 `partition-stats-blob`
- 读端：planner 不会去读 partition stats puffin

## ❌ Partition spec 修复 / 等价合并

Spec：偶尔需要把"语义等价、字面不同"的 partition spec 合并（例如 `bucket(64, x)` 在不同 spec 中重复出现）。

**TODO**：未实现，不常用，路线图低优。
