# Iceberg 聚合物化视图增量维护设计

**Status:** Draft for user review
**Date:** 2026-04-26
**Builds on:**
- `docs/superpowers/specs/2026-04-23-mv-on-iceberg-phase1-design.md`
- `docs/superpowers/specs/2026-04-24-mv-on-iceberg-phase2-design.md`

## 1. 目标

本阶段目标是让 NovaRocks 的单表 Iceberg 聚合物化视图增量刷新在执行形态上对齐 StarRocks IVM：

- MV 物理表包含 hidden `__ROW_ID__` 和 hidden `__AGG_STATE_*` 列；
- `__ROW_ID__` 由 `GROUP BY` keys 编码得到，用作内部 upsert key；
- 首次 full refresh 就写入 hidden state 布局；
- 后续 append-only incremental refresh 只扫描 Iceberg append delta；
- delta rows 先聚合成 delta state，再和已有 MV state 合并；
- 合并后的结果通过 MV 内部 primary-key/upsert sink 写回同一个 `__ROW_ID__`。

这不是用户可见的通用 primary-key table 功能。本阶段只把 upsert sink 暴露给 `REFRESH MATERIALIZED VIEW` 内部使用。

## 2. 范围

支持的 MV 形状：

```sql
SELECT <group keys>, count(*) | count(col) | sum(col)
FROM iceberg_catalog.namespace.table
[WHERE <deterministic predicate>]
GROUP BY <group keys>
```

约束：

- 单个 Iceberg base table；
- base table snapshot lineage 必须是 append-only；
- group key 必须能生成稳定 `__ROW_ID__`；
- aggregate call 必须是顶层简单表达式；
- visible output columns 必须来自 group keys 和受支持 aggregate calls；
- refresh mode 仍然是手动 `REFRESH MATERIALIZED VIEW`。

本阶段不支持：

- join、union、subquery、CTE；
- `DISTINCT` aggregate；
- `avg`、`min`、`max`、HLL、bitmap、percentile 等复杂 state；
- `HAVING`、window、rollup、cube、grouping sets；
- Iceberg delete、overwrite、position delete、equality delete；
- 对用户开放 `CREATE TABLE ... PRIMARY KEY` 或手写 upsert。

## 3. 用户可见语义

用户创建和刷新 MV 的 SQL 表面不变：

```sql
CREATE MATERIALIZED VIEW mv AS
SELECT k, count(*) AS c, sum(v) AS s
FROM ice.ns.orders
WHERE v > 0
GROUP BY k;

REFRESH MATERIALIZED VIEW mv;
```

`SELECT * FROM mv` 只返回 visible columns：

```text
k, c, s
```

用户看不到 hidden `__ROW_ID__` 和 `__AGG_STATE_*`。第一版中，显式查询 hidden column 也按 unknown column 处理，避免把内部布局变成用户契约。

刷新策略：

| 条件 | 行为 |
|---|---|
| 没有已存 base snapshot | full refresh，写入完整 hidden-state 物理布局 |
| 当前 snapshot 等于已存 snapshot | no-op metadata refresh |
| 当前 snapshot 是 append-only descendant | incremental refresh，执行 delta aggregate + upsert merge |
| lineage 缺失、delete、overwrite、schema 不兼容 | fail fast，不自动 fallback full refresh |

## 4. 物理 Schema

聚合 MV 的物理 schema 为：

```text
__ROW_ID__        hidden, key, non-null
<visible group columns>
<visible aggregate result columns>
__AGG_STATE_0     hidden
__AGG_STATE_1     hidden
...
```

示例：

```sql
SELECT k, count(*) AS c, sum(v) AS s
FROM ice.ns.orders
GROUP BY k;
```

物理列：

```text
__ROW_ID__          hidden key
k                   visible
c                   visible
s                   visible
__AGG_STATE_c       hidden
__AGG_STATE_s       hidden
```

第一版 state 类型使用标量：

- `count(*)` / `count(col)` state 为 `BIGINT`；
- `sum(col)` state 为 sum result type；
- visible aggregate result 等于 finalized state。

该选择只服务第一版 `count/sum`。后续扩展 Iceberg MV 或复杂 aggregate 时，应把 state 抽象升级为可序列化 state codec，例如 `BINARY` state blob 或函数专用 `STRUCT` state。

## 5. Metadata 和 Hidden Column

当前 tablet schema 已经携带 column `visible` 字段，managed catalog rebuild 也会过滤 `visible=false` 列。但 SQLite `table_columns` 目前只持久化 name、type、nullability。本阶段需要补齐持久化列属性：

```text
table_columns.visible INTEGER NOT NULL DEFAULT 1
table_columns.is_key INTEGER NOT NULL DEFAULT 0
```

要求：

- `table_schemas.tablet_schema_pb` 与 `table_columns` 对 hidden/key 信息一致；
- catalog 注册普通查询时只暴露 visible columns；
- MV refresh 内部构造写入计划时使用 full physical schema；
- `SHOW MATERIALIZED VIEWS` 和普通 schema 展示只显示 visible columns；
- 旧 projection/filter MV 不会被解释成 aggregate-state MV。

## 6. Shape 分析

`mv_shape.rs` 从单一 projection/filter classifier 扩展为：

```rust
enum IncrementalMvShape {
    ProjectionFilter(ProjectionFilterMvShape),
    Aggregate(AggregateMvShape),
}
```

`AggregateMvShape` 记录：

```text
base_table
group_keys
aggregate_calls
visible_output_columns
state_columns
row_id_expr
```

CREATE 和 REFRESH 都重新 classify。CREATE 负责拒绝不支持形状并生成物理 schema；REFRESH 负责确认 stored SQL 仍能按 aggregate IVM 执行。

## 7. Refresh 数据流

### 7.1 首次 full refresh

首次 full refresh 执行 rewritten aggregate query，输出 full physical schema：

```text
__ROW_ID__
visible group columns
visible aggregate result columns
__AGG_STATE_*
```

写入仍走 staged partition swap：

```text
full Iceberg files
  -> aggregate query with hidden state outputs
  -> staged MV partition
  -> publish partition swap
  -> update MV metadata
```

这样首次成功后，active MV 已经具备后续 incremental refresh 所需的 old state。

### 7.2 后续 incremental refresh

已有 snapshot 时：

```text
Iceberg append delta files
  -> delta aggregate query
  -> delta rows keyed by __ROW_ID__
  -> internal MV upsert merge
  -> update MV metadata
```

merge 规则：

```text
for each delta row:
    old = lookup active MV row by __ROW_ID__
    if old exists:
        count_state = old_count + delta_count
        sum_state = old_sum + delta_sum
        write UPSERT row with same __ROW_ID__
    else:
        write INSERT row
```

增量刷新必须保证 MV 中同一个 `__ROW_ID__` 只保留一行。

## 8. Internal Upsert Sink

新增 MV 内部写入 API：

```rust
write_chunks_into_managed_partition_for_aggregate_mv_upsert(
    state,
    plan,
    delta_chunks,
    aggregate_layout,
    refresh_metadata,
)
```

职责：

- 读取当前 active MV rows；
- 按 `__ROW_ID__` 构建 old state map；
- 校验 old active 数据没有重复 `__ROW_ID__`；
- 合并 delta chunks 内同 key 行；
- 对 old state 和 delta state 做 state union；
- 写出 upsert/replacement rowset；
- 原子更新 visible version 和 MV refresh metadata。

该 API 不实现用户可见通用 PK/upsert 语义。错误信息应明确标记为 aggregate MV internal upsert。

实现可以先用最小可行的 read-old-state map。后续如果 managed-lake 增加真正 primary-key index 或 delete vector，可以把内部实现替换掉，调用方语义不变。

## 9. 和 StarRocks 的对齐点

对齐：

- hidden `__ROW_ID__`；
- hidden `__AGG_STATE_*`；
- aggregate MV 以 `__ROW_ID__` 作为 upsert key；
- delta refresh 执行 append-only Iceberg delta scan；
- delta aggregate 后与 MV old state 做 state union；
- aggregate MV refresh 写入是 UPSERT，不是 append。

有意收窄：

- StarRocks 支持 join、union all 和更多 aggregate function；本阶段只做单表 `count/sum`；
- StarRocks 使用更通用的 agg-state combinator；本阶段 state 先用标量；
- StarRocks 依赖 PK table 存储能力；NovaRocks 第一版只在 MV 内部实现 upsert sink。

## 10. 错误处理

| 失败点 | 行为 |
|---|---|
| Iceberg delta planning 发现 delete/overwrite | fail fast，不写 MV |
| snapshot lineage 缺失 | fail fast，不写 MV |
| CREATE aggregate shape 不支持 | CREATE 失败 |
| REFRESH 重新 classify 失败 | REFRESH 失败 |
| old MV active 数据有重复 `__ROW_ID__` | REFRESH 失败，提示 MV state corruption |
| delta chunks 内有重复 `__ROW_ID__` | 内部先合并 |
| count/sum overflow | REFRESH 失败，不更新 metadata |
| upsert 写入失败 | 不更新 refresh metadata |

## 11. 测试策略

单元测试：

- aggregate shape classifier accept/reject；
- physical schema 生成 hidden `__ROW_ID__` 和 `__AGG_STATE_*`；
- hidden columns 不进入普通 catalog visible columns；
- `count/sum` state merge；
- duplicate old `__ROW_ID__` fail fast；
- append-only delta 继续复用现有 `plan_append_delta` 约束。

集成测试：

- 创建单表聚合 MV：

  ```sql
  SELECT k, count(*), count(v), sum(v)
  FROM ice.ns.orders
  WHERE v > 0
  GROUP BY k;
  ```

- 首次 refresh 结果等于 full query；
- append Iceberg data 后 incremental refresh 结果等于重新 full query；
- 同一个 group key 不产生重复 MV rows；
- `SELECT * FROM mv` 不显示 hidden columns；
- 显式查询 hidden columns 报 unknown column；
- Iceberg delete/overwrite 后 refresh 仍然失败。

## 12. 完成标准

本阶段完成时：

- projection/filter MV 现有行为不回退；
- 单表 append-only aggregate MV 可以 CREATE、首次 REFRESH、后续增量 REFRESH；
- `count(*)`、`count(col)`、`sum(col)` 的结果和 full query 一致；
- MV 物理布局包含 hidden state，但用户查询只看到 visible columns；
- incremental refresh 不 append duplicate groups，而是按 `__ROW_ID__` upsert merge；
- delete/overwrite snapshot 继续 fail fast。
