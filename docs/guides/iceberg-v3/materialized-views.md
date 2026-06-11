# 物化视图与 IVM（NovaRocks 差异化）

> NovaRocks 的差异化卖点是"以查询为主力优化点的 Iceberg 引擎"，**MV + IVM 是这条路线的核心**。读路径已经覆盖到 MERGE / UPDATE 增量刷新；最大单点缺口仍是 **MV 自动 query rewrite**。

| 能力 | 状态 | 备注 |
| --- | --- | --- |
| Iceberg-backed MV 定义 | ✅ | `src/connector/starrocks/table/mv_*.rs` |
| MV 全量刷新 | ✅ | |
| IVM —— Insert | ✅ | |
| IVM —— V2 position-delete | ✅ | |
| IVM —— V3 deletion vector | ✅ | |
| IVM —— Equality delete | ✅ | |
| IVM —— Insert Overwrite（fallback 全刷） | ✅ | |
| IVM —— V3 row-lineage 行级 delete 复用 `_row_id` | ✅ | |
| IVM —— Schema evolution 安全 fallback | ✅ | 不安全演进触发全刷 |
| IVM —— Partition evolution | ✅ | |
| IVM —— COW UPDATE | ✅ | PR #76 |
| IVM —— MOR UPDATE | ✅ | PR #76 |
| IVM —— MERGE INTO（COW + MOR） | ✅ | PR #78 |
| 投影 / 过滤 MV | ✅ | |
| 聚合 MV：SUM / COUNT / AVG / MIN / MAX | ✅ | |
| **MV 自动 query rewrite** | ❌ | NovaRocks 差异化最关键的单点 |
| MV freshness contract / staleness budget | ❌ | |
| 多基表 JOIN 的 IVM | ❌ | |
| Window 函数 MV | ❌ | |
| 含 DISTINCT / 子查询的 MV | ❌ | |
| 跨 catalog 的 MV | ❌ | |
| MV 物化结果存到 Iceberg | ❌ | 当前物化在 StarRocks table |

---

## ✅ 定义一个 Iceberg-backed MV

```sql
CREATE MATERIALIZED VIEW orders_daily
DISTRIBUTED BY HASH(user_id) BUCKETS 8
REFRESH ASYNC
AS
SELECT
  date_trunc('day', ts) AS day,
  user_id,
  SUM(amount) AS total,
  COUNT(*)    AS cnt
FROM ice.demo.orders
GROUP BY date_trunc('day', ts), user_id;
```

> 当前 MV 物化在 StarRocks table；让 MV 自身也作为 Iceberg 表对外暴露还在路线图上。

## ✅ 增量刷新（IVM）覆盖范围

下列基表变更都会**走增量 delta 路径**，不会触发全刷：

- INSERT
- DELETE（V2 position-delete / V3 DV / equality-delete 三种 delete 模型）
- UPDATE（COW + MOR）
- MERGE INTO（COW + MOR，PR #78）
- INSERT OVERWRITE：fallback 到全刷（spec 行为，不是 bug）
- Schema evolution（add/drop/rename/widen）：安全演进继续增量；不安全的演进（例如 ARRAY 元素 widen 当前未支持）触发全刷
- Partition evolution：增量

## ✅ 聚合 MV：SUM / COUNT / AVG / MIN / MAX

聚合 MV 在基表 INSERT / DELETE / UPDATE / MERGE 后做"组合 / 反组合"运算更新，不需要重算。AVG 通过保持 sum + count 实现增量；MIN / MAX 通过额外维护辅助状态实现。

## ❌ MV 自动 query rewrite

> NovaRocks 路线图上"以查询为主力优化点"立得住与否的最大单点。

Spec / 业界实践：用户写 SQL 不显式引用 MV，optimizer 基于 MV 定义判定能否改写、改写到哪条 MV、改写后的 cost 是否更低。Spark 的 MV、Snowflake 的 MV、Materialize 的 view 都依赖这一步。

NovaRocks 当前**只支持显式引用 MV**：

```sql
-- ✅ 显式引用
SELECT * FROM orders_daily WHERE day >= '2026-05-01';

-- ❌ 改写后命中（暂不可用）
SELECT date_trunc('day', ts) AS day, user_id, SUM(amount)
  FROM ice.demo.orders
 WHERE ts >= '2026-05-01'
 GROUP BY date_trunc('day', ts), user_id;
-- 当前 optimizer 不会自动改写到 orders_daily
```

**TODO**：核心改写规则（aggregate / projection / filter / join）+ MV 选择 cost model + freshness 校验（见下条）。

## ❌ MV freshness contract / staleness budget

Spec / 工程实践：MV 声明可接受的过期时间（"最多落后基表 5 分钟"），optimizer 在自动改写时把过期 MV 排除掉。

**TODO**：未实现。

## ❌ 多基表 JOIN 的 IVM

当前 IVM 仅支持 single-table base。多基表 JOIN 的 IVM 需要 delta + outer join / semi-join 的代数（参考 IncMV / DBToaster），是路线图项。

**TODO**：未实现。

## ❌ Window 函数 MV

Window 函数（`ROW_NUMBER` / `LAG` / 滑动聚合等）的 IVM 算法复杂度高，当前 MV 不允许包含 window 函数。

**TODO**：未实现。

## ❌ 含 DISTINCT / 子查询的 MV

`SELECT DISTINCT` / 嵌套子查询当前在 MV 定义中会被 reject。

**TODO**：未实现。

## ❌ 跨 catalog 的 MV

例如 base table 在 REST catalog A，MV 物化在 catalog B。

**TODO**：未实现。当前 MV 只能与基表同 catalog。

## ❌ MV 物化结果存到 Iceberg

当前 MV 物化在 StarRocks table 内部表。让 MV 自身也作为 Iceberg 表对外可读（被 Spark / Trino / PyIceberg 消费）能更好融入湖仓生态。

**TODO**：未实现。
