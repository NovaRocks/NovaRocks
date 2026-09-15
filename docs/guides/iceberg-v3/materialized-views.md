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

# 物化视图与 IVM（NovaRocks 差异化）

> NovaRocks 的差异化卖点是"以查询为主力优化点的 Iceberg 引擎"，**MV + IVM 是这条路线的核心**。读路径已经覆盖到 MERGE / UPDATE 增量刷新；MV 自动 rewrite 已支持受限的 Iceberg 聚合/投影/过滤形状，但仍不是任意 SQL 的通用改写器。

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
| **MV 自动 query rewrite** | ✅（受限） | Iceberg MV 的可证明聚合/投影/过滤形状；由 session 与 `MvRewrite` rule 双重开关控制 |
| MV freshness contract / staleness budget | ❌ | |
| 多基表 JOIN 的 IVM | ❌ | |
| Window 函数 MV | ❌ | |
| 含 DISTINCT / 子查询的 MV | ❌ | |
| 跨 catalog 的 MV | ❌ | |
| MV 物化结果存到 Iceberg | ✅ | MV target 是 external Iceberg catalog 中的 Iceberg 表 |

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

> 当前 MV target 是 external Iceberg catalog 中的 Iceberg 表；native 不创建内部 StarRocks 表。

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

## ✅ MV 自动 query rewrite（受限）

用户可以在 session 中显式开启 rewrite。optimizer 只在候选 MV、精确 base/target
binding、输出语义和成本都能证明时才选择 MV；无法证明时保留 base-table 计划，不会以
最新 snapshot、模糊候选或空结果替代原查询。`EXPLAIN` 会在命中时包含
`rewritten with mv: <name>`。

```sql
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT region, SUM(amount) FROM orders GROUP BY region;
-- 命中时包含：rewritten with mv: agg_mv

-- 可独立关闭优化规则，关闭后必须回到 base-table plan。
SET disable_optimizer_rules = 'MvRewrite';
```

当前已覆盖的具体形状以 `tests/sql/correctness/mv-rewrite/` 为准。跨 catalog、任意
join/window/DISTINCT/子查询，以及 freshness/staleness budget 均不因该受限 rewrite 而
获得支持。分布式执行时，选中的 MV target 在 dispatch 前冻结；后续刷新发布新 target
不能改写在途查询已证明的读取绑定。

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

## ✅ MV 物化结果存到 Iceberg

当前 MV target 直接创建在 external Iceberg catalog 中，不依赖内部 StarRocks 表。外部引擎能否读取还取决于所用 catalog、表格式版本与该 MV 的可见列契约。
