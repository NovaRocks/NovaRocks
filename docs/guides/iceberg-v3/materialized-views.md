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

# Iceberg 物化视图与增量刷新

NovaRocks 的受管物化视图以 Iceberg 表保存结果。本文说明当前支持的入口和边界；部署中 FE 与 BE 使用正常的原生角色，测试采用 1 FE + 3 BE。

## 使用前提

- 使用 Iceberg REST catalog。原生 Hadoop/Hive catalog 不提供受管 MV 写入能力，管理操作在外部副作用前拒绝。
- 基表是 Iceberg format v3，且准确所选快照带 row range。`write.row-lineage` 不参与判断；从 v2 升级但尚未进行 v3 提交的快照会被拒绝，报错包含所选快照与恢复办法：执行一次 v3 提交，例如 OPTIMIZE，再重试。
- MV 目标表只要求格式 v3；新建目标不注入 `write.row-lineage`。
- MV 目标与基表位于同一 Iceberg catalog。定义的关系、字段和目标布局必须能由当前 provider 事实准确绑定；无法证明的形状拒绝，不猜测旧字段 ID 或 snapshot。
- 首次刷新需要基表当前有可钉住的 snapshot。刚创建、从未产生 snapshot 的空基表当前使刷新走 `SkipEmpty`；有 snapshot 但可见行数为零的基表可以发布零行结果。

## 创建与刷新

```sql
SET CATALOG ice_rest;
USE analytics;

CREATE MATERIALIZED VIEW orders_by_region
DISTRIBUTED BY HASH(region) BUCKETS 1
REFRESH DEFERRED MANUAL
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT region, COUNT(*) AS order_count, SUM(amount) AS total_amount
FROM ice_rest.analytics.orders
GROUP BY region;

REFRESH MATERIALIZED VIEW orders_by_region WITH SYNC MODE;
SELECT region, order_count, total_amount FROM orders_by_region ORDER BY region;
```

`CREATE` 只建立目标和定义、解释、配置文档；它不发布数据 snapshot。首次成功刷新创建结果 snapshot 及与它准确绑定的发布文档。后续刷新按已发布输入与 provider 的准确版本选择增量路径；结果为零行的增量也发布一个新 snapshot，以推进已处理输入的水位。刷新策略及暂停状态只更新配置文档，不制造数据 snapshot。

显式 `REFRESH MATERIALIZED VIEW ... FULL` 从准确当前基表版本重算，并通过同一发布会话覆盖已发布目标；当前基表没有 snapshot 时会在准备阶段拒绝。已用原生 1 FE + 3 BE 验证投影 MV 在基表变化后及输入不变时重复 FULL 均不追加重复行，并验证聚合 MV 在重启和管理接续后再次 FULL。增量能力由定义形状、基表变更和精确字段绑定共同决定。已覆盖的投影、过滤、聚合、受限连接与 UNION 形状以 [`iceberg-ivm` 用例](../../../tests/sql/correctness/iceberg-ivm/sql/) 为准，不能推断任意 SQL 都能增量维护。基表引用列改名目前因缺少 occurrence-aware SQL 字段重绑定而拒绝；增量 CROSS JOIN 也未开放。

## 自动刷新与暂停

创建时可以选择 `REFRESH ASYNC ON CHANGE` 或 `REFRESH ASYNC EVERY INTERVAL ...`；也可以通过 `ALTER MATERIALIZED VIEW ... SET REFRESH` 更新策略，通过 `PAUSE REFRESH` 与 `RESUME REFRESH` 控制调度。这些是受管配置写入，走 MV 管理入口并只更新配置文档。当前策略与暂停/恢复的原生回归见 [`iceberg-mv-scheduler` 用例](../../../tests/sql/correctness/iceberg-mv-scheduler/sql/)。

FE 重启后，湖中已发布的 MV 仍可读；新进程默认不能立即接管旧进程的管理写入。运维应先查看 `novarocks_mv_management_status`，再按[物化视图管理接续指南](../deployment/mv-management-continuation.md)提供适用证据。读到目标不等于获得管理权。

受管 MV 的自动维护当前关闭，包括自动压缩、position delete 重写和 snapshot 过期。维护路径尚不能为新输出准确附着发布文档并证明历史文档保留；手工维护受管 MV 也会在副作用前拒绝。普通 Iceberg 表的维护能力不受此限制。

## 查询改写

查询改写只在候选 MV 的定义、输入与输出版本、列绑定和查询语义都能证明匹配时使用。可以在 session 中开启并用 `EXPLAIN` 检查是否命中：

```sql
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT region, SUM(amount)
FROM ice_rest.analytics.orders
GROUP BY region;
```

命中时计划包含 `rewritten with mv: <name>`。没有充分证明时保留基表计划；开启开关不保证每条查询都命中。当前匹配范围以 [`mv-rewrite` 用例](../../../tests/sql/correctness/mv-rewrite/sql/) 为准。

## 验证范围

`mv-storage-contract` 使用独立 REST catalog 与 MinIO、原生 1 FE + 3 BE，检查文档发布、重启后湖恢复和管理接续。`mv-storage-physical-occ` 使用另一套独立 REST catalog 与 MinIO，验证外部写者推进同一目标后，冻结 main 条件的 MV 发布被拒绝，旧结果不会重放；该场景会留下没有新 P 附着的外部输出，因此不与重启恢复场景共用湖。`iceberg-ivm` 覆盖增量形状，`iceberg-mv-scheduler` 覆盖自动刷新策略。自动维护输出附着、历史文档保留与 Unknown 清理仍待后续工作。
