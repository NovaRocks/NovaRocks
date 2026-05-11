# Iceberg MV A8 Target Catalog Design

## 背景

A8 的目标是把 Iceberg-backed MV 的 target 从 NovaRocks 内部 managed-lake
假壳中拆出来，明确成一张普通 Iceberg table。

当前代码已经有两个雏形：

- target 物理数据写入 Iceberg table。
- NovaRocks 在 SQLite 中保存 `select_sql`、base refs、snapshot 等刷新关系。

但当前实现仍有架构偏差：

- target 默认写入内部 `__nova_mv__` catalog，而不是当前用户选择的 Iceberg
  catalog / namespace。
- `CREATE MATERIALIZED VIEW` 会往 managed-lake `tables` 写一行
  `kind = MaterializedView`、`current_schema_id = 0` 的假表行。
- 查询注册通过枚举当前 Iceberg data files，把 target 降级成
  `TableStorage::S3ParquetFiles`，而不是 first-class Iceberg table。
- 启动恢复和 catalog registration 混用了 managed-lake table rebuild 与
  Iceberg MV 特化逻辑。

## 设计原则

Iceberg-backed MV target 本质上就是一张普通 Iceberg table。

外部系统视角：

- Spark / Trino / Flink 看到的是 `catalog.namespace.table`。
- target table 的 schema、snapshots、manifests、data files、delete files、
  table properties 全部属于 Iceberg catalog。
- 外部系统不需要知道这张表在 NovaRocks 内部被当作 MV target 维护。

NovaRocks 视角：

- NovaRocks 额外维护的是 MV 到 base tables 的刷新关系。
- 关系元数据包括 target identifier、base identifiers、`select_sql`、last
  refresh base snapshots、last target snapshot、refresh state 等。
- A8 继续用 SQLite 保存关系元数据，但不再把 Iceberg MV 伪装成 managed-lake
  table。后续 A12 再把这层抽成 `MvMetadataStore` trait。

## 目标

- `CREATE MATERIALIZED VIEW ... PROPERTIES('storage_engine'='iceberg')` 在当前
  session catalog/database 下创建 target Iceberg table。
- target table name 等于 MV name。
- `SELECT * FROM mv` 走普通 Iceberg table 查询路径。
- `SHOW/REFRESH/DROP MATERIALIZED VIEW` 读取 NovaRocks 的 MV 关系元数据。
- 不保留旧 `__nova_mv__` 兼容路径，不做旧 metadata 迁移。
- 保留现有 refresh 能力边界，不把 A9/A10 的 delete/update apply 合入 A8。

## 非目标

- 不支持 adopt 已存在的 Iceberg table 作为 MV target。
- 不支持 `DROP MATERIALIZED VIEW ... KEEP DATA`。
- 不实现 `MvMetadataStore` trait；该工作记录到 A12。
- 不新增 delete/update-bearing IVM apply 能力。
- 不做旧 `__nova_mv__` metadata 或物理表迁移。
- 不默认创建 Iceberg v3 row-lineage target。A8 默认创建普通 Iceberg v2 table。

## SQL 语义

Iceberg MV 复用内表 MV 的入口风格：

```sql
SET CATALOG ice;
USE analytics;

CREATE MATERIALIZED VIEW mv_orders
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT ...
```

语义：

- target identifier 是 `ice.analytics.mv_orders`。
- 当前 catalog 必须是 Iceberg catalog，否则报错。
- 当前 database / namespace 必须能解析为 Iceberg namespace，否则报错。
- target table 必须不存在，否则报错。
- `AS SELECT` 中的 base tables 必须是 Iceberg tables，允许跨 catalog。
- `CREATE` 只创建空 target table 和 MV 关系元数据，不写数据。
- 显式 `REFRESH MATERIALIZED VIEW` 才做 initial load。

## 关系元数据

A8 继续使用 SQLite，但语义上它保存的是 NovaRocks 私有 MV 关系元数据，而不是
target Iceberg table 的物理 metadata。

建议关系字段：

- `mv_id`
- `logical_mv_name`
- `target_catalog`
- `target_namespace`
- `target_table`
- `select_sql`
- `base_table_refs`
- `base_table_uuids`
- `last_refresh_snapshots`
- `last_target_snapshot_id`
- `refresh_in_progress`
- `refresh_target_snapshots`
- `created_at_ms`
- `updated_at_ms`

重要变化：

- 不再调用 `allocate_iceberg_mv_table_row`。
- 不再往 managed-lake `tables` 插入 MV 假行。
- `iceberg_table_identifier` 不再是 managed-lake MV 的附属字段；target
  identity 是 Iceberg MV 关系元数据的核心字段。
- `last_refreshed_iceberg_snapshot_id` 语义整理为 `last_target_snapshot_id`，
  用于判断 target 是否被外部写入。

## CREATE 流程

1. 从 session 读取 current catalog 和 current database。
2. 验证 current catalog 是 Iceberg catalog。
3. 构造 target identifier：`current_catalog.current_database.mv_name`。
4. 检查 target table 不存在；存在则 fail fast。
5. 分析 `AS SELECT`，提取 base refs。
6. 验证所有 base refs 都是 Iceberg tables，允许跨 catalog。
7. 根据 SELECT 输出列创建空 Iceberg v2 target table。
8. 写入 MV 关系元数据。
9. 用标准 Iceberg table registration path 注册 target 到 in-memory catalog。
10. 不执行 initial refresh。

如果 target table 创建成功但关系元数据写入失败，A8 应尽量 drop target table
做补偿；如果补偿失败，返回错误并提示 orphan target table。

## REFRESH 流程

1. 根据 current catalog / database / MV name 查 MV 关系元数据。
2. load target Iceberg table。
3. 检查 target current snapshot 是否等于 `last_target_snapshot_id`。
   - 不一致说明 target 被外部写入或手动修改，fail fast。
4. load base Iceberg tables。
5. 检查 base table UUID 和关系元数据中记录的 UUID 一致。
6. 保留当前 refresh 能力边界：
   - initial refresh
   - append-only incremental refresh
   - 现有 rebuild / fallback 策略
7. 写 target 后更新 `last_target_snapshot_id` 和 base snapshot map。
8. refresh 成功后重新注册 target table，让后续 SELECT 看到最新 Iceberg snapshot。

A8 不新增 delete/update-bearing target apply 能力。如果当前 change batch 触发
既有 fallback / rebuild / unsupported 行为，保持该行为。

## DROP 流程

`DROP MATERIALIZED VIEW mv` 的语义：

1. 根据 current catalog / database / MV name 查 MV 关系元数据。
2. drop target Iceberg table。
3. 删除 MV 关系元数据。
4. 从 in-memory catalog 移除 target table。

A8 不支持 keep data。因为 target table 由 `CREATE MATERIALIZED VIEW` 创建，
NovaRocks 是 owner。

如果 target drop 成功但关系元数据删除失败，返回错误并记录清晰日志。A7/A12
后续再系统化处理 crash recovery 和 commit/finalize 原子性。

## SHOW 和 SELECT

`SHOW MATERIALIZED VIEWS`：

- 只读取 NovaRocks MV 关系元数据。
- 不通过 Iceberg catalog 反推哪些表是 MV。

`SELECT * FROM mv`：

- 走普通 Iceberg table 查询路径。
- 不读取 MV 关系元数据。
- 不区分该 table 是否是 MV target。

这个边界保证外部系统和 NovaRocks 普通 SELECT 都看到同一张普通 Iceberg table。

## 错误处理

必须 fail fast 的情况：

- 当前 catalog 不是 Iceberg catalog。
- 当前 database / namespace 未设置或无法解析。
- target table 已存在。
- base table 不是 Iceberg table。
- base table UUID 与关系元数据记录不一致。
- target current snapshot 与 `last_target_snapshot_id` 不一致。
- target 创建后关系元数据写入失败且补偿 drop 也失败。

错误信息应说明具体 catalog / namespace / table identifier，避免只报
`not found` 或 `unsupported`。

## 测试计划

测试重点是 catalog/metadata 边界，不扩大 IVM 算子能力。

1. `CREATE` target identity
   - `SET CATALOG ice; USE analytics; CREATE MATERIALIZED VIEW mv ...`
   - 验证 target 是 `ice.analytics.mv`。
   - 验证不写 managed-lake `tables` 假行。
   - 验证 SQLite 只保存 MV 关系元数据。

2. target 已存在报错
   - 先创建普通 Iceberg table `ice.analytics.mv`。
   - 再 create same-name MV。
   - 期望 fail fast。

3. 当前 catalog 不是 Iceberg 报错
   - 切到非 Iceberg catalog。
   - 创建 `storage_engine='iceberg'` MV。
   - 期望 fail fast。

4. 跨 catalog base
   - target 在 `ice_target.analytics.mv`。
   - base 在 `ice_src.sales.orders`。
   - 创建成功，关系元数据保存完整 base identifier。

5. `CREATE` 不写数据
   - CREATE 后 target table 存在但为空。
   - REFRESH 后才有数据。

6. SELECT 普通 Iceberg path
   - REFRESH 后 `SELECT * FROM mv` 能读到 target 表。
   - 查询不依赖 MV 关系元数据。

7. SHOW / REFRESH / DROP 走关系元数据
   - `SHOW MATERIALIZED VIEWS` 能列出 MV。
   - `DROP MATERIALIZED VIEW mv` 删除关系元数据和 target table。

8. 重启恢复
   - CREATE + REFRESH 后重启 standalone-server。
   - `SHOW MATERIALIZED VIEWS` 仍能看到关系。
   - `SELECT * FROM mv` 仍按普通 Iceberg 表查询。

9. 外部写入检测
   - REFRESH 后通过普通 Iceberg write path 修改 target table。
   - 再 REFRESH MV。
   - 期望 target snapshot mismatch 报错。

10. 保留现有 refresh 能力边界
   - initial refresh / append-only incremental 继续通过。
   - delete/update-bearing change batch 不在 A8 中新增 apply 能力。

## 后续工作

- A10：target 写入接入统一 `IcebergCommitCollector` / `run_iceberg_commit`。
- A9：定义 target row identity 与 delete/update incremental apply 协议。
- A12：抽象 `MvMetadataStore` trait，SQLite 只是当前实现。
- A7：refresh transaction 与 crash recovery。
