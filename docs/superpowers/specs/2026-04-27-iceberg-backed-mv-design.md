# Iceberg-Backed Materialized View 设计

**Status:** Draft for user review
**Date:** 2026-04-27
**Builds on:**
- `docs/superpowers/specs/2026-04-23-mv-on-iceberg-phase1-design.md`
- `docs/superpowers/specs/2026-04-24-mv-on-iceberg-phase2-design.md`
- `docs/superpowers/specs/2026-04-26-mv-on-iceberg-aggregate-ivm-design.md`

## 1. 目标

把 NovaRocks 物化视图的物理 storage 从 managed lake 内表切换为 Iceberg 表,
落在 NovaRocks 内嵌的私有 Iceberg catalog,达到 "开放 lakehouse" 产品定位:
物理文件全部用开放格式 (Iceberg metadata + Parquet data files),元数据访问
通过 NovaRocks 接口控制 visible/hidden schema。

本 phase 称为 phase4,分两段实施:

- **phase4a**: projection/filter MV on Iceberg,append-only incremental refresh
- **phase4b**: 单表 `count/sum` aggregate IVM on Iceberg,equality-delete + new
  data file 模拟 upsert

phase1/2/3 的 managed lake 内表实现保留并存,通过 `storage_engine` PROPERTIES
切换。本 design 覆盖 phase4a + phase4b 总体范围;实施时 phase4a 和 phase4b 各
对应一个独立 implementation plan,分别 review 和 land。

## 2. 范围

### 2.1 phase4a 支持

- base 是 Iceberg 表(单表)
- projection/filter MV(无 GROUP BY,无 join,无 subquery)
- 手动 `REFRESH MATERIALIZED VIEW`
- 首次 full refresh + 后续 append-only incremental refresh

### 2.2 phase4b 支持

- 在 phase4a 基础上加 single-table append-only `count(*)` / `count(col)` /
  `sum(col)` aggregate MV
- 通过 hidden `__row_id__` 和 `__agg_state_*` 列做 IVM
- equality-delete + new data file 模拟 upsert(Iceberg v2 row-level update)

### 2.3 phase4 不支持

- `min` / `max` / `avg` / `DISTINCT` / HLL / bitmap / percentile / window /
  `HAVING` / rollup / cube
- join / union / subquery / CTE
- 多 base table
- query rewrite(用户仍需 `SELECT * FROM mv`)
- automatic / scheduled refresh
- 用户直接 DDL 操作 NovaRocks 内嵌 catalog `__nova_mv__`
- base 上的 delete / overwrite / equality-delete snapshot(沿用 phase2/3
  fail-fast)
- MV chain(MV 依赖 MV)
- partition-level refresh
- 外部引擎(Spark / Trino)直接读 Iceberg 表时的 visible-only schema —
  外部访问必须经 NovaRocks 元数据接口

## 3. 用户可见语义

`CREATE MATERIALIZED VIEW`:

```sql
CREATE MATERIALIZED VIEW mv
DISTRIBUTED BY HASH(k) BUCKETS 2
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT k, count(*) AS c
FROM ice.ns.orders
WHERE v > 0
GROUP BY k;
```

- `storage_engine` 取值 `iceberg` 或 `managed_lake`
- 缺省由 system config `mv_default_storage_engine` 决定
- 初期 default = `managed_lake`(兼容 phase1-3 现有行为)
- phase4b 稳定后 default 切到 `iceberg`,phase1-3 内表路径 deprecated

`SELECT * FROM mv` / `REFRESH MATERIALIZED VIEW` / `DROP MATERIALIZED VIEW` 在两
种 storage 上 SQL 表面完全一致,SQLite metadata 路由内部分发。

`SHOW MATERIALIZED VIEWS` 增加一列展示当前 `storage_engine`。

## 4. NovaRocks 内嵌 Iceberg Catalog

### 4.1 Catalog 实例

复用现有 `HadoopFileSystemCatalog`(`src/connector/iceberg/catalog/hadoop_catalog.rs`),
新建一个 NovaRocks 内嵌专用实例:

- catalog name = `__nova_mv__`(双下划线前缀防止用户命名冲突)
- 由 NovaRocks 启动时 bootstrap,注册到现有 `catalog::registry`
- **不暴露给 `CREATE EXTERNAL CATALOG` 语法**(parser 层 reject 用户使用
  `__nova_mv__` 作为 catalog name)
- `SHOW CATALOGS` 不展示 `__nova_mv__`

### 4.2 Warehouse 路径

- 默认 `${managed_lake_root}/_nova_iceberg_mv/`
- 可通过 system config `mv_iceberg_warehouse_location` 覆盖
- 与 managed lake 内表 warehouse 完全隔离

### 4.3 Namespace / 表名映射

用户 SQL `CREATE MATERIALIZED VIEW <user_db>.<mv_name>` →
Iceberg 表 identifier `__nova_mv__.<user_db>.<mv_name>`。

- 跟用户 db 平铺,易于运维定位
- 删除用户 db 时联动清理对应 namespace
- catalog 不暴露给用户,但 physical layer 仍按用户 db / mv_name 组织

## 5. MV Metadata Schema

所有 MV metadata 仍存 NovaRocks managed lake SQLite,**不引入 Iceberg snapshot
properties 作为 source of truth**。phase3 `materialized_views` 表新增字段:

```sql
ALTER TABLE materialized_views
  ADD COLUMN storage_engine TEXT NOT NULL DEFAULT 'managed_lake';
ALTER TABLE materialized_views
  ADD COLUMN iceberg_table_identifier TEXT;
ALTER TABLE materialized_views
  ADD COLUMN last_refreshed_iceberg_snapshot_id INTEGER;
```

- `storage_engine` ∈ {`managed_lake`, `iceberg`}
- 当 `storage_engine = 'iceberg'` 时:
  - `iceberg_table_identifier` 必填,值如 `__nova_mv__.user_db.mv_name`
  - `last_refreshed_iceberg_snapshot_id` 记录 MV 表本身最近一次 refresh
    commit 后的 snapshot id(用于 cross-check 和后续 reconcile)
  - 已有的 `last_refreshed_base_snapshot_id` / `last_refresh_state` /
    shape / SQL definition 字段全部继续用
- 当 `storage_engine = 'managed_lake'` 时新字段为 NULL,phase3 行为不变

SQLite migration: v5 → v6 ALTER TABLE 加列,默认值保证旧 MV 不受影响。

## 6. Phase 4a — Projection/Filter MV on Iceberg

### 6.1 物理 schema

MV Iceberg 表只有 visible columns,跟 base 投影一致:

```text
__nova_mv__.user_db.mv:
  k  INT
  v  BIGINT
  ...
```

无 hidden 列。Iceberg `Schema` / `PartitionSpec` / `SortOrder` 由 NovaRocks 在
CREATE 时确定;phase4a 不允许用户指定 partition by,所有表为 unpartitioned。

### 6.2 CREATE 流程

1. parse MV definition,跑 `mv_shape::classify_projection_filter`
2. 构造 Iceberg `Schema`(列名 / 类型来自 analyzed projection)
3. 通过 `__nova_mv__` catalog 的 `create_namespace`(若不存在)+ `create_table`
4. 在 SQLite `materialized_views` 插入 row:`storage_engine = 'iceberg'`,
   `iceberg_table_identifier = __nova_mv__.user_db.mv_name`
5. CREATE 不触发 refresh(跟 phase1 行为一致)

### 6.3 First refresh

1. 解析 MV definition,classify shape
2. 用 base 当前 snapshot 跑 `SELECT ... FROM base WHERE ...`
3. 输出 chunks 喂给 Iceberg sink(复用 `src/connector/iceberg/sink.rs`),
   写 N 个 Iceberg data files
4. `Transaction::new(table).fast_append().add_data_files(...).commit()` →
   MV 表新增 snapshot S1
5. 更新 SQLite `materialized_views`:
   - `last_refreshed_base_snapshot_id` = base 当前 snapshot id
   - `last_refreshed_iceberg_snapshot_id` = S1
   - `last_refresh_state` = OK

### 6.4 Atomic publish

Iceberg snapshot commit + SQLite update 顺序执行,无两阶段事务。两步之间崩溃
时:

- Iceberg 已 commit,SQLite 未更新 → 下次 refresh 通过 SQLite 状态认为还没刷
  新过,会重做一次。由于 MV 表已存在数据但 SQLite 无 lineage,**为简化第一版
  ,first refresh 在 SQLite 写入失败时通过 catalog `drop_table` + 重建 empty
  回滚 Iceberg 表**。
- Incremental refresh 中崩溃留下的 inconsistent state(Iceberg 有 snapshot
  S2 但 SQLite 仍指向 S1)→ 下次 refresh 时检测 `last_refreshed_iceberg_snapshot_id`
  与 Iceberg 表 current snapshot 不一致 → 标记为 inconsistent state,fail
  fast 报错,要求人工介入。**phase4 不实现自动 reconcile**。

后续可演进到 Iceberg snapshot properties 双写实现真正 atomic;本 phase 不做。

### 6.5 Incremental refresh

1. 比对 base 当前 snapshot 与 SQLite `last_refreshed_base_snapshot_id`,确认是
   append-only descendant(复用 phase2 `plan_append_delta`)
2. 跑 `SELECT ... FROM base_delta WHERE ...`,delta = 仅新 data files
3. Iceberg sink 写新 data files
4. `Transaction::new(table).fast_append().add_data_files(...).commit()` →
   MV 表新增 snapshot S2
5. 更新 SQLite metadata

跟 phase2 内表 incremental 行为对齐,差异只在物理写路径。

### 6.6 DROP

1. SQLite `materialized_views` 删除 row
2. catalog `drop_table(__nova_mv__.user_db.mv)` 物理删除 Iceberg 表
   metadata + data files

DROP 顺序:先删 SQLite metadata,再删 Iceberg 表。中间崩溃时遗留 orphan
Iceberg 表 — phase4 不实现 GC,运维通过 catalog list 比对 SQLite
处理。

## 7. Phase 4b — Aggregate count/sum IVM on Iceberg

### 7.1 物理 schema

Iceberg 表 schema 全展开:

```text
__nova_mv__.user_db.mv:
  __row_id__         BINARY  (sort key)
  k                  visible group key
  c                  visible aggregate result
  s                  visible aggregate result
  __agg_state_c      hidden state column (BIGINT for count)
  __agg_state_s      hidden state column (sum result type)
```

Iceberg 没有 hidden column 概念,所有列都在 Iceberg `Schema` 里。NovaRocks 元
数据接口层(`src/connector/starrocks/managed/catalog.rs` 在 phase3 加的 visible
filter)负责对 NovaRocks SQL surface 只暴露 visible 列;`__row_id__` /
`__agg_state_*` 在 NovaRocks SQL 里 resolve 为 unknown column。

外部引擎直接读 Iceberg 表会看到完整 schema — 这是预定的 trade-off,跟
brainstorming 中"外部访问必须经 NovaRocks 元数据接口"前提一致。

### 7.2 `__row_id__` 编码

复用 phase3 `mv_agg_state::encode_row_id`(group keys → 稳定字节序列)。Iceberg
列类型用 `BINARY`。

### 7.3 Aggregate state codec

复用 phase3 标量 state:`count` → BIGINT,`sum` → sum result type。phase4b 不
引入 binary state codec,跟 phase3 函数集 (count/sum) 对齐。

### 7.4 CREATE 流程

跟 4a 类似,但物理 schema 多 `__row_id__` 和 `__agg_state_*`,且 Iceberg 表
`SortOrder` 设为 `__row_id__` ASC(为 incremental refresh 的按 key lookup 优
化;第一版即使没 sort 仍正确,只是慢)。

### 7.5 First refresh

跟 4a first refresh 类似,但 query 输出多了 `__row_id__` 和 `__agg_state_*`
列。无 equality-delete(没有要 merge 的旧数据)。

### 7.6 Incremental refresh

1. base append-only descendant 校验(同 4a)
2. 跑 delta aggregate query 得到 delta chunks(每行带 `__row_id__` + group
   keys + state 列)
3. 读 active MV rows by `__row_id__`(全表 scan + 按 `__row_id__` 构建
   in-memory map,跟 phase3 第一版一致)
4. delta state 与 active state merge(复用 phase3 `mv_agg_state::merge_aggregate_state`)
5. 同一个 Iceberg `Transaction` 内:
   - 新建 equality-delete file,equality 字段 = `__row_id__`,内容 = 所有被
     merge 替换的 old `__row_id__`
   - 新建 data file,内容 = merge 后的 new rows(包括新 group)
   - `commit()` 一次 — Iceberg snapshot 同时 add data file + add
     equality-delete file → atomic
6. 更新 SQLite metadata

### 7.7 Equality-delete writer

新增 `src/connector/iceberg/equality_delete.rs`,跟现有 `position_delete.rs`
平行。需要遵循 Iceberg spec:

- equality-delete file 的 partition 必须跟它要删除的 data file 一致
- 第一版 MV 表无 partition,简化为单 partition
- equality field ids 写入 file metadata 的 `equality_ids` 字段

### 7.8 读 MV

NovaRocks 读自己的 MV Iceberg 表(在 incremental refresh 第 3 步 + 用户
SELECT 时)走 PR #49 的 merge-on-read 路径。**关键确认点**:PR #49 当前覆盖
position-delete read,equality-delete read 是否覆盖需在 phase4b 实施第一步
verify;若不覆盖,phase4b plan 内补 equality-delete reader。

## 8. Storage Backend 路由

`src/connector/starrocks/managed/mv_refresh.rs`(phase3 已有)成为 dispatch 入
口:

```rust
fn refresh_materialized_view(state, mv_id) -> Result<()> {
    let row = sqlite::load_mv(mv_id)?;
    match row.storage_engine {
        ManagedLake => mv_refresh_managed_lake::refresh(state, row),
        Iceberg    => mv_refresh_iceberg::refresh(state, row),
    }
}
```

CREATE / DROP / SHOW 类似 dispatch。

phase4 新增 module:

- `src/connector/starrocks/managed/mv_refresh_iceberg.rs`(phase4 refresh 流程)
- `src/connector/starrocks/managed/mv_iceberg_catalog.rs`(NovaRocks 内嵌
  catalog instance bootstrap)
- `src/connector/iceberg/equality_delete.rs`(phase4b)

phase1/2/3 现有代码以最小侵入方式抽出 `mv_refresh_managed_lake` 子 module(rename
而非重写)。

## 9. 错误处理

| 失败点 | 行为 |
|---|---|
| base snapshot lineage 缺失 / delete / overwrite | refresh fail,不写 MV |
| Iceberg sink 写 data file 失败 | refresh fail,不 commit snapshot |
| Iceberg snapshot commit 失败 | refresh fail,不更新 SQLite |
| SQLite update 失败(在 first refresh Iceberg commit 之后) | 通过 catalog `drop_table` 回滚 Iceberg 表 |
| SQLite update 失败(在 incremental refresh Iceberg commit 之后) | 标记为 inconsistent state,后续 refresh fail-fast 要求人工介入 |
| MV Iceberg 表中出现重复 `__row_id__`(phase4b) | refresh fail,标记为 MV state corruption |
| equality-delete file 写入失败(phase4b) | refresh fail |
| `count/sum` 溢出 | refresh fail |
| `storage_engine = 'iceberg'` 但 phase4 路径未 ready | CREATE fail with 明确错误 |

## 10. 测试策略

### 10.1 单元测试

- 内嵌 `__nova_mv__` catalog bootstrap + Iceberg 表 create/drop round-trip
- SQLite v5 → v6 migration + storage_engine routing
- phase4a refresh 写 Iceberg 表 → 读回校验
- phase4b row_id encoding / state merge(直接复用 phase3 unit test)
- phase4b equality-delete writer round-trip

### 10.2 SQL regression

`sql-tests/write-path/`:

- `iceberg_backed_mv_projection_filter.sql` — phase4a,base 是 Iceberg,
  MV `storage_engine = 'iceberg'`,full + incremental refresh,SELECT 验证
- `iceberg_backed_mv_aggregate.sql` — phase4b,count/sum aggregate,full +
  incremental refresh,验证 upsert merge 正确

phase1-3 现有 case 共存:default storage_engine 改动后所有 phase1-3 case 应
显式标 `PROPERTIES('storage_engine' = 'managed_lake')` 保持原行为通过。

## 11. 完成标准

### 11.1 Phase 4a 完成

- 用户能用 `PROPERTIES('storage_engine' = 'iceberg')` 创建 projection/filter
  MV
- MV 物理表落在 `__nova_mv__.<user_db>.<mv_name>`
- First refresh + incremental refresh 行为跟 phase1/2 internal-table 路径一致
- DROP MV 清理 SQLite + Iceberg 表
- SQL regression `iceberg_backed_mv_projection_filter` 通过

### 11.2 Phase 4b 完成

- 用户能用 `PROPERTIES('storage_engine' = 'iceberg')` 创建 count/sum
  aggregate MV
- First refresh 写入完整 hidden state 布局
- Incremental refresh 用 equality-delete + new data file 实现 upsert,结果跟
  phase3 内表 aggregate IVM 等价
- 不污染 phase1-3 现有 SQL surface
- SQL regression `iceberg_backed_mv_aggregate` 通过

## 12. 风险与已知 Open Items

1. **PR #49 MoR 是否覆盖 equality-delete read** — phase4b 实施前需 verify。
   如不支持,需新增 equality-delete reader,工作量增加。
2. **iceberg-rust 0.9.0 的 equality-delete write API 完整度** — phase4b 第一
   步需实测 `iceberg-rust` 是否提供完整 equality-delete writer 接口。如不完
   整需先升级 iceberg crate 或补 PR。
3. **First refresh atomic publish 简化的回滚路径** — 当前设计是 SQLite 失败
   → drop Iceberg 表回滚。如 drop 也失败,会留 orphan 表,需要 reconcile job
   (本 phase 不实现)。
4. **Default storage engine 切换时机** — 初期 `managed_lake`,phase4b 稳定后
   切 `iceberg`。具体何时切由 phase4b stability metric 决定,不在本 design
   范围。
5. **Iceberg 表 schema evolution** — 用户对 base table 做 schema change(加
   列 / 改类型)时,MV Iceberg 表如何同步 schema 没有定义。第一版要求用户先
   DROP MV 再 CREATE。
