# Iceberg target aggregate and join aggregate IMV design

- 状态：待用户 review
- 日期：2026-05-16
- 范围：`storage_engine='iceberg'` 的 Iceberg MV target；单表 aggregate state target；两表 inner equi-join aggregate IMV；两侧 base retract 变化
- 非范围：managed-lake target join aggregate 支持

## 1. 背景

当前 NovaRocks 的 MV 能力分成两条路径：

- managed-lake target 已支持单表 aggregate MV。它的物理布局是 `__row_id__`、visible output columns、`__agg_state_*`，refresh 时会把 aggregate SELECT rewrite 成 state-shaped SELECT，再把 delta state 与旧 state 合并。
- Iceberg MV target 已支持 projection/filter 和两表 join projection/filter。join 路径已经有 multi-base snapshot pin、telescoping delta branch、join row key、target row-delta apply。

但当前 Iceberg MV target 不支持 aggregate shape。`create_iceberg_mv` 会直接拒绝 `IncrementalMvShape::Aggregate`，refresh 也只接受 `ProjectionFilter` 和 `JoinProjectionFilter`。

本任务把两个相邻能力合成一个交付：先为 Iceberg MV target 增加 aggregate state target，再在同一套 state target 上支持两表 join aggregate IMV。

## 2. 目标

1. `CREATE MATERIALIZED VIEW ... PROPERTIES('storage_engine'='iceberg') AS SELECT ... GROUP BY ...` 支持单表 aggregate MV。
2. 支持两表 inner equi-join aggregate MV，例如：

   ```sql
   SELECT d.region, COUNT(*), SUM(f.amount)
   FROM fact f JOIN dim d ON f.dim_id = d.id
   GROUP BY d.region
   ```

3. 首版必须支持两侧 base 的 retract 变化：fact delete/update、dim delete/update 都要能增量刷新正确。
4. Iceberg target 物理表保存 aggregate state，而不是只保存 visible aggregate result。
5. managed-lake target 行为保持不变，不新增 join aggregate 支持。

## 3. 非目标

- 不支持 managed-lake target join aggregate。
- 不支持 outer join、semi join、anti join、non-equi join。
- 不支持三表及以上 join。
- 不支持 `DISTINCT` aggregate、window、rollup、cube、grouping sets、subquery、CTE、set operation。
- 不把普通用户表 upsert 或 primary-key Iceberg table 功能暴露出来。
- 不重新设计 `REFRESH MATERIALIZED VIEW ... FULL`。如果现有 Iceberg-backed full refresh policy 仍要求用户重建，则保留明确错误策略。

## 4. 支持的 SQL 形态

### 4.1 单表 Iceberg aggregate target

```sql
CREATE MATERIALIZED VIEW mv
DISTRIBUTED BY HASH(region) BUCKETS 2
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT region, COUNT(*) AS c, SUM(amount) AS s
FROM ice.ns.fact
GROUP BY region;
```

### 4.2 两表 join aggregate target

```sql
CREATE MATERIALIZED VIEW mv
DISTRIBUTED BY HASH(region) BUCKETS 2
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT d.region, COUNT(*) AS c, SUM(f.amount) AS s
FROM ice.ns.fact AS f
JOIN ice.ns.dim AS d ON f.dim_id = d.id
GROUP BY d.region;
```

约束：

- join 必须是显式 two-table inner equi-join。
- join condition 必须是 `AND` 连接的列等值谓词。
- group key 必须全部出现在 projection 中。
- aggregate 支持 `COUNT(*)`、`COUNT(expr)`、`SUM(expr)`、`AVG(expr)`。
- `MIN/MAX` 首版在 Iceberg target CREATE 阶段拒绝。当前 Iceberg-backed full refresh 语义禁用，先放行 `MIN/MAX` 只会把 delete-bearing refresh 推到不可自动修复的错误路径。
- base table 必须是 Iceberg format-version 3 且 `write.row-lineage=true`。

## 5. 物理 Schema

Iceberg target aggregate MV 使用 state layout：

```text
__row_id__                         internal apply key, required string
<visible group key columns>         visible
<visible aggregate output columns>  visible
__agg_state_*                       internal aggregate state columns
```

示例：

```sql
SELECT d.region, COUNT(*) AS c, SUM(f.amount) AS s
...
GROUP BY d.region
```

物理列：

```text
__row_id__
region
c
s
__agg_state_c
__agg_state_s
```

`__row_id__` 由 group key 的 stable encoding 得到。它是 target apply key，用于定位旧 group row。join aggregate 不使用 `__nova_join_row_key` 作为 target key，因为 target 的一行代表一个 group，不代表一对 joined base rows。

Iceberg 没有真正 hidden column。NovaRocks 的 standalone catalog registration 必须只暴露 visible columns；internal refresh path 使用 physical schema。

## 6. Shape Model

`IncrementalMvShape` 扩展为：

```rust
enum IncrementalMvShape {
    ProjectionFilter(ProjectionFilterMvShape),
    Aggregate(AggregateMvShape),
    JoinProjectionFilter(JoinProjectionFilterMvShape),
    JoinAggregate(JoinAggregateMvShape),
}
```

`JoinAggregateMvShape` 复用 aggregate shape 的 group key / aggregate call / visible output 模型，并包含 join shape 信息：

```rust
struct JoinAggregateMvShape {
    join: JoinProjectionFilterMvShape,
    group_keys: Vec<GroupKeyShape>,
    aggregates: Vec<AggregateCallShape>,
    visible_outputs: Vec<VisibleAggregateOutput>,
}
```

分类顺序应先识别 aggregate surface，再区分 single-base aggregate 和 join aggregate。含 aggregate + join 的 query 不能落入 `JoinProjectionFilter`。

## 7. CREATE 流程

CREATE Iceberg aggregate target 的流程：

1. canonicalize SELECT。
2. analyze SELECT，取得 visible output columns。
3. classify `Aggregate` 或 `JoinAggregate`。
4. validate base refs：
   - single aggregate 需要一个 Iceberg base；
   - join aggregate 需要两个 Iceberg base，并匹配 join shape。
5. validate row-lineage contract。
6. build aggregate state layout。
7. create Iceberg target table with physical state columns。
8. persist MV definition and schema contract。
9. register target in standalone catalog with visible-only surface.

Schema contract 需要记录：

- single aggregate：base field-id lineage、group key lineage、aggregate input lineage、target visible columns、target `__row_id__` contract、state layout version。
- join aggregate：两个 base contracts、join predicate lineage、group key lineage、aggregate input lineage、target `__row_id__` contract、state layout version。

## 8. First Refresh

First refresh 对 target 写完整 aggregate state：

1. 捕获 base snapshot pin。
2. 将 SELECT 中所有 base table 注入 `FOR VERSION AS OF <pin>`。
3. rewrite SELECT 为 state-shaped aggregate SELECT：
   - `AVG(expr)` 展开为 `SUM(expr)` 和 `COUNT(expr)` state。
   - 如果没有 `COUNT(*)`，追加 hidden retraction count state。
4. 执行 query，得到 state-shaped result。
5. materialize 成 physical state chunks。
6. 写入 Iceberg target data files。
7. commit staging branch，publish，finalize metadata。

single aggregate 和 join aggregate first refresh 都是 full state materialization，差别只在 SELECT 是否包含 join。

## 9. Incremental Refresh: Single Aggregate

单表 Iceberg aggregate target 的 incremental refresh 复用 managed-lake aggregate 的数学模型，但 target apply 改为 Iceberg row-delta：

1. plan base change batch。
2. 构造 delta source。
3. rewrite SELECT 为 signed delta state：
   - `COUNT(*)` -> `SUM(__change_op)`
   - `COUNT(expr)` -> `SUM(CASE WHEN expr IS NOT NULL THEN __change_op ELSE 0 END)`
   - `SUM(expr)` -> `SUM(expr * __change_op)`
   - `AVG(expr)` -> signed sum + signed count
4. materialize delta state chunks。
5. scan current Iceberg target physical state rows。
6. merge old state + delta state by `__row_id__`。
7. 对 changed / removed groups 定位旧 target row，写 position deletes。
8. 对 kept / inserted groups 写新 data files。
9. commit, publish, finalize.

## 10. Incremental Refresh: Join Aggregate

Join aggregate 的目标变化量为：

```text
L1 join R1 - L0 join R0
```

当两侧都变化时，使用 telescoping 分解：

```text
DeltaL join R0 + L1 join DeltaR
```

具体流程：

1. 捕获 `{left: L1, right: R1}` snapshot pin。
2. 读取 `{left: L0, right: R0}` last refresh snapshots。
3. 分别 plan left/right change batch，判断是否有 insert/delete-bearing delta。
4. 生成 branch plans：
   - only left changed: `DeltaL(L0->L1) JOIN R0`
   - only right changed: `L1 JOIN DeltaR(R0->R1)`
   - both changed: 上述两条都执行
5. 每个 branch rewrite 成 signed aggregate state SELECT。
6. 所有 branch 的 delta state chunks 合并成一个 delta stream。
7. 读取 target old state，按 group `__row_id__` merge。
8. 对旧 group row 写 position delete，对新 group state 写 data file。
9. commit, publish, finalize.

Branch 中只有 delta side 提供 `__change_op`。snapshot side 不提供 change op。signed aggregate rewrite 必须引用 delta side 的 `__change_op`，而不是未限定的 ambiguous column。

## 11. Target State Scan and Apply

需要新增 Iceberg aggregate target state helper，职责类似 managed-lake `mv_agg_state` + `write_chunks_into_managed_partition_for_aggregate_mv_upsert` 的 Iceberg 版本：

- load active target physical rows by scanning visible + internal state columns。
- validate duplicate `__row_id__` fail fast。
- merge old and delta state。
- produce:
  - rows to delete by `__row_id__`
  - rows to insert with full physical state schema
  - new total row count
- use target locator to map `__row_id__` to `_file` / `_pos` for position deletes。
- write new data files and delete files through existing Iceberg commit collector。

Target locator can mirror existing join-key locator, but key type is string `__row_id__`.

## 12. Error Semantics

CREATE fail fast:

- unsupported shape；
- non-Iceberg target context；
- base table is not Iceberg v3 row-lineage；
- hidden column name collision；
- ambiguous unqualified column in join aggregate；
- unsupported aggregate function。

REFRESH fail fast:

- schema contract incompatible；
- base table uuid changed；
- target table uuid changed；
- duplicate target `__row_id__`；
- unsupported aggregate function somehow reaching refresh；
- branch snapshot mismatch；
- target row not found for a delete key；
- target already has equality deletes that locator cannot safely interpret。

No silent fallback to an imprecise result. If a full rebuild policy is required but explicit full refresh remains disabled, return a clear user-facing error telling the operator to drop/create/refresh manually.

## 13. Testing

Rust tests:

- `mv_shape` accepts single aggregate for Iceberg target and join aggregate。
- `mv_shape` rejects join aggregate unsupported forms: outer join, non-equi join, three-table join, missing projected group key。
- state SQL rewrite handles join aggregate and qualifies `__change_op` to the delta side。
- branch planner emits telescoping branches for left-only, right-only, and both-changed cases。
- target state merge deletes zero-count groups and inserts updated groups。
- target locator rejects duplicate `__row_id__`。

SQL tests in `sql-tests/iceberg-ivm`:

- `iceberg_ivm_aggregate_target.sql`:
  - create single-base Iceberg aggregate target；
  - first refresh；
  - append update；
  - delete-bearing refresh；
  - hidden columns are not visible。
- `iceberg_ivm_join_aggregate.sql`:
  - create fact/dim v3 row-lineage tables；
  - create join aggregate MV；
  - first refresh；
  - fact insert；
  - fact delete/update；
  - dim update/delete；
  - each refresh result matches base join aggregate query。

Focused verification:

```bash
cargo fmt -- --check
cargo test --lib connector::starrocks::managed::mv_shape -- --nocapture
cargo test --lib engine::mv::iceberg_refresh -- --nocapture
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm --mode verify --only iceberg_ivm_aggregate_target,iceberg_ivm_join_aggregate
```

## 14. Implementation Notes

Keep implementation layered:

1. Shape and layout: classify `JoinAggregate`, build Iceberg target physical schema。
2. CREATE/contract: persist enough state layout and base lineage to validate refresh。
3. First refresh: full aggregate state materialization into Iceberg target。
4. Single aggregate incremental: state merge + target row-delta apply。
5. Join aggregate incremental: telescoping branch + signed aggregate delta state。
6. SQL coverage and cleanup.

Do not extend managed-lake target. Existing managed-lake aggregate tests should remain green without semantic changes.
