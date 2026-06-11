# OQ-3: Predicate-Aware Cardinality Propagation — 设计

Date: 2026-05-31
Tasks:
- OQ-3 in [Optimizer Plan Quality Roadmap](../../../../../../../Documents/Obsidian/NovaRocks%20TODO/NovaRocks%20Roadmap.md#optimizer-plan-quality-roadmap)
Predecessor: OQ-1 (column pruning, PR #208), OQ-2 (join NULL filter derivation) — 三者可并行
Successor: OQ-4 (SplitAgg，**强依赖本任务产出的 post-filter / group-by 基数**)、OQ-5 (runtime filter wiring)
Scope: 单条主线（Iceberg-focused cardinality 修复），范围见 §3 / §4

战略约束（用户在 brainstorming 中明确）：
1. **严格按 roadmap 顺序**：OQ-3 是 OQ-4 的前置，先做 OQ-3。
2. **主线是 Iceberg v3**：当前 NovaRocks 不对 `StarRocks`-type（managed-lake）表做过多统计优化。OQ-3 的真实统计工作落在 Iceberg 路径。
3. **套件会转 Iceberg**：用户会在实现开始前把 join / optimizer 等套件的表从 `ENGINE=OLAP`（managed-lake）改为 Iceberg 表，因此 OQ-3 的真实统计能直接惠及这些套件的验收。

---

## 1. 目标

让**有真实统计的 Iceberg v3 表**在 `EXPLAIN COSTS` / `EXPLAIN VERBOSE` 中显示的基数反映谓词 selectivity，并让优化器内部基数（cost model 当下消费、OQ-4 后续消费）基于真实统计逐算子传播。完成时：

- 对 Iceberg 表，`SCAN t WHERE k1 < 100` 的 `stats={rows=N}` 反映 post-filter 基数（与 StarRocks `cardinality=N` 同数量级，不要求精确相等，1 个数量级以内）。
- 物理 plan 的每个节点都携带 **per-column 统计**（min/max/NDV/null fraction），不再在 extraction 阶段丢失。
- `EXPLAIN COSTS` 输出 per-node per-column 统计，对齐 StarRocks `EXPLAIN COSTS` 的可读性。
- Join / Aggregate 节点的基数不再是默认乘积式垃圾值（当前 baseline 出现 `rows=1000000000`），而是基于 join key NDV / group-by NDV 的估算，给 OQ-4 提供可用的 agg-input 基数。
- iceberg-rest 套件新增 golden case，用 `-- @explain_contains` 锁住 post-filter 基数，防回归。

---

## 2. 背景与根因

### 2.1 症状（roadmap 记录）

`EXPLAIN COSTS` 对带谓词的 scan 显示原始表行数而非 post-filter 估算：roadmap 记录 `scan k1 < 100` 显示 `rows=100000`，StarRocks 显示 `cardinality=10`。optimizer 基线 golden `sql-tests/optimizer/result/baseline_inner_join.result` 更极端：表只 `INSERT VALUES (1,10),(2,20)`（2 行），SCAN 却显示 `stats={rows=100000}`、JOIN 显示 `stats={rows=1000000000}`，且该 query **无任何谓词**。

### 2.2 关键认识修正（区别于 roadmap 的初始判断）

roadmap 写「底层 selectivity 已算但没用对」。核查代码后修正：

- selectivity 函数 `estimate_selectivity`（`src/sql/optimizer/stats.rs:1192`）存在且覆盖 eq/range/in/isnull/and/or/not。
- 它**已经接进 stats 推导链路**：`derive_scan` 的有统计分支（`stats.rs:702`）会乘 selectivity，结果写进 `LogicalProperties.row_count`，经 `extract.rs` 挂到节点、由 `format_stats_trailer`（`src/sql/explain.rs:18`）显示。
- 因此对**有列统计的 Iceberg 表，scan+filter 基数链路基本已通**。`rows=100000` 这个症状**主要是 managed-lake fallback 的产物**，不是传播数学错。套件转 Iceberg 后，`k1 < 100` 大概率已能收敛（取决于 `k1` 的 manifest min/max 是否真实解码）。

这把 OQ-3 的重心从「修 scan 基数数学」转移到 §2.3 的几个真实缺口。

### 2.3 根因栈（定位到代码行）

1. **统计来源只覆盖 Iceberg data files。** `build_table_stats_from_plan` → `collect_scan_stats`（`src/engine/mod.rs:2936` / `:2952`）只对 `ScanSource::IcebergDataFiles` 调用 `build_table_statistics_with_ndv`。`ScanSource` 的其它变体——`StarRocks{db_id,table_id}`（managed-lake）、Iceberg metadata/delta/version——都进不了 `table_stats` map。
   - 影响：managed-lake 表（当前 join/optimizer 套件用的就是它，`ENGINE=OLAP` + `replication_num`）拿不到任何统计。
   - **本任务对策**：依战略约束 2，**不**给 managed-lake 造统计来源；改为依赖战略约束 3（套件转 Iceberg）。仅确保 Iceberg 路径的统计真实、完整。

2. **无统计退化分支完全忽略谓词。** `derive_scan` 的 `else` 分支（`src/sql/optimizer/stats.rs:728`）直接返回 `estimate_default_row_count(table_name)`（按表名猜：`store_sales`→1M、`*_dim`→10000、未知表→100000，见 `stats.rs:750`），列统计全填 `ColumnStatistic::unknown()`，**且不乘任何 selectivity**。→ 任何无统计表上谓词被彻底忽略，原样返回默认行数。

3. **extraction 阶段丢列统计（核心缺口）。** `LogicalProperties`（`src/sql/optimizer/logical_props.rs:106`）只携带 `row_count: f64`，不带列统计。`derive_group_statistics`（`stats.rs:608`）算出完整 `Statistics`（含 `column_statistics`）后只把 `output_row_count` 写进 `logical_props.row_count`。`extract.rs` 的 `group_statistics()`（`src/sql/optimizer/extract.rs:123`）再用 `logical_props.row_count` 重建 `Statistics`，**`column_statistics` 置空**。
   - 后果一：物理节点 `PhysicalPlanNode.stats.column_statistics` 永远是空 map → `EXPLAIN COSTS` 无法显示 per-column 统计。
   - 后果二：任何 post-extraction 消费者（含未来 OQ-4/OQ-5）拿不到列统计。

4. **Join / Aggregate 基数估算缺失或粗糙。** baseline 显示 JOIN `rows=1000000000`，是默认乘积式估算。`src/sql/optimizer/rewrite/rules/join_reorder/cardinality.rs` 已有 join reorder 用的 join 基数逻辑，但**未被统一用到节点显示 / 传播基数**上。

5. **Iceberg 列统计不完整。** `build_table_statistics_with_columns`（`src/sql/optimizer/statistics.rs:123`）解码 int/float min/max，但 **跳过 string bounds**（见 test `build_table_statistics_skips_string_bounds`）；NDV 依赖 Puffin sketch（`load_iceberg_puffin_ndv`），缺失时退启发式。

6. **EXPLAIN COSTS 显示极简。** `ExplainLevel::Costs`（`src/sql/explain.rs`）只在 `stats={rows=N}` 外加 `(rows=N)`，没有 StarRocks `EXPLAIN COSTS` 那种 per-column `[min, max, ndv, nulls]`。

7. **selectivity 覆盖有空洞。** `Between` / `Like` 退默认 `PREDICATE_UNKNOWN_FILTER`；range 公式（`stats.rs:1304`）的 `Le/Ge` 分支有可疑 `+1.0`（对非整型 / 小 range 不准）；无直方图。

---

## 3. 非目标

- **不给 managed-lake（`StarRocks` source）表造统计来源。** 它们保留表名启发式（但 §4.2 的 fallback 修正后至少会应用默认 selectivity）。依战略约束 2。
- **不做直方图（Histogram）。** StarRocks `ColumnStatistic` 带 histogram + MCV；本任务用 min/max + NDV 的均匀分布假设。
- **不做多列联合统计（multi-column combined stats）。**
- **不做 `ANALYZE TABLE` + 持久化统计存储子系统。** 统计仍在 plan 时从 Iceberg manifest/Puffin 即时获取。
- **不做 string min/max 编码**（§2.3-5），列为可选 polish，不阻塞主线（`k1 < 100` 是 INT，主路径不依赖它）。

以上若未来需要，作为独立完整统计子系统任务（非 OQ-3 前置职责）。

---

## 4. 架构设计

### 4.1 统计来源（Iceberg 补强）

保持 `collect_scan_stats` 仅对 Iceberg 收集统计的现状，但核查并补强 Iceberg 列统计：

- 确认 `build_table_statistics_with_ndv` 对 INT/BIGINT 列（如 `k1`）真实解码 manifest min/max → 让 `estimate_range_selectivity` 走真实 range 分支而非 0.5 默认。
- 确认 NDV：Puffin sketch 优先；缺失时启发式（当前 `build_table_statistics_without_columns_uses_heuristic_ndv` 路径）至少给出 `> 1.0` 的 NDV，让 `estimate_eq_selectivity` 走 `1/ndv` 而非 unknown。
- 若发现 Iceberg 列统计实际未填充（manifest bounds 未传到 `ColumnStatistic`），在此修复。这是 §4.2/§4.4 生效的前提，需在实现首步用 iceberg-rest 真实表验证。

### 4.2 传播正确性（核心工作）

#### 4.2.1 extraction 不丢列统计

把 `LogicalProperties`（`logical_props.rs:106`）扩展为携带列统计：

```rust
pub(crate) struct LogicalProperties {
    pub(crate) row_count: f64,
    pub(crate) column_statistics: HashMap<ColumnId, ColumnStatistic>, // 新增
    pub(crate) output_columns: Vec<OutputColumn>,
    pub(crate) equivalence_classes: ColumnEquivalenceClasses,
    pub(crate) unique_columns: Vec<ColumnIdSet>,
}
```

- `derive_group_statistics`（`stats.rs:608`）把算出的 `Statistics.column_statistics` 一并写入 `logical_props.column_statistics`。
- `extract.rs::group_statistics()`（`extract.rs:123`）从 `logical_props` 还原列统计到 `PhysicalPlanNode.stats.column_statistics`。
- **键的统一**：当前 `Statistics.column_statistics` 按 lowercased name 键；`LogicalProperties` 内宜按 `ColumnId` 键（稳定、不受 alias 影响）。需在 derive/extract 边界做 name↔ColumnId 映射（`output_columns` 已有 `column_id`）。这是本任务最需要小心的实现细节，单列一个子任务。

#### 4.2.2 无统计 fallback 应用 selectivity

修 `derive_scan` 的 `else` 分支（`stats.rs:728`）与 `PhysicalScan` 对应分支（`stats.rs:274` 附近）：返回前对 `scan.predicates` 应用 selectivity（列统计为 `unknown()` 时走默认 selectivity，谓词仍能降基数）。这是表无关的廉价正确性修复，对任何残留无统计表（含 managed-lake）都改善 plan 形状。

#### 4.2.3 基础 join / aggregate 基数（传播深度 = scan+filter + 基础 join/agg）

> 用户在 brainstorming 中未直接否定此默认，且其将套件转 Iceberg、OQ-4 需消费 post-join/agg 基数。故取此深度。可在 review 推翻为「严格只做 scan+filter」。

- **Join 基数**：等值 join 输出 ≈ `left_rows * right_rows / max(ndv_leftkey, ndv_rightkey)`，clamp 到 `[1, left*right]`；semi/anti 取 `left_rows * sel`，outer 取 `max(inner_est, 外侧保留行)`。**复用 `join_reorder/cardinality.rs` 已有逻辑**，统一为单一基数估算入口，避免重复实现。
- **Aggregate 基数**：输出 ≈ group-by 列 NDV 连乘，按 StarRocks `computeGroupByStatistics` 的相关性衰减 `0.75^i`（第 i 个 group-by 列），clamp 到 input rows；scalar agg（无 group-by）→ 1 行。

落点：`derive_statistics`（`stats.rs`）的 Join / Aggregate 分支。

### 4.3 selectivity 覆盖与公式（对齐 StarRocks）

以 StarRocks `PredicateStatisticsCalculator` / `BinaryPredicateStatisticCalculator` 为参考校正 `estimate_selectivity`（`stats.rs:1192`）：

| 谓词 | 公式 | 无统计默认 | 后置列统计更新 |
| --- | --- | --- | --- |
| `col = const` | `1 / NDV`（const 在 [min,max] 内）；否则 0 | `PREDICATE_UNKNOWN_FILTER` | min=max=const, nulls=0 |
| `col < / <= const` | `(const - min) / (max - min) · (1 - nulls)` | 0.5 | max=const, nulls=0 |
| `col > / >= const` | `(max - const) / (max - min) · (1 - nulls)` | 0.5 | min=const, nulls=0 |
| `col IS NULL` | `nulls_fraction` | 0.1 | nulls=1 |
| `col IS NOT NULL` | `1 - nulls_fraction` | 0.9 | nulls=0 |
| `col IN (v…)` | `min(len / NDV, 1)` | 0.5 | NDV=min(NDV,len) |
| `a AND b` | `sel(a) · sel(b)`（顺序应用） | — | 依次收窄 |
| `a OR b` | `sel(a)+sel(b)-sel(a)·sel(b)` | — | NDV 取并 |
| 未知谓词 | — | 0.25 | 不变 |
| `col BETWEEN x AND y` | 拆为 `col>=x AND col<=y`（新增） | 0.25→改进 | range 收窄 |

公式细节：
- 校正 range 分支的 `Le/Ge` `+1.0`（`stats.rs:1305`）：仅对整型语义合理；改为 StarRocks `StatisticRangeValues.overlapPercentWith` 的 intersect 比例模型（`(min(h1,h2)-max(l1,l2))/(h1-l1)`），列为 polish。
- 所有 selectivity 后 `output_row_count` clamp `[1, MAXIMUM_ROW_COUNT]`（对齐 StarRocks `Statistics.clampOutputRowCount`）。
- StarRocks 常量参考（`StatisticsEstimateCoefficient.java`）：`PREDICATE_UNKNOWN=0.25`、`IS_NULL=0.1`、`IN=0.5`、`OVERLAP_INFINITE=0.5`。NovaRocks 侧已有同名常量（`PREDICATE_UNKNOWN_FILTER` / `IS_NULL_FILTER` / `IN_PREDICATE_DEFAULT_FILTER`），核对取值一致。

### 4.4 EXPLAIN COSTS 显示

扩展 `ExplainLevel::Costs`（`src/sql/explain.rs`，`format_physical_node` ~`:289`）：

- 在 `(rows=N)` 基础上，对每个输出列追加 `[min, max, ndv, nulls]`，对齐 StarRocks `EXPLAIN COSTS` 的 `* col-->[min, max, rows, nulls, size]` 风格（简化版即可）。
- 依赖 §4.2.1 的列统计还原。
- `Verbose` / `Analyze` 维持只显示 `stats={rows=N}`（不膨胀），per-column 仅在 `Costs` 出现。

---

## 5. 数据结构变更汇总

- `LogicalProperties`：新增 `column_statistics: HashMap<ColumnId, ColumnStatistic>`（§4.2.1）。
- 无新增 logical / physical operator，无 codegen / fragment_builder 改动（统计仅供 cost / display）。
- `ColumnStatistic` / `Statistics`（`statistics.rs:8-34`）结构不变，仅填充更完整。

---

## 6. StarRocks 参考对齐

每个子 PR 描述需引用对应出处：

- 标量统计 walk：`fe/fe-core/.../statistics/StatisticsCalculator.java`（`visitLogicalOlapScan` / `computeFilterNode` / `visitOperator` 的 `estimateStatistics` / `computeGroupByStatistics`）。
- 谓词 selectivity：`statistics/PredicateStatisticsCalculator.java`、`statistics/BinaryPredicateStatisticCalculator.java`（`estimatePredicateRange`）、`statistics/StatisticRangeValues.java`（`overlapPercentWith` / `intersect`）。
- 常量：`statistics/StatisticsEstimateCoefficient.java`。
- 模型：`statistics/ColumnStatistic.java`、`statistics/Statistics.java`（`clampOutputRowCount`、min 1 row）。

---

## 7. 验证方案

1. **iceberg-rest 套件新增 cardinality golden**（依战略约束 3）：
   - 建 Iceberg 表 → 插已知分布数据（如 100K 行、`k1` 值域已知）→ `EXPLAIN COSTS SELECT ... WHERE k1 < 100` → `-- @explain_contains` 锁 `stats={rows=<post-filter>}`。
   - 覆盖 eq / range / IN / IS [NOT] NULL / AND / OR 各一例。
   - 覆盖 join 基数（两表等值 join 不再出 1e9）与 aggregate 基数（group-by NDV 连乘）各一例。
2. **roadmap 三条标杆**（套件转 Iceberg 后）：`join_one_key` q22、`join_linear_chained` q31、一个简单 INNER `count(*)`，对照 StarRocks plan 写 cardinality diff，确认 scan 后基数同数量级。
3. **回归**：`cargo test`（含 `stats.rs` / `statistics.rs` / `cardinality.rs` 既有 unit test）；optimizer 套件 golden（managed-lake 表的 golden 因 fallback selectivity 修正会变化，需 record 更新并人工确认合理）。
4. **PR 自检**：对齐 roadmap「Optimizer Plan Quality Roadmap」PR checklist 第 1/3/4/7 条（标明 OQ-3 阶段、quote plan diff、加 golden、cte/join/aggregate/filter 无回归）。

---

## 8. 风险与开放点

- **Iceberg 列统计是否真实填充**：§4.1 是整条链路生效前提。实现首步必须用 iceberg-rest 真实表确认 `k1` 的 min/max/NDV 已进 `ColumnStatistic`；若未进，OQ-3 第一块工作就在补这里，否则后续全是默认 selectivity。
- **name↔ColumnId 键映射**（§4.2.1）：derive 阶段 `Statistics` 按 name 键、`LogicalProperties` 宜按 ColumnId 键，边界映射出错会让列统计错位。需 targeted unit test。
- **optimizer 套件 golden 漂移**：fallback selectivity 修正会改变 managed-lake 表 golden 的 `rows=`。需 record + 人工核对，避免把「现在变对了」误判为回归。
- **join/agg 基数与 join_reorder 复用**：统一入口时注意不要改变 join reorder 既有决策（否则牵动 join 顺序，超出 OQ-3 范围）。宜让 OQ-3 复用其估算但不反向影响其内部使用。
- **传播深度可推翻**：若 review 决定「严格只做 scan+filter」，§4.2.3 整块移除，join/agg 基数留给 OQ-4。

---

## 9. 大致实现顺序（粗略；详细分解交给 writing-plans）

1. 核查并补强 Iceberg 列统计填充（§4.1），用 iceberg-rest 真实表验证 `k1` min/max/NDV 到位。
2. `LogicalProperties` 携带列统计 + extraction 还原（§4.2.1），含 name↔ColumnId 映射与 unit test。
3. EXPLAIN COSTS per-column 显示（§4.4）。
4. 无统计 fallback 应用 selectivity（§4.2.2）。
5. selectivity 覆盖与公式校正（§4.3，含 Between、range polish）。
6. 基础 join / aggregate 基数（§4.2.3，复用 `join_reorder/cardinality.rs`）。
7. iceberg-rest cardinality golden + 三条标杆复核（§7）。

---

## 10. 验收标准（Definition of Done）

- Iceberg 表 `SCAN ... WHERE k1 < 100` 的 `EXPLAIN COSTS` 显示 post-filter 基数，与 StarRocks `cardinality` 同数量级（1 个数量级以内）。
- 物理节点 `stats.column_statistics` 非空；`EXPLAIN COSTS` 显示 per-column `[min,max,ndv,nulls]`。
- 两表等值 join 的 `stats={rows=...}` 不再是乘积式垃圾值；scalar agg→1、group-by agg 反映 group-by NDV。
- iceberg-rest 套件 cardinality golden 全绿；cte/join/aggregate/filter 套件无回归。
- OQ-4 能从 plan 直接读到 post-filter / post-join / group-by 真实基数（前置职责达成）。
