# OQ-12 · Stats 与 NDV Robustness 设计

- 日期：2026-06-03
- 对应 roadmap：`OQ-12 · Stats and NDV robustness`（阶段 1 · CBO 输入与高内存 plan 风险）
- 依赖：OQ-3 cardinality propagation baseline（见 `2026-03-30-tpcds-type-system-and-statistics-design.md`）
- 下游消费：OQ-8 distribution search、OQ-9 predicate placement、OQ-10 RF gate、OQ-11 SplitAgg cost；
  EXPLAIN 信号被 OQ-16 plan-diff automation 解析

## 1. 一句话

把 optimizer 的统计（row count / NDV / selectivity）从"能展示"提升为"能支撑 CBO 决策"的可靠输入，
通过**共享公式内核 + Confidence 模型 + 饱和算术**根除 row-count collapse、overflow、粗糙 NDV 与复杂谓词
选择率问题。

## 2. Brainstorming 阶段已定的决策

| 决策点 | 结论 |
|---|---|
| 范围 | 完整 OQ-12 作为一个 spec，内部拆 6 个可独立实现的阶段 |
| 三套 join 基数路径 | **方向 B：抽取共享公式内核**，三个调用点（`join_reorder/cardinality.rs`、`stats.rs` 的 `derive_join`、`stats.rs` 的 `PhysicalHashJoin`/`PhysicalNestLoopJoin`）全部委托给它，根除数值漂移 |
| 验收方式 | **自包含 golden 为主**：合成表（确定基数）写 golden 进 CI；FE 仅作开发期人工量级参照，不进 CI |
| 与 OQ-16 边界 | OQ-12 只**产出** EXPLAIN 可检测信号（无裸 i64::MAX、`conf=`、`>=` overflow 前缀）；OQ-16 负责**解析**这些信号做 FE-vs-NR 报告 |

## 3. 验收线 → 病根 → 修复层

通过对当前实现的核对，4 条验收线的真正病根已定位：

| 验收项 | 真正病根（已核实） | 修复层 |
|---|---|---|
| `tpc-ds/q72` 不再 `9223372036854775807` | 全部 stats 是 `f64`（不存在 i64）；cross/非等值 join 无上限连乘 `left_rows * right_rows` → f64 溢出 → `format_stats_trailer` 用 `(rows.round() as i64)` 饱和成 `i64::MAX` | **饱和原语**（源头钳住）+ **渲染兜底**（正无穷/超大值不再转 i64::MAX） |
| `tpc-ds/q85` 不再大面积 `rows=1` | 多谓词 AND 朴素独立性连乘（`0.25ⁿ`）趋零 → 被 `.max(1.0)` 钉死在 1 | **谓词选择率**：AND 指数阻尼 + selectivity floor（并降 confidence 使其可观测） |
| `tpc-h/q9/q20` join side 与 FE 同数量级 | 多键 join 取 `max(NDV)` 而非组合 → 低估；三套路径公式不一致；scan 基数依赖 name-heuristic | **共享 join 基数**（多键阻尼连乘）+ **scan/Iceberg stats 接入** |
| 新增 6 类 golden | 缺 confidence 与一致公式 | **Confidence 类型** + **可观测** |

> 重要纠正：roadmap 文档担心多键"连乘到 0"，但当前代码实际取 `max(NDV)`，问题是**低估（输出偏大）**而非
> collapse；q85 的 `rows=1` collapse 主要来自**谓词 AND 连乘**，不是多键 join。

## 4. 当前实现核对（事实基线）

- 行计数与列统计类型：`Statistics { output_row_count: f64, column_statistics }`（`statistics.rs`），
  `ColumnStatistic { min_value, max_value, nulls_fraction, average_row_size, distinct_values_count }`。
  **无 confidence/unknown 字段**，unknown 靠哨兵值（NDV=1.0、min=-∞）表达。
- 行计数**全程 f64、无饱和**；唯一下界是各处 `.max(1.0)`。
- **三套 join 基数实现都 live**：`JoinReorder` rewrite rule（走 `LogicalPlan` 树，决定 join 顺序）→
  Cascades `derive_group_statistics`（走 `Memo`，定物理 plan 与 EXPLAIN 显示）。公式不同步
  （例：semi join 在 `cardinality.rs` 用真实 selectivity，在 `PhysicalHashJoin` 写死 `0.3`）。
- 选择率常量：`PREDICATE_UNKNOWN_FILTER=0.25`、`IS_NULL_FILTER=0.1`、`IN_PREDICATE_DEFAULT_FILTER=0.5`、
  `UNKNOWN_GROUP_BY_CORRELATION=0.75`、`ANTI_JOIN_SELECTIVITY=0.4`、`DEFAULT_FILTER_SELECTIVITY=0.3`。
- 渲染：`format_stats_trailer`（`explain.rs:14-27`）对 NaN/负数→`rows=?`，但**正无穷/超大值未挡**，
  `(rows.round() as i64)` 饱和为 `i64::MAX`。trailer 注释明确"append keys after `rows=`; never reorder"。
- Iceberg Puffin NDV 确能贯穿（见 `iceberg_statistics_optimizer.sql`），name-heuristic（1M/100K/10K）仅兜底。
- runner 现支持 `@explain_contains` / `@normalize_explain_timing` / `@skip_result_check` / `@catalog` /
  `@db` / `@sequential`；**无** `@explain_not_contains`。

## 5. 设计详述

### 5.1 核心类型

新增 `Confidence` 枚举（放在 `statistics.rs`，紧挨它标注的类型）：

```rust
/// Trustworthiness of a statistic. Ordered: Exact > Estimated > Fallback.
pub enum Confidence {
    Exact,     // sourced from real catalog/Iceberg stats (Puffin NDV, metadata row_count)
    Estimated, // derived via formula from at-least-partially-real inputs
    Fallback,  // relied on a heuristic/default (name-based rows, default selectivity, default NDV)
}
```

组合规则（单调、易测）：
- `combine(a, b) = min(a, b)`（最不可信者胜）；
- 任何**公式推导**结果最高只能到 `Estimated`（join 基数即使输入 Exact 也只是估计）；
- 本步用了**任何默认值** → 直接 `Fallback`。

形式化：`derive(inputs, used_default) = if used_default { Fallback } else { min(Estimated, min(inputs)) }`。

挂载到两类型（各加一个 confidence 字段）：

```rust
pub struct Statistics {
    pub output_row_count: f64,
    pub row_count_confidence: Confidence,   // 新增
    pub column_statistics: HashMap<String, ColumnStatistic>,
}
pub struct ColumnStatistic {
    pub min_value: f64, pub max_value: f64,
    pub nulls_fraction: f64, pub average_row_size: f64,
    pub distinct_values_count: f64,
    pub confidence: Confidence,             // 新增：这列 stats 多可信
}
```

**Churn 对策**：加字段会波及所有字面量构造点（含 `explain.rs` 多处测试）。给两类型实现 `Default`
（confidence 默认 `Fallback`，row_count 默认 0.0）+ 构造 builder，用 `..Default::default()` 收口，把改动面降到最低。

### 5.2 内核模块布局

新增 `src/sql/optimizer/estimate/`（纯函数、带单测、不持状态）：

```text
estimate/
  mod.rs          // 再导出 + Confidence 组合逻辑
  arith.rs        // 饱和算术原语 + 上限 + saturation trace
  cardinality.rs  // estimate_join_cardinality / set-op / aggregate 行数
  selectivity.rs  // estimate_selectivity（从 stats.rs 迁出并重构）
  ndv.rs          // NDV 传播 helper（filter / join key 等价类 / agg group）
```

`stats.rs`、`join_reorder/cardinality.rs`、`join_reorder/cost.rs` 全部变为 `estimate::*` 的调用方。

### 5.3 饱和算术（`arith.rs`）

```rust
pub const MAX_ROW_COUNT: f64 = 1e15;  // 远低于 i64::MAX/2 与任何真实表；饱和哨兵
pub fn sat_mul(a: f64, b: f64) -> (f64, bool); // (clamp 到 [0, MAX] 的值, 是否触顶)
pub fn sat_add(a: f64, b: f64) -> (f64, bool);
pub fn sat_div(a: f64, b: f64) -> (f64, bool); // 除零守卫：分母<=0 返回 (a, true)
```

- 触顶返回 `true`，调用方据此把 confidence 降到 `Fallback` 并打 `debug_assert!`/trace（定位 overflow 源）。
- 行计数全程经此钳制 → q72 在**源头**就不会溢出成 inf，内部值也永不达 `i64::MAX`。

### 5.4 Join 基数（单一入口）

```rust
pub struct JoinCardInput {
    pub left: (f64, Confidence),  pub right: (f64, Confidence),
    pub kind: JoinKind,
    pub eq_key_ndvs: Vec<(f64, f64, Confidence)>, // 每个等值键 (左NDV, 右NDV, conf)
    pub non_equi_selectivity: Option<(f64, Confidence)>,
}
pub fn estimate_join_cardinality(input: &JoinCardInput) -> (f64, Confidence);
```

公式：
- **单键 inner**：`L·R / max(ndv_l, ndv_r)`（containment，保持现状）。
- **多键 inner**：每键 selectivity = `1/max(ndv_l_i, ndv_r_i)`；按**指数阻尼连乘**得组合 selectivity；
  `out = sat_mul(L·R, sel_combined)`。封顶 `L·R`（不超过 cross 积）与 `MAX_ROW_COUNT`。
- **outer**：`max(inner, 被保留侧行数)`（现状正确，保留）。
- **semi**：有条件用真实 selectivity，无条件 `L·DEFAULT_FILTER_SELECTIVITY`；**anti**：`L·(1-…)`，常量集中内核
  （消除三路径漂移）。
- **cross / 非等值**：`sat_mul(L, R)`（非等值再乘 selectivity）；触顶 → confidence=`Fallback`。

**指数阻尼连乘**（治 collapse 与低估的统一手法）：将若干 selectivity 升序排列（最强在前），
组合值 = `s₁ · s₂^½ · s₃^¼ · …`。最强项全额生效，后续递减衰减——既不趋零，也不忽略。阻尼指数为可调常量。

### 5.5 谓词选择率（`selectivity.rs`，q85 主修复）

- **AND**：不再朴素连乘（`0.25ⁿ` 是 collapse 元凶），改**指数阻尼连乘**。
- **OR**：`s_l + s_r - s_l·s_r`（inclusion-exclusion，现状保留）。
- **=** `1/ndv`；**range** 用 min/max；**IS [NOT] NULL** 用 null_fraction；**IN** `min(len/ndv, 1)`；
  **LIKE** 前缀可估则按区间否则稳定 fallback；**未知函数** 稳定 fallback。补 confidence 线程化。
- **选择率 floor**：组合 selectivity 设下限，避免 `rows<1` 被 `.max(1.0)` 钉死；**floor 一旦起作用即降
  confidence**，让 collapse 风险对 OQ-16 可见。

### 5.6 NDV 传播（`ndv.rs`）

- **filter**：`ndv_out = min(ndv_in, rows_out)`（NDV 不超过存活行数——当前缺此 cap）。
- **join**：等值键合并等价类，`ndv(a)=ndv(b)=min(...)`；输出列 NDV 封顶到输出行数。
- **aggregate**：group 行数 = group-key NDV 的**阻尼连乘**，封顶 `child_rows·correlation`，floor 1；
  group key 输出 NDV = 行数（每组唯一）；agg 函数输出列 NDV 未知 → `Fallback`。
- **set-op**：union all=`sat_add`；union distinct=`sat_add · dedup`；intersect=`min(inputs)·f`；
  except=`first·f`。输出列 stats 做**真正的 merge**（min(mins)/max(maxs)/NDV 合并），不再"取第一个输入"。
- **window**：行数不变（conf 透传），窗口输出列 NDV=`Fallback`。

### 5.7 Scan / Iceberg stats 接入

- `TableStatistics` 有真实 row_count → `Exact`；列有真实 Puffin NDV/min/max/null → `Exact`。
- 缺失 → name-heuristic 行数 = `Fallback`，`ColumnStatistic::unknown()` = `Fallback`（不再"看起来像真值"）。
- scan 谓词走同一 selectivity 函数，conf 组合。
- **实现期须核实**：`TableStatistics` 填充路径是否把 file metrics（row_count / min / max / null_count）真正灌入，
  而不止 Puffin NDV。

### 5.8 可观测与渲染

**渲染兜底**（`format_stats_trailer`，`explain.rs:21-27`）：

```rust
let rows_str = if rows.is_nan() || rows < 0.0      { "?".into() }
    else if rows.is_infinite() || rows >= MAX_ROW_COUNT { format!(">={:.0e}", MAX_ROW_COUNT) } // ">=1e15"
    else { (rows.round() as i64).to_string() };
```

**confidence 展示（向后兼容优先）**：
- `conf=` 只在 `EXPLAIN COSTS` / `ANALYZE` 出现，且仅当 `!= Exact`；**plain `VERBOSE` 不加**，保护现有大量
  VERBOSE golden 的 `stats={rows=N}` 文本不变。
- 追加在 `rows=` 之后。例：`stats={rows=8 conf=estimated}`、`stats={rows=1 conf=fallback}`。
- fallback **reason** 串（如 `conf=fallback(name-heuristic-rows)`）只在 `COSTS`/`ANALYZE` 或 debug 开关下给。

**给 OQ-16 的信号契约**：

| 现象 | plan 文本信号 |
|---|---|
| overflow | `rows=>=1e15`（`>=` 前缀即溢出标记） |
| rows=1 collapse | `rows=1` + `conf=fallback`（floor 触发即降 conf，区分"真 1 行"与"塌缩"） |
| unknown stats | `conf=fallback` |

### 5.9 错误处理 / fail-fast 调和

- stats 计算**永不 panic、永不 hard-fail 查询**（纯 advisory）；所有算术走饱和原语；渲染器之外**禁止**对
  无界 f64 做裸 `as i64`。
- 饱和触顶处打 `debug_assert!`/trace（定位 overflow 源）。
- selectivity/NDV 除零守卫 → 返回 Fallback 默认，不产生 NaN/inf。
- CLAUDE.md 的"fail fast / 禁 fallback"针对 **FE-compat 路径的 plan/type 元数据**；基数估计本质启发式，
  本设计的 fallback 全部降 confidence、可观测、不静默掩盖 planner bug，二者不冲突。

## 6. 实现阶段（一个 spec，内部 6 阶段）

- **P0 内核地基**：`Confidence` 枚举 + 两类型加字段（`Default`/builder 收口）+ `arith.rs` 饱和原语 +
  `estimate/` 骨架 + 渲染兜底。直接绿掉 q72 overflow。
- **P1 join 基数**：`estimate_join_cardinality` 单一入口；三路径委托；多键阻尼连乘；semi/anti 一致化。
- **P2 谓词选择率**：`estimate_selectivity` 迁出 + AND 阻尼 + floor 降 conf。绿掉 q85 collapse 代理。
- **P3 NDV 传播**：filter/join/agg/set-op/window 规则 + confidence 线程化。
- **P4 scan/Iceberg 接入**：真实 stats 优先（Exact）、heuristic 降级 Fallback；核实填充路径。
- **P5 可观测 + golden**：`conf=` 渲染（COSTS/ANALYZE）+ `@explain_not_contains` 指令 + 6 类 golden + 单测。

> 每阶段都保持可编译、可测；建议 `dev-opt` profile 跑 SQL 回归。

## 7. 测试与验收

**金字塔（底厚顶薄，贴合"自包含 golden 为主"）：**

1. **Rust 单测**（`estimate/*` + `explain.rs`）——骨干：
   - `arith`：sat_* 触顶/正常/除零守卫。
   - `Confidence::combine` 格；推导封顶 Estimated；用默认值→Fallback。
   - `estimate_join_cardinality`：inner 单/多键（阻尼 ≠ max-NDV 且 ≠ collapse）、outer ≥ 保留侧、
     semi/anti 一致、cross 封顶 + Fallback。
   - `estimate_selectivity`：AND 阻尼（5×0.25 不趋零）、OR、range、IS [NOT] NULL、IN 上限、floor→降 conf。
   - `ndv`：filter 封顶、join 等价类合并、agg 阻尼连乘、set-op merge。
   - `format_stats_trailer`：补 `inf→rows=>=1e15`、`>MAX→同`；`conf=` 在 COSTS 渲染。
   - **漂移守卫**：同一 `JoinCardInput` 下 `cardinality.rs` 路径 == Cascades 路径（构造上相等，锁死防回归）。

2. **sql-tests/optimizer/ golden**（合成表、确定数值，覆盖文档点名的 6 类）：

   | 文件 | 覆盖 | 关键断言 |
   |---|---|---|
   | `stats_multikey_join_ndv.sql` | 多键 join NDV | pin 阻尼后行数（≠ max-NDV 结果） |
   | `stats_or_selectivity.sql` | OR 选择率 | inclusion-exclusion 行数 |
   | `stats_outer_semi_anti_card.sql` | outer/semi/anti | outer ≥ 保留侧；semi/anti 有界 |
   | `stats_aggregate_group_ndv.sql` | agg group NDV | pin 行数，封顶 child |
   | `stats_setop_rowcount.sql` | set-op 行数 | union all/distinct、intersect、except 各公式 |
   | `stats_no_collapse_and_chain.sql` | **q85 代理** | `EXPLAIN VERBOSE` + `@explain_not_contains=stats={rows=1}` + pin 阻尼行数 |
   | `stats_overflow_saturation.sql` | overflow 渲染 | 小表 cross 积有界、`@explain_not_contains=rows=>=` |

   > **断言写法注意**：负向/正向断言一律带闭合 `}`（用 `stats={rows=1}` 而非 `stats={rows=1`），
   > 否则 `rows=1` 会误匹配 `rows=10`/`rows=100`。`conf=` 仅在 COSTS/ANALYZE 渲染，故 VERBOSE 下
   > 塌缩节点恰为 `stats={rows=1}`（无 `conf=`），negative 断言用 VERBOSE 最干净；需要校验 `conf=`
   > 的用例单独用 `EXPLAIN COSTS`。

3. **新增 runner 指令** `@explain_not_contains=<substr>`（对照 `@explain_contains`，改
   `tests/sql-test-runner/src/{parser.rs,types.rs,results.rs}`）。

4. **命名查询（不进 CI，仅开发期人工参照）**：q72/q85/q9/q20 对着装好数据的 TPC 套件 + FE 量级比对，
   spec 记为 dev-time validation；其暴露行为已由合成 golden 把关。

**诚实的覆盖边界：**
- 真·`i64::MAX` overflow **无法在 SQL golden 复现**（没法插 1e18 行）→ 由渲染器 + `sat_mul` 单测覆盖；
  `stats_overflow_saturation.sql` 只证"有限 cross 积渲染干净"。
- q9/q20 **绝对 FE 量级 parity 不进 CI**（CI 无 FE）→ 合成多键 golden 证**公式**修复，FE parity 靠人工。

## 8. 代码入口（受影响文件）

新增：
- `src/sql/optimizer/estimate/{mod,arith,cardinality,selectivity,ndv}.rs`
- `sql-tests/optimizer/sql/stats_*.sql`（7 个）

修改：
- `src/sql/optimizer/statistics.rs`（Confidence、字段、Default/builder、常量集中）
- `src/sql/optimizer/stats.rs`（逐算子 derivation 委托内核）
- `src/sql/optimizer/rewrite/rules/join_reorder/{cardinality.rs,cost.rs}`（委托内核）
- `src/sql/explain.rs`（渲染兜底 + COSTS/ANALYZE 的 `conf=`）
- `src/sql/optimizer/derive/scan.rs` 及 scan derivation 段（Iceberg stats 优先 + confidence）
- `tests/sql-test-runner/src/{parser.rs,types.rs,results.rs}`（`@explain_not_contains`）

## 9. 风险与回归面

- **join 顺序回归**：`cardinality.rs` 改用阻尼连乘后，`JoinReorder` 选出的顺序可能变化 → 现有 optimizer
  golden 的 plan-shape 可能漂移。对策：先跑 `optimizer` 套件 record-diff，逐个确认是改进而非回退。
- **现有 golden 文本变化**：仅 `COSTS`/`ANALYZE` 加 `conf=`；`VERBOSE` 文本理论不变（除病态 overflow 行，
  本就是 bug）。需复跑 `optimizer` 套件确认。
- **字段新增的编译波及面**：靠 `Default`/builder 收口，但需一次性扫净所有字面量构造点。
- **阻尼指数 / floor / MAX_ROW_COUNT 的取值**：初值给保守缺省，留作可调常量；用合成 golden 锁定行为。
