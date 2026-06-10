# OQ-11: Split aggregate coverage parity — 设计

Date: 2026-06-09
Tasks:
- OQ-11 split-aggregate-coverage-parity（`~/Documents/Obsidian/NovaRocks TODO/OQ-11-split-aggregate-coverage-parity.md`）
Predecessor: OQ-4 SplitAggregateRule 两阶段聚合（`docs/superpowers/specs/2026-05-31-oq-4-split-aggregate-rule-design.md`）
依赖: OQ-8 distribution-aware physical search、OQ-12 stats/NDV robustness
Scope: 把 SplitAgg 从“简单 grouped/scalar 可用”推进到复杂 plan shape 可用，向 StarRocks FE 的 aggregate 形态计数收敛

---

## 1. 目标

把 NovaRocks standalone optimizer 的两阶段聚合（SplitAgg）从 OQ-4 的“简单 grouped/scalar aggregate 可用”推进到**复杂查询可用**，让 scan-only aggregate、aggregate-over-derived-table / project / filter / set-op / rollup / window 周边的聚合也能稳定生成 local/global 两阶段形态，向 StarRocks FE 的 aggregate update/merge 计数收敛。

完成时：

- 可拆聚合函数集合由**能力驱动的单一真相源**判定，而非硬编码 name 白名单；`avg` 及其它有真实 merge 实现的函数自动纳入。
- `tpc-h/q1` 出现与 FE 同向的 local/global aggregate（当前被 `avg` 卡死）。
- `tpc-h/q7/q8/q9` 的 aggregate update/merge 数量向 FE 收敛。
- `tpc-ds/q28/q44/q54/q67/q75/q85` 中覆盖一批 derived/set-op/rollup/window 周边 split。
- `ssb` 13 条 query 不回退。
- 新增 optimizer plan golden + 结果正确性测试锁住各 split shape，避免后续回退。

---

## 2. 当前状态与问题

OQ-4 已落地基础设施，且执行层完备程度高于 todo 预期。本设计基于对当前代码（`origin/main`，含 #270–#274）的实测：

**已具备：**

- `SplitAggregateRule`（`src/sql/optimizer/cascades_rules/split_aggregate.rs`）是独立 cascades transformation，把符合条件的 `Single` `LogicalAggregate` 拆成 `Global(merge) over Local`，CBO 按 cost 在 Single/Split 间选择。
- `AggToHashAgg`（`src/sql/optimizer/cascades_rules/implement.rs:617`）是纯 stage→physical lowering，不再承担 split 枚举。
- distribution 派生（`src/sql/optimizer/derive/hash_aggregate.rs`）：Local=`Any`、Global=`ShuffleAgg(group keys)` 或 scalar `Gather`，enforcer 自动插入。
- **执行层 / codegen 已支持任意有 intermediate state 的函数的两阶段 merge**：`src/sql/codegen/fragment_builder.rs:2542`（`visit_hash_aggregate`）的 Local slot 类型由编译后表达式的 `intermediate_type` **独立推导**（不依赖 split rule 的 `OutputColumn.data_type`），Global 通过 `compile_merge_aggregate_call` 对 child 的 intermediate 列做 merge，`need_finalize` 仅在 `Single|Global` 为真。`avg` 的 `merge_avg` / intermediate type 已在 `src/exec/expr/agg/functions/avg.rs` 实现。
- cost model（`src/sql/optimizer/cost.rs:120`）：`Single=input_size`、`Local=0.5×`、`Global=0.3×`，叠加 distribution/exchange cost。`split_aggregate_grouped` golden 显示**即使 12 行小表也会选 split**——cost gate 本身偏向拆分。

**主要问题（差距来源）：**

实测代表 query 的聚合函数，差距聚成两类：

1. **Bucket 1 · eligibility 卡死。** `is_eligible`（`split_aggregate.rs:71`）+ `is_splittable_aggregate`（`split_aggregate.rs:79`）的 whitelist 只有 `sum/min/max/count`，且要求**所有**聚合函数都可拆——一个 `avg` 就让整个聚合节点退回 Single。命中：`tpc-h/q1`（avg×3）、`tpc-ds/q28`（avg×6,count×12）、`tpc-ds/q44`（avg×6）、`tpc-ds/q85`（avg×6）。执行层已支持 avg merge，故此类差距是 **optimizer-only**。
2. **Bucket 2 · 可拆却没拆的结构。** `tpc-h/q7/q8/q9`、`tpc-ds/q54/q75` 只用 `sum/count`（已 eligible）却没产生 split；`tpc-ds/q67` 含 rollup + window。这些是 aggregate-over-derived-table / project / set-op(UNION) / rollup 形态。根因待 NR `EXPLAIN` 实测确认（见 §6）。

**基线已过期。** todo 引用的 `logs/plan-quality/20260603-fe-nr-plan-diff/` 已被 `20260609-fe-nr-plan-diff/`（runtime-filter 专用，未采聚合数据）覆盖。todo 的计数（tpc-h 23 vs 28、tpc-ds 142 vs 217）不可信，必须重建。

---

## 3. 非目标

- 不在本任务实现所有 distinct aggregate 高级 rewrite；`SplitDistinctAgg`（`src/sql/optimizer/cascades_rules/split_distinct_agg.rs`）单独推进，本任务只保证 distinct 被普通 SplitAgg 跳过。
- 不实现完整的下游 distribution-aware 复用（Global 的 `ShuffleAgg` 输出被下游 join/agg 复用免重复 exchange）——属 OQ-8 职责，本任务只保证不破坏。
- 不为单个 benchmark query 强行 split，也不在 stats 不可信时盲目 split（结合 OQ-12 的 rows/NDV 质量）。
- 不重写执行层 aggregate operator；只在必要边界改 optimizer rule、capability oracle、property/cost、codegen stage 映射。
- 不改变 FE-compatible thrift plan lowering；只服务 standalone SQL optimizer。

---

## 4. 架构：程序结构与分阶段

本任务是 **diagnose → fix → re-measure** 的迭代程序，采用 **Hybrid 验收**：Phase 0 一次性建 FE-vs-NR 基线拿权威工作清单；中间 phase 用 NR-only `EXPLAIN` + optimizer golden 自验、阶段性确认收敛；Phase 5 末尾再跑一次 FE diff 出 parity 报告。核心新抽象是 **可拆能力 oracle（单一真相源）**。

### Phase 0 · 基线与归因

- 起 StarRocks FE（`~/project/starrocks`）+ NR standalone-server（`NOVA_ENV_MYSQL_PORT`，默认本 worktree 为 9170），两边灌 ssb/tpch/tpcds（各 suite 的 `init.sql`）。
- 从 `tools/plan-quality/rf_plan_diff.py` 派生 `tools/plan-quality/agg_plan_diff.py`：复用其 FE/NR `EXPLAIN VERBOSE` over mysql 的骨架，把 `RF_PATTERNS` 换成 aggregate 形态识别（`AGGREGATE (LOCAL` / `AGGREGATE (GLOBAL` / `AGGREGATE (SINGLE` / update / merge / `ShuffleAgg` / `Gather`），输出逐 query 的 local/global/single 计数。
- 跑 tpc-h / tpc-ds / ssb，得到**当前**逐 query FE-vs-NR 计数，并把每个差距 query 归因到桶：`eligibility` / `structural` / `pushdown` / `distinct` / `already-converged`。
- 产出权威工作清单，校正 todo 的过期数字。

### Phase 1 · 能力驱动 eligibility（Bucket 1）

抽出共享 oracle，`SplitAggregateRule` 委托它；逐新增函数补**结果正确性**测试。详见 §5.1。

### Phase 2 · 结构覆盖（Bucket 2）

用 NR `EXPLAIN` 根因 Bucket 2 query 为何 eligible 却不拆，按根因修。详见 §5.2。

### Phase 3 · 与 AggregatePushdown 协调

RBO pushdown 与 CBO split 的相位/顺序、`already_pushed`/`is_split` 标记保持、不重复 rewrite。详见 §5.3。

### Phase 4 · distribution / required columns / RF probe path 保持

split 后 property、列裁剪、runtime filter probe path 不丢；ssb 13 条不回退。详见 §5.4。

### Phase 5 · 验收

重跑 Phase 0 的 `agg_plan_diff.py` 出 parity 收敛报告；锁定新 golden 与结果正确性测试。详见 §6。

---

## 5. 技术设计

### 5.1 能力驱动 eligibility（单一真相源）

**问题**：`infer_agg_function_types`（`src/sql/codegen/expr_compiler.rs:2720`）对几乎所有函数都返回 `Some(intermediate)`（末尾约 2916 行有兜底 `assume output same as first arg`），所以“能 infer”不等于“可安全两阶段”。需要一个比它更严、可被 optimizer 与 codegen 共用的判据。

**落点**：新增共享模块（候选 `src/sql/agg_mergeability.rs`，实现时按模块依赖最终确定），位于 optimizer 与 codegen 共同可依赖的 `sql` 层（不让 optimizer 反向依赖执行层 `src/exec/`）：

```rust
pub(crate) enum AggMergeability {
    /// Local emits intermediate state, Global merges. Safe two-phase split.
    TwoPhase,
    /// Cannot be safely two-phased (distinct / ordered / order-sensitive / unknown).
    SinglePhaseOnly,
}

pub(crate) fn aggregate_mergeability(name: &str, distinct: bool, ordered: bool) -> AggMergeability;
```

**判定顺序：**

1. `distinct` → `SinglePhaseOnly`（交给 `SplitDistinctAgg`）。
2. `ordered` 非空 → `SinglePhaseOnly`（local update + global merge 不保序）。
3. 顺序敏感函数（`group_concat` / `string_agg` / `array_agg` / `array_agg_distinct`）→ `SinglePhaseOnly`（并行 partition 的 merge 会改变拼接/数组顺序语义）。
4. 其余有真实 `merge_batch` 实现的函数 → `TwoPhase`：`sum` / `min` / `max` / `count` / `count_if` / `avg` / `stddev*` / `variance*` / `var_*` / `bool_or` / `bool_and` / `any_value` / `bitmap_union*` / `hll*` / `approx_count_distinct` / state-combinator 函数等。
5. 未知名 → `SinglePhaseOnly`（保守）。

**接线**：`split_aggregate.rs` 的 `is_splittable_aggregate` 改为委托 `aggregate_mergeability(...) == TwoPhase`。`is_eligible` 保持 all-or-nothing（单个 aggregate 节点的所有函数共享同一组 group key 与两阶段结构，不能半拆；这与 FE 一致）。

**防漂移契约（关键测试）**：新增单测遍历所有判 `TwoPhase` 的函数名，断言：
- 执行层 `src/exec/expr/agg/functions/mod.rs::merge_batch`（约 477 行）确实能 merge 它；
- `infer_agg_function_types` 对它返回**非兜底**的 intermediate type。
任一函数被误判可拆而执行层无法 merge → 测试红。这就是“单一真相源 + 不漂移”的保证。

### 5.2 结构覆盖（Bucket 2，假设驱动）

`SplitAggregateRule` 匹配任意 `LogicalAggregate`，所以 eligible 的 q7/q8/q9 **理应**已生成 split 备选。没拆只可能是“被 cost 淘汰”或“被 prune 判废”。Phase 2 第一步用 NR `EXPLAIN`（开/关 `disable_optimizer_rules='SplitAggregateRule'` 对比）逐 query 确认，再按根因修。候选根因与对应改动：

- **H1 · cost gate**：grouped golden 连 12 行也拆 → cost 偏向拆分，H1 概率低。仅核 join-fragment child 的 exchange cost 是否异常；确认后才校准 `cost.rs` 系数，不为 golden 强制。
- **H2 · group-by 投影列 ColumnId**：q7 group by `o_year`(派生 `extract(year ...)`) + nation 名。若某 group key `ColumnId::UNSET` 触发 `src/sql/codegen/id_binding_verifier.rs:148` 把 split 备选判废，只剩 Single。修：复用/加固 `local_output_columns` 与 `aggregate_group_key_output_ref` 对 non-ColumnRef group key 的 by-position id 绑定（OQ-4 已对该类做过 by-position 修复，需扩到 derived-table 列）。
- **H3 · set-op 穿透（q75 UNION ALL）**：FE 的额外 aggregate update 来自 **branch-local partial aggregate 下推进每个 UNION 分支**——这是 pushdown-through-setop，不是顶层两阶段。属新机制，落在 AggregatePushdown 体系（与 §5.3 协同），SplitAgg 优先做 branch-local split。
- **H4 · rollup / grouping-sets（q67）**：aggregate over Repeat 节点，grouping sets 上的 group key 含 grouping_id；验证 intermediate merge 在 Repeat 展开后语义正确，必要时加 guard 或显式支持。
- **H5 · window 周边（q44/q67 rank）**：window 是独立节点，通常不挡聚合自身拆分；确认后若无问题则仅记录。

**诚实声明**：Bucket 2 的具体改动取决于 Phase 2 实测分类。本设计锁定**调查方法**（NR EXPLAIN + rule disable 对比）与**候选机制**；每个根因确认后落对应改动并补 golden。

### 5.3 与 AggregatePushdown 协调

事实：AggregatePushdown 在 `src/sql/optimizer/rewrite/rules/aggregate_pushdown/`（RBO/rewrite 相位，cascades 前）；SplitAggregateRule 是 cascades transformation（CBO 相位）。两相位天然分离。

- **顺序契约**：RBO 先下推 partial aggregate → CBO 再对（下推后的 partial + 原 above-join aggregate）各自评估两阶段。
- **下推后的 partial 是否再 split**：默认允许（它是普通 `Single` `LogicalAggregate`），但必须用组合 golden 验证不产生重复 rewrite / 重复聚合。若实测出现重复，则在 split eligibility 中尊重 `already_pushed`。
- **标记保持**：`LogicalAggregateOp.already_pushed` 与 `is_split` 在所有新增 / 修改的 clone 路径保持（遵守 CLAUDE.md 约定：“other rules must preserve the flag when cloning”）。
- **golden**：现有 `sql-tests/optimizer/sql/aggregate_pushdown_*.sql` 不回退；新增 pushdown∘split 组合 golden。

### 5.4 distribution / required columns / RF probe path 保持

- **distribution**：沿用 `derive/hash_aggregate.rs`（Local=`Any`、Global=`ShuffleAgg(group keys)` / scalar `Gather`）。验证 split 后 fragment 在 distribution enforcer 处正确切分；scalar split 的 Local→Global 之间的 `Gather` 不被 root-level 消除。
- **required columns**：Local 必须输出 group keys + intermediate state 供 Global merge；OQ-1/OQ-2 的列裁剪需看穿 Local→Global，不裁掉 intermediate 列。
- **RF probe path**：聚合处于 join probe 侧并携带 runtime filter probe 时，split 不能断 probe 接线（衔接刚落地的 OQ-10）。用带 RF 的 ssb / tpc-ds query 验证。
- **ssb 13 条不回退**作为硬门槛（plan shape + 结果）。

---

## 6. 测试与验收

### 6.1 Phase 0 / Phase 5 harness

- `tools/plan-quality/agg_plan_diff.py`（从 `rf_plan_diff.py` 派生）：FE/NR `EXPLAIN VERBOSE` over mysql，抽 aggregate LOCAL/GLOBAL/SINGLE/update/merge 计数，输出逐 query 表 + JSON summary 到 `logs/plan-quality/<date>-agg-split-parity/`。
- 调用方式与 `rf_plan_diff.py` 一致：`--fe-port <FE> --nr-port <NR> --output-dir <dir>`。FE 与 NR 需预先起好并灌数。

### 6.2 结果正确性测试（不止 plan）

每个**新纳入可拆**的函数（avg / stddev / variance / 及实测命中的其它）必须有结果一致性测试：同一 query 跑两次——默认（可能 split）vs `SET disable_optimizer_rules='SplitAggregateRule'`（强制 single）——断言结果完全一致。覆盖 nullable、decimal（avg(decimal) 的 scale 规则）、空输入、单分区/多分区。

### 6.3 新增 optimizer golden（`sql-tests/optimizer/sql/`）

- `split_aggregate_avg`：avg 两阶段，`-- @explain_contains=HASH AGGREGATE (LOCAL` / `(GLOBAL`。
- `split_aggregate_stddev_variance`：stddev/variance 两阶段。
- `split_aggregate_over_derived`：aggregate over subquery/project。
- `split_aggregate_over_setop`：aggregate / partial 下推进 UNION 分支。
- `split_aggregate_rollup`：grouping sets/rollup 上的 split。
- `split_aggregate_with_pushdown`：pushdown∘split 组合，断言无重复聚合。
- 负样本：`split_aggregate_distinct_negative`、`split_aggregate_ordered_negative`、`split_aggregate_group_concat_negative`（顺序敏感不拆）。
- 复核现有 `split_aggregate_{grouped,scalar,disabled}` 与 `aggregate_pushdown_*` 不回退。

### 6.4 验收标准

- `tpc-h/q1` 出现 FE 同向 local/global aggregate。
- `tpc-h/q7/q8/q9` 的 aggregate update/merge 数量向 FE 收敛。
- `tpc-ds/q28/q44/q54/q67/q75/q85` 覆盖一批 derived/set-op/rollup/window 周边 split。
- `ssb` 13 条 query plan + 结果不回退。
- Phase 5 的 FE diff 报告显示聚合计数较 Phase 0 基线向 FE 方向收敛（记录每条 query 的 before/after/FE 三列）。
- §5.1 防漂移测试、§6.2 结果正确性测试、§6.3 golden 全绿。

---

## 7. StarRocks 参考对齐

- `~/project/starrocks/fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/transformation/SplitAggregateRule.java`
  - `createNormalAgg(...)`、`getIntermediateType(...)`：函数分解与 intermediate type，对照 §5.1 的能力判定。
- `~/project/starrocks/fe/fe-core/.../SplitTwoPhaseAggRule.java`
  - `check(...)` / `transform(...)`：两阶段结构与 guard。
- FE 的 `PushDownAggregateRule`（含 push-into-union）对照 §5.2 H3 / §5.3。

明确差异：NovaRocks 用 Cascades transformation + CBO alternatives + 能力 oracle 表达 split，不完全同构 FE 的 Java operator builder；distinct 多阶段走独立历史实现；开关用 `disable_optimizer_rules`。

---

## 8. 风险与应对

- **能力 oracle 误判可拆。** 某函数被判 `TwoPhase` 但执行层 merge 语义不对 → 结果错。应对：§5.1 防漂移测试 + §6.2 逐函数结果一致性测试；新增函数默认保守，确认 merge 正确再纳入。
- **Bucket 2 根因与设计假设不符。** 应对：Phase 2 第一步即实测分类，改动跟随实测而非假设；设计只锁调查方法。
- **pushdown∘split 重复聚合 / 反复 rewrite。** 应对：组合 golden + 标记保持测试；必要时 split 尊重 `already_pushed`。
- **FE/NR 环境起不来或灌数不一致。** 应对：Phase 0 显式校验两边 schema/行数一致再采计数；harness 失败 fail-fast，不静默产出半截基线。
- **cost 误调导致 ssb 回退。** 应对：ssb 13 条作为硬门槛；cost 仅在 H1 确认时按 targeted test 校准，不为 golden 强制。
- **set-op / rollup 穿透引入语义错误。** 应对：优先 branch-local + 结果一致性测试覆盖 grouping sets / union 边界。

---

## 9. 大致实现顺序（详细分解交给 writing-plans）

1. **Phase 0**：派生 `agg_plan_diff.py`；起 FE+NR 灌数；采当前基线；归因工作清单。
2. **Phase 1**：新增 `agg_mergeability.rs` + 防漂移测试；`split_aggregate.rs` 委托；avg/stddev/var 结果正确性测试 + golden。
3. **Phase 2**：NR EXPLAIN 根因 Bucket 2；按 H2/H3/H4(/H1) 落改动 + golden。
4. **Phase 3**：pushdown∘split 顺序与标记保持；组合 golden。
5. **Phase 4**：distribution / 列裁剪 / RF probe path 验证；ssb 不回退。
6. **Phase 5**：重跑 FE diff 出 parity 报告；锁定 golden；更新 Roadmap 进度。

---

## 10. Definition of Done

- 可拆判定由 `agg_mergeability.rs` 单一真相源驱动，含 distinct/ordered/顺序敏感 guard 与防漂移测试。
- `tpc-h/q1` 与含 avg 的 tpc-ds 代表 query 出现 FE 同向 local/global。
- Bucket 2 代表 query（derived/set-op/rollup）按实测根因覆盖一批 split。
- pushdown∘split 不产生重复聚合；现有 `aggregate_pushdown_*` 不回退。
- distribution / 列裁剪 / RF probe path 保持；ssb 13 条不回退。
- Phase 5 FE diff 报告显示聚合计数向 FE 收敛；所有新增 golden 与结果正确性测试全绿。
