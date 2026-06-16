# 优化器统计推导去重 — 设计文档

- 日期：2026-06-16
- 状态：已评审，待实现
- 范围：`src/sql/optimizer/`（standalone Cascades 优化器）
- 目标：消除两处「同一份 group 统计被重复计算、结果完全相同」的冗余，且**零 plan 影响**。

---

## 1. 背景与问题

### 1.1 统计推导机制（背景）

NovaRocks 的 CBO 是 Cascades 架构，核心是 Memo：

- `Group`（`memo.rs:93`）：一组逻辑等价表达式，含 `logical_exprs`、`physical_exprs`、`logical_props: Option<LogicalProperties>`。
- `LogicalProperties`（`memo.rs:107`）装着该 group 的统计：`row_count`、`column_statistics` 等。
- 约定一：统计是 **group 级**的，存在 `logical_props` 里，它是该 group 统计的权威缓存位置。
- 约定二：`derive_group_statistics_for`（`stats.rs:719`）用 `logical_exprs.first()`（否则 `physical_exprs.first()`）**代表整个 group** 算一次。
- `derive_statistics`（`stats.rs:57`）只读直接 child group 已缓存的 `logical_props`，不递归整棵子树——单群推导是 O(算子逻辑 × 列数)，前提是 child 统计已缓存。

`optimize()`（`mod.rs`）的统计相关时序：

1. `logical_plan_to_memo`（`mod.rs:127`）建 memo，所有群 `logical_props=None`。
2. `derive_group_statistics`（`mod.rs:130`）**第一次全量** derive。
3. `run_multi_join_reorder`（`mod.rs:139`，受 `MultiJoinReorder` 开关控制）注入候选 join 顺序；新建的 join 群在 `copy_in_join_tree`（`stats.rs:790`）创建时**立即 stamp** 统计。
4. `explore()`（`mod.rs:143`）：往已有群**追加** logical 候选 / 新建群。
5. `implement()`（`mod.rs:149`）：往已有群追加 physical 实现 / 新建中间群（如 AggSplit）。
6. `derive_group_statistics`（`mod.rs:167`）**第二次全量** derive。
7. `optimize_group` 搜索（`search.rs`）。
8. `extract_best`。

### 1.2 两处重复

**A — 第二次全量 derive 重算已算好的群。** `derive_group_statistics`（`stats.rs:703`）是 `for group_idx in 0..memo.groups.len()` 无 guard 全量循环。第二次调用（`mod.rs:167`）的真实职责（其注释原话）只是「补 implement 新建的群（如 AggSplit）」，但实现把**所有群**从头重算并覆盖——第一次已算的老群、reorder 创建时已 stamp 的群，都被重算，结果逐字节相同，纯浪费。

**B — 搜索阶段 `own_stats` 在 `for alt` 循环里被重复现算。** `optimize_group` 内层循环 `search.rs:168`：
```rust
let own_stats = derive_statistics(expr, memo, &self.table_stats);
```
`own_stats` 只依赖 `expr + memo`、**不依赖 `alt`（物理属性候选）**，却被放在 `for alt` 循环里对每个属性候选重新递归 derive 一次。

---

## 2. 验证结论（对抗式）

本设计的两条安全假设经过一个 9-agent workflow 调研 + 对抗验证（每条安全结论由 2 个独立 agent 努力找反例）。

### 2.1 A1 假设 — 成立（高置信）

假设：第一次 derive（`mod.rs:130`）与第二次（`mod.rs:167`）之间，没有任何已有群的 `logical_exprs.first()`、其 children、或其 children 的统计发生变化，因此跳过已 `Some` 的群是 value-preserving。

证据（两个对抗 agent 均无法 refute）：

- 优化器是 append-only memo：表达式只通过 `new_group`（新群，`logical_props=None`）或 `add_expr_to_group`（`memo.rs:93-99`，**push 到末尾**）加入。全仓库（生产 + 测试）grep 不到 `insert(0)`/`push_front`/`splice`/`iter_mut`/`first_mut`/在原地改 `MExpr.op`/`MExpr.children`。所以已有群的 `.first()` 全程不变。
- `explore()`（`mod.rs:254-292`）/`implement()`（`mod.rs:322-354`）都是「clone 表达式 → apply 规则 → `add_expr_to_group` 追加」，纯追加。
- 唯一的生产 `logical_props` 写入点：`derive_group_statistics_for`（`stats.rs:742`）、`copy_in_join_tree`（`stats.rs:790`，只写**新建**的 join 群）、`equivalence_predicate.rs:298`（只写 `add_filter_group` 新建的群）。**没有任何点覆盖已有群的 `logical_props`。** 其余所有 `logical_props =` 写入都在 `#[cfg(test)]` 内。
- 第二次 derive 真正需要算的群（`SplitAggregateRule` 的 Local agg 群、implement 新建的中间群）都是 `logical_props=None`，guard 放行，仍会算。

调用面（`callers` 调研）：`derive_group_statistics` 共 18 个调用点（3 生产 + 15 测试）。除 `mod.rs:167` 外都是 fresh memo（所有群 `None`），guard 是 no-op；aggregate-pushdown bridge `derive_logical_plan_statistics`（`stats.rs:632`）也是每次 `Memo::new()` 的 fresh memo，不受影响。`derive_group_statistics_for` 在 stats.rs 外无调用者。

### 2.2 B「读缓存」被证伪 — 关键修正（高置信）

原始设想是把 `search.rs:168` 换成读 group 缓存（`stats_for_group`）。**两个对抗 agent 各自独立证明这会改变 cost，因此改 plan，必须排除。**

反例（`SplitAggregateRule`，默认开启）：它把 Global 聚合 append 进 Single 聚合的**同一个 group**，实现后两者都是 `PhysicalHashAggregate`，但 `own_stats` 不同：

- 设 child C：`row_count=200`，一个 group-by key，列 NDV=100。
- 缓存（从 `logical_exprs.first()` = Single 算，`stats.rs:725`）：`agg_group_rows([100],200)=min(100, 200×0.75)=100`。
- Single physical 的 `own_stats` = 100（= 缓存）。
- Global physical 跑在 Local 群（行数已降到 100）之上：`agg_group_rows([100],100)=min(100, 100×0.75)=75`。`UNKNOWN_GROUP_BY_CORRELATION=0.75`（`statistics.rs:398`）在 Global 阶段被**二次施加**。

结果：缓存=100、Single=100、**Global=75**。同理 `PhysicalHashJoin`（`stats.rs:376`，用 `eq_key_ndvs`）与 `PhysicalNestLoopJoin`（`stats.rs:437`，用 `non_equi_selectivity`）共群时基数公式不同。

结论：**同 group 内不同 physical expr 的 `own_stats` 确实不同**，读缓存会把 Global 的 75 变成 100 → 改 cost → 可能改 plan。违反「零 plan 影响」硬约束。

安全替代（`b_variants` 调研）：`own_stats` 不依赖 `alt`，把它**移出 `for alt` 循环**即可消除冗余重算，每个 physical expr 仍各算各的，结果完全不变。`own_stats` 在 `search.rs:168` 之后**仅**用于 `search.rs:200` 的 `compute_cost_with_properties(&own_stats, …)`，不被存储/返回，enforcer 路径（`search.rs:222-223`）另行调 `stats_for_group`、不复用 `own_stats`——所以移出 `for alt` 不影响 enforcer cost。

---

## 3. 方案

### 3.1 A1 — 第二次 derive 加记忆化 guard + 护栏

在 `derive_group_statistics`（`stats.rs:707`）循环里加 guard：

```rust
for group_idx in 0..memo.groups.len() {
    // Memoized derive: a group's logical_props are computed exactly once,
    // when first needed (StarRocks isStatsDerived semantics). Safe because the
    // memo is append-only — rules only append new exprs and never rewrite an
    // existing group's logical_exprs.first() in place, so a recompute would
    // reproduce the identical value.
    if memo.groups[group_idx].logical_props.is_some() {
        continue;
    }
    derive_group_statistics_for(memo, group_idx, table_stats);
}
```

效果：第一次调用（`mod.rs:130`）所有群 `None`，guard 全不触发，仍是完整全量；第二次（`mod.rs:167`）只算新建的 `None` 群，跳过所有已算群。一处改修两个调用点，等价 StarRocks `isStatsDerived` 记忆化。

**护栏（防未来回归）**：在 guard 处加 doc-comment 钉住 append-only 不变量；并在 `derive_group_statistics_for` 的文档里写明「若未来有规则原地改写已有群的 first expr，必须先把该群 `logical_props` 重置为 `None`，否则记忆化会服务过时统计」。可选：加 `debug_assert` 表达该约束。

### 3.2 B' — `own_stats` 移出 `for alt` 循环

把 `search.rs:168` 的
```rust
let own_stats = derive_statistics(expr, memo, &self.table_stats);
```
从内层 `for alt` 循环提到 `for expr_idx` 循环体顶部——即取到 `let expr = &memo.groups[group_id].physical_exprs[expr_idx];`（`search.rs:126`）之后、`derive_required_alternatives`（`search.rs:128`）之前。每个 physical expr 仍各算各的 `own_stats`，只是不再为每个 `alt` 重算。纯行为保持。

**不做**：替换成 `stats_for_group(group_id)`（已证伪，是行为变更）；不把 `own_stats` 提到 `for expr_idx` 之外（同群不同 physical expr 的 `own_stats` 不同，见 2.2）。

---

## 4. 明确排除 / 留 future

- **B 读缓存（own_stats 用 group 缓存）**：是行为变更（改 plan），需单独 gate + benchmark + golden 重录，**不进本次**。若未来要做，前置是统一 SplitAggregate / HashJoin-vs-NestLoop 在同群内的 own_stats 语义。
- **统一记忆化入口（独立 `group_statistics()` + 脏标记）**：YAGNI。A1 的 guard 已经是 StarRocks 式记忆化，`logical_props` + `stats_for_group` 已是事实上的记忆化入口，无需再造一层抽象（StarRocks 自身也只是 `group.statistics` + flag）。

---

## 5. 测试与验收

- **硬门：plan 字节不变。** A1 + B' 都是零行为变更，跑 `sql-tests/optimizer/` golden plan 必须完全一致（含 `EXPLAIN` 各级别）。
- **单元（B' 防退化）**：构造 SplitAggregate 群（child 200 行、key NDV=100），断言 B' 后 Single / Global 两个 physical expr 的 `own_stats.output_row_count` 仍为 100 / 75（证明没退化成读缓存）。
- **单元（A1 等价性）**：构造一个含老群 + 一个 implement 新建群的 memo，断言加 guard 后第二次 derive 只算新群、老群 `logical_props` 与不加 guard 时逐字节相同。
- **收益度量**：宽表大 join（如 33 列、>1M 行的 `LEFT SEMI`，对应曾吃满 budget 的 case）的 `optimize()` 墙钟 before/after；并加一个临时 `derive_statistics` / `derive_group_statistics_for` 调用计数器证明调用次数下降。计数器是临时量化手段，度量完即删（不留长期代码）。
- **连带验证**：A1 + B' 落地后，`OPTIMIZE_TIMEOUT`（`mod.rs:50`，曾因 join-suite 从 10s 抬到 30s）可评估回调，作为独立后续，不在本次范围。

---

## 6. 风险与回滚

- **A1 风险**：仅在「未来某规则原地改写已有群 first expr 而不重置 `logical_props`」时才会服务过时统计——当前不存在此类代码（全部 append）。由护栏 doc-comment / `debug_assert` 兜底。
- **B' 风险**：极低，纯移动一行、不跨 expr 边界、不改值。
- **回滚**：A1、B' 各自是局部小改，独立 commit；出问题直接 revert 单个 commit，无 config flag（无行为分叉，只有快慢）。

---

## 7. StarRocks 对照

| StarRocks（task-based Cascades） | 本设计的等价物 |
| --- | --- |
| `DeriveStatsTask.java:50` `if (isStatsDerived()) return;` 记忆化 | A1：`if logical_props.is_some() { continue; }`（`logical_props: Option` 即「算过没」标记） |
| 统计存 `Group.statistics`，一群一份（`Group.java:56`） | 统计存 `group.logical_props`，一群一份 |
| 无「全量重扫」，按需自底向上 + 记忆化 | A1 让第二次 derive 退化为「只补新群」 |
| cost 阶段读 `group.getStatistics()`，绝不现算（`ExpressionContext.java:70`） | B' 不在 `for alt` 重复现算（注：本设计因 own_stats 的 per-expr 语义差异，保留 per-expr 现算而非读 group 缓存，与 StarRocks 在此点有意分歧） |

注：StarRocks 的 group 统计对每个 GroupExpression 算并取最优（`needUpdateGroupStatistics`），比本引擎「first expr 代表全群」更精细；本次不改这一语义，只去重。

---

## 附录：关键代码引用

- `stats.rs:703-710` `derive_group_statistics` 全量循环（A1 改这里）
- `stats.rs:719-750` `derive_group_statistics_for` 单群原语（#317 引入）
- `stats.rs:790` `copy_in_join_tree` 创建 join 群时立即 stamp
- `mod.rs:130` / `mod.rs:167` 两次全量 derive 调用点
- `mod.rs:139` `run_multi_join_reorder`；`mod.rs:143` `explore`；`mod.rs:149` `implement`
- `search.rs:126-128` `for expr_idx` 顶部（B' 提到这里）
- `search.rs:168` `own_stats = derive_statistics(...)`（B' 改这里）
- `search.rs:200` `compute_cost_with_properties(&own_stats, …)`（own_stats 唯一消费点）
- `search.rs:278` `stats_for_group`（读缓存包装，B 读缓存被证伪的对象）
- `split_aggregate.rs:64` Global agg append 进 Single 群（B 反例来源）
- `statistics.rs:398` `UNKNOWN_GROUP_BY_CORRELATION=0.75`；`ndv.rs:79` `agg_group_rows`
- `memo.rs:93-99` `add_expr_to_group`（append-only，A1 安全性基石）
