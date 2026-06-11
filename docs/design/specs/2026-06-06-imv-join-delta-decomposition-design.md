# Iceberg IMV join-delta 组合化(纯 `Delta(Join)` 规则)设计

日期：2026-06-06

来源：
- 设计讨论（本会话）：把 `RewriteJoinAggregateDelta` 的 `Aggregate(Join)` **复合匹配**拆成一条纯
  `Delta(Join)` 展开规则，使 delta 能穿过 agg 与 join 之间的透传节点（`Filter`/`Project`）、并支持多层
  inner/cross join 嵌套。
- 承接 spec：[2026-06-04 IMV Refresh Property Framework 设计](2026-06-04-iceberg-imv-refresh-property-framework-design.md)
  §7（逐算子推导规则：`Filter`/`Project` 透传、`Join` delta 积）、§11（构造子封闭不变量）、§13
  （rule 目录 per-structural-form：新结构形态 = +1 条可组合 rule）。
- 承接 plan：[2026-06-05 IMV Phase 4](../plans/2026-06-05-imv-phase4-retire-incremental-mv-shape.md) 的
  "Known limitation"——`Aggregate(Filter(Join))` 不可刷新，本 spec 即兑现那条 follow-up。
- 关联：`docs/design/2026-05-25-general-logical-rewrite-framework.md`（通用 rewrite 框架 + fixpoint 模型）。

---

## 1. 目标

让顶层为 aggregate 的 Iceberg IMV 在以下两类结构上支持增量刷新（当前 fail-fast）：

1. **agg 与 join 之间夹透传节点**：`Aggregate( (Filter|Project)* ( Join ) )`——典型是带 `WHERE` 的
   join-aggregate MV。这是用户最初指出、且 Phase 4 文档明确记录的 pre-existing 限制。
2. **多层 inner/cross join 嵌套**：`Aggregate( Join( Join(A,B), C ) )` 等任意深度的 join 树，叶子是
   base 表（经 project/filter）。

实现手段：把现有 `RewriteJoinAggregateDeltaRule`（一条同时匹配 `Aggregate` 和 `Join` 两层的复合规则）
**拆解**为一条纯 `RewriteJoinDeltaRule`（只认 `ImvDelta(Join)`），并让它与已有的
`PushDeltaThroughUnaryRule` 共享同一个 stage 的 fixpoint。这是 property-framework §13 所说的"+1 条局部、
可组合、fail-fast 的 rule"，**不引入新 marker、不扩 apply-key 词汇表、不改 refresh driver**。

## 2. 非目标 / 范围边界

本 spec **只做"第一档"**：顶层 aggregate ⟹ target apply key 恒为 `GroupRowId`，旧状态从 target MV 表按
group 回读（`AggregateStateMerge.old_input`，见承接机制），**与 join 嵌套深度无关**。因此本改动**纯属
rewrite 层**，不触碰 apply 侧物理表示，也不引入新的增量代数证明。

显式留作**独立后续 spec**（在 unsupported 套件里以 fail-fast 守住边界）：

- **第二档 · join 一侧是 aggregate**（`Agg(Join(Agg(X), B))`）：触及 property-framework §12.3 nested-agg。
  难点是中间聚合**旧状态的来源**（单层聚合从 target 表回读；内层聚合无 target 表，需重算 `base@from` 或额外
  持久化）+ signed-state delta 参与 join 的代数正确性证明。正确性可达但带性能退化与新证明，**不在本 spec**。
- **第三档 · 顶层 join-projection MV 的嵌套 `JoinRowKey`**（`Join(Join(A,B),C)` 无顶层 agg）：触及
  property-framework §12.2。`stable_join_row_key`（`src/engine/mv/iceberg_join_coalesce.rs:342`）的签名
  固定 **2-arity**（`left_uuid,left_row_id,right_uuid,right_row_id`），隐藏列只有
  `JOIN_LEFT_ROW_ID_COLUMN`/`JOIN_RIGHT_ROW_ID_COLUMN` 两列，`RefreshIdentity::JoinRowKey` 注释明写
  "two-table"。三表身份是三元组，需泛化 apply-key arity——apply 侧结构改造，**不在本 spec**。

其它沿用承接 spec 的 fail-fast 边界：outer join（`join_delta_kind_supported` 仍只允许 inner/cross）、
nested aggregation、非 ALL set op、非白名单聚合函数——保持拒绝。

## 3. 背景与根因

### 3.1 现状

`RewriteJoinAggregateDeltaRule`（`src/sql/optimizer/rewrite/imv/join_delta.rs:36-47`）的 `matches` 硬性
要求 `ImvDelta(root) → Aggregate → Join` **紧邻**：

```rust
LogicalPlan::ImvDelta(delta) if delta.is_root
    && matches!(delta.input.as_ref(),
        LogicalPlan::Aggregate(aggregate)
            if matches!(aggregate.input.as_ref(), LogicalPlan::Join(_)))   // agg 的直接孩子必须是 Join
```

agg 与 join 之间夹一个 `Filter` 就不匹配；join 侧是 `Join`（嵌套）则被 `mark_scan`
（`join_delta.rs:170-188`，只接受 `Scan`/`Project`/`Filter`）fail-fast。

### 3.2 为什么"组合化了还断"

IMV pipeline 是 **stage 内 fixpoint、stage 间严格顺序不回头**
（`src/sql/optimizer/rewrite/pipeline.rs:74-110`）：

```rust
for stage in &self.stages {                 // 严格顺序,不回头
    for iteration in 1..=max_iterations {   // stage 内 fixpoint:rule 反复跑到不变
        for rule in &stage.rules { ... }
        if !phase_changed { break; }
    }
}
```

当前 stage 顺序：`imv-join-delta`(4) → `imv-union-delta`(5) → `imv-aggregate-state`(6) →
`imv-delta-pushdown`(7)。对 `Aggregate(Filter(Join))`：join-delta(4) 不匹配（agg 下是 Filter 不是
Join）跳过；aggregate-state(6) 把 delta 下推到 agg 输入得到 `ImvDelta(Filter(Join))`；delta-pushdown(7)
推过 Filter 得到 `Filter(ImvDelta(Join))`，再对裸 `ImvDelta(Join)` **fail-fast**
（`delta_pushdown.rs:63-65`）——而能展开它的 join-delta stage(4) 早已跑完、不回头。

**根因**：现有 join-delta 不是纯 `Delta(Join)` 规则，而是把 `Aggregate`+`Join` 焊死的复合规则，绕过了
property-framework §7 把 `Filter` 当透传算子的组合路径。要害是让"`Delta(Join)` 展开"与"delta 下推"处在
**同一个 fixpoint** 里。

## 4. 设计（方案 A）

### 4.1 核心规则与 pipeline 结构

**组件改动：**

1. **新增 `RewriteJoinDeltaRule`（纯版）**，替代旧复合规则：
   - `matches`：`ImvDelta(node) if node.input is Join`，**不要求 `is_root`**；join kind 仍由
     `join_delta_kind_supported` 限 inner/cross。
   - `apply`：复用现有 `mark_delta_scan` / `mark_version_scan` / `normalize_branch_output`，展开成
     `Union( Project(Join(ΔL, Version(R,from))), Project(Join(Version(L,to), ΔR)) )`。`action_column` 取自
     delta marker；内嵌 delta 的 `branch_scope = None`。
2. **删除** `RewriteJoinAggregateDeltaRule` 及 `imv-join-delta` stage(4)。
3. **`PushDeltaThroughUnaryRule` 的 `Join` 分支**：从 fail-fast 改为返回 `Unchanged`（交给同 stage 的
   join-delta）。`Aggregate`/`Union` 分支**仍保留 fail-fast**——它们应已在 aggregate-state / union-delta
   被消费；走到这里还遇到裸的就是真错误。
4. **pipeline**：把 `RewriteJoinDeltaRule` 并入 `imv-delta-pushdown` stage，与
   `PushDeltaThroughUnaryRule` 同 stage 共享 fixpoint。

**数据流走查 ① `Aggregate(Filter(Join))`：**

```
delta-marker / branch-union → ImvDelta(root, Aggregate(Filter(Join)))
imv-aggregate-state          → AggregateStateMerge(old, Project(Aggregate( ImvDelta(Filter(Join)) )))
imv-delta-pushdown（同一 fixpoint）:
  iter① pushdown 过 Filter   → …Aggregate(Filter( ImvDelta(Join) ))
  iter② join-delta 展开      → …Aggregate(Filter(Union( ΔL⋈V(R,from), V(L,to)⋈ΔR )))
  iter③ pushdown 把 branch 里 Δ 推到 base 叶子 → 收敛
```

**数据流走查 ② 嵌套 `Aggregate(Join(Join(A,B),C))`：** 同一 fixpoint 内，外层 `ImvDelta(Join(Join(A,B),C))`
先展开，branch 里出现 `ImvDelta(Join(A,B))`，下一轮再展开到 A/B 叶子——**靠 fixpoint 递归，无需多 stage**。

**等价性**：单层 `Aggregate(Join)`（现有已支持）不再走旧复合规则，改走"aggregate-state 下推 → 纯
join-delta 展开"，最终 signed aggregate 的输入仍是 `Aggregate(Union(ΔL⋈V, V⋈ΔR))`，**语义等价**（plan 可能
column-id 重编号，护栏不锁 id，见 §5）。composed 分支同理（branch-union 拆出的每个
`ImvDelta(root, Aggregate, branch_scope)` 走同一路）。

### 4.2 `mark_scan` 放宽

`mark_scan`（`join_delta.rs:170-188`）新增 `Join` 分支，**按 marker 类型分别处理**：

| join 侧节点 | Delta marker | Version marker |
|---|---|---|
| `Scan` | wrap `ImvDelta(Scan, …)` | wrap `ImvVersion(Scan, role)` |
| `Project`/`Filter` | 递归进 child | 递归进 child |
| **`Join`（新增）** | wrap `ImvDelta(Join, is_root:false, …)`——**整个 join 包成 delta，交给 join-delta 递归展开** | **递归下推到两侧** `Join(mark(L,version), mark(R,version))`，**两侧用同一 role** |

关键不对称：**Delta marker 在 Join 上不分裂**（join 的 delta 不是两侧 delta 的简单并，必须靠 join-delta 的
防双计展开）；**Version marker 在 Join 上自由下推**（`Version(Join(A,B), role) ≡ Join(Version(A,role),
Version(B,role))`，全量快照在 join 上满足分配律）。

### 4.3 正确性：防双计 / 嵌套 / self-join

**单层防双计（基础）**：现有展开 `Δ(L⋈R) = ΔL ⋈ Version(R,from) ∪ Version(L,to) ⋈ ΔR`。第二项 `L@to`
含 ΔL，故 `ΔL⋈ΔR` 只在第二项算一次；第一项 `R@from` 不含 ΔR，不重复。纯规则**原样复用**，不重写代数。

**嵌套归纳**：外层 `Δ(Join(J,C))`（`J = Join(A,B)`）：
```
外层 left  = ImvDelta(Join(A,B)) ⋈ Version(C, from)      （待递归展开）
外层 right = Join(Version(A,to), Version(B,to)) ⋈ ImvDelta(C)
```
下一轮 fixpoint 展开内层 `ImvDelta(Join(A,B))`：`ImvDelta(A)⋈V(B,from) ∪ V(A,to)⋈ImvDelta(B)`。

- `ImvVersionRef` 是抽象 role（From/To），`BindIcebergScanRule` 在 leaf 把 role 绑定到各 base 自己的
  旧/新 snapshot。Version 下推到嵌套 join 时**保持 role 标签一致**（§4.2 不变量）。
- 归纳：外层两项用 from/to 错开（不交叉），内层两项同样错开；**没有跨层交叉项**。任意深度由 fixpoint
  逐层展开，每层独立防双计。

**self-join（`base b JOIN base b`）**：`Δb ⋈ Δb` 在 left branch 用 `V(b,from)`（不含本次 Δb）、right
branch 用 `V(b,to)`（含 Δb），算一次——复用现有机制，纯规则不引入新逻辑。fixture 现有 self-join 测试可直接
迁移。

### 4.4 marker 传播与终止性

- **`action_column`**：纯 join-delta 取自 delta marker（aggregate-state 已分配）；展开产物所有 delta scan
  共用同一 `__change_op`，嵌套内层从外层继承，保持单列。
- **`branch_scope`**：纯路径下由 aggregate-state 在 `old_input` 消费（target 表 row/partition filter）；纯
  join-delta 拿到的是 `is_root:false`、`branch_scope:None` 的 delta，不携带 scope。与旧复合规则在 root
  delta 上"搬运 scope"语义等价（composed 的 scope 已在 aggregate-state 用过）。
- **终止性**：`matches` 是 `ImvDelta(node) if node.input is Join`。展开产物外层是
  `Project(Join(marker_L, marker_R))`（marker 在两侧孩子里），**不再匹配**；只有 `mark_delta_scan(Join)`
  产生的内层 `ImvDelta(Join(A,B))` 会被再匹配——正是预期递归。配合 `max_iterations` fixpoint 上限，不会
  无限循环。

## 5. 回归护栏与测试矩阵

### 5.1 回归护栏（已有不退步）

| 层级 | 锁的是什么 | 路 B 是否破坏 |
|---|---|---|
| `iceberg-ivm` e2e | `MV == 全量重算` cross-check | 否——语义等价 |
| `optimizer/imv_*_logical_cutover.sql` | `@explain_contains`（AggregateStateMerge / UNION / IcebergVersionTable / IcebergMvTargetState / sum_state_signed） | 否——子串仍出现 |

**两层护栏均不比对 column-id**，故 column-id 重编号对护栏透明。

### 5.2 规则级单测连带改写（机械）

- `join_delta.rs` 旧 rule 级单测（`assert_supported_join_rewrite` 等）→ 改写为纯 `RewriteJoinDeltaRule`
  的单测：`matches ImvDelta(Join)`（任意 `is_root`）、展开两 stable branch、嵌套 `ImvDelta(Join(Join,C))`
  展开外层后留 `ImvDelta(Join(A,B))` 待下一轮。
- `delta_pushdown.rs::rejects_delta_over_join`（:316-326）→ 反向断言：遇 Join 返回 `Unchanged`；
  Aggregate/Union 仍 fail-fast。
- `mark_scan` 新增 Join 分支两种 marker 的走查单测。
- pipeline 测试：移除 `imv-join-delta` stage 顺序断言；新增"纯 join-delta 与 PushDeltaThroughUnary 同在
  `imv-delta-pushdown` stage"断言 + "`imv-join-delta` stage 不存在"负向断言。
- `branch_union.rs::pipeline_branch_union_of_aggregate_over_join_composes` 保持绿色；内部 marker 形态断言
  按需更新。

### 5.3 新增正向 e2e（`sql-tests/iceberg-ivm/`，走 `MV==全量重算`）

1. **`iceberg_ivm_aggregate_filter_join.sql`** — 带 `WHERE` 的 join-aggregate MV
   （`SELECT d.region, SUM(f.amt) FROM fact f JOIN dim d ON … WHERE f.amt>0 GROUP BY d.region`）。
   矩阵：initial + INSERT(fact) + INSERT(dim) + DELETE(fact)，每步 REFRESH 后 cross-check；断言
   `@explain_contains=Filter,UNION,sum_state_signed,IcebergMvTargetState`。
2. **`iceberg_ivm_aggregate_nested_join.sql`** — 三表嵌套
   （`fact JOIN dim JOIN dim2 GROUP BY dim2.region`）。矩阵：各 base 各自 INSERT/DELETE → cross-check；
   断言 `@explain_contains=UNION,IcebergVersionTable,AggregateStateMerge`。嵌套展开会产生外内两组 UNION，
   但 `@explain_contains` 是子串包含、**不保证出现次数**；"外内两层都展开"由 §5.4 的 EXPLAIN 人工核对兜底，
   不在此处用计数断言。
3. **更新 `iceberg_ivm_union_shape_rejects_unsupported.sql`**：移除/迁出 `Aggregate(Filter(Join))`
   reject（现已支持）；保留**真不支持**的 join-of-aggregate（`Agg(Join(Agg(X),B))`）作显式 reject，守住
   第二档独立 spec 的边界。

### 5.4 optimizer plan-shape（`@explain_contains` 风格，无 `.result`）

- `optimizer/imv_aggregate_filter_join_logical.sql`：EXPLAIN REFRESH 断言
  `AggregateStateMerge / UNION / Filter / sum_state_signed / IcebergVersionTable` 齐现。
- `optimizer/imv_aggregate_nested_join_logical.sql`：三表 join。`@explain_contains=UNION,IcebergVersionTable`
  确保两者齐现；"外内两层各一组 UNION"在该 case 的 EXPLAIN 输出里**人工核对一次**（runner 不做出现次数断言），
  作为嵌套展开正确性的 plan-shape 凭证。

## 6. 风险与缓解

| 风险 | 缓解 |
|---|---|
| 删旧复合规则后单层 `Aggregate(Join)` 走新路径，中间 plan 形态变 | `@explain_contains` 不锁形态 + e2e `MV==重算` 覆盖正确性；手动 EXPLAIN 一例确认子串齐现（按 §5.1 决策不额外加文本对比断言） |
| fixpoint 迭代次数增多（嵌套递归 + 下推交替） | `max_iterations` 已有上限；典型嵌套深度 ≤ 3；加断言：fixpoint 收敛轮数有界 |
| `mark_scan` 放宽误让 outer join 等滑过 | 保留 `join_delta_kind_supported`（仅 inner/cross），outer 仍 fail-fast |
| 嵌套展开与现有 self-join 测试不兼容 | self-join 走同一机制（§4.3），fixture 直接迁移；新增嵌套 self-join 单测 |

## 7. 验收标准

- `Aggregate(Filter(Join))` 与 `Aggregate(Join(Join(A,B),C))` 的 IMV 增量刷新端到端正确
  （iceberg-ivm 新增 case `MV==全量重算` 全绿，含 insert/delete/mixed）。
- 现有 iceberg-ivm 套件 + optimizer imv plan-shape 全量不回归。
- `imv-join-delta` stage 删除；`RewriteJoinDeltaRule` 与 `PushDeltaThroughUnaryRule` 同 stage；outer
  join / join-of-aggregate 仍显式 fail-fast。
- `cargo build`/`fmt`/`clippy` 干净；`cargo test --lib`（imv rewrite 模块）全绿。

## 8. 实现要点索引（file:line）

- `src/sql/optimizer/rewrite/imv/join_delta.rs`：`matches`(:36-47)、`apply` 展开(:81-133)、`mark_scan`
  (:170-188)、`wrap_scan_marker`(:190-203)、`join_delta_kind_supported`(:16-21)。
- `src/sql/optimizer/rewrite/imv/delta_pushdown.rs`：`matches`(:32-45)、Join fail-fast(:63-65)。
- `src/sql/optimizer/rewrite/imv/pipeline.rs`：`imv-join-delta` stage(:46-50)、`imv-delta-pushdown`
  stage(:64-68)。
- `src/sql/optimizer/rewrite/imv/aggregate_rewrite.rs`：`RewriteAggregateStateRule::matches`(:54-60)、
  `signed_aggregate` 包非 root delta(:612-621)、`build_aggregate_state_merge`(:86-171)。
- `src/sql/optimizer/rewrite/imv/branch_union.rs`：拆 root delta + branch_scope(:89-130)。
- `src/sql/optimizer/rewrite/pipeline.rs`（引擎）：stage/fixpoint(:67-113)。

## 9. 后续

- 完成后在 `NovaRocks Roadmap.md` 的 Iceberg v3 Incremental MV 项更新状态，并把 Phase 4 文档的 "Known
  limitation"（`Aggregate(Filter(Join))`）标记为已解。
- 第二档（join-of-aggregate / nested-agg）、第三档（嵌套 `JoinRowKey` apply-key arity 泛化）各自走
  brainstorming → 独立 spec。
