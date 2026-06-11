# Iceberg IMV Refresh Property Framework 设计

日期：2026-06-04

来源：
- 设计讨论（本会话）：把 IMV refresh contract 从 `RefreshStrategy` / `IncrementalMvShape` 的 shape 枚举中解耦，改成 logical plan rewrite 过程中合成的 *property*。
- 承接 spec：[2026-06-02 Iceberg IMV UNION ALL Delta Rewrite 设计](2026-06-02-iceberg-imv-union-all-delta-rewrite-design.md)（已落地，建好全部底层原语）。
- 承接 spec：[2026-05-26 IVM VARBINARY state & distinct-count 设计](2026-05-26-ivm-varbinary-state-and-distinct-count-aggregates-design.md)（aggregate state 编码）。
- 关联：`docs/design/2026-05-25-general-logical-rewrite-framework.md`（通用 rewrite 框架）。
- Roadmap 索引：`NovaRocks Roadmap.md`（Iceberg v3 Incremental MV Roadmap）。

本文是 IMV refresh 契约推导的**架构 spec**（what / why / 不变量 / 边界 / 阶段），作为后续各 Phase plan 的参照系。它**不重述**承接 spec 已记录的原语机制（branch_id 原理、复合 apply-key、apply 侧定位），只引用并在其上重构。

---

## 1. 目标

把"一条 MV 定义 SQL 需要哪些 refresh 信息"的推导，从**两套并行的扁平 shape 分类器 + refresh driver 的 shape dispatch**，改成**一次结构归约（attribute grammar）产出的 capability property**，使：

1. **refresh driver 不再识别 SQL pattern**——它消费 `(target identity, state, apply, snapshot policy)` 这组封闭 capability，不认识 "union over aggregate" 这类 SQL 形状。
2. **契约是 rewrite 的副产物**，不是第二套 compile/classifier——plan 与 contract 同源、不可能 drift。
3. **新的 MV SQL 形状通过 rewrite rule 组合支持**（局部、可组合的改动），而不是每次 `+1` 个 shape 枚举 + N 处 dispatch + 跨枚举对账。
4. **不可增量的 SQL 在 CREATE 期 fail-fast**，且给出精确原因（哪个 function / 哪个节点），而不是 refresh 期静默出错或笼统拒绝。

非破坏性约束：对当前已支持的所有 MV 形态，refresh 行为与产出的最终 plan **逐字节不变**（回归护栏）。

## 2. 非目标

- 不改 StarRocks（非 Iceberg）MV 路径——`IncrementalMvShape` 在 `mv_apply_policy.rs` / `mv_refresh.rs` / `mv_ddl.rs` 仍是 load-bearing 的主分类，**显式不在本范围**。
- 不引入 cost-based 增量策略选择（MIN/MAX 的 detail-state vs 重算等代价权衡）——本框架是**正确性 typing 纪律**，不进 CBO memo；cost 层是未来叠加。
- 不支持 `UNION` / `INTERSECT` / `EXCEPT`（去重/交/差）增量；不新增当前未支持的 aggregate function；不支持隐式 cast。沿用承接 spec 的 fail-fast 边界。
- 不写兼容代码 / metadata 迁移（项目无历史用户）。引擎升级若改变推导逻辑 → MV recompile，不做 contract 迁移。
- 不在本框架内放宽 SQL 覆盖面（如 outer join、nested aggregation 的实际支持）——本框架定义**如何推导与分派**；放宽覆盖面是各 Phase plan 里加 rewrite rule 的事。

## 3. 范围决策（已与用户确认）

| 决策点 | 选择 | 含义 |
| --- | --- | --- |
| 契约来源 | rewrite 副产物（填 `ImvPlanAnnotation`） | 不做第二套 classifier；plan 与 contract 同源 |
| "同构 branch" 的定义 | 锚定在 **synthesized property** `(identity ctor, state ctor)`，非 SQL shape | `Union(Agg over t1, Agg over (t2⋈t3))` 视为同构、支持 |
| 异构 branch | 当前**拒绝**；per-branch contract 留作未来方向（DP1） | `BranchScoped(per_branch_contract)` 是后续模型 |
| 可增量性证明深度 | per-function proof 进 `FragmentProperty`，CREATE 期 fail-fast（DP2） | MIN/MAX-on-delete、nested-agg 等在 CREATE 期显式拒绝 |
| property 性质 | 确定性 typing，不进 CBO memo | determinism > optimality |
| 契约权威性 | 落盘 contract 是 source of truth | refresh 重跑 rewrite 只为产出可执行 plan + 校验运行时前置条件，不 re-derive 对账 |
| 范围分期 | Phase 1（rewrite 层组合化）/ 2（property→契约）/ 3（driver capability 化 + 退枚举） | 每 Phase 独立可测、独立 plan |

## 4. 现状判断（为什么要做）

Iceberg IMV 路径当前**三套分类并存**，前两套是冗余的 shape 枚举：

| 层 | 类型 | 形态 | 角色 |
| --- | --- | --- | --- |
| AST 分类 | `IncrementalMvShape`（5 variant，`mv_shape.rs:1`） | 裸 sqlparser AST 决策树（`mv_shape.rs:156`） | Iceberg 路径已退化为"喂 layout + 交叉校验" |
| refresh 策略 | `RefreshStrategy`（7 variant，`refresh_contract.rs:8`） | resolved query 推导，或从 capability flag 反推（`iceberg_refresh.rs:4764`） | refresh driver 的主 dispatch key（执行 `:2445` + 计划 `:4861` 两处 match） |
| 增量代数 | rewrite rule pipeline（`imv/pipeline.rs`） | 线性可组合 rule | 真正的增量改写引擎 |

**已经组合化的（基座，无需重建）：**
- plan rewrite 是 marker-based 增量代数：`ImvDelta`/`ImvVersion` marker + 保留名列（`__change_op`/`_row_id`/`__branch_id__`/`__nova_base_row_id`）+ scan-source 枚举做胶水；无中心 shape 枚举驱动。
- target identity 已是统一抽象：语义维 `ApplyKeySource{BaseRowId, JoinRowKey, GroupRowId}` × 物理维 `ApplyKeyValueType{Int64, Utf8, BranchInt64, BranchUtf8}`（`iceberg_merge_sink.rs:38`）；merge sink 按 `ApplyKeyValueType` **数据驱动**分派（`:196`），非 shape。
- branch 维已被建模为 target-identity 的一个 `Option`：plan 层 `Option<BranchScope>`（`catalog.rs:161`）、scan 层 `IcebergMvTargetStateRowFilter::DeltaInputRowIds{ branch_scope: Option<BranchScope> }`（`catalog.rs:168`）、apply 层 `BranchApplyKey{branch_id, base_row_id}`。
- 增量内核 `incremental_refresh_iceberg_mv_with_changes`（`iceberg_refresh.rs:10063`）对所有 strategy 共用，由 `ApplyKeyContract` 字段驱动；commit/staging/collector/state 编码完全统一、不因 branch 而异。
- `ImvPlanAnnotation`（`annotation.rs:16`）是**已声明、空着**的契约累加器，doc 注释明写预期字段 "branch identity / marker node ids / action column refs"。

**仍是枚举式分派的（外壳，本框架要替换）：**
- 两个 load-bearing `match strategy`（执行 + 计划，写了两遍），各扇出 6 个 wrapper，wrapper 间大量重复（`refresh_branch_union_aggregate_*` 与 `refresh_fan_in_aggregate_*` 几乎逐字节相同）。
- 两套冗余枚举 + `stored_strategy_matches_legacy_shape` 对账（`iceberg_refresh.rs:4797`）。

**症状（已实测确认）：** `RewriteBranchUnion` 对 `Union(Aggregate(Join), Aggregate(Join))` 返回 `Changed` 但留下一个孤儿 `ImvDelta` 挂在 `Join` 上（无下游规则绑定）→ validation 拒绝。根因：它 inline 调 `build_aggregate_state_merge` 且跑在 join-delta 之前，把分支内 join 的 delta 展开短路了。**承接 spec 的 §2 明确把"分支内嵌 join" fail-fast 留作后续——本框架就是那个"后续"，且把它从"再加一种 shape"变成"组合已有 rule"。**

一句话：**基座（rewrite 代数 / identity / state / apply / commit）已组合化；外壳（driver 的 shape dispatch + 两个枚举）仍是 shape 枚举。`BranchUnionAggregate` 本质 = `TargetIdentity::BranchScoped(GroupRowId)`，是身份的一维,不是 shape。**

## 5. 核心模型：refresh capability 是 logical plan 上的 attribute grammar

把"MV 需要什么 refresh 信息"建模成 logical plan 上的一类 property，类似物理计划的 distribution/ordering，但**是 typing 纪律不是 cost 属性**（见 §11）。

- **Synthesized 属性**（自底向上合成）：`TargetIdentity`、`StateContract`、`produces_signed_delta`、`DeleteHandling`、`base_refs`、`obligations`。每个 plan 节点从孩子合成出自己的 `RefreshFragmentProperty`。这部分处理**任意嵌套**——递归即是普适性的来源。
- **Inherited 属性**（自顶向下传递）：`branch_scope`（UNION ALL 分支号）、CREATE-vs-REFRESH 模式（schema 产出 vs 绑定）。通过 **delta marker 携带**（`ImvDeltaNode.branch_scope`），是承接 spec 的 `__branch_id__` 机制的形式化。
- **Root finalize**（需要全树 + 外部 target schema）：`snapshot_policy`（结构部分可 synth，见 §12）、`apply_contract`（由 root identity 派生）、`schema_binding`、`commit_contract`。

这回答了用户反复问的"如何从 MV SQL 推导 refresh 信息"：**不靠 refresh 看 SQL pattern，而靠一次镜像 plan 结构的 property 归约；`branch_id` 在 `UnionAll` 节点产生，与孩子复杂度无关，因为每个孩子的 inner identity 由递归各自解出。**

## 6. Property / Contract 类型定义

```text
TargetIdentity =                       # synthesized；对组合封闭
    BaseRowId                          # scan：_row_id（base 行身份）
  | JoinRowKey(Box<TargetIdentity>, Box<TargetIdentity>)   # join：两侧身份的对
  | GroupRowId(keys)                   # aggregate：group key 坍缩
  | BranchScoped(Box<TargetIdentity>)  # UNION ALL：(branch_id, inner)；嵌套 flatten

StateContract =
    Stateless                          # projection/filter/join projection
  | AggregateState(layout, per_function_roles)   # 见 §8 aggregate

DeleteHandling =
    AppendOnly                         # 只 append，无需定位
  | NeedsLocator                       # delete 需按 apply key 定位目标行
  | NeedsFullRecompute(scope)          # 受影响 group/partition 需回源重算（MIN/MAX-on-delete）

RefreshObligation =                    # 带强制阶段标签（见 §12）
    RowLineageRequired(base)           # REFRESH-admission
  | SnapshotWindowRequired(base)       # REFRESH-admission
  | TargetStateLayoutRequired          # CREATE（建列）
  | TargetApplyKeyRequired             # CREATE（建 apply-key 列，含 __branch_id__）
  | LocatorRequired                    # REFRESH-execute

RefreshFragmentProperty {              # 每个 plan 节点 synthesized 产出
  identity: TargetIdentity,
  state: StateContract,
  produces_signed_delta: bool,         # 能否发可回撤（带 sign）的变更
  delete_handling: DeleteHandling,
  base_refs: Set<BaseRef>,             # monoid
  obligations: Set<RefreshObligation>,
}

RefreshContract {                      # root finalize 产出，落盘
  root_fragment: RefreshFragmentProperty,
  snapshot_policy,                     # 全局：single / all-bases / join-pair（结构部分）
  apply_contract,                      # 由 root_fragment.identity 派生 → ApplyKeyValueType + 列
  schema_binding,                      # CREATE：产出 target schema；REFRESH：绑定/校验
  commit_contract,                     # 当前退化为统一 merge-sink；命名占位，待第二语义再长
}
```

与现有持久化契约的对应（复用，不另起炉灶）：
- `TargetIdentity::BranchScoped(inner)` ↔ `BranchUnionContract{ branch_id_column, branch_count, inner_apply_key_source }`（`mv_contract.rs:115`）。`inner_apply_key_source` 就是 `inner`。
- `apply_contract` ↔ `HiddenApplyKeyContract` + `ApplyKeyValueType`。`BranchScoped` ⇒ `Branch*` 变体。
- `StateContract::AggregateState` ↔ `AggregateStateContract`（`mv_contract.rs`）。

`RefreshStrategy::BranchUnionAggregate` 在此模型里**不存在**——它等价于 `root_fragment = { identity: BranchScoped(GroupRowId), state: AggregateState, .. }`。

## 7. 逐算子推导规则（核心）

下表是"从 MV SQL 推导 refresh 信息"的全部规则。每个节点从孩子 synthesize：

| 算子 | identity | state | produces_signed_delta | delete_handling | obligations / 备注 |
| --- | --- | --- | --- | --- | --- |
| `Scan(base)` | `BaseRowId` | `Stateless` | true（来自 change stream） | base 决定：`AppendOnly` 或 `NeedsLocator` | `+RowLineageRequired(base)`, `+SnapshotWindowRequired(base)`；`base_refs+=base` |
| `Filter(c)` / `Project(c)` | `identity(c)` | `state(c)` | `=c` | `=c` | 透传；Project 必须保留 identity 列 |
| `Aggregate(c, keys, fns)` | `GroupRowId(keys)` | `AggregateState(layout, per-fn roles)` | true（signed state） | per-fn（见 §8） | 要求 `c.produces_signed_delta`；per-fn proof 失败 → CREATE fail-fast；`+TargetStateLayoutRequired` |
| `Join(l, r)`（inner/cross） | `JoinRowKey(identity(l), identity(r))` | `compose(state(l), state(r))`（通常 `Stateless×Stateless`） | `l ∧ r` | 合并 | delta 积 `dA⋈B' ∪ A⋈dB`；`base_refs=l∪r`；outer join fail-fast |
| `UnionAll(c0..cn)`（同构） | `BranchScoped(identity(c0))` | `state(c0)` | `∧ ci` | 取最严 ci | 要求各 ci 的 `(identity ctor, state ctor)` 相同（同构判定，见 §11）；`branch_scope` inherited 下传；`+TargetApplyKeyRequired(__branch_id__)`；异构 fail-fast |

**关键：`UnionAll` 的 `BranchScoped` 与孩子复杂度无关。** `Union(Agg(Join), Agg(Union))` 两个孩子各自递归出 `GroupRowId`（同构）→ 顶层 `BranchScoped(GroupRowId)`。这就是承接 spec §2 "分支内嵌 join fail-fast" 被本框架解锁的机制：分支 core 由现有 join-delta/aggregate-state 递归分解，branch-tag 正交叠加。

`branch_scope` 是 **inherited 属性**：`UnionAll` 给第 i 个分支的 marker 打 `branch_scope=Some(i)`，join-delta 透传它，aggregate-state 消费它（传给 `build_aggregate_state_merge`）。

## 8. Aggregate 的 per-function 可增量性证明

`Aggregate` 不能笼统认为可增量。`StateContract::AggregateState` 的 `per_function_roles` 必须逐函数给出证明，否则 CREATE fail-fast：

| function | state | delete 行为 |
| --- | --- | --- |
| `COUNT` / `SUM` | signed accumulation（`RetractionCount` role） | 来自孩子的 signed delta，`NeedsLocator` |
| `AVG` | 分解为 `SUM` + `COUNT` 两个 state | 同上 |
| `MIN` / `MAX` | detail state 或重算证明 | 孩子可 delete 时 ⇒ `NeedsFullRecompute(affected groups)` |
| `COUNT(DISTINCT)` / `APPROX` | sketch / HLL state（承接 `2026-05-26` spec） | 特殊 state layout |
| 其它 | 无证明 | **CREATE fail-fast，给出函数名** |

白名单当前由 `AggregateFunctionKind`（`mv_shape.rs:124`）+ `signed_state_function`（`aggregate_rewrite.rs`）表达；本框架把它显式化为 `FragmentProperty` 的一部分。

## 9. 契约即 rewrite 副产物（机制）

不引入独立的 `compile(plan) -> Contract` 第二次遍历。**每条 rewrite rule 在 fire 时把自己那块 fragment 写进 `ImvPlanAnnotation`**：

- `BindIcebergScanRule` → `BaseRowId` + base obligations。
- `RewriteAggregateStateRule` → `GroupRowId` + `AggregateState` + per-fn roles。
- `RewriteJoinAggregateDeltaRule` → `JoinRowKey`。
- `RewriteBranchUnionRule` → `BranchScoped`（并把 `branch_scope` 作为 inherited 属性下传，见 §7）。

rule 此刻**本就知道**自己产出的身份（已在 `allocate_column_id()`、已在决定 `branch_scope`），顺手记进 annotation 即可。plan 与 contract 同源、不可能 drift。这正是 `ImvPlanAnnotation` doc 注释预告的用途。

**CREATE vs REFRESH 是同一推导的两个模式**（inherited）：
- CREATE：`schema_binding` **产出** target schema（分配 field id、建 state/apply-key 列），落盘 `RefreshContract`。无法归约到封闭 capability ⇒ CREATE fail-fast。
- REFRESH：加载落盘 `RefreshContract`（**权威**）→ 校验当前 base/target 运行时 obligation（schema 还匹配？row-lineage 还在？snapshot window 有效？）→ 重跑 rewrite **产出可执行 plan**（绑定当前 snapshot）→ assert 派生结构与落盘一致（廉价 bug tripwire，**非** re-derive 对账）→ 执行。

## 10. 层职责与边界

| 映射 | 性质 | 归属 |
| --- | --- | --- |
| SQL → capability | 开放（任意 SQL 结构），需要递归归约（marker 下推、fixpoint） | **rewrite/分析层**（有 RBO 底座） |
| capability → execution | 封闭且小（identity 4 ctor、state 2、apply 1、snapshot 3） | **refresh driver**（数据驱动执行） |

当前 bug 是 refresh 层（`RefreshStrategy`/`IncrementalMvShape`）也在做第一行那件开放的事，却用了比 RBO 弱的扁平分类器。修法**不是给 refresh 塞一个 RBO**，而是让它停止推导结构、只消费封闭 capability。**refresh 的可扩展性因此是"继承"来的**：rewrite 层加一条 rule 把新 SQL 形状映射到既有 capability 词汇表，refresh 零改动即可执行（只要组合产物落在封闭集内）。

## 11. 关键不变量

1. **构造子封闭**：每条 rewrite rule 的产出必须能用既有 capability 构造子表达；否则即是引入新底层能力（见 §13）。`branch ∘ branch = branch`（flatten）即封闭性体现。守住它 ⇒ refresh driver 永久稳定。
2. **同构定义在 property 上**：UNION ALL 同构 = 各分支 `(identity ctor, state ctor)` 相同，**非** SQL shape 相同。`Union(Agg(scan), Agg(join))` 同构、支持。
3. **branch-scope 是安全默认**：UNION ALL 默认 `BranchScoped`（一般无法证明跨分支身份不相交）；省掉 branch-scope 是需要不相交证明的优化。
4. **typing 不是 cost**：property 是确定性归约，成功或 fail-fast，**不进 CBO memo**。同一语义的多种合法增量策略（如 MIN/MAX 的 detail vs 重算）走 canonical 确定选择；cost 选择是未来叠加层。
5. **落盘 contract 是权威**：refresh 不 re-derive 对账（否则引擎升级会让旧 MV 失败——可扩展性与对账自相矛盾）。

## 12. 已知难点（各 Phase plan 必须正面处理）

1. **Synthesized → inherited 回边**：`Aggregate` 在孩子可 delete 时 synthesize 出 `delete_handling=NeedsFullRecompute`，这要求**孩子的 scan 扩大读取范围**（回读受影响 partition，而非只读 delta）→ 改变 `SnapshotWindowRequired`。即父节点合成的属性要作为 inherited 要求**回注**descendant。两遍（synth 上 / inherit 下）**有数据依赖、非独立**，需明确 pass 顺序或 fixpoint。
2. **`JoinRowKey` 物理可表示性**：`JoinRowKey(inner, inner)` 嵌套（join-of-join、join-of-aggregate）要对组合封闭，且要落到 apply-key 封闭物理词汇表——当前 `ApplyKeyValueType` 只有 `Int64/Utf8/Branch*`，无任意 arity 复合 key。"join of aggregates" 点亮时卡点不只是缺 rule，还有 `JoinRowKey(GroupRowId, GroupRowId)` 能否被 apply 层表示（扩 `ApplyKeyValueType` 或 Utf8 复合编码）。
3. **nested aggregation**（`Agg(Agg)`）/ agg over union-of-aggs（group key 重叠）有真 IVM 难度——当前 fail-fast；点亮需专门 rule + 证明。
4. **snapshot policy 结构 vs 运行时拆分**：结构约束（single/all-bases/join-pair）可 synth 成半格；但 `JoinPairPartialInitialSkip` 含**运行时**判定（两侧 snapshot 相对状态决定是否跳过 initial），不是结构属性 → 留 refresh 期 root-finalize-with-runtime-context，不进 CREATE 期 synthesized property。
5. **obligation 强制阶段**：每条 obligation 标 CREATE / REFRESH-admission / REFRESH-execute，在单一 admission gate 一次性校验，避免 pipeline 深处晚爆、报错难懂。

## 13. 何时才真正需要新增底层能力（vs 仅新 SQL 形状）

迁移完成后，普通 SQL 形状不该再触发任何枚举扩展。只有以下四类是真正的底层能力扩展点：

1. **新 target identity**——超出 `base_row_id / join_row_key / group_row_id` 的定位方式，或 branch 之外的第二正交身份维 → 新 `TargetIdentity` ctor + merge-sink locator + 可能扩 `ApplyKeyValueType`。
2. **新 state layout**——超出当前 agg-state VARBINARY 编码（如 HLL/sketch、窗口 state）→ 新 `per_function_roles` / 新 layout 版本。
3. **新 commit/apply 语义**——如 partial-partition overwrite、COW vs MOR → 让今天退化的 `commit_contract`/`apply_contract` 长出真实变体。
4. **新 snapshot 协调**——如多表一致性快照、time-travel refresh → `snapshot_policy` 扩展。

而 plan rewrite 层的诚实边界：组合**基座完整**（marker 协议、共享 state-merge helper、scan-binding、通用列传播），但 **rule 目录仍 per-structural-form**。`UNION-of-joins`、`join-of-aggregates`、outer-join delta 等真·新结构形态仍需 **+1 条 rewrite rule + 放宽对应 `is_supported_*` 白名单**——但这是 +1 条局部、可组合、fail-fast 的 rule，**不是** +1 个 shape 枚举 / 新 marker / 新 apply-key 机制。

## 14. 与现有 spec / 代码的关系

- **承接而非推翻 `2026-06-02` spec**：那份 spec 在用户决策下务实地用 shape 分类 + dispatch 落地三种 union，并建好全部底层原语（`__branch_id__`、复合 apply-key、`BranchScope`、composite contract、apply 侧 branch 定位）。本框架在原语齐备后，把"shape 枚举 + dispatch"这层债换成 property 合成。承接 spec 的 §5（branch_id 原理）、§7.5（apply 侧）继续有效。
- **`RefreshStrategy` 退场**：迁移后它要么删除，要么降为从 capability 派生的 EXPLAIN/telemetry label。`stored_refresh_strategy_for_plan` 反推 + `stored_strategy_matches_legacy_shape` 对账删除（它们只为调和冗余表示而存在）。
- **`IncrementalMvShape` 仅 Iceberg 路径退场**：Iceberg 路径删除其 layout 派生 + 交叉校验（layout 直接读 `AggregateStateContract`）。StarRocks 非 Iceberg 路径保留，**显式划界**，不被本框架触及。
- **`ImvPlanAnnotation` 填空**：本框架是它 doc 注释预告字段的兑现。

## 15. 迁移阶段（设计层；各自有独立 plan）

### Phase 1 — 组合式 `RewriteBranchUnion`（rewrite 层）
**已有可执行 plan**：[2026-06-04-imv-property-framework.md](../plans/2026-06-04-imv-property-framework.md)。
给 `ImvDeltaNode` 加 inherited `branch_scope`；`RewriteBranchUnion` 改 tag-and-delegate（给分支 core 打 `Delta{root, branch_scope:Some(i)}` + 追加 `__branch_id__`，分解交给现有规则）；aggregate-state 消费、join/union-delta 透传 `branch_scope`。
**交付**：当前已支持的 branch-union MV 产出**逐字节相同**的最终 plan；`Union(Agg(Join))` / `Union(Agg(Union))` 从孤儿 marker 变为可解析 plan。无契约/driver 改动。
**边界**：仅 rewrite 层；新形态的 end-to-end *refresh* 由 Phase 2/3 启用。

### Phase 2 — `RefreshFragmentProperty` 填进 annotation + root finalize + 持久化
**待展开独立 plan**（`<date>-imv-refresh-property-derivation.md`）。
定义 §6 类型；rule fire 时写 fragment；root finalize 产 `RefreshContract`；落盘并替换 DDL 期 `derive_imv_refresh_contract` 分类器 + Iceberg 路径 `IncrementalMvShape` 交叉校验。per-function aggregate proof（§8）。`RefreshStrategy` 暂保留为可派生 label，对账降为 bug tripwire。
**依赖**：Phase 1。**难点**：§12.1（synth→inherited 回边）、§12.2（`JoinRowKey` 表示）。

### Phase 3 — refresh driver capability 化 + 退枚举
**待展开独立 plan**（`<date>-imv-refresh-capability-driver.md`，落地前需读 `iceberg_refresh.rs` 对应片段）。
两处 `match strategy` 改 capability 分支；6 wrapper 收敛到 ~3 条 snapshot-policy-keyed 路径；`refresh_branch_union_aggregate_*` 并入 fan-in；first-refresh 的 `__branch_id__` 注入上移到投影层（删 `append_branch_id_to_first_refresh_chunks`）；execute/plan 两路扇出去重；删 `RefreshStrategy` + Iceberg 路径 `IncrementalMvShape` + 对账。
**依赖**：Phase 2。**风险**：snapshot 编排按 source policy 真不同（目标 ~3 路非 1）；commit/state 已统一、应无需 per-capability 分支。

## 16. 错误处理 / fail-fast（无 silent fallback）

- 无法归约到封闭 capability 构造子的 SQL → CREATE 期 fail-fast，报精确原因（哪个节点/函数）。
- per-function 无可增量证明（MIN/MAX-on-delete 无 detail state、非白名单函数）→ CREATE fail-fast。
- 异构 UNION ALL 分支、outer join、nested aggregation、非 ALL set op → CREATE fail-fast（沿用承接 spec 边界）。
- REFRESH-admission 期运行时 obligation 不满足（row-lineage 缺失、snapshot window 无效、target schema drift）→ 执行前一次性报错。
- 派生结构与落盘 contract 不一致（bug tripwire）→ fail-fast。

## 17. 测试策略（按 capability 轴，非按 shape）

测**轴的笛卡尔积**，不测命名 shape：

- **identity × state 组合**：`base_row_id`+stateless（projection/filter）、`join_row_key`+stateless（join projection）、`group_row_id`+agg-state（single）、fan-in、join-aggregate、**`branch_scoped(group_row_id)`+agg-state**（branch union——证明 branch = 身份+1）。
- **组合等价**：`Union(Agg(Join), Agg(scan))`（同构 on property，Phase 1 点亮的代表 case）与对应 fan-in 在 kernel/commit/state 上的行为一致（modulo branch_id 列）。
- **plan-golden**（`optimizer` 套件）：branch union 产 `Union(Project(AggregateStateMerge with branch_scope=Some))`、single 产 `branch_scope=None`——证明一个 helper、两种参数化。
- **capability round-trip**（单测）：每个 contract → capability 元组 → 唯一 driver 路径，无 strategy 枚举。
- **CREATE fail-fast**：MIN/MAX-on-delete、非白名单函数、异构 branch、outer join、nested agg。
- **增量正确性矩阵**：每个 identity 轴 × {insert-only, delete（需 locator）, mixed} × {no-state, agg-state, branch}。
- **end-to-end**（`iceberg-rest` 套件，Phase 3）：上述每个 identity×state 组合的真实 refresh，含 `UNION ALL` of aggregate-over-join 的端到端执行（Phase 1 的 plan-shape，此时被执行）。

golden 记录用 `--mode record --record-from target`（NovaRocks-only ref，见项目约定）。

## 18. 风险与缓解

| 风险 | 缓解 |
| --- | --- |
| Phase 1 重构改变 `RewriteBranchUnion` 中间产物，连带改 rule 级单测 | 回归护栏移到 **pipeline 级**特征测试（锁最终 plan 逐字节不变）；rule 级单测改断言中间 `ImvDelta` 形状 |
| synth→inherited 回边（§12.1）被漏 | Phase 2 plan 显式设计 pass 顺序/fixpoint；MIN/MAX-on-delete 专门 case |
| `JoinRowKey` 物理不可表示（§12.2） | "join of aggregates" 点亮前先扩 `ApplyKeyValueType` / 定 Utf8 复合编码；在那之前 fail-fast |
| 落盘 contract 与升级后引擎派生不一致 | contract 为权威；不 re-derive 对账；引擎升级 → recompile（无 compat） |
| 范围大跨多子系统 | 三 Phase 各自独立可测、独立 plan；每 Phase 不破坏已支持形态 |
| 误把 typing 做进 CBO | §11.4 不变量：property 确定性、不进 memo |

## 19. 验收标准（按 Phase）

- **Phase 1**：branch-union 组合化；`Union(Agg(scan))` 最终 plan 逐字节不变；`Union(Agg(Join))`/`Union(Agg(Union))` 可解析；`imv` rewrite 全量不回归。
- **Phase 2**：`ImvPlanAnnotation` 产出 `RefreshContract` 并落盘；CREATE 期对不可增量 SQL 精确 fail-fast；per-function proof 生效；与旧 `RefreshStrategy` 对账过渡期一致。
- **Phase 3**：refresh driver 无 `match strategy`、capability 驱动；`RefreshStrategy` + Iceberg 路径 `IncrementalMvShape` 删除；`iceberg-rest` 全量不回归 + 新形态 end-to-end 全绿；capability round-trip 单测绿。
- **全局**：未来新增"已被既有 rule 覆盖的 SQL 组合"无需新增 shape/strategy 枚举、无需改 refresh driver。

## 20. 后续计划入口

1. Phase 1 plan 已就绪：[../plans/2026-06-04-imv-property-framework.md](../plans/2026-06-04-imv-property-framework.md)。
2. Phase 2 / 3 在 Phase 1 落地后各自走 writing-plans skill 展开为独立 plan，引用本 spec 的类型定义（§6）、不变量（§11）、难点（§12）作为参照。
3. 完成后更新 `NovaRocks Roadmap.md` 中 Iceberg v3 Incremental MV 相关任务状态，并把 `RefreshStrategy` / `IncrementalMvShape`（Iceberg 路径）标记为已退场。
