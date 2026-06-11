# OQ-8 · Distribution-aware physical search design

- 日期: 2026-06-04
- 对应 roadmap: `OQ-8 · Distribution-aware physical search`
- 依赖: `OQ-12 · Stats and NDV robustness` 已合入 `main`（`#245`）
- 状态: Spec - pending implementation plan
- StarRocks 参考: 本地 `/Users/harbor/project/starrocks`, FE optimizer property/cost/search 语义

## 1. 一句话

把 NovaRocks 的 hash join distribution 选择从 `PhysicalHashJoinOp.distribution`
枚举改为 StarRocks-style 的 property-driven 选择：同一个 physical hash join
通过多套 child required properties 枚举 broadcast 和 shuffle 形态, search 在看到
children 的真实 output properties 后计算成本、复用已有 distribution、必要时插入 exchange。

目标不是追求 StarRocks 的 colocate/bucket 全量模型, 而是迁移它解决 OQ-8 问题的核心结构:

- required-property alternatives 负责枚举 broadcast/shuffle 候选;
- child-output guarantor 负责复用已有 property 或补 enforcer;
- cost model 以 children output properties 作为输入区分 broadcast/shuffle 执行代价;
- extracted plan 携带最终 join execution distribution 给 explain、runtime filter 和 fragment builder。

## 2. Brainstorming 决策

| 决策点 | 结论 |
|---|---|
| 方案选择 | 采用方案 3: property-driven hash join, 而不是继续让 `JoinDistribution` 承担搜索候选 |
| StarRocks 可迁移部分 | required alternatives、child output aware cost、broadcast gate、parent required key alignment |
| StarRocks 暂不迁移部分 | `LOCAL` / `BUCKET` / range colocate / tablet colocate / skew join |
| 兼容策略 | 分阶段弱化 `JoinDistribution`: 先变成 extracted execution metadata, 再从 optimizer operator 中移除 |
| 验收目标 | `tpc-h/q9/q20` 与 `tpc-ds/q4/q64/q85` 从系统性 broadcast 转向 partitioned/shuffle, 且不破坏 SSB baseline |

## 3. 当前 NovaRocks 基线

当前 optimizer 已具备以下基础:

- `DistributionSpec::{Any, Gather, HashPartitioned { cols, source }}`。
- `HashSource::{ShuffleAgg, ShuffleJoin}` 及 source-aware satisfies 规则。
- `derive/hash_join.rs` 能对 broadcast/colocate 做 left-preserving output 派生, shuffle join
  能产出 `ShuffleJoin` hash property。
- `search.rs` 已是 top-down `(group, required_props)` winner cache, 并能在 provided 不满足 required 时插
  `PhysicalDistribution` / `PhysicalSort` enforcer。
- `cost.rs` 已有粗粒度 broadcast/shuffle cost, 但成本只看 operator enum, 不看 children output properties。
- OQ-12 已把 row-count / NDV / confidence 作为 CBO 输入接入, `Statistics` 有
  `row_count_confidence`。

主要缺口:

- `JoinToHashJoin` 直接生成两个 physical expr:
  `PhysicalHashJoin(distribution=Shuffle)` 和 `PhysicalHashJoin(distribution=Broadcast)`。
- `derive_required()` 只能返回一套 child requirements, 不能表达 StarRocks 的
  broadcast/shuffle alternatives。
- `Winner` 没有记录选中的 child requirements, `extract_best()` 会重新 derive, 因此无法支持同一个
  expr 的多套 alternatives。
- `compute_cost()` 不接收 children output properties, 无法判断是否真的复用了已有 hash distribution。
- `fragment_builder`、`runtime_filter_pass`、`explain` 读取 `PhysicalHashJoinOp.distribution`,
  这把 optimizer decision 和 execution metadata 耦合在一个字段里。
- `DistributionSpec` 没有 `Broadcast`, 当前 broadcast join 用 right child `Gather` 近似, 语义不够精确。

## 4. StarRocks 参考结论

本设计参考 StarRocks FE 的以下结构, 但不逐行复制:

- `HashJoinImplementationRule` 只生成一个 `PhysicalHashJoinOperator`, 不提前拆 broadcast/shuffle。
- `RequiredPropertyDeriver.visitPhysicalHashJoin` 产生两套 child properties:
  - broadcast: left `EMPTY`, right `BROADCAST`;
  - shuffle: left/right `SHUFFLE_JOIN(keys)`;
  - only-broadcast / only-shuffle join 通过 helper gate 收窄候选。
- `PropertyDeriverBase.computeShuffleJoinRequiredProperties` 会根据 parent required shuffle key
  调整 child shuffle key 顺序, 让下游 hash property 有机会复用。
- `ChildOutputPropertyGuarantor` 在看到 children output properties 后判断是否复用 colocate/shuffle,
  不满足时才插 enforcer。
- `CostModel.calculateCostWithChildrenOutProperty` 把 children output properties 传给 cost model。
- `HashJoinCostModel` 通过 right child distribution 判断 broadcast vs shuffle:
  - broadcast memory 按 BE 数放大;
  - shuffle build cost 按 parallel factor 摊薄;
  - broadcast probe penalty 高于 shuffle。
- `EnforceAndCostTask.checkBroadcastRowCountLimit` 对 broadcast 做 hard gate, 但只在 build side
  不显著小于 probe side 时应用 row-count limit, 避免过度强制 shuffle 大 probe side。

迁移原则:

- 迁移 property-driven search shape。
- 迁移 child-output-aware cost。
- 迁移 conservative broadcast gate。
- 不迁移 StarRocks FE 专属的 colocate table index、bucket shuffle、range colocate、skew split。

## 5. 设计目标

1. Broadcast / shuffle 不再是两个 hash join physical operators。
2. 同一个 hash join expression 可枚举多套 child required property alternatives。
3. Search 成本必须看到 children chosen output properties。
4. 如果 child 已满足 hash distribution, 不插重复 exchange。
5. Broadcast 必须用 row/byte/confidence gate 限制高内存 plan。
6. Extracted physical plan 必须保留最终 join execution distribution, 供 EXPLAIN、runtime filter、
   fragment builder 使用。
7. 阶段性实现必须保持现有 SQL suite 可编译、可运行、可回滚。

## 6. 非目标

- 不做 multi-BE fragment splitting 架构改造。
- 不实现 StarRocks colocate/bucket join。
- 不为 TPC query 特判 join strategy。
- 不追求 FE exchange 数量逐条完全一致。
- 不把 join reorder DP 直接扩展成完整 physical distribution DP; 第一版仍让 Cascades search 负责
  physical distribution selection。

## 7. 核心数据模型

### 7.1 `DistributionSpec::Broadcast`

新增:

```rust
pub(crate) enum DistributionSpec {
    Any,
    Gather,
    Broadcast,
    HashPartitioned {
        cols: Vec<ColumnId>,
        source: HashSource,
    },
}
```

satisfies 规则:

| Provided | Required | Satisfy |
|---|---|---|
| any property | `Any` | yes |
| `Broadcast` | `Broadcast` | yes |
| `Broadcast` | `Gather` | no |
| `Broadcast` | hash | no |
| `Gather` | `Broadcast` | no |
| hash | `Broadcast` | no |

说明:

- `Broadcast` 表示 replicated child output, 对应 StarRocks `ReplicatedDistributionSpec`。
- 它不能伪装成 `Gather`; gather 是单节点/单接收端, broadcast 是多接收端 replicated。
- `PhysicalDistribution(Broadcast)` 在 fragment builder 中生成 broadcast edge。

### 7.2 Child requirement alternatives

新增类型:

```rust
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) enum PropertyAlternativeKind {
    Default,
    BroadcastJoin,
    ShuffleJoin,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct ChildRequirementAlternative {
    pub kind: PropertyAlternativeKind,
    pub child_props: Vec<PhysicalPropertySet>,
}
```

新增 dispatcher:

```rust
pub(crate) fn derive_required_alternatives(
    op: &Operator,
    parent_required: &PhysicalPropertySet,
    num_children: usize,
) -> Vec<ChildRequirementAlternative>;
```

兼容策略:

- 普通 operator 的 `derive_required_alternatives` 包一层旧 `derive_required()` 结果, kind=`Default`。
- Hash join 专门返回 broadcast/shuffle alternatives。
- 旧 `derive_required()` 可以短期保留给现有代码和单测, 但 search/extract 迁到 alternatives。

### 7.3 Hash join operator 与 execution metadata 分离

目标形态:

```rust
pub(crate) struct PhysicalHashJoinOp {
    pub join_type: JoinKind,
    pub eq_conditions: Vec<PhysicalHashJoinEqCondition>,
    pub other_condition: Option<TypedExpr>,
}

pub(crate) enum JoinExecutionDistribution {
    Broadcast,
    Partitioned,
    Colocate,
}
```

第一阶段为了减少 churn, 可以暂时保留 `JoinDistribution` enum, 但职责变更:

- 不再由 `JoinToHashJoin` 生成两个 operator 作为候选;
- 不再参与 `derive_required()` 或 `compute_cost()` 的候选选择;
- 只在 extract/codegen 过渡期作为 final execution metadata 使用;
- 最终迁移到 `PhysicalPlanNode` 的 execution metadata, 再删除 operator 字段。

建议在 `PhysicalPlanNode` 加轻量 metadata:

```rust
pub(crate) struct PlanExecutionProps {
    pub output_property: PhysicalPropertySet,
    pub child_output_properties: Vec<PhysicalPropertySet>,
    pub join_distribution: Option<JoinExecutionDistribution>,
}
```

如果改动面要更小, 第一阶段也可以只给 hash join node 存 `join_distribution`, 但长期应保留
`output_property` 以支持 EXPLAIN/debug。

## 8. Hash join alternatives

### 8.1 Candidate generation

`JoinToHashJoin` 改为只生成一个 hash join:

```text
LogicalJoin(equi) -> PhysicalHashJoin
```

不再生成:

```text
PhysicalHashJoin(distribution=Shuffle)
PhysicalHashJoin(distribution=Broadcast)
```

### 8.2 Required alternatives

对 hash join:

```text
BroadcastJoin:
  left  = Any
  right = Broadcast

ShuffleJoin:
  left  = ShuffleJoin(left_keys)
  right = ShuffleJoin(right_keys)
```

only-broadcast:

- cross join;
- inner join with no equi key;
- null-aware left anti;
- explicit broadcast hint when hint support 接入 standalone optimizer 后。

only-shuffle:

- right outer / right semi / right anti / full outer, 第一版保守处理;
- explicit shuffle/bucket/skew hint when hint support 接入 standalone optimizer 后。

说明:

- 当前 NovaRocks no-eq join 已走 `PhysicalNestLoopJoin`; hash join alternatives 只处理 equi join。
- right/full outer 是否允许 broadcast 可以后续细化; OQ-8 第一版以安全和 plan-risk 消除为先。

### 8.3 Parent-required key alignment

如果 parent required 是 `ShuffleJoin(keys)` 且这些 keys 与 hash join left/right keys 是同一集合,
则按 parent key 顺序重排 left/right child requirement。

示例:

```text
join keys:       left(a, b) = right(x, y)
parent requires: ShuffleJoin([b, a])
child reqs:      left ShuffleJoin([b, a]), right ShuffleJoin([y, x])
```

如果 parent required 无法映射到 join key 集合, 使用 join 原始 key 顺序。

收益:

- 下游 join/aggregate/window 更容易复用当前 join 的 partitioned output。
- 与 StarRocks `computeShuffleJoinRequiredProperties` 的核心语义一致。

## 9. Search algorithm

当前 search loop:

```text
for physical expr:
  child_reqs = derive_required(expr, required)
  optimize children once
  derive output
  bridge output -> required
```

改为:

```text
for physical expr:
  alternatives = derive_required_alternatives(expr, required)
  for alt in alternatives:
    apply hard gate before optimizing children if possible
    optimize each child with alt.child_props[i]
    collect child winner output properties
    optionally normalize/enforce child outputs for join compatibility
    own_cost = compute_cost_with_child_outputs(expr, own_stats, child_stats, child_outputs, alt.kind)
    provided = derive_output_for_alternative(expr, required, child_outputs, alt.kind)
    bridge provided -> required if needed
    update best winner
```

`Winner` 新增:

```rust
pub(crate) struct Winner {
    pub(crate) expr_index: usize,
    pub(crate) cost: Cost,
    pub(crate) enforcer: Option<EnforcerInfo>,
    pub(crate) output: PhysicalPropertySet,
    pub(crate) alt_kind: PropertyAlternativeKind,
    pub(crate) child_props: Vec<PhysicalPropertySet>,
    pub(crate) child_outputs: Vec<PhysicalPropertySet>,
}
```

关键约束:

- `extract_best()` 必须使用 `winner.child_props`, 禁止重新 derive child props。
- child output compatibility check 必须在 search 中完成, 否则 cost 与 extraction 可能漂移。
- `in_progress` cache key 仍然是 `(group, required_props)`, alternative 是 group 内局部枚举, 不进 cache key。

## 10. Child output guarantor, NovaRocks 版

NovaRocks 第一版不需要 StarRocks 的完整 `ChildOutputPropertyGuarantor`, 但需要一个局部函数:

```rust
fn guarantee_child_outputs(
    op: &Operator,
    alt: &ChildRequirementAlternative,
    child_outputs: &[PhysicalPropertySet],
) -> GuaranteedChildren
```

职责:

- BroadcastJoin:
  - right child output 必须 satisfy `Broadcast`;
  - left child 可为 `Any`/hash/gather;
  - 不额外插 hash exchange。
- ShuffleJoin:
  - 左右 child 必须 satisfy 对应 `ShuffleJoin(keys)`;
  - 如果 child output 已满足, 不插 exchange;
  - 如果不满足, search 之前的 child required 已会选择 enforcer winner, 因此这里通常只做断言和 metadata 整理;
  - 后续可扩展为 StarRocks 风格的“根据左侧已有 hash key 调整右侧 key”。

第一版不做:

- local/bucket/colocate fast path;
- range distribution normalization;
- set-op colocate。

## 11. Cost model

### 11.1 Cost API

把 `compute_cost` 改成:

```rust
pub(crate) fn compute_cost(
    op: &Operator,
    own_stats: &Statistics,
    child_stats: &[&Statistics],
    child_outputs: &[&PhysicalPropertySet],
    alt_kind: PropertyAlternativeKind,
    cost_options: &CostOptions,
) -> Cost;
```

`CostOptions` 第一版可内置默认值, 后续接 session/config:

```rust
pub(crate) struct CostOptions {
    pub backend_factor: f64,
    pub broadcast_row_limit: f64,
    pub broadcast_byte_limit: f64,
    pub broadcast_right_table_scale_factor: f64,
    pub fallback_broadcast_row_limit: f64,
    pub network_cost: f64,
    pub memory_cost_weight: f64,
}
```

### 11.2 Hash join exec mode

从 child output properties 推导:

```text
BroadcastJoin:
  right output Broadcast -> Broadcast

ShuffleJoin:
  left/right output HashPartitioned(ShuffleJoin) satisfying required keys -> Partitioned
```

不再从 `PhysicalHashJoinOp.distribution` 推导。

### 11.3 Broadcast cost

Broadcast candidate cost 包含:

```text
exchange_cpu     = build_bytes * backend_factor
exchange_memory  = build_bytes * backend_factor
exchange_network = max(build_bytes * backend_factor, 1)
join_build_cpu   = build_bytes
join_probe_cpu   = probe_bytes * broadcast_probe_penalty(build_rows, key_width)
join_memory      = build_bytes * backend_factor
```

第一版可以简化为:

```text
broadcast_cost =
  probe_bytes
  + build_bytes * network_cost * backend_factor
  + build_bytes * memory_cost_weight * backend_factor
```

但接口要保留拆分余地。

### 11.4 Shuffle cost

Shuffle candidate cost:

```text
exchange cost only appears if child required winner inserted PhysicalDistribution
join_build_cpu = build_bytes / parallel_factor
join_probe_cpu = probe_bytes * shuffle_probe_penalty(build_rows, key_width)
join_memory    = build_bytes
```

第一版可用:

```text
shuffle_cost =
  probe_bytes
  + build_bytes / backend_factor
```

真实 shuffle exchange 成本由 child enforcer cost 计入, 因为 child required properties 会导致 child winner
带 `PhysicalDistribution(HashPartitioned)`。这样自然支持“已满足 hash distribution 时不重复收费”。

### 11.5 Broadcast hard gate

参考 StarRocks 但加入 OQ-12 confidence:

```text
reject broadcast if:
  broadcast disabled
  OR build_rows > row_limit AND build is not obviously tiny relative to probe
  OR build_bytes > byte_limit
  OR row_count_confidence == Fallback AND build_rows > fallback_row_limit
```

“build is not obviously tiny relative to probe”:

```text
probe_bytes < build_bytes * backend_factor * broadcast_right_table_scale_factor
```

含义与 StarRocks 一致: 如果 build 真的远小于 probe, 不要因为 row limit 盲目把巨大 probe 两边 shuffle。

初始默认建议:

```text
backend_factor = 3.0 for local standalone conservative default
broadcast_row_limit = 15_000_000.0, aligned with current StarRocks default
broadcast_right_table_scale_factor = 10.0
fallback_broadcast_row_limit = 500_000.0
byte_limit = 512 MiB equivalent, if avg row size available
```

如果不想引入配置, 先放在 `cost.rs`/`search.rs` 常量并在 spec/test 中记录。

## 12. Output property derivation

### 12.1 BroadcastJoin output

Broadcast join output follows preserved/probe side when semantically safe:

| Join type | Output distribution |
|---|---|
| Inner | left child distribution, with eq-equivalence enrichment |
| LeftOuter | left child distribution, with conservative null handling |
| LeftSemi / LeftAnti | left child distribution |
| Cross | left child distribution |
| RightOuter / RightSemi / RightAnti / FullOuter | first version returns `Any` unless only-shuffle path handles it |

说明:

- Broadcast build side does not repartition probe rows.
- Preserved left rows keep left child distribution.
- Right/full-family joins are more subtle because preserved side is not the left probe side; first version chooses correctness over reuse.

### 12.2 ShuffleJoin output

If parent required is hash and can map to join eq keys:

```text
output = parent-compatible ShuffleJoin(mapped/dominated keys)
```

Otherwise:

```text
output = ShuffleJoin(left join keys enriched with right eq equivalents)
```

Join type handling:

- inner/left-family: dominated output keys from left required keys;
- right-family: dominated output keys from right required keys;
- full outer: return null-relaxed or `Any` in first version if null semantics are unclear.

NovaRocks currently has no `DistributionCol.nullStrict`; first version should avoid overclaiming full outer hash property.

## 13. Extract / codegen / runtime filter

### 13.1 Extract

`extract_best()` changes:

- lookup winner by `(group, required)`;
- use `winner.child_props[i]` for recursive extraction;
- attach `PlanExecutionProps`:
  - `output_property = winner.output`;
  - `child_output_properties = winner.child_outputs`;
  - `join_distribution = derive_join_execution_distribution(winner.alt_kind, child_outputs)`;
- wrap top enforcer exactly as today when `winner.enforcer` exists.

### 13.2 EXPLAIN

EXPLAIN hash join label should come from extracted execution metadata:

```text
HASH JOIN (type=INNER, dist=BROADCAST)
HASH JOIN (type=INNER, dist=PARTITIONED)
```

Optional COSTS/ANALYZE signal:

```text
props={out=ShuffleJoin([c1,c2]) children=[Any,Broadcast]}
```

Plain VERBOSE should stay compact unless golden updates intentionally include property output.

### 13.3 Fragment builder

`fragment_builder` must not read `PhysicalHashJoinOp.distribution` for final strategy.

First version:

- `JoinExecutionDistribution::Broadcast` -> thrift join distribution broadcast, RF layout broadcast/local.
- `JoinExecutionDistribution::Partitioned` -> partitioned/shuffle join, RF layout remote-capable.
- `PhysicalDistribution(Broadcast)` creates broadcast fragment edge.
- `PhysicalDistribution(HashPartitioned)` creates hash edge as today.
- `PhysicalDistribution(Gather)` stays gather.

### 13.4 Runtime filter pass

`runtime_filter_pass` should use extracted join execution distribution from `PhysicalPlanNode`,
not `PhysicalHashJoinOp.distribution`.

Until metadata is threaded through every path, a temporary helper can infer:

```text
if plan node join_distribution exists -> use it
else if right child top op is PhysicalDistribution(Broadcast) -> Broadcast
else if child_output_properties are shuffle -> Shuffle
else fallback to old enum during transition
```

The fallback must be removed in the final phase.

## 14. Implementation phases

### P1 - Property model and metadata scaffolding

- Add `DistributionSpec::Broadcast`.
- Add `PropertyAlternativeKind` and `ChildRequirementAlternative`.
- Add `derive_required_alternatives` dispatcher.
- Add `Winner.alt_kind`, `Winner.child_props`, `Winner.child_outputs`.
- Add extracted plan execution metadata or a transitional join distribution field.
- Keep old behavior via compatibility wrappers.

Validation:

- Rust tests for `Broadcast` satisfies.
- Rust tests that normal operators produce exactly one default alternative.
- Rust tests that extract uses `winner.child_props`.

### P2 - Hash join alternatives

- `JoinToHashJoin` emits one `PhysicalHashJoin`.
- Hash join `derive_required_alternatives` emits broadcast/shuffle alternatives.
- Add only-broadcast/only-shuffle helper.
- Add parent-required key alignment for shuffle join.

Validation:

- Rust tests for broadcast + shuffle alternatives.
- Rust tests for parent key order alignment.
- Existing join SQL suite still compiles.

### P3 - Child-output-aware search and cost

- Search enumerates alternatives.
- Cost receives child output properties and alternative kind.
- Broadcast hard gate uses rows, bytes, confidence.
- Shuffle candidate only pays exchange through child enforcer winner, not duplicate cost.

Validation:

- Rust tests for large build rejecting broadcast.
- Rust tests for fallback stats conservative broadcast gate.
- Rust tests for child hash distribution reuse.

### P4 - Extract/codegen/RF cutover

- Extract attaches final join execution distribution.
- EXPLAIN reads execution metadata.
- Fragment builder reads execution metadata.
- Runtime filter pass reads execution metadata.
- `PhysicalHashJoinOp.distribution` no longer participates in execution choice.

Validation:

- Fragment builder tests for broadcast/hash/gather edges.
- Runtime filter tests for broadcast vs shuffle layout.
- Explain tests for selected distribution.

### P5 - Golden and plan-quality validation

- Add optimizer goldens:
  - big build rejects broadcast;
  - hash-distributed child reused;
  - downstream aggregate/join reuses partitioned output;
  - fallback stats avoids risky broadcast.
- Run representative plan-shape checks:
  - `tpc-h/q9`, `tpc-h/q20`;
  - `tpc-ds/q4`, `tpc-ds/q64`, `tpc-ds/q85`;
  - SSB sanity baseline.

## 15. Test strategy

### Rust unit tests

- `property.rs`: `Broadcast` satisfies only `Any` and `Broadcast`.
- `derive/hash_join.rs`: alternatives, key alignment, output property for broadcast/shuffle.
- `search.rs`: winner records `alt_kind`, `child_props`, `child_outputs`.
- `cost.rs`: broadcast memory/backend factor, shuffle build parallel factor, fallback stats gate.
- `extract.rs`: extraction uses recorded child props.
- `fragment_builder.rs`: broadcast distribution edge maps to broadcast thrift partition.
- `runtime_filter_pass.rs`: RF distribution inferred from execution metadata.

### SQL golden tests

Candidate files:

- `sql-tests/optimizer/distribution_join_broadcast_gate.sql`
- `sql-tests/optimizer/distribution_join_shuffle_reuse.sql`
- `sql-tests/optimizer/distribution_join_downstream_reuse.sql`
- `sql-tests/optimizer/distribution_join_fallback_stats_gate.sql`

Golden assertions:

- `@explain_not_contains=BROADCAST` for oversized build case.
- `@explain_contains=HASH EXCHANGE` for partitioned path.
- `@explain_contains=HASH JOIN (... dist=PARTITIONED ...)` once explain label is stable.
- `@explain_not_contains` should include complete substrings to avoid matching `rows=10` via `rows=1`.

### Development-only plan diff

Use roadmap baseline queries:

- `tpc-h/q9`, `tpc-h/q20`;
- `tpc-ds/q4`, `tpc-ds/q64`, `tpc-ds/q85`;
- SSB full suite as no-obvious-regression check.

Expected direction:

- broadcast join count decreases on TPC-H/TPC-DS representative queries;
- partitioned join / hash exchange count increases where FE uses partitioned joins;
- SSB does not flip small dimension joins into unnecessary shuffle.

## 16. Risks and mitigations

| Risk | Mitigation |
|---|---|
| Search/extract mismatch | Store `child_props` in `Winner`; extraction never re-derives child requirements |
| Cost double-counts shuffle | Exchange cost charged through child enforcer only; join self-cost excludes child exchange |
| Broadcast over-pruned | StarRocks-like scale-factor gate: do not reject broadcast when build is clearly tiny relative to probe |
| Full/right outer property unsound | First version returns `Any` or only-shuffle for subtle preserved-side cases |
| Codegen lacks final distribution | Add extracted execution metadata before removing operator enum usage |
| Golden churn | Keep VERBOSE compact; expose detailed property info primarily in COSTS/debug |
| Compile blast radius | Implement in phases with compatibility wrappers around old `derive_required` and `JoinDistribution` |

## 17. Open implementation questions

1. `backend_factor` source:
   - use static default for standalone first;
   - later derive from distributed runtime/session.
2. Broadcast byte limit:
   - fixed constant vs session/config variable.
3. Whether `DistributionSpec::Broadcast` should be represented as `PhysicalDistribution(Broadcast)` in all cases
   or only in extracted child edges.
4. Whether `PhysicalPlanNode` should carry full `PlanExecutionProps` immediately or start with a narrower
   `join_distribution` field.
5. Whether right/full outer shuffle output can safely carry null-relaxed hash property without adding
   StarRocks-like `DistributionCol.nullStrict`.

## 18. Acceptance criteria

- `JoinToHashJoin` no longer creates separate broadcast/shuffle physical hash join expressions.
- Hash join search enumerates broadcast/shuffle via child property alternatives.
- Cost model differentiates broadcast and shuffle from children output properties.
- Broadcast is rejected or heavily penalized for large/fallback build sides.
- Existing child hash distribution can be reused without duplicate exchange.
- Extract/codegen/runtime-filter use final execution distribution metadata, not optimizer candidate enum.
- Representative TPC-H/TPC-DS plan shapes move toward FE direction; SSB remains stable.
