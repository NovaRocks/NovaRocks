# G4 — HashDistribution Source Type

**Date**: 2026-05-22
**Roadmap ref**: [SQL Layer Roadmap §2.3](2026-05-20-sql-layer-roadmap-design.md#23-g4--hashdistribution-source-type-p1) (G4)
**Status**: Spec — pending implementation plan
**Scope**: `src/sql/optimizer/` physical property model, output/required property derivation, enforcer insertion semantics, explain/golden coverage. 不修改 FE-compatible lowering、execution runtime、connector storage layout、Iceberg writer/reader 语义，且不引入 colocate/bucket join。

---

## §0 Goal

G4 第一版把 NovaRocks 当前的裸 `HashPartitioned(Vec<ColumnId>)` 改成 source-aware hash distribution，用来修正现在过宽且方向错误的 hash distribution satisfies 判断。

当前 `DistributionSpec::satisfies` 对所有 hash 分布使用同一个 contains 判断：

```rust
required_cols.iter().all(|c| provided_cols.contains(c))
```

这会让 `hash(a, b)` 被认为满足 `hash(a)`。这对 aggregate/window 这类需要同一 partition key 全部 colocated 的 operator 是不安全的：同一个 `a` 值会因为不同 `b` 被分散到不同 hash bucket。StarRocks 的核心做法是给 hash distribution 加 source type，并按 source 分支判断 satisfies。NovaRocks G4 采用这个思路，但只保留当前主线真实需要的 source。

成功标准：

1. Hash distribution 带 source，调用点不能再构造无来源的 hash spec。
2. `ShuffleAgg` 与 `ShuffleJoin` 的 satisfies 规则分开，修正 aggregate/window/sort 需求的方向。
3. Existing shuffle/broadcast join、aggregate、window、partitioned sort、distribution enforcer 的 property derivation 继续可解释、可测试。
4. SQL golden 能证明原先会错误跳过 exchange 的 plan shape 得到修正。

---

## §1 Non-goals

本阶段明确不做：

- StarRocks `LOCAL` / `BUCKET` / `SHUFFLE_ENFORCE` source。
- Iceberg `bucket(...)` transform 到执行层 colocate distribution 的映射。
- Colocate join、bucket shuffle join、bucket-aware scan output property。
- StarRocks `EquivalentDescriptor`、`DistributionCol.nullStrict`、`aggStrict`。
- 用 G7 logical equivalence classes 扩展 hash distribution satisfies。
- Join strategy costing 或 broadcast/shuffle 枚举策略调整。
- G2 `LogicalPlan` 合并、G5 pattern rule framework、G6 fragment IR。

这些都可以后续做，但需要额外前提：scan 能产出可信的 physical distribution、bucket 数/哈希函数/分区兼容性可证明、调度层能保证同 key 同地执行。当前 NovaRocks 以 Iceberg / standalone 查询为主，standalone parser 还会剥离 `JOIN [bucket]` / `JOIN [colocate]` 等 hint，因此第一版不引入这些 source。

---

## §2 Current Baseline

当前 main 的相关边界：

- `src/sql/optimizer/property.rs` 定义：

```rust
pub(crate) enum DistributionSpec {
    Any,
    Gather,
    HashPartitioned(Vec<ColumnId>),
}
```

- `HashPartitioned` satisfies 只有一个全局 contains 规则，缺少 source 语义。
- `PhysicalHashAggregateOp` 的 Global / DistinctGlobal child requirement 使用 `HashPartitioned(group_by_cols)`；Local / Single aggregate output 也会产出 hash distribution。
- `PhysicalWindowOp` 与 analytic `PhysicalSortOp` 对 partition key 使用 hash distribution requirement。
- `PhysicalHashJoinOp` 的 shuffle variant 会使用 join eq keys 产出 hash requirement/output；G7 后 join output 会携带左右等价列，方便某些下游 consumer 命中。
- `PhysicalDistributionOp` 是泛型 enforcer：当 provided 不满足 required 时，`needed_enforcers` 直接插入 required spec，enforcer output 等于自身 spec。
- `JoinDistribution::Colocate` 目前不是主路径 producer：implementation rule 只生成 Shuffle 和 Broadcast。Colocate 主要存在于 enum、成本模型、单测和 FE-like explain/codegen 边界中。

这些事实决定 G4 第一版应收敛在 source-aware `ShuffleAgg` / `ShuffleJoin`，而不是照搬 StarRocks 全量 distribution source 模型。

---

## §3 StarRocks Reference, Trimmed for NovaRocks

StarRocks 的 `HashDistributionDesc.SourceType` 有：

```java
LOCAL, SHUFFLE_AGG, SHUFFLE_JOIN, BUCKET, SHUFFLE_ENFORCE
```

其中：

- `SHUFFLE_AGG` 用于 aggregate-like partitioning。StarRocks 的 analytic/window requirement 也复用这一路。
- `SHUFFLE_JOIN` 用于 shuffle hash join，对 key 顺序与兼容性要求更严格。
- `LOCAL` / `BUCKET` 服务 scan-local colocate、bucket shuffle、tablet distribution 等场景。
- `SHUFFLE_ENFORCE` 主要出现在 StarRocks 的 child output guarantor 特殊路径里。

NovaRocks 当前没有 StarRocks FE 的 colocate tablet property，也没有把 Iceberg bucket transform 提升为 optimizer scan-local distribution。泛型 enforcer 也不需要单独 source 表示“enforced output”；它可以直接输出 required 的 source。因此 G4 第一版只引入：

```rust
pub(crate) enum HashSource {
    ShuffleAgg,
    ShuffleJoin,
}
```

---

## §4 Data Model

`DistributionSpec` 改为：

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum DistributionSpec {
    Any,
    Gather,
    HashPartitioned {
        cols: Vec<ColumnId>,
        source: HashSource,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum HashSource {
    ShuffleAgg,
    ShuffleJoin,
}
```

约束：

- `cols` 不允许为空；无 hash key 时使用 `Any` 或 `Gather`。
- 构造入口集中成 helper，例如 `DistributionSpec::shuffle_agg(cols)` 和 `DistributionSpec::shuffle_join(cols)`。
- helper 负责去重（保留首次出现顺序），过滤 `ColumnId::UNSET`，并在空结果时返回 `Any` 或由调用点显式决定 `Gather`。
- `Debug` / `EXPLAIN` 输出应展示 source，避免测试和排查时只看到 `HASH EXCHANGE` 而不知道语义来源。

---

## §5 Satisfies Semantics

### §5.1 Common Rules

- provided satisfies `Any`。
- `Gather` 只由 `Gather` 满足。
- 非 hash distribution 不满足 hash requirement。
- Hash satisfies 必须看 `HashSource`，不能再全局 contains。

### §5.2 ShuffleAgg

`ShuffleAgg` 表示“按某些 key 分组或分区执行即可”，用于：

- Global / DistinctGlobal aggregate 的 child requirement。
- aggregate-like output distribution。
- Window `PARTITION BY` 的 child requirement/output。
- Analytic sort 的 partitioned sort requirement/output。

规则：

```text
provided ShuffleAgg(P) satisfies required ShuffleAgg(R)
iff P is a subset of R, order-insensitive
```

原因：如果数据按 `hash(a)` 分布，则同一个 `(a, b)` 一定也在同一个 `a` 分区内，因此可以满足 `GROUP BY a, b` 或 `PARTITION BY a, b`。反过来，`hash(a, b)` 不能满足 `GROUP BY a`，因为同一个 `a` 会被不同 `b` 分散。

### §5.3 ShuffleJoin

`ShuffleJoin` 表示 shuffle hash join 的 key 对齐语义，用于：

- Shuffle hash join 的 left/right child requirement。
- Shuffle hash join 的 output distribution。

第一版规则保守：

```text
provided ShuffleJoin(P) satisfies required ShuffleJoin(R)
iff P == R, order-sensitive
```

这比 StarRocks 少了 `EquivalentDescriptor` 兼容判断，但符合 NovaRocks 当前数据模型。G7 logical equivalence classes 暂不参与 physical distribution satisfies，因为 `HashPartitioned` 的 `cols` 同时承担了真实 hash key 与等价替代列的表达；直接用等价类放宽判断容易把“可替代描述”误当成“真实 hash key”。

### §5.4 Cross-source Rules

第一版只保留两个可解释的 StarRocks-like 交叉规则：

```text
provided ShuffleAgg(P) satisfies required ShuffleJoin(R)
iff P == R, order-sensitive

provided ShuffleJoin(P) satisfies required ShuffleAgg(R)
iff P is a subset of R, order-insensitive
```

这能支持“已经按完全相同 key 聚合分布的数据参与 shuffle join”与“join 输出如果只按某个 key 分布，可继续满足更细的 aggregate requirement”。但它不会让 `ShuffleJoin([l, r])` 满足 `ShuffleAgg([l])`，这是 G4 要修正的关键风险。

这两个交叉规则属于 G4 第一版范围；implementation plan 不应扩大到 `Local` / `Bucket` / `ShuffleEnforce`。

---

## §6 Producer / Consumer Mapping

### §6.1 Hash Aggregate

- `Global` / `DistinctGlobal` child requirement：`ShuffleAgg(group_by_column_ids)`。
- `Local` / `Single` aggregate output：如果有 group-by output ColumnId，产出 `ShuffleAgg(group_by_output_column_ids)`；否则 `Gather`。
- `DistinctLocal` child requirement 仍为 `Any`。

### §6.2 Window

- 有 `PARTITION BY` 且所有 partition expr 都是 `ColumnRef`：child requirement 为 `ShuffleAgg(partition_cols)`。
- Output 继承同样的 `ShuffleAgg(partition_cols)`。
- 无 partition key：保持 `Gather` requirement。

### §6.3 Sort

- Top-level sort 仍为 `Gather + Required ordering`。
- Analytic precursor sort 若带 partition expr：child requirement/output 使用 `ShuffleAgg(partition_cols)`。

### §6.4 Hash Join

Shuffle hash join 使用 `ShuffleJoin`。

实现优先级：

1. 保持现有 fragment builder 可工作的前提下，把 current `HashPartitioned(all_eq_cols)` 改成 `ShuffleJoin(all_eq_cols)`。
2. 若 implementation plan 发现可以低风险改为 left child 使用 left eq cols、right child 使用 right eq cols，则可以作为同一 PR 的局部清理；否则不强行改变 codegen 对“all eq cols, child scope 自行解析”的依赖。
3. Shuffle join output 可以继续携带左右 eq columns，但 source 必须是 `ShuffleJoin`，且不能因此满足 `ShuffleAgg([one_side])`。

Broadcast join 保持 preserves-left 逻辑，但如果 enrich 左侧 distribution 的 eq-equivalent columns，会保留原 source，不把 Broadcast join output 伪装成 `ShuffleJoin`。

Colocate join 不作为主路径 producer；现有测试需要保留时，应显式标注为 legacy/test-only distribution decision。

### §6.5 Distribution Enforcer

NovaRocks 的 enforcer 是泛型 property enforcer，不引入 `ShuffleEnforce`。

- 如果 required 是 `ShuffleAgg(cols)`，enforcer output 就是 `ShuffleAgg(cols)`。
- 如果 required 是 `ShuffleJoin(cols)`，enforcer output 就是 `ShuffleJoin(cols)`。
- `PhysicalDistributionOp` 不需要知道“为什么插入”，只携带 required spec。

这样和当前 `needed_enforcers` / `PhysicalDistributionOp::derive_output` 模型一致，避免引入没有独立 producer 的 source。

---

## §7 Explain and Codegen

### §7.1 Explain

`PhysicalDistribution` explain 应展示 source：

```text
HASH EXCHANGE (source: ShuffleAgg, hash: [c1, c2])
HASH EXCHANGE (source: ShuffleJoin, hash: [c1, c2])
```

这会让 optimizer golden 能直接断言 source-aware plan shape。

### §7.2 Fragment Builder

`fragment_builder` 对 exchange partition expr 的实际构造仍基于 `cols` 和 child scope 解析；`source` 只影响 optimizer property 和 explain，不改变 thrift partition 表达式语义。

如果 join child 仍使用 all-eq-cols 规格，fragment builder 继续只选 child scope 可解析的 ids。若 implementation plan 改成 child-specific join cols，则需要同步更新相关单测，确认 exchange expr 不丢列。

---

## §8 Testing

### §8.1 Unit Tests

更新 `src/sql/optimizer/property.rs`：

- `ShuffleAgg([a])` satisfies `ShuffleAgg([a, b])`。
- `ShuffleAgg([a, b])` does not satisfy `ShuffleAgg([a])`。
- `ShuffleJoin([a, b])` satisfies only exact `ShuffleJoin([a, b])`。
- `ShuffleJoin([a, b])` does not satisfy `ShuffleAgg([a])`。
- `ShuffleAgg([a, b])` satisfies `ShuffleJoin([a, b])` only when exact/order-compatible.

更新 derive 单测：

- hash aggregate output/required 使用 `ShuffleAgg`。
- window output/required 使用 `ShuffleAgg`。
- analytic sort output/required 使用 `ShuffleAgg`。
- shuffle join output/required 使用 `ShuffleJoin`。
- broadcast preserves-left 保留 child source。
- distribution enforcer output 等于 required source。

### §8.2 SQL Golden

新增或更新 optimizer suite plan cases：

1. `Window(PARTITION BY l.k)` over shuffle join `l.k = r.k`：不能仅因为 join output carries `[l.k, r.k]` 就跳过 required `ShuffleAgg(l.k)` exchange。
2. `GROUP BY l.k` over shuffle join：同样需要防止 `ShuffleJoin([l.k, r.k])` 错满足 `ShuffleAgg([l.k])`。
3. Aggregate two-phase case：`ShuffleAgg([g])` 能满足更细的 `ShuffleAgg([g, x])` requirement 时不插入多余 exchange。

Plan golden 不要求性能最优，只要求 distribution source 与 exchange 位置正确。

### §8.3 Verification Commands

Implementation PR 至少运行：

```bash
cargo fmt --check
cargo test property::tests --lib
cargo test optimizer::derive --lib
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite optimizer --mode verify
```

如 SQL runner 需要 standalone server，按 AGENTS.md 从 `docker/iceberg-rest/runtime/current/env.sh` 发现端口和 config，不硬编码 `9030`。

---

## §9 Risks and Mitigations

### Risk: Losing a currently valid optimization

G4 会让一些原本跳过 exchange 的 plan 多一个 exchange。只要旧计划依赖 `hash(a,b)` 满足 `hash(a)`，那就是 correctness-first 的修正。后续可通过真实 equivalence/partition descriptor 逐步找回安全优化。

### Risk: Join output `cols` mixes real keys and equivalent descriptors

当前 G7 后 `HashPartitioned` vector 可能携带左右等价列。G4 不把这个 vector 当成 StarRocks `EquivalentDescriptor`。第一版通过 source-aware satisfies 保守处理，避免 `ShuffleJoin([l, r])` 满足 `ShuffleAgg([l])`。

### Risk: Large mechanical churn

把 enum variant 从 tuple 改成 struct-like 会触达多个 tests。Implementation plan 应先加 helper constructors，再机械替换 call sites，最后收紧 direct construction。

### Risk: Explain golden churn

Explain 输出加 source 会改 golden。为降低噪音，只在 `PhysicalDistribution` label 中显示 source，不改变其他 node format。

---

## §10 Deferred Follow-ups

- `ShuffleEnforce`：只有当 NovaRocks 引入类似 StarRocks `ChildOutputPropertyGuarantor` 的特殊 join child enforcement 路径时再加。
- `Local` / `Bucket`：需要 scan-local distribution producer、bucket metadata、hash function/桶数兼容和调度保证。
- Iceberg bucket-aware distribution：单独设计，不把 Iceberg partition transform 直接等同于 execution colocate。
- Physical `EquivalentDescriptor`：可在 G7 logical equivalence 与 G4 physical distribution 之间建立显式桥，但不能把 logical equality 直接当作 physical hash key。
