# Strict-only 优化器等价事实设计

日期：2026-06-21

## 结论

本轮目标不是制造新的 plan win，而是做 optimizer logical properties 的卫生修复：

1. `LogicalProperties.equivalence_classes` 只保存普通 `=` 推导出的 strict 等价事实。
2. `<=>` 不再进入 `equivalence_classes`，避免污染 strict 消费者。
3. multi-join reorder 读取 root logical properties 里的权威等价事实，不再从 join chain 的 raw predicates 重新构造局部事实。

PR #344 已经解决了大多数 reorder 图不完整问题。本轮补的是一个更窄的缺口：atom
内部已经证明的 `a1 = a2` 没有被 reorder 的链内局部 union-find 看到。这个 case
真实但不常见，因此本轮价值主要是单一权威来源和语义去污染，不应宣传成广泛计划质量提升。

## 背景

PR #344 让 multi-join reorder 能利用传递等值边，并且没有把完整 pairwise 闭包物化回
逻辑计划。当前实现仍有两个问题。

第一，`flatten_join_chain` 会从当前 join chain 里能看到的字面谓词重新构造一份局部
`EquivalenceClasses`，没有使用 `run_multi_join_reorder` 之前已经推导好的
`LogicalProperties.equivalence_classes`。这会漏掉 atom 内部或更早逻辑属性已经证明的等价关系。

例如：

```text
Atom A 已证明：a1 = a2
Join chain 谓词：a1 = b AND a2 = c
```

root logical properties 可以证明 `{a1, a2, b, c}`。当前 #344 的 flattener 只能看到
`{a1, b}` 和 `{a2, c}` 两个局部集合，因此不能把 `b` 和 `c` 当作可合成的 strict
传递 join 边。

第二，当前 logical-property derivation 把普通等号 `=` 和 null-safe 等号 `<=>` 混在同一个无类型
`EquivalenceClasses` 里。所有现有消费者都把该 store 当 strict 等价事实使用，因此
`<=>` 进入 store 会污染后续推理。

## 目标

1. 让 `LogicalProperties.equivalence_classes` 成为 strict column equivalence 的唯一权威来源。
2. 让 logical props 只从普通 `BinOp::Eq` 推导 `equivalence_classes`。
3. 修正 `PhysicalHashJoin` 派生：只有 `!eq.null_safe` 的 hash key 才能 merge 进 strict store。
4. 修正 multi-join reorder：从 root logical properties 读取 strict facts，而不是从 raw predicates 重算。
5. 保留现有 `EquivalenceClasses` 单 store，不引入本轮无消费者的 `NullSafe` store。

## 非目标

1. 不把传递谓词全局物化进逻辑计划。
2. 不为 `<=>` 建第二套 union-find。
3. 不实现 null-safe 等价事实消费者。
4. 不实现 `strict ⊆ null-safe` 的 null-safe 闭包查询。
5. 不把 statistics、hash join 实现、runtime filter、MV rewrite 或 low-card dictionary 改成消费 logical properties 的等价事实。

## 设计取舍

### 选定方案：strict-only 单 store

保留现有 `EquivalenceClasses` 类型和 `LogicalProperties.equivalence_classes` 字段名，但重新收紧字段语义：

```rust
pub(crate) struct LogicalProperties {
    pub(crate) equivalence_classes: EquivalenceClasses,
}
```

该字段只表示普通 `=` 推导出的 strict column equivalence。能使用它的消费者必须满足：

1. 只需要 ordinary equality 语义。
2. 可以安全合成或证明普通 `col = col`。
3. 不依赖 `<=>` 的 NULL-NULL 为 true 语义。

`EqForNull` 不进入这个 store。这样 strict 消费者不用在每个调用点传 `EquivalenceKind`，也不会被 null-safe 事实污染。

### 被拒绝方案：双 store 类型化事实

双 store 方案会引入：

```rust
EquivalenceFacts {
    strict: EquivalenceClasses,
    null_safe: EquivalenceClasses,
}
```

并把 `EquivalenceKind` 穿透到每个调用点。本轮不采用，原因是：

1. 当前仓库没有任何 null-safe equivalence facts 消费者。
2. 现有消费者都是 strict 消费者，strict-only 已经能阻止 `<=>` 污染导致的 unsound strict 合成。
3. 双 store 如果把 `=` 和 `<=>` 完全分离，建模方向不完整：`a = b` 为真时可以推出
   `a <=> b` 为真，因此将来真有 null-safe 消费者时，null-safe 查询至少需要看到
   `closure(strict ∪ null_safe)`，而不是只读独立 `null_safe` store。

如果未来确实需要 null-safe equivalence consumer，应重新设计 public API，让
`classes(NullSafe)` 或等价查询语义明确包含 strict facts，而不是复用本轮被拒绝的 born-dead 双 store。

## 等价事实推导

`logical_props.rs` 应把当前混合语义的 `collect_column_equalities()` 改成只收集 strict equality。
注意这个函数不是 logical props 私有 helper：`equivalence_predicate.rs::join_column_pairs`
也会用它从活的 join condition 中取 `col = col` 对，用于 literal 传播定向。为了避免一个共享
helper 静默改变第二个生产调用点，本轮必须显式重命名并统一切换调用点。

推荐保守实现：

```rust
pub(crate) fn collect_strict_column_equalities(
    scalars: &ScalarArena,
    expr: ScalarId,
) -> Vec<(ColumnId, ColumnId)>;
```

收集规则：

```text
ColumnRef = ColumnRef     -> collect
ColumnRef <=> ColumnRef   -> ignore
```

`AND` 和 `Nested` 继续递归遍历。非列操作数忽略。

生产调用点统一使用 `collect_strict_column_equalities`：

1. logical-property derivation 用它写入 `LogicalProperties.equivalence_classes`。
2. `equivalence_predicate.rs::join_column_pairs` 也用它给 literal propagation 定向。

这会有一个有意识的保守 side effect：跨 `<=>` join 的 literal 定向本轮一并不做。例如
`a <=> b AND a = 5` 推出 `b = 5` 是 sound 的，但本轮 strict-only 边界不实现这个罕见优化。
这样可以保证 `equivalence_classes` 的 strict 语义和共享 helper 的行为一致，不留下“一处 strict、
一处仍混合”的 split-brain。

如果实现者希望保留类型化 collector 作为内部清晰度，也可以返回带 kind 的事实：

```rust
pub(crate) enum ColumnEqualityKind {
    Strict,
    NullSafe,
}

pub(crate) struct ColumnEqualityFact {
    pub(crate) kind: ColumnEqualityKind,
    pub(crate) left: ColumnId,
    pub(crate) right: ColumnId,
}
```

但落入 `LogicalProperties.equivalence_classes` 时只能 merge `Strict`，`NullSafe` 必须当场丢弃，并加注释说明：

```text
Null-safe facts currently have no logical-properties consumer. Do not store them in
the strict equivalence store.
```

logical property derivation 规则：

1. `LogicalFilter` 和 `PhysicalFilter` 继承 child facts，再 merge predicate 中的 strict equalities。
2. `LogicalJoin` 只在 `JoinKind::Inner` 时继承左右 child facts，再 merge join condition 中的 strict equalities。
3. `PhysicalHashJoin` 只在 `JoinKind::Inner` 时继承左右 child facts；遍历 `eq_conditions` 时必须判断
   `if !eq.null_safe`，只有普通 hash key 才 merge 进 strict store。
4. `PhysicalHashJoin.other_condition` 继续通过 strict collector 收集普通 `=`。
5. pass-through 节点继续继承 child facts，并按 output columns 裁剪。
6. outer、semi、anti 和其他非 pass-through operator 保持当前边界，除非已有专门的 strict equivalence 规则。

`retain_subset_of` 和 `extend_from` 继续作用于单一 strict store。

## CTE 边界

`LogicalCTEProduce` / `PhysicalCTEProduce` 当前作为 pass-through 继承 child facts。

`LogicalCTEConsume` / `PhysicalCTEConsume` 当前不在 pass-through 列表中。这个边界应保持显式，而不是被“pass-through 继承”的文字误覆盖：CTE consume 会 remap column id；如果未来要继承 produce 侧等价事实，必须通过 CTE column mapping 重新映射后再 merge。第一轮不做这件事，consume 按无等价事实处理。

## MultiJoinReorder 迁移

multi-join reorder 使用 `LogicalProperties.equivalence_classes` 作为 strict facts。

`flatten_join_chain` 停止调用链内局部 `build_equi_classes(&raw_predicates, ...)`。它应读取 root group 的 logical properties：

```rust
let classes = memo.groups[root]
    .logical_props
    .as_ref()
    .map(|props| props.equivalence_classes.classes())
    .unwrap_or(&[]);
```

读取 root logical properties 是有意的边界选择：root 是当前 contiguous inner/cross join
chain 的最顶层 join group，输出列是左右子树输出的拼接；当前 `PruneJoinColumns` 对这类场景不裁剪，
join 条件列也会下推到 scan/atom 输出。因此 atom 内部 strict 等价类不会因为 root
`retain_subset_of` 被意外裁掉。若未来 join column pruning 变成有效规则，必须同步调整这里的代表列投影逻辑和测试。

随后把这些 strict classes 投影到当前 join chain 的 atoms，构造 reorder 内部临时 `EquiClass`。
这是替换来源，不是删除 `raw_predicates`：raw predicates 仍用于 join edge / mask 分类和原始
condition materialization；只切换 `equi_classes` 的来源。

临时 `EquiClass` 可以继续保存：

```rust
columns: Vec<ColumnId>
reps: Vec<(atom_index, ScalarId)>
```

representative 必须从 atom output metadata 构造，而不是从列是否出现在 raw predicate 中判断。builder 应在 atom group 的 `LogicalProperties.output_columns` 中找到对应 `ColumnId`，并 intern 一个带正确类型、nullable 和 display metadata 的 `ScalarNode::ColumnRef`。

这点需要特别注意：重新 intern 的 `ColumnRef` 的 nullability 可能与原始谓词 scalar 上携带的上下文不同。合成谓词时应以 output metadata 为准，不能从旧 predicate operand 偷用不匹配的标量节点。

因此停用链内局部 `build_equi_classes` 后，旧的 `build_equi_classes` / `column_equality` /
`column_ref` helper 应删除，避免后续代码继续复用“从旧谓词 operand 偷 representative”的路径。

`connecting_condition_scalars` 保持 #344 的约束：

1. 每个 strict class 对每个 cut 最多产出一个 equality。
2. 如果已有 literal 或 column predicate 能连接 cut，优先使用已有谓词。
3. 否则只从 strict class 合成普通 `col = col`。

因为 `<=>` 不进入 strict store，纯 null-safe 链不会合成普通 `=`。

## 其他消费者

### TopN 和排序证明

`topn_proof.rs` 和 `topn_compactness.rs` 可以继续读取 `equivalence_classes`，因为该字段已经被收紧为 strict-only。

本轮不把 `<=>` 引入排序证明，不是因为 `<=>` 一定不 sound。若某个 null-safe equivalence 对所有输出行都成立，`sort by a` 与 `sort by b` 在 NULL 摆放上也可以一致。限制的真实原因是本轮不维护 null-safe facts，也不引入 null-safe consumer。

### 等价谓词传播

`equivalence_predicate.rs` 可以继续读取 `equivalence_classes` 做 column equivalence 和 literal propagation，因为该字段已经 strict-only。

`a = b AND a = 5` 仍可推出 `b = 5`。

`a <=> b AND a = 5` 事实上可以推出 `b = 5`，因为 `a = 5` 使 `a` 非空，`a <=> b` 在这个上下文中坍缩为普通相等。但本轮不实现这类混合推理，原因是保守和架构边界，不是 soundness 禁止。

### Statistics、物理实现和 Runtime Filter

statistics 与物理实现路径继续读取实际谓词。

hash join 实现仍然需要同时支持 `=` 和 `<=>`，并在物理执行中保留 null-safe flag。这条路径与 logical properties 的 strict equivalence store 分离。

runtime filter 推导继续使用实际 join predicates 和已有 null-safe 限制。

### MV rewrite 和 Low-card Dictionary

MV rewrite 与 low-card dictionary 已有各自的 `<=>` 处理边界。本轮不让它们读取 logical properties 的等价事实，也不把它们作为 null-safe store 的需求来源。

## 缺失事实与错误边界

1. 如果 group 没有 logical properties，消费者按“没有等价事实”处理。
2. 如果 strict class 不能在至少两个 join chain atoms 上表示，reorder 忽略该 class。
3. 如果 class 中某列属于某个 atom，但找不到 output metadata，该 atom 不贡献 representative。
4. 缺失 facts 只能降低优化机会，不能改变查询语义。
5. `<=>` 不进入 strict store；任何需要 `<=>` 推理的规则必须读实际谓词或另行设计 null-safe fact API。

## 测试要求

### Property 和 Logical Props

新增或调整测试覆盖：

1. `EquivalenceClasses` 继续支持 strict classes 传递合并。
2. `collect_strict_column_equalities` 对 `=` 返回列对，对 `<=>` 不返回列对。
3. `LogicalFilter` / `PhysicalFilter` 只从普通 `=` merge strict facts。
4. `LogicalJoin` 只从普通 `=` merge strict facts。
5. `PhysicalHashJoin` 对 `eq.null_safe = false` merge strict facts。
6. `PhysicalHashJoin` 对 `eq.null_safe = true` 不 merge strict facts。
7. `retain_subset_of` 继续裁剪 strict store。

### MultiJoinReorder

新增 #344 不完整性的回归测试：

```text
Atom A 已证明：a1 = a2
Join chain 谓词：a1 = b AND a2 = c
```

期望行为：

1. root strict facts 包含 `{a1, a2, b, c}`。
2. `flatten_join_chain` 把该 class 投影到 atoms A、B、C。
3. B-C cut 可以合成一个 strict `b = c` 谓词。

新增 null-safe 守门测试：

```text
Atom A 已证明：a1 <=> a2
Join chain 谓词：a1 = b AND a2 = c
```

期望行为：`a1 <=> a2` 不进入 root strict facts，因此不能借它合成 B-C 的普通 `=` 谓词。
这是 strict-only 的保守守门，不是在证明该具体推理一定不 sound。

新增纯 null-safe 链负例：

```text
a1 <=> a2 AND a1 <=> b AND a2 <=> c
```

期望行为：不合成任何普通 strict transitive equality。这个 case 是必要守门：全 NULL 行在 null-safe 链中可以存活，错误合成 `b = c` 会过滤掉它。

保留 #344 的有界性测试：

1. k=12 synthesis 有界。
2. 不物化 C(k,2) 谓词。
3. 每个 strict class 对每个 cut 最多一个 equality。
4. 如果已有连接 cut 的谓词可用，优先使用已有谓词而不是 synthetic predicate。

### TopN 和等价谓词传播

TopN 和 equivalence predicate 的既有 strict 测试应继续通过。新增守门测试即可：

1. `a = b` 仍能被这些消费者读取为 strict equivalence。
2. `a <=> b` 不会通过 logical properties 暴露给这些消费者。
3. `equivalence_predicate.rs::join_column_pairs` 不再从 `<=>` join condition 返回列对；跨
   `<=>` join 的 literal 定向本轮不发生。

不新增 born-dead 的 null-safe store 测试。

### SQL Tests

新增聚焦 optimizer suite cases：

1. multi-join reorder explain case：atom 内部 strict equivalence 加 chain predicates 能打开传递候选。
2. null-safe 负例：`<=>` 不触发普通 `=` synthesis。

## 验证命令

实现阶段至少运行：

```bash
cargo fmt --check
cargo test --lib equivalence_classes_
cargo test --lib logical_props
cargo test --lib multi_join_reorder
cargo test --lib topn
cargo test --lib equivalence_predicate
```

新增 SQL case 后运行对应 optimizer SQL suite。

如果 join reorder 在更大范围计划中产生计划形态变化，发布前还应运行 optimizer golden suite，以及 #344 使用过的 q72 聚焦验证。

## 后续扩展条件

只有出现真实 null-safe equivalence facts 消费者时，才重新引入 null-safe fact API。届时设计必须处理：

1. `strict ⊆ null-safe`，即普通 `=` 事实也应对 null-safe 查询可见。
2. `classes(NullSafe)` 或等价查询是否返回 `closure(strict ∪ null_safe)`。
3. null-safe facts 是否允许合成 `<=>`，以及哪些消费者可以使用。
4. strict literal 与 null-safe class 混合推理的安全子集，例如 `a <=> b AND a = 5 ⊢ b = 5`。

这些都不属于本轮实现范围。
