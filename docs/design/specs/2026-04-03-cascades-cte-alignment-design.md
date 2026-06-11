# Cascades CTE 结构对齐设计

日期：2026-04-03
状态：Draft

## 1. 背景

当前 NovaRocks 的 CTE 实现与 Cascades Phase 2+3 的目标结构不一致，主要问题有三点：

1. CTE 在 analyzer 阶段就按引用次数被预先分流：
   - 单次引用直接 inline
   - 多次引用放入共享注册表
2. planner 产出的是双轨结构：
   - `QueryPlan { cte_plans, main_plan }`
   - 主计划与 CTE 定义不是同一棵计划树
3. 新的 Cascades 主路径对 CTE 没有完整接入：
   - 非 CTE 查询走 `planner -> cascades -> fragment_builder`
   - CTE 查询仍走旧的 `optimize_query_plan -> fragment::plan_fragments -> physical::emit_multi_fragment`

这与 StarRocks 的做法不同。StarRocks 将非递归 CTE 统一保留为 `CTEAnchor / CTEProduce / CTEConsume` 结构，再由后续优化阶段决定 inline 或 reuse，最终由 `PlanFragmentBuilder` 统一处理 multicast / exchange。

本设计的目标是只对齐 StarRocks 的 CTE **结构与职责分布**，不在首版引入其完整的 CTE 启发式优化集。

## 2. 目标

本次设计覆盖以下目标：

1. 非递归 CTE 统一进入 Cascades 新路径，不再按“有 CTE / 无 CTE”分流执行入口。
2. CTE 结构对齐为单棵计划树：
   - `CTEAnchor`
   - `CTEProduce`
   - `CTEConsume`
3. CTE 的 inline / reuse 决策后移到优化阶段。
4. 首版仅实现一条简单、稳定的决策主线：
   - `consume_count <= 1` 默认 inline
   - `consume_count > 1` 默认 reuse
5. `PlanFragmentBuilder` 按 StarRocks 风格处理：
   - `CTEProduce` 生成 multicast fragment
   - `CTEConsume` 生成 exchange consumer
   - `CTEAnchor` 只负责作用域和遍历顺序

## 3. 非目标

本次明确不做以下内容：

1. 不支持递归 CTE。
2. 不实现 StarRocks 的 CTE 启发式策略：
   - `inlineCTERatio`
   - `maxCTELimit`
   - `force reuse`
   - `MATERIALIZED`
   - 基于节点规模的强制物化
3. 不实现 consume 侧谓词下推到 produce。
4. 不实现 consume 侧 limit 下推到 produce。
5. 不在首版复制 StarRocks 中 `CTEConsume` “可带 child / 可不带 child”的双形态表示。

## 4. 现状差距

### 4.1 当前 NovaRocks 行为

- analyzer 在 `WITH` 处理中使用两套路径：
  - `shared_cte_ids`
  - `ctes`
- `resolve_from` 遇到 CTE 名称时：
  - 若在 `shared_cte_ids` 中，则生成 `Relation::CTEConsume`
  - 若仅在 `ctes` 中，则直接 inline 为子查询
- planner 的顶层输出是：

```text
QueryPlan
|- cte_plans[]
`- main_plan
```

- 旧的 multi-fragment 路径通过 `fragment.rs` 先把 `QueryPlan` 切成 fragment DAG，再交给 emitter。

### 4.2 目标形态

目标形态是单棵 `LogicalPlan` / `PhysicalPlan` 树，例如：

```text
CTEAnchor(a)
|- CTEProduce(a)
|  `- plan(a)
`- CTEAnchor(b)
   |- CTEProduce(b)
   |  `- plan(b)
   `- plan(main)
```

这样 CTE 定义与主查询在同一棵树内，Cascades 与 FragmentBuilder 都可以在统一结构上工作。

## 5. 总体设计

### 5.1 Analyzer 职责

Analyzer 不再决定 CTE 是否共享或 inline，而是只负责：

1. 收集当前查询作用域内的全部非递归 CTE 定义。
2. 为每个 CTE 分配稳定 `cte_id`。
3. 产出每个 CTE 的：
   - `name`
   - `cte_id`
   - `resolved_query`
   - `output_columns`
4. 在引用点统一生成 `Relation::CTEConsume`。

新的 `CTERegistry` 语义应从“共享 CTE 注册表”调整为“当前查询作用域中的非递归 CTE 定义表”。

### 5.2 Planner 职责

Planner 不再以 `QueryPlan { cte_plans, main_plan }` 作为主输出。

新的主输出改为单棵 `LogicalPlan`，并新增：

- `LogicalCTEAnchor`
- `LogicalCTEProduce`
- 现有 `LogicalCTEConsume`

Planner 处理顺序：

1. 分别规划每个 CTE definition，得到 `plan(cte_i)`。
2. 规划主查询，得到 `plan(main)`。
3. 按 `WITH` 中的声明顺序，将 `CTEProduce` 与主查询缝合为 anchor 链。

## 6. Analyzer 详细设计

### 6.1 CTERegistry 调整

`CTERegistry` 调整为保存全部非递归 CTE 定义：

- 不再只保存“引用次数 >= 2”的共享 CTE
- 不再依赖引用计数来决定是否注册

建议结构保持简单：

```text
CTERegistry
`- entries: Vec<CTEEntry>

CTEEntry
- id
- name
- resolved_query
- output_columns
```

引用计数不再属于 analyzer 决策输入，而属于后续优化阶段的 `CTEContext` 统计结果。

### 6.2 WITH 处理策略

Analyzer 进入 `WITH` 后：

1. 先按声明顺序为每个 CTE 分配 `cte_id`。
2. 将前面已经定义的 CTE 放入当前作用域，供后续 CTE 与主查询引用。
3. 对每个 definition 做语义分析，产出 `resolved_query`。
4. 将显式列别名应用到 `output_columns`。

首版作用域规则：

1. 仅允许引用当前层中已经声明在前的 CTE。
2. 对 forward reference 直接报错。
3. 对递归引用直接报错。

### 6.3 FROM 中的 CTE 引用

`resolve_from` 遇到 CTE 名称时，统一生成：

```text
Relation::CTEConsume {
    cte_id,
    alias,
    output_columns,
}
```

不再在 analyzer 阶段 inline 为子查询。

### 6.4 错误处理

Analyzer 阶段新增或保留以下错误：

1. `Recursive CTE is not supported`
2. `forward CTE reference is not supported`
3. `internal error: cte_id not found in registry`

错误必须在 analyzer/planner 阶段明确失败，不允许在执行阶段猜测补救。

## 7. Planner 详细设计

### 7.1 LogicalPlan 新节点

在 `src/sql/plan/mod.rs` 中新增：

```text
LogicalPlan::CTEAnchor(CTEAnchorNode)
LogicalPlan::CTEProduce(CTEProduceNode)
LogicalPlan::CTEConsume(CTEConsumeNode)
```

建议结构：

```text
CTEAnchorNode {
    cte_id,
    produce: Box<LogicalPlan>,
    consumer: Box<LogicalPlan>,
}

CTEProduceNode {
    cte_id,
    input: Box<LogicalPlan>,
    output_columns,
}
```

`CTEConsumeNode` 保持叶子节点语义，不带 child。

### 7.2 Anchor 链构造

对于：

```sql
WITH a AS (...), b AS (...)
SELECT ...
```

Planner 构造：

```text
CTEAnchor(a)
|- CTEProduce(a)
|  `- plan(a)
`- CTEAnchor(b)
   |- CTEProduce(b)
   |  `- plan(b)
   `- plan(main)
```

这样满足两个要求：

1. 后定义的 CTE 与主查询处于前定义 CTE 的作用域内。
2. FragmentBuilder 可以按先 produce、后 consume 的顺序遍历。

### 7.3 旧结构退出主路径

`QueryPlan` 与 `fragment.rs` 不再作为新主路径的一部分。

实现阶段允许临时保留旧结构以减少一次性删除风险，但以下约束必须成立：

1. `execute_query` 的主路径不再依赖 `QueryPlan`
2. `explain_query` 的主路径不再依赖 `QueryPlan`
3. Cascades CTE 路径不再依赖 `fragment.rs`

## 8. Cascades 详细设计

### 8.1 Operator 扩展

在 `src/sql/cascades/operator.rs` 中新增：

- `LogicalCTEAnchor`
- `PhysicalCTEAnchor`

并保留：

- `LogicalCTEProduce`
- `LogicalCTEConsume`
- `PhysicalCTEProduce`
- `PhysicalCTEConsume`

`PhysicalCTEAnchor` 不代表真实执行节点，只保留结构与作用域信息，供 `extract_best` 与 `PlanFragmentBuilder` 使用。

### 8.2 convert 规则

`logical_plan_to_memo` 需要支持：

1. `LogicalCTEAnchor`
2. `LogicalCTEProduce`
3. `LogicalCTEConsume`

对应 child 关系：

- `CTEAnchor` 有两个 child：`produce_group`、`consumer_group`
- `CTEProduce` 有一个 child：definition
- `CTEConsume` 无 child

### 8.3 CTEContext

新增轻量 `CTEContext`，只维护：

1. `produces: HashSet<CteId>`
2. `consume_count: HashMap<CteId, usize>`

来源：

- 遍历逻辑计划或 Memo 前做一次 CTE 收集
- 不依赖 analyzer 中的引用计数结果

### 8.4 InlineSingleUseCTE

新增一个简单的 CTE 决策 rewrite，规则如下：

1. `consume_count <= 1`：
   - inline 对应 `CTEConsume`
   - 删除无用的 `CTEProduce`
   - 删除无用的 `CTEAnchor`
2. `consume_count > 1`：
   - 保留 CTE 结构直到 physical plan

本规则必须在 fragment build 之前完成，否则单次消费 CTE 会错误地生成 multicast / exchange。

### 8.5 首版不采用双形态 CTEConsume

与 StarRocks 不同，NovaRocks 首版不让 `CTEConsume` 既可带 child 又可不带 child。

统一约束：

1. 逻辑表示中 `CTEConsume` 始终为引用叶子。
2. inline 通过 rewrite 从匹配的 `CTEAnchor/CTEProduce` 中直接替换子树。

这样可以降低实现复杂度，并避免一半在 transform 阶段 inline、一半在 optimizer 阶段 inline 的职责混乱。

## 9. PlanFragmentBuilder 详细设计

### 9.1 遍历职责

`PlanFragmentBuilder` 中新增：

- `visit_cte_anchor`

并完善：

- `visit_cte_produce`
- `visit_cte_consume`

职责划分如下：

1. `visit_cte_anchor`
   - 先 visit produce 分支
   - 再 visit consumer 分支
   - 返回 consumer 分支结果
2. `visit_cte_produce`
   - visit child definition
   - 将 child fragment 注册为 `cte_id` 对应的 multicast fragment
   - 不向父节点继续暴露 child plan node
3. `visit_cte_consume`
   - 为 `cte_id` 创建 exchange consumer
   - 将 consumer exchange 注册到 producer fragment 的目的端列表

这与 StarRocks 的职责切分保持一致：

- anchor 负责作用域与顺序
- produce 负责 multicast fragment
- consume 负责 exchange consumer

### 9.2 Fragment 行为

对于复用 CTE：

```text
CTEProduce(cte_id=1)
|- fragment P: real producer subtree
`- sink: multicast

CTEConsume(cte_id=1)
`- fragment C_i: exchange node
```

对于单次消费 CTE：

- inline 先发生
- physical plan 中不再出现 `CTEProduce / CTEConsume / CTEAnchor`
- 因此 fragment builder 不会看到该 CTE，也不会生成 multicast / exchange

### 9.3 Coordinator 元数据

`BuildResult` 仍需保留：

- `cte_id`
- `cte_exchange_nodes`

以便 coordinator 在 fragment instance ID 已知后回填 multicast sink 的真实目的端信息。

## 10. Explain 与执行入口

### 10.1 Explain

Explain 必须支持打印：

- `CTE ANCHOR`
- `CTE PRODUCE`
- `CTE CONSUME`

要求：

1. 逻辑 explain 可直观看到 anchor 链结构
2. physical explain 可区分 inline 与 reuse：
   - inline 后不再出现 CTE 节点
   - reuse 时出现 `CTE PRODUCE / CTE CONSUME`

### 10.2 执行入口

`src/standalone/engine.rs` 中的查询执行主路径统一为：

```text
analyze
-> planner
-> cascades
-> fragment_builder
-> coordinator/execute
```

不再保留：

- 非 CTE 走新路径
- CTE 走旧路径

这种分流。

## 11. 兼容性与迁移

### 11.1 行为兼容

首版行为与当前 NovaRocks 目标保持一致：

1. 单次引用 CTE 最终 inline
2. 多次引用 CTE 最终 reuse
3. 非递归 CTE 支持
4. 递归 CTE 不支持

### 11.2 结构迁移

迁移顺序要求：

1. 先让 analyzer/planner 产出新结构
2. 再让 Cascades 支持新结构
3. 再统一 explain / execute 入口
4. 最后移除旧主路径依赖

实现过程中允许短期保留旧类型和旧函数，但不得继续让主路径分流。

## 12. 测试与验收

### 12.1 单元测试

至少覆盖：

1. analyzer
   - 非递归 CTE 注册
   - 列别名传递
   - forward reference 报错
   - recursive CTE 报错
2. planner
   - 单个 CTE 生成单层 anchor
   - 多个 CTE 生成 anchor 链
   - `CTEConsume` 正确携带 `cte_id` 与 alias
3. Cascades
   - `consume_count == 1` 时 inline
   - `consume_count > 1` 时保留 reuse
4. fragment builder
   - reuse CTE 生成一个 producer fragment 和多个 consume exchange

### 12.2 Explain 测试

至少覆盖：

1. 单次引用 CTE 的 explain 中不出现 physical CTE 节点
2. 多次引用 CTE 的 explain 中出现 `CTE PRODUCE / CTE CONSUME`
3. 多层 CTE 定义时逻辑 explain 中 anchor 顺序正确

### 12.3 SQL 回归

至少覆盖以下 SQL 形态：

1. 单 CTE，单次引用
2. 单 CTE，多次引用
3. 多个 CTE 串联引用
4. CTE 引用后再做 join / aggregate / order by
5. `WITH t(a, b) AS (...)`

### 12.4 验收标准

满足以下条件才算完成：

1. 所有非递归 CTE 查询统一走 Cascades 新路径
2. 单次引用 CTE 被 inline
3. 多次引用 CTE 生成 multicast / exchange 复用结构
4. explain 可区分 inline 与 reuse
5. 主路径不再依赖 `QueryPlan + fragment.rs` 的旧 CTE 执行方式

## 13. 文件影响范围

预期修改重点文件：

- `src/sql/cte.rs`
- `src/sql/analyzer/mod.rs`
- `src/sql/analyzer/resolve_from.rs`
- `src/sql/ir/mod.rs`
- `src/sql/plan/mod.rs`
- `src/sql/planner/mod.rs`
- `src/sql/cascades/operator.rs`
- `src/sql/cascades/convert.rs`
- `src/sql/cascades/rules/implement.rs`
- `src/sql/cascades/extract.rs`
- `src/sql/cascades/fragment_builder.rs`
- `src/sql/explain.rs`
- `src/standalone/engine.rs`

旧路径相关文件在迁移完成后应退出主路径：

- `src/sql/fragment.rs`
- `src/sql/optimizer/mod.rs` 中面向 `QueryPlan` 的 CTE 主路径
- `src/sql/physical/emitter/mod.rs` 中面向 `FragmentPlan` 的 CTE 主路径

## 14. 最终决策

本设计最终确定以下决策：

1. 对齐 StarRocks 的 CTE 结构，而不是继续沿用 NovaRocks 当前的双轨 `QueryPlan`。
2. 明确排除递归 CTE。
3. CTE 是否 inline / reuse 的决策后置到优化阶段。
4. 首版只实现简单稳定的主线：
   - 单次默认 inline
   - 多次默认 reuse
5. 首版不实现 StarRocks 的 CTE consume 侧下推与 force reuse 启发式。
6. 首版 `CTEConsume` 保持无 child 的单一表示，避免双形态实现复杂度。
