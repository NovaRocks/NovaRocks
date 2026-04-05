# Cascades 子项目：PhysicalDistribution 多 Fragment 落地设计

日期：2026-04-03
状态：已确认待评审

## 1. 概述

本文档定义一个收敛后的子项目：把 `PhysicalDistribution` 从当前的 cost/explain 节点，落成 standalone Cascades 路径中的真实 fragment split + exchange 执行边界。

本子项目直接承接以下已有设计与计划：

- `docs/superpowers/specs/2026-04-01-cascades-phase2-3-design.md`
- `docs/superpowers/plans/2026-04-01-cascades-phase2-3.md`

目标不是一次完成 Phase 2/3 全量能力，而是优先补齐最关键的执行架构闭环，使 `PhysicalDistribution` 真正影响运行时 fragment 结构。

## 2. 目标与非目标

### 2.1 目标

1. 让 `extract` 产出的 `PhysicalDistribution` 在 `PlanFragmentBuilder` 中形成真实 fragment 边界。
2. 让 `ExecutionCoordinator` 从当前“CTE 专用多 fragment 协调器”泛化为“通用多 fragment 协调器”。
3. 在 standalone 模式下保留真实 exchange 语义，即使底层仍是本地进程内 / 本地 gRPC 传输。
4. 第一版至少支持 `Gather` 和 `HashPartitioned` 两类 distribution 落地。
5. 保证现有 CTE multicast 路径不回退，并复用同一套多 fragment 编排框架。

### 2.2 非目标

以下内容明确不属于本子项目第一版范围：

- `AggSplit` 与两阶段聚合
- 去掉旧 `join_reorder` 依赖
- `TPC-DS 99/99` 最终回归门槛
- 更复杂的普通 exchange 多 sender 聚合场景
- 针对多机部署的真实远端节点调度

## 3. 当前现状与问题

当前仓库已经具备以下基础：

- `src/sql/cascades/search.rs` 能基于属性推导和 enforcer 生成 `PhysicalDistribution`
- `src/sql/cascades/extract.rs` 能把 enforcer 提取为 `PhysicalPlanNode`
- `src/sql/explain.rs` 能把 `PhysicalDistribution` 格式化为 `GATHER EXCHANGE` 等 explain 文本
- `src/standalone/engine.rs` 已切到 `cascades::optimize()` + `PlanFragmentBuilder::build()`

但运行时闭环尚未完成：

1. `src/sql/cascades/fragment_builder.rs` 的 `visit_distribution()` 当前直接透传 child，没有真实切 fragment。
2. `src/standalone/coordinator.rs` 目前主要围绕 CTE multicast 编排，尚未承接普通 distribution stream edge。
3. `src/sql/physical/nodes.rs` 的 `build_exchange_node()` 当前固定写死 `UNPARTITIONED`，不足以表达 `HashPartitioned`。
4. 多 fragment 元数据当前分散在 `cte_id` / `cte_exchange_nodes` 等 CTE 专用字段中，不适合作为通用 exchange 编排模型。

因此，当前的 `PhysicalDistribution` 仍然主要停留在 explain/cost 语义层，而没有成为执行层的真实边界。

## 4. 核心设计决策

### 4.1 保持优化器与执行编排解耦

`src/sql/cascades/` 内的 `search` 和 `extract` 继续只负责产出带 `PhysicalDistribution` 的物理计划树，不在优化器阶段引入 fragment id、exchange destination 或运行时地址分配。

换言之：

- 优化器决定“哪里需要 distribution”
- `PlanFragmentBuilder` 决定“在哪里切 fragment”
- `ExecutionCoordinator` 决定“这些 fragment 在运行时如何补线和启动”

### 4.2 PlanFragmentBuilder 是唯一切分点

`PlanFragmentBuilder` 必须成为唯一的 fragment split 决策点。普通物理算子不关心 child 是本地子树还是 exchange 输入；只有 `PhysicalDistribution`、`PhysicalCTEProduce`、`PhysicalCTEConsume` 会触发 fragment 边界语义。

这保证多 fragment 逻辑不会扩散到 `join` / `agg` / `sort` / `project` 等所有 visitor 中。

### 4.3 standalone 也保留真实 exchange 语义

即使当前 standalone 是单机，`PhysicalDistribution` 仍需对应真实 exchange 边界，而不是简单优化掉。原因是：

- 它是 Cascades 属性模型与运行时结构之间的关键映射点
- 后续 `AggSplit`、多阶段 join、更多 fragment 拓扑都会依赖这条语义链路
- 过早把 distribution 退化成“仅 explain 节点”会继续积累假进度

唯一允许的保守优化是 `root gather elision`：

- 如果最外层只是把结果汇总到 result sink 的 `PhysicalDistribution(Gather)`，且没有其他 fragment 边界需求，builder 可以继续折叠该根 gather，避免把所有简单查询都强行变成多 fragment。
- 但 join/agg 中间层为了满足属性要求而插入的 `PhysicalDistribution`，第一版必须真实切 fragment。

## 5. 数据流设计

目标数据流如下：

1. `extract` 产出 `PhysicalPlanNode` 树，其中包含 `PhysicalDistribution`。
2. `PlanFragmentBuilder` 递归访问物理计划。
3. 遇到普通算子时，继续在当前 fragment 上构造 plan node。
4. 遇到 `PhysicalDistribution` 时：
   - 为 child 子树创建新的上游 fragment 上下文
   - 递归构建 child fragment
   - 固化 child fragment，并记录一条普通 stream edge
   - 在当前 fragment 中创建对应的 `ExchangeNode`
   - 把该 `ExchangeNode` 当作 child 输入返回给父算子
5. `PlanFragmentBuilder` 输出统一的多 fragment 结果：`fragments + edges + root_fragment_id`
6. `ExecutionCoordinator` 根据边信息补齐 sink destination、sender 数量和实例 ID，并按依赖顺序启动 fragment

CTE multicast 沿用相同框架，只是在边类型和 sink wiring 上使用特殊分支，不再作为唯一的多 fragment 来源。

## 6. 结构变更

### 6.1 通用边模型

在 `src/sql/cascades/fragment_builder.rs` 和 `src/sql/physical/mod.rs` 对齐引入通用边模型：

```rust
pub(crate) struct FragmentEdge {
    pub source_fragment_id: FragmentId,
    pub target_fragment_id: FragmentId,
    pub target_exchange_node_id: i32,
    pub output_partition: partitions::TDataPartition,
    pub edge_kind: FragmentEdgeKind,
}

pub(crate) enum FragmentEdgeKind {
    Stream,
    CteMulticast { cte_id: CteId },
}
```

约束如下：

- 普通 `PhysicalDistribution` 产出 `Stream`
- CTE multicast 产出 `CteMulticast`
- 第一版即使是 CTE multicast，也统一表达成 edge，避免 coordinator 持续依赖分散的 CTE 专用字段

### 6.2 MultiFragmentBuildResult 升级

`src/sql/physical/mod.rs` 中的 `MultiFragmentBuildResult` 从当前的：

- `fragment_results`
- `root_fragment_id`

升级为：

- `fragment_results`
- `root_fragment_id`
- `edges`

第一版允许暂时保留 `FragmentBuildResult` 中的 `cte_id` / `cte_exchange_nodes` 字段用于兼容已有测试，但新逻辑必须以 `edges` 为主，旧字段只作为过渡兼容层。

### 6.3 Builder 内部引入 fragment 上下文

`PlanFragmentBuilder` 需要从“收集单棵 plan node 列表”升级为“按 fragment 上下文递归构造”。

推荐新增内部结构：

```rust
struct FragmentFrame {
    fragment_id: FragmentId,
    plan_nodes: Vec<plan_nodes::TPlanNode>,
    cte_exchange_nodes: Vec<(CteId, i32)>,
}
```

设计意图：

- 普通 visitor 在当前 `FragmentFrame` 内继续构造 plan node
- `visit_distribution()` 新建 child frame
- child frame 完成后固化为 `FragmentBuildResult`
- 当前 frame 只接收一个 `ExchangeNode` 作为 child 输入

这样普通算子仍保持“只消费逻辑 child 结果”的编程模型，不需要理解 fragment 边界细节。

### 6.4 Exchange 节点与分区类型

`src/sql/physical/nodes.rs` 中的 `build_exchange_node()` 不再固定写死 `UNPARTITIONED`，改为显式接收 partition 类型或 distribution 信息。

第一版映射规则：

- `DistributionSpec::Gather` -> `UNPARTITIONED`
- `DistributionSpec::HashPartitioned(cols)` -> `HASH_PARTITIONED`，并由 builder 基于 child 输出 scope 把 `cols` 编译成 `partition_exprs`
- `DistributionSpec::Any` 不应作为已提取的 `PhysicalDistribution` 输入；若出现，builder 直接报错

## 7. PlanFragmentBuilder 行为定义

### 7.1 visit_distribution

`visit_distribution()` 的第一版行为固定为：

1. 校验输入只有 1 个 child
2. 根据当前 distribution 为 child 新建上游 fragment
3. 递归构建 child 子树到该上游 fragment
4. 为当前 fragment 分配一个 `ExchangeNode`
5. 记录 `FragmentEdge`：
   - `source_fragment_id = child fragment`
   - `target_fragment_id = current fragment`
   - `target_exchange_node_id = exchange node id`
   - `output_partition = distribution 对应的数据分区`
   - `edge_kind = Stream`
6. 返回 exchange node 对应的 scope / tuple 信息给父算子

### 7.2 visit_cte_produce / visit_cte_consume

CTE 路径不再独立定义一套编排模型，而是映射到通用框架：

- `visit_cte_produce()`：固化 producer fragment，并标记为 `CteMulticast`
- `visit_cte_consume()`：在消费者 fragment 中创建 exchange node
- coordinator 根据 `CteMulticast` edge 生成 `TMultiCastDataStreamSink`

这意味着 CTE 只是通用 edge 模型上的一个特殊 sink wiring，而不是单独的多 fragment 体系。

## 8. ExecutionCoordinator 泛化

`src/standalone/coordinator.rs` 从“CTE query execution”泛化为“通用多 fragment execution”。

推荐拆分为 4 组 helper：

1. `assign_instance_ids`
   - 为所有 fragment 统一分配 query id / fragment instance id

2. `wire_stream_edges`
   - 根据 `Stream` edges 为 source fragment 构造 `TDataStreamSink`
   - 为 target fragment 填充 `per_exch_num_senders`

3. `wire_cte_multicast_edges`
   - 根据 `CteMulticast` edges 为 producer fragment 构造 `TMultiCastDataStreamSink`
   - 为所有 consumer exchange node 填充目的地

4. `launch_fragments`
   - 非 root fragment 先启动
   - root fragment 最后启动并收集结果
   - 第一版可按拓扑顺序或“所有非 root 后台启动 + root 前台执行”的保守方式实现

## 9. 错误处理

第一版要求 fail fast，不做猜测性修复：

- `PhysicalDistribution` child 数量不是 1：报错
- `DistributionSpec::Any` 落到 builder：报错
- `HashPartitioned` 缺失 hash 列：报错
- edge 引用不存在的 fragment 或 exchange node：报错
- CTE produce 没有 consumer：继续报错，与当前行为保持一致
- 同一个普通 exchange node 被第一版不支持的复杂拓扑复用：明确报错，而不是静默退化

## 10. 分阶段实现顺序

建议按以下顺序推进：

1. 改 `fragment_builder` 返回模型，引入 `FragmentEdge`
2. 实现 `visit_distribution()` 的真实切分，仅支持 `Gather` / `HashPartitioned`
3. 泛化 `ExecutionCoordinator`，先让普通 stream edges 跑通，再挂接 CTE multicast
4. 补 builder / explain / integration 验证

这个顺序优先稳定数据结构和编排边界，避免先改 coordinator 导致逻辑继续围绕 CTE 特化。

## 11. 第一版验收标准

以下标准全部满足后，本子项目视为完成：

1. 一个包含真实 `PhysicalDistribution` 的非 CTE 查询，`build_result.fragments.len()` 明显大于 1。
2. 至少一个普通 join 查询能够通过通用 coordinator 跑通，不依赖 CTE 特例路径。
3. 现有 single-use / multi-use CTE Cascades 测试不回退。
4. `EXPLAIN` 中的 `GATHER EXCHANGE` / `HASH EXCHANGE` 与真实 fragment 切分语义一致。
5. 普通 stream edge 和 CTE multicast edge 共用同一套 fragment/edge 编排框架。

以下不作为第一版完成条件：

- `AggSplit`
- 去掉旧 `join_reorder`
- `TPC-DS 99/99`
- 更复杂的普通 exchange fan-in / fan-out 拓扑

## 12. 预期收益

完成本子项目后，Cascades 路径会首次形成完整的：

`Distribution property -> PhysicalDistribution -> fragment split -> exchange wiring -> multi-fragment execution`

这条链路。它是后续继续推进 `AggSplit`、更完整 join 枚举、以及最终替换 legacy optimizer 的关键前置条件。
