# OQ-5 Runtime Filter Optimizer Wiring Design

日期：2026-06-01

来源：`NovaRocks Roadmap.md` → Optimizer Plan Quality Roadmap，OQ-5「Runtime filter optimizer wiring」。

前置 spec：

- `docs/design/specs/2026-04-05-standalone-runtime-filter-design.md`
  —— v1：standalone RF 生成 + coordinator 参数组装（产出了今天的
  `runtime_filter_planner.rs`）。本设计是它的 v2 延续。
- `docs/design/specs/2026-04-08-agg-topn-runtime-filter-design.md`
  —— TopN / Agg `MinMax` runtime filter（`TOPN_FILTER`，另一 feature）。
  **明确不在 OQ-5 范围**；OQ-5 只做 join `JOIN_FILTER`。

StarRocks 对齐出处：

- `fe/fe-core/.../planner/JoinNode.java::buildRuntimeFilters()`（build 侧 gating）
- `fe/fe-core/.../planner/RuntimeFilterDescription.java::canProbeUse()`（probe 侧 selectivity gate）
- `fe/fe-core/.../planner/PlanNode.java::pushDownRuntimeFilters()`（递归下推）
- `fe/fe-core/.../qe/SessionVariable.java`（`global_runtime_filter_*` 变量）

---

## 0. TL;DR

OQ-5 在 roadmap 里被描述为「runtime filter 完全没接，optimizer 没生成
RuntimeFilter operator」。**代码核实结论：这一描述是过时的。** RF 自 2026-04
的 v1 工作起就已经端到端生成并执行：

- 生成：`runtime_filter_planner.rs`（codegen 后处理 thrift 节点）
- 组装：`coordinator.rs::setup_runtime_filter_params`
- 执行：`runtime_filter_hub` + hash join build sink 发布 + scan probe 应用

roadmap 之所以记成「空白」，是因为 **EXPLAIN 从不打印 RF**，PR #200 的 plan
对比看不到任何 RF 行。

因此 OQ-5 的真实工作不是「从零接 RF」，而是把 v1 升级到 StarRocks parity：

1. **EXPLAIN 可见性**（解锁 roadmap 自己的验收 gate + golden）
2. **基于 cardinality 的 gating**（v1 对每个 eligible equi-conjunct 一律生成）
3. **递归 probe 下推**（v1 只定位直接拥有 tuple 的 scan，不穿透 project/agg/exchange）
4. **复刻 session 变量**（gating 阈值可调、与 StarRocks 对成）
5. **跨 exchange 下推**（多 BE 分布式场景）

核心架构动作：把 RF planning 从「codegen 后的 thrift 后处理」**上移为
`PhysicalPlanNode` 物理树 pass**，使 EXPLAIN 与 codegen 共享同一标注（单一事实源）。

---

## 1. 现状盘点（v1 已有 / 缺什么）

| 环节 | v1 现状 | 位置 |
|---|---|---|
| RF 描述符生成 | ✅ 有，codegen 后处理 thrift 节点 | `src/sql/optimizer/runtime_filter_planner.rs:33`，被 `src/sql/codegen/fragment_builder.rs:305` 无条件调用 |
| 回填 join 节点 | ✅ `hj.build_runtime_filters = Some(...)` | `runtime_filter_planner.rs:184` |
| coordinator 组装 params | ✅ `setup_runtime_filter_params` | `src/runtime/coordinator.rs:156` / `:484` |
| 下发 fragment 执行 | ✅ `dispatcher` 传 `runtime_filter_params` | `src/runtime/dispatcher.rs:747` |
| build sink 发布 / scan probe | ✅ 经 `runtime_filter_hub` | `src/exec/operators/hashjoin/hash_join_build_sink.rs`、`src/runtime/runtime_filter_hub.rs` |
| IN-vs-Bloom 类型选择 | ✅ BE 运行时按 `build_row_count` 决定 | build sink `:346` / `:480` / `:487` |
| **cardinality gating** | ❌ 无任何 gating，全量生成 | `runtime_filter_planner.rs:74` 循环 |
| **递归 probe 下推** | ❌ 只 `scan_tuple_owners` 直接定位 | `runtime_filter_planner.rs:86` |
| **跨 exchange 下推** | ❌ 仅 `has_remote_targets` 标记，无穿透逻辑 | `runtime_filter_planner.rs:91` |
| **EXPLAIN 显示 RF** | ❌ join 行无 RF 子行 | `src/sql/explain.rs:464` |
| **gating 的 session 变量** | ❌ 无 | — |

> 路径迁移提示：v1 spec 写的 `src/sql/cascades/` 现为 `src/sql/codegen/`，
> `src/standalone/coordinator.rs` 现为 `src/runtime/coordinator.rs`。

---

## 2. 目标与范围

### In scope（本 roadmap 任务终态）

- RF planning 上移为 `PhysicalPlanNode` 物理树 pass（替换 thrift 后处理）。
- EXPLAIN VERBOSE / COSTS 显示 `build runtime filters:` / `probe runtime filters:`。
- StarRocks 三层 gating（build-max / build-min·probe-min / selectivity）。
- 递归 probe 下推：穿透 Project / Filter / Aggregate，并跨 Exchange / Set-op。
- 复刻 4 个 `global_runtime_filter_*` session 变量 + master 开关。
- golden plan 锁形状 + RF on/off 等价性 fixture + join suite wall_time 验证。

### Out of scope（拆后续 / 不做）

- **TopN / Agg `TOPN_FILTER`**（见 2026-04-08 spec，另一 feature）。
- **Filter 类型选择的 optimizer 化**：IN-vs-Bloom 是 BE 运行时决策（与
  StarRocks 一致），optimizer 不做。OQ-5 仅**核对**该阈值并确认 min/max
  filter 是否生成（见 §9）。
- **Skew join RF / broadcast speculative delivery / 多实例 partial merge**：
  沿用 v1 单实例简化（`builder_number=1`，merge node=self）。

### 验收量化（roadmap）

- 三条标杆 query（`join_one_key` q22、`join_linear_chained` q31、简单 INNER
  `count(*)`）的 `EXPLAIN` 在 build/probe RF 维度与 StarRocks 收敛。
- join suite `-j 1 --mode verify` wall_time 相对 Stage 0 基线下降（roadmap
  预估 OQ-5 单独 -20% ~ -40%）。

---

## 3. 架构决策：物理树 pass（方案 A）

### 为什么从 thrift 后处理上移

v1 把 RF planning 放在 codegen 后的 thrift 节点上。但 OQ-5 的两项核心需求都
要求**在物理树层**做决策：

- **gating 需要 cardinality**：thrift `TPlanNode` 不带 stats；
  `PhysicalPlanNode.stats.output_row_count` 才有（`src/sql/optimizer/physical_plan.rs:9`）。
- **EXPLAIN 要显示 RF**：EXPLAIN 渲染的是 `PhysicalPlanNode` 树
  （`src/sql/explain.rs:331 explain_physical_plan`），不是 thrift。

`optimize()` 末尾 `extract_best` 产出 `PhysicalPlanNode` 树
（`src/sql/optimizer/mod.rs:145`），EXPLAIN 与 codegen
（`PlanFragmentBuilder::build`，`src/engine/mod.rs:2787`）消费**同一棵树**。
把 RF pass 插在二者之前，标注一次、双方共享 —— 单一事实源，且与 StarRocks
「RF 挂在 PlanNode 上、EXPLAIN 打印、thrift 序列化」的模型一致。

### 数据流

```text
analyzer → optimize() ──extract_best──> PhysicalPlanNode 树
                                              │
                          【新增】RuntimeFilterPlanner::annotate(&mut tree, &opts)
                            · 资格判定 + build gating（用 build child stats）
                            · 每 equi-conjunct 递归 probe 下推（couldBound + canProbeUse）
                            · 标注 join.build_runtime_filters / node.probe_runtime_filters
                                              │
                ┌─────────────────────────────┴─────────────────────────────┐
        explain_physical_plan                                  PlanFragmentBuilder::build
        渲染 build/probe RF 行                          从标注生成 thrift TRuntimeFilterDescription
                                                        + probe specs；fragment 切分时定 has_remote
                                                                              │
                                                  coordinator.setup_runtime_filter_params
                                                  → dispatcher → 执行侧（hub/build sink/scan probe，不变）
```

### 关键不变量

- **执行侧零改动**：`TRuntimeFilterDescription` 形状、`runtime_filter_hub`、
  build sink、scan probe、gRPC transmit 全部不动。OQ-5 只换「喂数据的人」。
- `src/sql/optimizer/runtime_filter_planner.rs`（thrift 后处理）被新物理树
  pass 取代；其 thrift 描述符构造逻辑迁到 codegen 的「标注消费」端复用。
- `coordinator.rs::setup_runtime_filter_params` 保留，输入改为从标注派生的
  `RuntimeFilterPlanResult`。

---

## 4. 数据模型

在 `PhysicalPlanNode` 上加两个标注字段（RF 可落在任意节点，故挂通用节点，
对齐 StarRocks `PlanNode` 同时持 build/probe 两表）：

```rust
// src/sql/optimizer/physical_plan.rs
pub(crate) struct PhysicalPlanNode {
    pub op: Operator,
    pub children: Vec<PhysicalPlanNode>,
    pub stats: Statistics,
    pub output_columns: Vec<OutputColumn>,
    pub build_runtime_filters: Vec<RuntimeFilterDesc>,   // 新增：本 join 构建
    pub probe_runtime_filters: Vec<RuntimeFilterProbe>,  // 新增：本节点消费
}
```

optimizer 侧中间表示（codegen 翻成 thrift）：

```rust
pub(crate) struct RuntimeFilterDesc {
    pub filter_id: i32,
    pub build_expr: TypedExpr,              // eq.right（build 侧）
    pub probe_expr: TypedExpr,              // eq.left（probe 侧，下推时按节点 remap）
    pub expr_order: usize,                  // eq_conjunct 索引
    pub build_join_mode: JoinDistribution,  // Broadcast / Shuffle / Colocate
    pub build_cardinality: f64,             // build child output_row_count
    pub probe_target_node_id: i32,          // 下推命中的最深节点
    pub partition_by_exprs: Vec<TypedExpr>, // 多键 partitioned 用
}

pub(crate) struct RuntimeFilterProbe {
    pub filter_id: i32,
    pub probe_expr: TypedExpr,              // 本节点坐标系下的 probe 表达式
}
```

**不含** IN/Bloom/MinMax 类型 —— 那是 BE 运行时决策（§9）。

---

## 5. RF planning 算法

照抄 StarRocks `JoinNode.buildRuntimeFilters` + `PlanNode.pushDownRuntimeFilters`。
自顶向下遍历物理树，对每个 hash join：

### (a) 资格

- join 类型 ∈ {INNER, LEFT_SEMI, RIGHT_OUTER, RIGHT_SEMI, RIGHT_ANTI, CROSS}
  （沿用 v1 `is_rf_eligible_join_op`）。
- 仅 equi-conjunct；跳过 null-safe（`<=>`）键。

### (b) build 侧 gating（`JoinNode.java:173`）

- 仅 PARTITIONED / SHUFFLE 模式查：`build_card ≤ 0 || build_card > build_max_size`
  → 整个 join 跳过 RF。
- Broadcast / Colocate 不查。
- build cardinality = join 的 **build child（右 child）** `stats.output_row_count`。

### (c) 每 equi-conjunct 生成 + 递归 probe 下推

从 join 的 probe child（左 child）向下递归，每个节点判定：

- `couldBound`：probe expr 的 slots ⊆ 本节点 `output_columns`？
- `canProbeUse`（`RuntimeFilterDescription.java:233` selectivity gate）：
  - 本地 fragment 内：直接收。
  - 跨 exchange：`build_card ≤ build_min_size` 直接收；
    `probe_card < probe_min_size` 拒；
    否则 `build_card / probe_card ≤ 1 − probe_min_selectivity` 才收。
- 落在「能 bound 且过 gate 的**最深**节点」：穿透 Project / Filter / Aggregate；
  跨 Exchange / Set-op 时穿过对应物理节点（probe expr 按子节点输出列 remap）。
- 命中后在该节点 `probe_runtime_filters` 加一条，在 join `build_runtime_filters`
  加一条；`probe_target_node_id` 记命中节点 id。

### (d) 朝向 correctness（必查）

v1 假设 `build = eq.right / probe = eq.left`。实现 Stage 0 先验证「右 child =
build 侧」对所有分布式模式成立（broadcast / shuffle / colocate）。不成立则先修
朝向，否则 RF 会建在错误一侧。

---

## 6. Gating 阈值 + session 变量

复刻 StarRocks 4 个 session 变量（`SessionVariable.java`），默认值对齐：

| 变量 | 默认 | 作用 |
|---|---|---|
| `global_runtime_filter_build_max_size` | 64MB | build 端过大不建 |
| `global_runtime_filter_build_min_size` | 128KB | build 端足够小则 probe 无条件收 |
| `global_runtime_filter_probe_min_size` | 100KB | probe 端太小不收 |
| `global_runtime_filter_probe_min_selectivity` | 0.5 | selectivity 阈值 |

- size 用 `output_row_count × Statistics::avg_row_size`（`statistics.rs:49` 已有）
  近似字节。
- 变量接到 session（参考 `src/sql/optimizer/options.rs` 与现有 session 变量管线）。
- **master 开关**：RF pass 不是 Cascades transformation/implementation rule
  （它在 `extract_best` 之后跑），因此**不进 explore/implement 循环**；而是
  pass 自身在入口处调用 `OptimizerOptions::is_enabled("RuntimeFilterPushDown")`
  决定是否执行，规则名加入 `mod.rs::is_known_rule_name`。这样
  `SET disable_optimizer_rules = 'RuntimeFilterPushDown'` 能关闭它（对齐
  roadmap bisection 原则 + 其他 OQ rule 的开关体验）。若已存在等价的
  `enable_runtime_filter` 类 session 变量，一并尊重。

---

## 7. EXPLAIN 输出

在 `src/sql/explain.rs` join 行（`:464`）后、scan/中间节点行后，新增缩进子行
（VERBOSE / COSTS 级别）：

```text
HASH JOIN (BROADCAST, INNER JOIN, eq: [...])
    build runtime filters:
    - filter_id = 0, build_expr = (w1.c), remote = false
...
    SCAN opt_probe.t1
        probe runtime filters:
        - filter_id = 0, probe_expr = (t1.c)
```

措辞贴近 StarRocks `EXPLAIN VERBOSE`，便于 PR diff。golden 用
`-- @explain_contains=build runtime filters` / `-- @explain_contains=filter_id = 0`。

---

## 8. codegen 集成

- `visit_hash_join`：读 `node.build_runtime_filters` 标注 → 生成 thrift
  `THashJoinNode.build_runtime_filters`（复用 v1 `TRuntimeFilterDescription::new`
  构造逻辑，含 layout / join_mode 映射）。
- `visit_scan` / `visit_project` / `visit_agg` 等：读 `node.probe_runtime_filters`
  → 生成 thrift probe specs。
- **删除** `fragment_builder.rs:305` 的 thrift 后处理调用。
- `has_remote_targets` 在 fragment 切分时最终敲定：物理树 pass 阶段尚未分
  fragment，pass 只标注「probe 目标节点」；codegen 切分后比较 build join 与
  probe target 是否落在不同 fragment 来定 remote。
- `coordinator.rs::setup_runtime_filter_params` 仍消费结果（prober/builder/
  merge-nodes 组装不变），输入改为从标注派生的 `RuntimeFilterPlanResult`。

---

## 9. Filter 类型（核对项，非 optimizer 工作）

已确认 IN-vs-Bloom 在 BE 运行时由 `build_row_count` vs
`MAX_RUNTIME_IN_FILTER_CONDITIONS` 决定（build sink `:346` / `:480`），与
StarRocks（FE 不管类型，BE 按行数选，默认阈值 1024）一致。OQ-5 仅需：

1. 核对 `MAX_RUNTIME_IN_FILTER_CONDITIONS == 1024`（StarRocks 默认）。
2. 确认 join RF 的 min/max filter 是否生成；min/max 执行基建已由 2026-04-08
   TopN RF 工作落地（`RuntimeMinMaxFilter`、hub 支持）。若 join 路径未生成
   min/max，则在 build sink 补生成 + thrift 标记（小改）。

optimizer 侧不做类型选择。

> 核对结论(2026-06-02,Stage 4):`MAX_RUNTIME_IN_FILTER_CONDITIONS == 1024`
> (`src/exec/runtime_filter/merger.rs:123`)= StarRocks 默认 ✅。join build sink
> 生成 **IN(≤1024 行)+ membership(bloom/bitset)**,与 StarRocks 主路径
> (bloom + in-list)一致;**min/max 不为 join 生成**(它服务 TopN/Agg,见
> 2026-04-08 spec),这同样符合 StarRocks。结论:filter 类型选择已对齐,无需改动。

---

## 10. 分阶段 PR 计划

| Stage | 内容 | 验收 |
|---|---|---|
| **0 baseline** | 实测现有 RF 是否生效（join q22 EXPLAIN ANALYZE 行数 + wall_time）；验证 §5(d) build 朝向；记录基线 | 基线数据 + 朝向结论 |
| **1 core** | 物理树 RF pass + `PhysicalPlanNode` 标注 + EXPLAIN 显示 + **fragment 内**下推 + gating（常量阈值）+ codegen 改接 + 删 thrift 后处理 | 3 标杆 query EXPLAIN 显示 RF 且与 StarRocks 收敛；golden 落地；join/cte/filter suite 无回归 |
| **2 session vars** | 4 个 `global_runtime_filter_*` 变量 + master 开关 | `SET` 可调；disable rule 生效；golden 覆盖关闭态 |
| **3 cross-exchange** | probe 穿透 Exchange / Set-op 到下游 fragment scan | 多 BE / shuffle plan 下 RF 落底层 scan；`has_remote_targets` 正确 |
| **4 filter-type 核对** | 阈值核对 + min/max 补齐（若缺） | BE 类型选择 = StarRocks |
| **5 验证收口** | join suite `-j 1` wall_time + 三套 golden 锁定 | wall_time 较 Stage 0 下降；记录进 roadmap 进度区 |

每个 Stage 独立成 PR，独立可验收。Stage 1 是最大头（交付可见的核心）。

---

## 11. 错误处理 / fail-fast

遵循 `CLAUDE.md` 非协商规则（fail fast，不掩盖语义缺口）：

- RF 无法安全构建（不支持类型、null-safe 键、probe expr 无法 bound 到任何
  scan）→ **显式跳过该 RF**（非 query 失败 —— RF 是优化不是正确性前提），
  记可观测 debug 日志（跳过原因 + filter_id + join node）。
- 不引入隐式 full-scan fallback；gating 决策可观测（哪些 join 因 build 过大被
  跳过、哪些 probe 因 selectivity 被拒）。

---

## 12. 测试策略

- **plan golden**：`sql-tests/optimizer/runtime_filter_*.sql`，`@explain_contains`
  锁 build/probe RF 形状（含关闭态 golden）。
- **正确性 fixture**：同一 query RF on/off 结果 **byte-identical**（RF 只减不改
  语义）；覆盖 INNER / LEFT_SEMI / RIGHT_*；空 build 端；NULL 键；多 equi-key。
- **标杆对比**：三条 query `EXPLAIN` + `EXPLAIN COSTS` vs StarRocks（FE 9030 /
  Nova `$NOVA_ENV_MYSQL_PORT`，见 `starrocks-fe-on-novarocks` skill），diff 入 PR。
- **wall_time**：每 Stage 后 join suite `-j 1 --mode verify`，对比 Stage 0 基线。
- **回归 gate**：join / cte / aggregate / filter suite 无回归。

---

## 13. 关键文件 / seam 速查

| 用途 | 文件:行 |
|---|---|
| 物理树节点（加标注字段） | `src/sql/optimizer/physical_plan.rs:9` |
| optimize 产出物理树 | `src/sql/optimizer/mod.rs:145`（`extract_best`） |
| 新 RF pass 入口（插在 optimize 后） | `src/sql/optimizer/mod.rs`（extract_best 之后） |
| 被取代的 v1 thrift 后处理 | `src/sql/optimizer/runtime_filter_planner.rs`、调用点 `fragment_builder.rs:305` |
| EXPLAIN join 格式化 | `src/sql/explain.rs:464`、物理树入口 `:331` |
| codegen join → thrift | `src/sql/codegen/fragment_builder.rs::visit_hash_join`、`src/sql/codegen/nodes.rs:385`（`build_runtime_filters` seam） |
| coordinator RF 组装 | `src/runtime/coordinator.rs:156` / `:484` |
| 物理 hash join 算子 | `src/sql/optimizer/operator.rs:301`（`PhysicalHashJoinOp`） |
| JoinToHashJoin 朝向 | `src/sql/optimizer/cascades_rules/implement.rs:605` |
| build sink 类型选择 | `src/exec/operators/hashjoin/hash_join_build_sink.rs:346` / `:480` |
| stats / cardinality | `src/sql/optimizer/stats.rs`、`statistics.rs:49`（`avg_row_size`） |
| 规则开关 | `src/sql/optimizer/options.rs` |
| thrift RF 描述符 | `idl/thrift/RuntimeFilter.thrift`（`TRuntimeFilterDescription`） |

---

## 14. Correctness 风险

1. **build/probe 朝向**（§5d）：planner 假设右 child = build。Stage 0 必查。
   - 验证(2026-06-01,Stage 0):确认 children[1]=build 侧,build_expr=eq.right。证据:src/exec/pipeline/builder.rs:934-938（`probe_is_left=true`,`probe_build=left_build`,`build_build=right_build`）及 src/lower/node/hash_join.rs:174-187（`probe_keys←cond.left/left.layout`,`build_keys←cond.right/right.layout`）。
2. **跨 exchange remap**：probe expr 穿过 Exchange / Project 时必须按子节点输出
   列正确 remap，否则 probe 落错 slot。
   - 结论(2026-06-02,Stage 3):`PhysicalDistribution { spec }` 是纯数据搬运、
     无投影 → **跨 exchange 列 column_id 不变,无需 remap**;`could_bound` 在
     exchange 之下仍按 column_id 正确判定。仅穿越 `HashPartitioned`,Gather/Any
     仍为硬边界。
3. **gating 与执行侧一致**：optimizer 用 estimated cardinality 决定建不建 RF；
   BE 用 actual build_row_count 选类型。二者阈值需协调（optimizer 的
   build-min/probe-min 与 BE 的 `MAX_RUNTIME_IN_FILTER_CONDITIONS` 不冲突）。
4. **单实例简化边界 / 多 BE 隐患**：
   - 结论(2026-06-02,Stage 3 review):standalone 每 fragment **恰好 1 实例**,
     且 fragment 间数据 **all-to-one UNPARTITIONED**(codegen 的 HashPartitioned
     output_partition 协调器不读)→ build 侧拿全量 → **RF 完整** → 跨 exchange
     应用到未分区 probe scan **正确**。本地 DOP partial 在远程发送前已 merge。
   - **已全局禁用 cross-exchange(2026-06-03,rebase 到 D2 multi-BE #209 后)**:
     rebase 后发现 cross-exchange placement 在两种场景都有正确性 bug —— (1) **multi-BE**:
     build fragment fan-out 成 N>1 实例,每实例只产部分 RF,跨 exchange 应用到未分区
     probe scan 误删行,实测触发 exchange wire-meta 错误(`cross_process_two_be_multi_fragment`);
     (2) **standalone**:RF 跨 exchange 推过 OUTER join 到 outer 侧 scan,误删 OUTER /
     `OR ... IS NULL` 应保留的 null-key 行(`join_full_outer_with_using` query 64,
     semi-join 的 RF 越过 FULL OUTER 删掉了 `(NULL,NULL)` 行)。修复:
     `OptimizerOptions::allow_cross_exchange_rf` 默认 **false**(flag-off),
     `push_probe_down` 始终不跨 exchange → probe RF 回退 within-fragment(= main 安全
     行为,两个回归 case 均恢复 PASS)。Stage 3 的 placement 代码与 flag 保留,待后续
     stage 修正确性(probe 不跨 OUTER、multi-BE RF merge 反映真实实例数)后再开启。
     守门见 `runtime_filter_pass.rs::distribution_is_crossable` 与 `push_probe_down`。

---

## 15. StarRocks 参考出处

| OQ-5 部分 | StarRocks 出处 |
|---|---|
| 生成时机（物理 plan 后处理 + 递归下推，非 Cascades rule） | `PlanFragmentBuilder.buildRuntimeFilters` + `PlanNode.pushDownRuntimeFilters` |
| build 侧 gating | `JoinNode.java:173`（`buildMaxSize` 判定） |
| probe 侧 selectivity gate | `RuntimeFilterDescription.java:233`（`canProbeUse`） |
| 跨 exchange 可穿透判定 | `RuntimeFilterDescription.java:449`（`canPushAcrossExchangeNode`） |
| session 变量 | `SessionVariable.java`（`globalRuntimeFilter*`） |
| 描述符序列化 | `RuntimeFilterDescription.toThrift` |

---

## 16. PR 自检（对齐 roadmap OQ checklist）

每个 Stage PR 开始前回答：

1. 服务 OQ-5 哪个 Stage？
2. PR 描述是否写明对应 StarRocks 出处（文件 + 类名 + 函数）？
3. 三条标杆 query 的 `EXPLAIN` / `EXPLAIN COSTS` 是否与 StarRocks 在本 Stage
   关注维度收敛？plan diff 入 PR。
4. 是否在 `sql-tests/optimizer/` 加了 `@explain_contains` golden？
5. 是否跑 join suite `-j 1 --mode verify`？wall_time 较上一基线降多少？更新
   roadmap 进度区。
6. 是否引入新 logical / physical operator？（本设计预期**不引入** —— RF 是
   节点标注，非新算子。）
7. cte / join / aggregate / filter suite 是否无回归？
