# OQ-10 Runtime Filter Hardening 设计

## 1. 背景

OQ-10 的目标不是从零接入 runtime filter（RF）。OQ-5 已经完成 optimizer RF
wiring、EXPLAIN 展示、codegen lowering、hash join build 发布和 scan/probe 消费
路径。OQ-8 和 OQ-9 之后，NovaRocks 已具备 distribution-aware physical search 和
更稳的 residual predicate placement，适合继续补复杂 join shape 下的 RF 覆盖面。

当前缺口集中在四类：

1. semi/anti/null-aware anti/outer join 的 RF eligibility 不够精细；
2. join reorder、child swap、eq key orientation 变化后，RF build/probe expr 容易依赖
   codegen 兜底；
3. project、derived table、set-op、decode、low-cardinality rewrite 等边界上的 probe
   slot 追踪不完整；
4. OQ-5 Stage 3 的 cross-exchange probe 下推因两个正确性缺陷全局禁用：
   multi-BE partial RF 漏行，以及跨 OUTER join null-producing 侧误删必须保留的
   NULL-key 行。

本设计选择完整覆盖 OQ-10 的设计边界，但分阶段交付。cross-exchange 采用保守恢复：
只允许 RF 完整性可证明的 build 来源跨 exchange，不把 partitioned multi-BE partial
RF merge 作为本轮目标。

## 2. 目标

- 刷新 StarRocks FE 与 NovaRocks 的 plan diff 基线，替代当前工作树中缺失的旧
  `20260603-fe-nr-plan-diff` 产物。
- 提升 fragment 内 RF coverage，覆盖 semi/anti、orientation flip、project/derived/
  set-op/decode remap、多跳传播、重复 RF 去重和 cost gate。
- 保守恢复 cross-exchange RF：只在 broadcast/global-complete build 场景跨 exchange，
  并加 outer/null-producing boundary guard。
- 通过代表 query 精确验收：`tpc-h/q4`、`tpc-h/q22`、`tpc-ds/q41`、`tpc-ds/q72`
  出现预期 RF 形态，`ssb` 13 条 query 不回退。
- 为两个历史 cross-exchange bug 增加回归保护。

## 3. 非目标

- 不重新设计 RF runtime 数据结构。
- 不实现 partitioned multi-BE partial RF 的 global merge 通路。
- 不声称所有 StarRocks FE RF 形态完全等价；验收聚焦代表 query、方向、安全性和结果正确性。
- 不为所有 join 无条件生成 RF；低收益、高风险或无法证明安全的路径应跳过或退回
  build-only descriptor。
- 不把基线采集脚本和 optimizer 语义混在一起。

## 4. 总体架构

### Stage 0: 刷新基线

使用本机 `${STARROCKS_ROOT}` 当前分支作为 StarRocks FE 参考，按
`starrocks-fe-on-novarocks` 联调规则构建、部署和运行：

- `STARROCKS_ROOT`、`NOVAROCKS_ROOT`、`FE_RUNTIME_ROOT`、`BE_RUNTIME_ROOT` 等路径
  从环境变量或 skill 默认规则取得；
- FE 端口和 BE heartbeat 端口从配置读取，不硬编码；
- FE 部署时复制全部 jar，不只复制 `starrocks-fe.jar`；
- 涉及本地服务和 Iceberg/MinIO 时禁用代理；
- Codex 桌面端需要长时间验证时，优先用持久 PTY 托管 FE。

Stage 0 生成新的 plan-quality 目录：

```text
logs/plan-quality/<date>-fe-nr-plan-diff/
  fe/
    tpc-h__q4.out
    tpc-h__q22.out
    tpc-ds__q41.out
    tpc-ds__q72.out
    ssb__q*.out
  nr/
    tpc-h__q4.out
    tpc-h__q22.out
    tpc-ds__q41.out
    tpc-ds__q72.out
    ssb__q*.out
  status/
    aggregate_summary.json
    representative_queries.md
```

这些产物只用于观测和验收，不影响 optimizer 行为。

### Stage 1: Fragment 内 RF hardening

主体改动在 `src/sql/optimizer/runtime_filter_pass.rs`。当前 pass 假设
`children[0] = probe`、`children[1] = build`，并把 eq `left/right` 当成 probe/build
标签。OQ-10 需要把这层隐式假设改成显式语义模型：

- join 语义判定：join kind、preserved side、输出 side、可提前过滤 side；
- key orientation：通过 `ColumnId` 判定每个 eq key 实际绑定哪个 child；
- descriptor 生成：按真实 build/probe expr 生成 `RuntimeFilterDesc`；
- probe placement：在 fragment 内把 probe 下推到最深安全 target；
- dedup/gating：保持现有 size/selectivity gate，增加 query 级上限和稳定去重。

`src/sql/codegen/fragment_builder.rs` 继续把 optimizer descriptor 降到
`TRuntimeFilterDescription`，但 orientation 兜底只作为防御性校验，不再是主路径。

### Stage 2: 保守恢复 cross-exchange

`allow_cross_exchange_rf` 的语义从“允许穿过 shuffle exchange”改为“启用保守
cross-exchange 规则”。默认开启的前提是：

- build 来源是 broadcast 或显式 global-complete；
- partitioned multi-BE build 没有 global merge 标记时视为 partial RF，不跨 exchange；
- 下推路径不能越过 outer/null-producing boundary；
- `Gather` / `Any` exchange 仍是硬边界；
- 两个历史 bug 的回归测试通过。

Stage 2 不实现 partial RF merge，只为未来 global merge 留出清晰接口。

### Stage 3: 验证收口

重跑 targeted Rust tests、optimizer RF plan golden、runtime-filter suite、代表 query
plan diff。确认代表 query RF presence 达标、`ssb` 不回退、cross-exchange 两个历史
正确性问题不复现。

## 5. Optimizer 组件边界

`runtime_filter_pass.rs` 内部拆成几类纯函数，减少跨阶段耦合。

### 5.1 Join side model

新增内部模型描述每个 physical hash join 的 RF 语义：

```text
JoinRfSides {
  build_child_index,
  probe_child_index,
  preserved_children,
  output_children,
  allows_rf,
  reason_if_rejected
}
```

该模型不暴露到 codegen；它只用于 optimizer pass 内部决策。

### 5.2 Eq key orientation

对每个 `PhysicalHashJoinEqCondition`：

- 收集 `left` 和 `right` 的 `ColumnId`；
- 判定表达式是否唯一绑定 child 0 或 child 1；
- 如果一侧跨两个 child，跳过该 key；
- 如果两侧都绑定同一个 child，跳过该 key；
- 如果能唯一绑定 build/probe child，生成真实 `build_expr` 和 `probe_expr`。

这样可避免 join reorder 或 child swap 后把 probe RF 放到错误子树。

### 5.3 Probe placement

probe placement 仍追求“最深安全 target”，但需要显式处理边界：

- filter：不改变 schema，允许继续下推；
- project/derived/decode：只有表达式能按 `ColumnId` 或别名映射完整 remap 时才继续；
- set-op：只有每个 branch 都能建立等价 remap 时才下推到 branch；否则停在 set-op 上方
  或保留 build-only；
- hash exchange：仅在 Stage 2 保守 cross-exchange 规则通过时穿越；
- gather/any exchange：停止；
- outer/null-producing boundary：停止；
- unsupported node：停止或 build-only。

### 5.4 Dedup 与 gate

保留现有 StarRocks 风格阈值：

- `global_runtime_filter_build_max_size`;
- `global_runtime_filter_build_min_size`;
- `global_runtime_filter_probe_min_size`;
- `global_runtime_filter_probe_min_selectivity`;
- `SET disable_optimizer_rules = 'RuntimeFilterPushDown'`。

新增 query 级 RF 数量上限和稳定去重规则：

- 去重 key 建议使用 `(target_node_identity, normalized_probe_expr, build_expr, join_node)`；
- 同一 target/expression 多次出现时保留更深 target 或先生成的稳定项；
- 超限时按 plan traversal 稳定顺序保留，避免 explain 抖动。

## 6. Join 语义规则

RF eligibility 的判断标准是：提前过滤 probe 行是否会改变 join 结果。

### 6.1 允许生成

- `INNER`：安全，保留当前基线行为。
- `LEFT SEMI` / `RIGHT SEMI`：只过滤实际 probe/output side；NULL key 按普通等值 join
  语义处理。
- `RIGHT OUTER`：如果 target 是非 preserved side，过滤安全。
- `RIGHT ANTI`：只有 RF 过滤的是不会作为 anti 输出保留的一侧时允许。
- `CROSS`：仅在实际 physical hash join 有 hash eq key 时沿用当前行为。

### 6.2 禁止或保守跳过

- `FULL OUTER`：默认禁止。
- `LEFT OUTER`：禁止过滤 left preserved side；只有 orientation 后 target 是非 preserved
  side 时才允许。
- `LEFT ANTI`：默认禁止过滤 left anti 输出侧。
- `NULL AWARE LEFT ANTI`：禁用 RF，避免 NULL 语义误删。
- null-safe equi key `<=>`：继续跳过 RF。

这些规则由 join side model 统一表达，不在多个 call site 复制硬编码。

## 7. Codegen 与 runtime 边界

`fragment_builder.rs` 仍负责：

- 根据 `RuntimeFilterDesc` 编译 build expr；
- 查找 probe target；
- 构造 `TRuntimeFilterDescription`；
- 设置 `build_join_mode`、layout、target map、`has_remote_targets`。

OQ-10 后，codegen 不再把 orientation 修正作为常规成功路径。若 optimizer 产生的
descriptor 无法绑定 build scope，codegen 应跳过该 RF 或转 build-only，并留下可观察
原因，而不是尝试改变语义。

执行侧 `src/exec/operators/hashjoin/**`、`src/runtime/runtime_filter_*` 不做结构性重构。
本轮只在 cross-exchange 需要时补目标信息、完整性标记或回归观测。

## 8. Cross-exchange 规则

cross-exchange 放置必须同时满足完整性和语义边界。

### 8.1 完整性

允许跨 exchange 的 RF：

- broadcast build：每个 probe 消费方可获得完整 build key 集；
- future global-complete build：由明确 metadata 标记，不靠猜测。

禁止跨 exchange 的 RF：

- partitioned/shuffle build 且没有 global merge/global-complete 标记；
- multi-BE partial RF；
- target fragment/实例信息不完整；
- 无法证明 build key 集完整的路径。

### 8.2 语义边界

穿越 exchange 前沿 probe 路径检查：

- 不越过 outer join preserved/null-producing side；
- 不越过会保留 unmatched 行的 anti/output side；
- 不越过无法 remap 的 project/set-op/decode；
- 不越过 gather/any exchange。

如果任一检查失败，RF 保持 fragment 内或 build-only。

## 9. 错误处理与可观察性

RF 是优化，不是正确性前提。失败策略：

- child binding 不唯一：跳过该 key；
- remap 不完整：停止下推或 build-only；
- target expr 编译失败：不记录 probe target；
- build/probe gate 不通过：跳过 RF；
- RF 数量超限：稳定裁剪；
- cross-exchange 安全检查失败：不跨 exchange。

可观察性：

- EXPLAIN VERBOSE/COSTS/ANALYZE 显示 build/probe RF；
- 新增或保留 debug 日志记录 skip reason；
- plan-quality summary 汇总代表 query 的 build/probe RF 行。

## 10. 测试策略

### 10.1 Unit tests

扩展 `runtime_filter_pass.rs` 单测：

- semi join 生成 RF；
- anti/null-aware anti guard；
- left/right orientation flip；
- null-safe key 跳过；
- project alias remap；
- exchange 默认不跨；
- conservative cross-exchange flag-on 只跨 broadcast/global-complete；
- outer/null-producing boundary stop；
- dedup 和 query-level limit。

### 10.2 Optimizer plan golden

扩展 `sql-tests/optimizer/sql/runtime_filter_*.sql`：

- semi join RF presence；
- anti join guard；
- orientation flip 后 build/probe expr 正确；
- project/derived alias remap；
- set-op branch remap 或保守停止；
- broadcast cross-exchange probe target；
- partitioned multi-BE partial 不跨 exchange。

使用 `@explain_contains` / `@explain_not_contains` 锁 plan-shape。

### 10.3 Runtime correctness

扩展 `sql-tests/runtime-filter/`：

- semi/anti/null-key 结果与 RF disabled 等价；
- outer join preserved/null-producing 行不被 RF 删除；
- right semi/right anti residual predicate 结果不变；
- RF enabled/disabled 对同一 query 结果一致。

### 10.4 历史 bug 回归

- multi-BE partial RF 场景：`cross_process_two_be_multi_fragment` 或等价回归，确认
  partitioned partial RF 不跨 exchange 且不漏行。
- outer null-key 场景：`join_full_outer_with_using` / q64 风格回归，确认 cross-exchange
  不越过 outer null-producing side。

### 10.5 代表 query 验收

刷新并对比：

- `tpc-h/q4`;
- `tpc-h/q22`;
- `tpc-ds/q41`;
- `tpc-ds/q72`;
- `ssb` 13 条 query。

最终验收不要求 RF 行数完全等于 FE，但要求代表 query 的 RF presence、方向和 target
安全性满足预期，`ssb` 不回退。

## 11. 交付顺序

1. Stage 0：基线刷新流程和产物。
2. Stage 1：fragment 内 RF side model、orientation、remap、dedup/gate。
3. Stage 2：保守 cross-exchange 与历史 bug 回归。
4. Stage 3：验证、plan diff 收口、OQ-10 状态更新。

每个阶段应能独立验证。Stage 1 不依赖 Stage 2 默认开启；Stage 2 不依赖
partitioned global RF merge。

## 12. 验收标准

- 新基线产物存在，包含 FE/NR 原始 explain 和 RF summary。
- `tpc-h/q4`、`tpc-h/q22`、`tpc-ds/q41`、`tpc-ds/q72` 出现设计预期的 RF。
- `ssb` 13 条 query RF coverage 不回退。
- `allow_cross_exchange_rf` 默认语义变为保守规则，且两个历史 bug 有回归保护。
- targeted Rust tests 通过。
- `sql-tests/optimizer` 相关 RF golden 通过。
- `sql-tests/runtime-filter` 通过。
- OQ-10 文档明确说明 partitioned multi-BE global RF merge 是后续项。
