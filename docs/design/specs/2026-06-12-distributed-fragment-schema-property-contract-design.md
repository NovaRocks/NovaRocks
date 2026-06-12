# Distributed Fragment Schema and Property Contract 设计

日期：2026-06-12
状态：Draft，等待评审
背景：PR #295 之后的 1FE+3BE 长期修复设计拆分

## 1. 背景

1FE+3BE CI 暴露出的很多问题在单机路径中不会出现，因为单机绕过了 Exchange、remote
root fetch、partial/global aggregate、distributed join/sort、runtime filter 等边界。
优化器如果只保证 logical equivalence，不显式表达 fragment boundary schema 和 distribution
property，runtime 就只能在边界处猜测和修补。

已有 OQ/G 系列设计在 logical properties、distribution-aware physical search、hash
distribution source 等方面打了基础。本 spec 聚焦把 optimizer physical property 与 runtime
execution schema contract 接起来。

## 2. 目标

- 每个 fragment boundary 都有明确 output execution/transport schema。
- distribution enforcer 显式声明是否改变 column order、slot id、nullable、transport type。
- Exchange、global aggregate、analytic sort、runtime filter 的跨 BE 能力可在 plan 中验证。
- EXPLAIN / SQL golden 能断言 fragment-boundary schema 和 distribution property。
- CI 可将“plan property bug”和“runtime operator bug”分开分类。

## 3. 非目标

- 不在本 spec 中重写 Cascades optimizer。
- 不新增复杂 cost model。
- 不把所有 SQL golden 都改成 plan-shape golden。
- 不改变 FE-compatible thrift plan 的外部语义。

## 4. 模型

```text
FragmentBoundaryContract {
  source_fragment_id,
  target_fragment_id,
  exchange_node_id,
  distribution: DistributionSpec,
  ordering: OrderingSpec,
  output_schema: ExecutionSchema,
  transport_schema: ExecutionSchema,
  capabilities: BoundaryCapabilities,
}

BoundaryCapabilities {
  supports_partial_global_agg,
  supports_merge_sort,
  supports_runtime_filter,
  supports_typed_root_result,
  supports_write_report,
}
```

`output_schema` 是 source fragment 的 execution output。`transport_schema` 是跨 BE
实际传输 schema，可包含 decimal payload widening。

## 5. Optimizer/codegen 职责

Physical planner/codegen 在产生 fragment edge 时必须填充 boundary contract：

- hash distribution keys 对应的 column id / slot id。
- partition/order expression 的 output type。
- source fragment output schema。
- target exchange expected schema。
- 是否需要 partial/global aggregate state layout。

如果 enforcer 插入 projection、exchange、sort、aggregate merge，它必须同时更新 contract。

## 6. Runtime 职责

runtime 使用 boundary contract 校验：

- Exchange sender 输出 schema 与 contract 一致。
- Exchange receiver decode 后 schema 与 target expected schema 兼容。
- global aggregate merge 输入 state layout fingerprint 一致。
- runtime filter build/probe expr type compatible。
- typed root result schema fingerprint 一致。

runtime 不负责修正 optimizer 丢失的 slot/schema 信息。缺失即 fail fast。

## 7. EXPLAIN/观测

新增 verbose/cost/analyze explain 字段：

```text
EXCHANGE_NODE id=...
  distribution=HASH(col#12)
  output_schema=[#12 BIGINT NOT NULL, #13 DECIMAL(38,2)]
  transport_schema=[#12 BIGINT NOT NULL, #13 DECIMAL128(38,2)]
  capabilities=[typed_result, merge_sort]
```

SQL tester 支持 `@explain_contains` 断言关键 fragment-boundary facts，例如：

- global aggregate state schema。
- exchange partition keys。
- transport decimal widening。
- runtime filter target expr type。

## 8. 错误分类

新增错误类别：

- `PlanBoundarySchemaMismatch`
- `PlanBoundaryDistributionMismatch`
- `RuntimeBoundaryPayloadMismatch`
- `UnsupportedDistributedStateMerge`

full CI summary 按类别记录，避免所有 failure 都被归为 result mismatch。

## 9. 落地顺序

1. 在 `MultiFragmentBuildResult` / `FragmentEdge` 中加入 boundary schema metadata。
2. Exchange sender/receiver 记录并校验 contract。
3. Aggregate merge 使用 `AggregateStateLayout` fingerprint。
4. EXPLAIN verbose 输出 boundary contract。
5. SQL tester 增加 boundary explain contains cases。
6. CI summary 按错误类别汇总。

## 10. 验证

- Unit tests 覆盖 fragment edge contract 构造。
- Optimizer golden 覆盖 distribution enforcer 后 schema 不漂移。
- SQL distributed tests 覆盖 partial/global agg、merge sort、runtime filter。
- Negative tests 覆盖 schema fingerprint mismatch fail fast。

## 11. 成功标准

- 1FE+3BE 专属问题能在 plan boundary 或 runtime payload 层明确定位。
- optimizer 插入任何 enforcer 都必须更新 boundary contract。
- runtime 不再靠 payload 猜 fragment schema。
