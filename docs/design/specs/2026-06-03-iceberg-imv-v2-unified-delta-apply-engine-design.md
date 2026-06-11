# Iceberg IMV-v2: Logical-Plan-Derived Refresh Contract 设计

日期：2026-06-03
状态：Spec / 待实现计划
范围：Iceberg-backed IMV 的 CREATE/REFRESH contract 派生、refresh strategy 与物理 refresh/apply 编排抽象

---

## 0. TL;DR

本任务不实现 B 族 `UNION ALL of aggregate branches` 的 refresh 执行。B 族只作为下一任务的预期应用方，用来校验本次抽象边界是否足够。

本期设计的核心不是再引入一个独立的“shape tree”中心类型，而是利用 CREATE/REFRESH 时已经存在的 SQL parse/analyze 流程：

```text
SQL
  -> parser AST
  -> analyzed query / logical relation tree
  -> derive_imv_refresh_contract(MvAnalysis)
  -> ImvRefreshContract { strategy, base refs, apply key, aggregate/join/branch contracts, ... }
  -> IcebergMvRefreshDriver
```

`RefreshStrategy` 只是 `ImvRefreshContract` 里的一个字段，表示物理 refresh contract 类型。它不是 SQL shape 本身，也不直接由 SQL 字符串枚举出来。

目标是：

- 复用 analyzer / logical plan 的 name resolution、type resolution、alias 展开和表达式绑定；
- 移除 refresh 层手写 SQL pattern classifier 对组合形态的依赖；
- 用一个稳定、可持久化、可对比的 `ImvRefreshContract` 连接 CREATE 和 REFRESH；
- 把物理 refresh 生命周期收敛到统一 driver；
- B 族可被 logical plan visitor 识别并记录为 unsupported future strategy，但本任务不接执行。

---

## 1. 当前问题

当前 Iceberg IMV 有两个问题叠在一起。

第一，逻辑层其实已经比较统一。IMV rewrite pipeline 通过 `ImvDelta` / `ImvVersion` marker，以及 join、union、aggregate、scan binding、action propagation、apply-key 等 rewrite rule 组合出 delta plan。差异主体应该留在 logical plan rewrite 和 operator 组合里。

第二，refresh 物理层仍然按 SQL 形态写了多套路径：

- single projection/filter；
- join projection/filter；
- UNION ALL projection/filter；
- single aggregate；
- A 族 `Aggregate(UNION ALL(...))`；
- join aggregate。

这些路径各自重复处理 base load、snapshot pin、previous/current 判断、first refresh、metadata-only refresh、incremental apply 和 refresh intent。

更深的问题是：如果继续用平铺 `IncrementalMvShape` 枚举“支持的 SQL 组合名”，未来会继续出现组合爆炸。例如 `Aggregate(Union)`、`Union(Aggregate)`、`Aggregate(Join)` 都会诱导出新的 bespoke variant 或字段。

正确方向是：不要在 refresh 层再手写一套 SQL classifier，也不要持久化完整 logical plan；而是从 analyzed logical plan 派生稳定的 IMV refresh contract。

---

## 2. 设计原则

### 2.1 Raw AST 不够

AST 只描述语法，不能可靠回答：

- 表名最终绑定到哪个 catalog/db/table；
- column 引用绑定到哪个 base field；
- output 类型和 nullable；
- alias 展开后的表达式；
- group key / join key / output lineage；
- schema evolution 后 field-id rebind 是否安全。

所以 IMV refresh contract 不应只从 raw AST 派生。

### 2.2 直接持久化 logical plan 也不合适

Logical plan 是 planner 内部结构，会随 optimizer 演进而变化。它也包含 refresh 不需要的大量执行细节。CREATE MV metadata 需要的是稳定 semantic contract，而不是完整 planner node。

### 2.3 派生稳定 contract

CREATE MV 和 REFRESH MV 都走 analyzed logical plan，然后派生同一类稳定摘要：

```text
ImvRefreshContract {
  strategy: RefreshStrategy,
  base_refs: Vec<IcebergTableRef>,
  apply_key: ApplyKeyContract,
  structure: ImvRefreshStructure,
  aggregate: Option<AggregateRefreshContract>,
  join: Option<JoinRefreshContract>,
  branch: Option<BranchRefreshContract>,
  schema: MvSchemaContract,
}
```

CREATE 阶段把 contract 的稳定字段持久化到现有 MV metadata / `MvSchemaContract`
等 repository contract 中。REFRESH 阶段重新 analyze stored SQL，再派生
current contract，并和 persisted contract 做 validate/rebind。不要持久化
完整 logical plan。

---

## 3. 核心架构

### 3.1 Contract Deriver

新增 logical-plan visitor，工作名：

```text
derive_imv_refresh_contract(MvAnalysis) -> ImvRefreshContract
```

它递归 walk `MvAnalysis.resolved_query` 中的 analyzed query / relation
结构，识别 IMV 支持的结构：

```text
Scan
Project / Filter
Join
Aggregate
UnionAll
```

visitor 输出稳定 contract，而不是输出完整 plan。实现中可以使用内部轻量结构辅助递归，但它不是 refresh 层的中心概念。

### 3.2 RefreshStrategy

`RefreshStrategy` 表示物理 refresh contract 类型：

```text
ProjectionFilter
JoinProjectionFilter
UnionProjectionFilter
SingleAggregate
FanInAggregate
JoinAggregate
UnsupportedBranchUnionAggregate   # B 族 future consumer
```

它由 contract deriver 从 logical plan 结构归约出来：

```text
Project/Filter(Scan)
  -> ProjectionFilter

Join(Scan, Scan)
  -> JoinProjectionFilter

UnionAll(Project/Filter(Scan), ...)
  -> UnionProjectionFilter

Aggregate(Scan)
  -> SingleAggregate

Aggregate(UnionAll(Scan or Project/Filter(Scan), ...))
  -> FanInAggregate

Aggregate(Join(Scan, Scan))
  -> JoinAggregate

UnionAll(Aggregate(...), Aggregate(...))
  -> UnsupportedBranchUnionAggregate
```

新增 `RefreshStrategy` 的标准不是 SQL 多一种写法，而是物理 refresh contract 发生变化：snapshot policy、first refresh materialization、apply key、schema/rebind 或 rewrite evidence 无法由现有 strategy 表达。

### 3.3 ApplyKeyContract

把当前散落在 `RewriteMergeRefreshOptions` 中的 apply 语义提升为一等 contract：

```text
ApplyKeyContract {
  column_name,
  value_type,
  source,
  rewrite_evidence,
  locator_preload,
  full_rebuild_fallback,
}
```

当前策略：

- projection/filter：base row id + `Int64`；
- UNION projection/filter：branch + base row id + `BranchInt64`；
- aggregate：group row id + `Utf8` + aggregate evidence；
- join aggregate：group row id + `Utf8` + join aggregate evidence。

B 族未来需要 branch-scoped aggregate identity。本任务只在 contract 结构上保留扩展空间，不实现 row-id 编码。

### 3.4 Refresh Driver

`IcebergMvRefreshDriver` 负责所有 shape 共享的生命周期：

1. load target / MV definition；
2. parse/analyze stored SQL；
3. derive current `ImvRefreshContract`；
4. validate/rebind against persisted contract；
5. preload base tables；
6. apply base snapshot policy；
7. capture `RefreshSnapshotPin`；
8. build `IcebergMvRefreshContext`；
9. dispatch first / metadata-only / incremental；
10. manage refresh intent、staging branch、abort、publish、finalize。

driver 不负责：

- SQL pattern matching；
- logical rewrite rule 语义；
- first-refresh SQL 的具体生成；
- apply key 的语义选择；
- B 族执行支持。

### 3.5 Stable CREATE/REFRESH Contract

CREATE MV：

```text
SQL
  -> analyze query / relation tree
  -> derive_imv_refresh_contract
  -> build target schema + schema contract
  -> persist StoredMvDefinition + stable contract fields
```

REFRESH MV：

```text
stored SQL
  -> analyze logical plan
  -> derive_imv_refresh_contract
  -> compare/rebind against persisted contract
  -> RefreshStrategy drives refresh lifecycle
```

这让 CREATE-time 判断和 REFRESH-time 判断复用同一套 derivation 逻辑。

---

## 4. B 族边界

B 族：

```text
UNION ALL(
  Aggregate(base_1),
  Aggregate(base_2),
  ...
)
```

本任务中：

- contract deriver 可以识别它；
- contract 中记录它是 `UnsupportedBranchUnionAggregate`；
- CREATE 阶段可以选择 fail-fast，或允许创建但 REFRESH fail-fast；本任务推荐 CREATE 阶段 fail-fast，除非现有 CREATE 行为已经允许并持久化 unsupported MV；
- 不新增 logical rewrite；
- 不解 ignored test；
- 不实现 branch-scoped aggregate row id。

后续 B 族任务只应补：

- B-specific logical rewrite；
- branch-scoped aggregate apply key / row identity policy；
- B-specific first refresh strategy；
- 将 `UnsupportedBranchUnionAggregate` 升级为可执行 `BranchUnionAggregate`。

---

## 5. 迁移计划

### Phase 1: Contract Deriver Foundation

- 从 CREATE/REFRESH 当前已有分析入口定位 analyzed logical plan；
- 新增 `ImvRefreshContract` / `RefreshStrategy` / `ApplyKeyContract`；
- 实现 logical-plan visitor 的最小结构识别；
- 为 projection/filter、aggregate、join、union、A 族、B 族写 contract derivation 单元测试。

### Phase 2: CREATE MV Contract Persistence

- CREATE MV 阶段由 logical plan 派生 contract；
- 用 contract 构造 base refs、schema contract、apply key source、branch/join/aggregate metadata；
- 移除 CREATE 阶段对手写 SQL shape classifier 的依赖；
- B 族保持明确 unsupported。

### Phase 3: REFRESH Strategy Selection

- REFRESH 阶段重新 analyze stored SQL；
- 重新 derive current contract；
- compare/rebind against persisted contract；
- refresh strategy 从 contract 读取，不再从 SQL AST classifier 读取。

### Phase 4: Refresh Driver

- 提取 base snapshot decision；
- 提取 `ApplyKeyContract`；
- 统一 first / metadata-only / incremental dispatch；
- 现有 refresh paths 降级为 strategy helper。

### Phase 5: Existing Shape Migration

逐步接入：

- projection/filter；
- UNION projection/filter；
- single aggregate；
- A 族 fan-in aggregate；
- join aggregate；
- join projection/filter。

B 族不在本期接入执行。

---

## 6. 测试计划

### Contract Derivation Tests

覆盖：

- `Project/Filter(Scan)` -> `ProjectionFilter`；
- `UnionAll(Project/Filter(Scan), ...)` -> `UnionProjectionFilter`；
- `Aggregate(Scan)` -> `SingleAggregate`；
- `Aggregate(UnionAll(...))` -> `FanInAggregate`；
- `Aggregate(Join(...))` -> `JoinAggregate`；
- `UnionAll(Aggregate(...), Aggregate(...))` -> B 族 unsupported contract；
- unsupported operator fail-fast。

### Refresh Policy Tests

覆盖：

- single base empty skip；
- previous snapshot missing fail-fast；
- all bases required partial first refresh fail-fast；
- join pair partial initial skip；
- unchanged metadata-only；
- changed incremental。

### Regression Tests

保持现有 `iceberg_refresh` 模块测试通过，重点覆盖：

- projection/filter first / metadata-only / incremental；
- UNION projection/filter branch apply；
- single aggregate first / incremental；
- A 族 fan-in aggregate first / incremental / delete；
- join projection/filter partial initial skip；
- join aggregate update/delete retraction。

SQL suite：至少跑 Iceberg IMV 相关 suite；B 族 ignored test 保持 ignored。

---

## 7. 验收标准

本任务完成时：

- CREATE MV 和 REFRESH MV 使用同一套 logical-plan-derived contract derivation；
- refresh strategy 不再由 raw SQL pattern classifier 直接决定；
- `ImvRefreshContract` 是 CREATE/REFRESH 的稳定语义边界；
- refresh lifecycle 的重复逻辑集中到 driver；
- `ApplyKeyContract` 成为 apply 语义的一等结构；
- 现有 supported shape 行为不变；
- B 族未实现，但 contract derivation 能识别并明确 unsupported。

---

## 8. 风险与约束

- 不要持久化完整 logical plan；
- 不要只依赖 raw AST；
- 不要在本任务中偷渡 B 族执行；
- join projection / join aggregate 的 partial initial skip 必须保留；
- single projection/filter 的 full-rebuild fallback 与 multi-base 禁止 fallback 的差异必须保留；
- schema rebind 仍然要基于 persisted contract 与 current analyzed plan 对比。

---

## 9. 关键代码位置

- `src/engine/mv/iceberg_refresh.rs`
  - refresh dispatcher；
  - current shape-specific refresh paths；
  - incremental apply core；
  - first-refresh helpers。
- `src/connector/starrocks/table/mv_shape.rs`
  - current AST-based classifier；本任务应逐步降级或迁移出 refresh decision 入口。
- `src/engine/mv/refresh_context.rs`
  - refresh execution context。
- `src/meta/repository/mv_contract.rs`
  - persisted schema / apply / branch / aggregate contracts。
- `src/sql/optimizer/rewrite/imv/pipeline.rs`
  - existing IMV logical rewrite pipeline。
