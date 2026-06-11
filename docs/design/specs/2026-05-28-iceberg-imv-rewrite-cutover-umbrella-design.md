# Iceberg IMV Rewrite Cutover Umbrella Design

日期：2026-05-28

来源：
- `iceberg-scan-delta-version-binding.md`
- `action-column-propagation.md`
- `aggregate-state-rewrite-over-iceberg-target.md`
- `join-delta-rewrite.md`
- `union-all-delta-rewrite.md`
- `union-all-aggregate-branches.md`
- `schema-contract-under-rewrite.md`

## 1. 目标

把 Iceberg MV refresh 从当前的 shape-specific 手写路径，迁移到 IMV
logical rewrite 生成可执行计划的架构。终态是当前所有 Iceberg MV refresh
shape 都由 `run_imv_rewrite` 产出可执行 outcome，并由统一 executor 消费：

- 单表 projection/filter
- 单表 aggregate
- join projection/filter
- join aggregate
- aggregate over `UNION ALL`
- top-level `UNION ALL` of aggregate branches

这份文档是 umbrella design，不是单个大 PR 的实现说明。它定义终态架构、
阶段边界、cutover 策略、失败语义和测试矩阵。后续每个阶段各自进入
implementation plan。

## 2. 非目标

- 不在一个 PR 内完成所有 rewrite 与执行迁移。
- 不改变普通 Iceberg table scan 的语义。
- 不改变 FE-compatible backend mode 的 plan lowering 语义。
- 不新增当前未支持的 aggregate function。
- 不支持 outer join delta rewrite。
- 不支持 `UNION` / `INTERSECT` / `EXCEPT` 增量 rewrite。
- 不做 target table schema 自动修复。
- 不做旧 MV definition 的复杂在线迁移；无法安全解释的历史 metadata 要求
  rebuild 或 recreate。

## 3. 当前上下文

现有代码已经具备以下基础：

- `src/engine/mv/refresh_context.rs` 提供 `IcebergMvRewriteContext` 和
  `IcebergMvRefreshContext`。
- `src/sql/optimizer/rewrite/imv/` 已有 IMV pipeline、`ImvDelta` /
  `ImvVersion` marker、root `Delta` wrap rule 和 unresolved marker
  validation。
- `src/sql/catalog.rs` 已有 `ScanSource::IcebergDeltaTable`。
- `src/sql/planner/mod.rs` 能把 analyzer 产生的 `IcebergDeltaScanRelation`
  规划成 `LogicalPlan::Scan` + `IcebergDeltaTable`。
- `src/sql/codegen/nodes.rs` 能把 `IcebergDeltaTable` emit 成
  `ICEBERG_DELTA_SCAN_NODE`。
- `src/lower/node/iceberg_delta_scan.rs` 能根据 snapshot window 调用
  `connector::iceberg::changes::plan_changes` 并构造 delta scan runtime。
- `src/connector/iceberg/read.rs` 已有 pinned snapshot read helper。
- `src/engine/mv/iceberg_refresh.rs` 中已有手写路径，使用
  `__nr_ivm_delta(...)`、`VERSION AS OF` 和 shape-specific helper 表达
  delta/version 语义。

迁移目标不是重新实现这些 runtime 能力，而是把当前 SQL 字符串/AST helper
表达的增量语义，提升到 logical rewrite 层，并让 refresh execution 消费
rewrite outcome。

## 4. 分阶段计划

### 阶段 1：Scan Binding

实现 `Delta(Scan)` 和 `Version(Scan)` 的 refresh-only binding。

- `Delta(IcebergScan)` 绑定到 `(from_snapshot_id, to_snapshot_id]`。
- `Version(IcebergScan, From)` 绑定到 `from_snapshot_id`。
- `Version(IcebergScan, To)` 绑定到 `to_snapshot_id`。
- binding 来源只能是 `IcebergMvRewriteContext` 中的 previous snapshots 和
  `RefreshSnapshotPin`。
- empty delta 生成 schema-preserving 空输入，不能退回 current snapshot。

阶段 1 只验证 outcome，不切执行。

### 阶段 2：Action Column

把现有 `__change_op` 字符串伪列语义迁移成 optimizer 内部 action column。

- action column 类型固定为非 nullable `Int8`。
- action domain 固定为 insert/upsert `+1`，delete `-1`。
- project/filter 透传 action column。
- join 和 union 为每个 child/branch 维护独立 action column，再映射到父节点。
- column pruning 不能剪掉仍被 target apply 或 aggregate signed state 使用的
  action column。

阶段 2 仍不切执行，只验证 action propagation 和 plan shape。

### 阶段 3：Projection/Filter Cutover

单表 projection/filter refresh 默认消费 rewrite outcome。

- legacy path 保留为显式 session/config fallback flag。
- 默认路径如果 rewrite 不完整，fail-fast。
- 成功 refresh 后 metadata 写入 pin 的 snapshot map，而不是执行时重新读取
  current snapshot。

### 阶段 4：Aggregate State Rewrite

把单表 aggregate IMV 的 delta-state 计算迁移到 rewrite outcome。

- `Delta(Aggregate(child))` 生成 delta aggregate state。
- action column 驱动 signed state 输入。
- 复用现有 aggregate layout、detail state、target state lookup 和 target
  apply。
- no-op delta 走 metadata-only refresh，不产生 target commit。

### 阶段 5：Join / Join Aggregate Rewrite

实现 join delta algebra，并迁移现有 `iceberg_join_branch` helper 的语义。

公式：

```text
Delta(A join B)
  = Delta(A) join Version(B, From)
    UNION ALL
    Version(A, To) join Delta(B)
```

约束：

- 只支持 inner join / cross join。
- 每个 delta branch 独立 clone child plan 和 action column。
- version side 必须使用 pinned `from` 或 `to`，不能读 current。
- join aggregate 继续复用 aggregate target state merge。

### 阶段 6：UNION ALL 与 Contract Hardening

支持两个 UNION ALL family，并补齐 plan-aware contract。

- `Delta(UNION ALL children) = UNION ALL(Delta(child_i))`。
- aggregate over `UNION ALL` 将多个 base branch 的 delta 合并到统一
  aggregate state。
- top-level `UNION ALL` of aggregate branches 使用 stable branch id +
  group row id 作为内部 apply key，保留 bag semantics。
- `MvSchemaContract` 扩展到能描述 branch、base field-id lineage、target hidden
  columns 和 branch-aware field-id rebind。

## 5. 核心架构

IMV rewrite 继续位于 `src/sql/optimizer/rewrite/imv/`。optimizer rule 只能读取
`IcebergMvRewriteContext`，不能依赖 `iceberg::Catalog`、
`IcebergCatalogEntry` 或 `iceberg::table::Table`。执行 handle 留在 refresh
层和 lowering/runtime 层。

### 5.1 新增逻辑概念

`ImvSnapshotWindow`

```text
base_fqn
from_snapshot_id
to_snapshot_id
table_uuid
```

由 `mv_ctx.previous_snapshot_ids` 和 `mv_ctx.pin` 解析得到。缺失或不一致时
fail-fast。

`IcebergMvScanBinding`

```text
Delta { base_fqn, from_snapshot_id, to_snapshot_id }
Version { base_fqn, snapshot_id, role: From | To }
```

它是 refresh-only binding，只允许出现在 IMV rewrite outcome 中。普通查询
不能构造它。

`ImvActionColumn`

```text
column_id
data_type = Int8
nullable = false
domain = { insert/upsert: +1, delete: -1 }
```

它是 optimizer 内部列，不能暴露到用户 visible schema。

`ImvExecutionPlan`

`run_imv_rewrite` outcome 的执行入口描述。早期可以包装一个已完全消解 marker
的 `LogicalPlan`。aggregate、join、UNION ALL cutover 后，它还需要携带 target
apply metadata、touched group/partition 信息和 branch identity。

### 5.2 Pipeline Stage

IMV pipeline 的 stage 顺序固定：

1. `imv-logical-normalize`
2. `imv-delta-marker`
3. `imv-scan-binding`
4. `imv-action-propagation`
5. `imv-algebra-rewrite`
6. `imv-contract-validation`
7. `imv-validation`

`imv-validation` 拒绝任何未消解的 marker 或不完整内部状态：

- `ImvDelta`
- `ImvVersion`
- unbound Iceberg scan
- missing action column
- nullable/wrong-type action column
- visible output 中泄漏的 action column

## 6. RefreshSnapshotPin 语义

`RefreshSnapshotPin` 是本次 refresh 的 base table 读版本清单。它钉住的是每个
base Iceberg table 在 refresh 开始时的 current snapshot id 和 table UUID。

示例：

```text
上次成功 refresh:
  ice.sales.orders -> snapshot 100

本次 refresh 开始:
  ice.sales.orders current snapshot = 120
  ice.sales.orders table UUID = abc

pin:
  ice.sales.orders -> { snapshot_id: 120, uuid: abc }
```

之后整个 refresh 只能使用这个 pin：

- `Delta(orders)` 读 `(100, 120]`。
- `Version(orders, From)` 读 snapshot `100`。
- `Version(orders, To)` 读 snapshot `120`。
- refresh 成功后，MV metadata 记录 `last_refresh_snapshots[orders] = 120`。
- 如果 refresh 过程中 base table 又提交到 snapshot `121`，本次 refresh 不读取
  `121`。

UUID 一起记录，是为了防止 drop/recreate 后 snapshot id 看似存在但实际已经不是
同一张表。

## 7. 数据流

一次 refresh 的终态数据流：

1. Capture pin：记录每个 base 的 `{fqn -> to_snapshot_id, table_uuid}`。
2. 构造 `IcebergMvRefreshContext`：
   - `previous_snapshot_ids` 来自上次成功 refresh。
   - `pin` 提供本次 refresh 的 `to_snapshot_id`。
   - `previous_table_uuids` 与 pin UUID 做 identity guard。
3. 用 refresh planning catalog 重新 analyze/plan canonical select。
4. `run_imv_rewrite` 执行 IMV pipeline：
   - `Delta(scan)` 绑定到 `(previous_snapshot_id, pinned_snapshot_id]`。
   - `Version(scan)` 绑定到 pinned `from` 或 `to`。
   - action column 贯穿所有内部 delta 子树。
   - aggregate/join/UNION ALL 按阶段完成 algebra rewrite。
5. `ImvRefreshExecutor` 判断 outcome 是否覆盖当前 shape。
6. 覆盖时执行 rewrite outcome。
7. 未覆盖时：
   - 未 cutover 的阶段可以走明确 legacy path，并记录 trace。
   - 已 cutover 的 shape fail-fast，不 silent fallback。
8. refresh 成功后，metadata 写入 pin 的 snapshot map 和 table UUID map。

## 8. 执行切换策略

`try_run_imv_rewrite_pipeline` 终态不再 log-and-discard，而是正式入口。迁移期
通过 shape readiness 控制是否执行 outcome。

| 阶段 | 默认执行路径 | Legacy 行为 |
| --- | --- | --- |
| 1 Scan binding | legacy | outcome 只验证 |
| 2 Action column | legacy | outcome 只验证 |
| 3 Projection/Filter | rewrite outcome | 显式 flag 才允许 fallback |
| 4 Aggregate | rewrite outcome | 显式 flag 短期保留，之后下线 |
| 5 Join / Join Aggregate | rewrite outcome | helper 语义迁移后不 fallback |
| 6 UNION ALL family | rewrite outcome | 无 legacy path |

empty delta 统一由 executor 处理：

- 所有 bound delta scan 都 empty：metadata-only refresh。
- 部分 branch empty：继续执行非空 branch。
- empty delta 不允许重新读取 current snapshot。

## 9. 错误处理

所有错误都 fail-fast，不能 silent full refresh。

- pin 缺 base：报错包含 base FQN，不能用 current snapshot 代替。
- previous snapshot 缺失：除 first refresh/full rebuild 外，增量 rewrite 报错。
- UUID drift：previous UUID 与 pin UUID 不同，要求 rebuild/recreate。
- stale/expired snapshot：`from` 不在 `to` lineage 上，报错包含 base FQN、from、
  to。
- snapshot mismatch：codegen/lowering/executor 发现实际 scan window 与 binding
  不一致时直接 error。
- marker leak：`ImvDelta` / `ImvVersion` 到 validation 仍存在时报错。
- action column missing/leak：内部 action column 被剪掉、类型错误、nullable 或
  暴露到 visible output 时报错。
- unsupported shape：未 cutover 阶段明确 legacy；cutover 后 fail-fast。
- schema contract drift：field id、类型、nullability 或 hidden apply key 不一致时
  fail-fast。
- branch contract drift：branch id 缺失、重复或与 target apply key 不一致时
  fail-fast。

## 10. 测试矩阵

Unit tests：

- snapshot window lookup
- scan binding
- empty delta binding
- stale baseline / expired snapshot
- UUID drift
- action propagation through project/filter
- action propagation through join/union
- join branch output mapping
- union branch arity/type/nullability mapping
- stable branch id
- contract validation

Planner/golden tests：

- `EXPLAIN` 或 rewrite trace 包含 bound delta/version scan 信息。
- validation 阶段能稳定报出 unresolved marker。
- action column 不出现在 visible output。

SQL suites：

- `iceberg-ivm` 全量不回归。
- 新增 append delta、delete delta、empty delta。
- 新增 snapshot window mismatch。
- 新增 single-side join delta、double-side join delta。
- 新增 join key update multiplicity。
- 新增 aggregate over `UNION ALL`。
- 新增 `UNION ALL` of aggregate branches：同 group key 跨 branch 不合并。

Cutover tests：

- 每个 shape 的默认路径符合阶段表。
- fallback flag 只影响未完全下线的 legacy path。
- 已 cutover shape rewrite 不完整时 fail-fast。
- refresh success metadata 使用 pin snapshot map。

## 11. 风险与缓解

范围风险：

这组能力横跨 optimizer、planner、codegen、lowering、runtime、MV metadata 和 SQL
tests。通过 umbrella design + 分阶段 implementation plan 控制范围，每阶段必须
能独立验证。

snapshot correctness 风险：

所有 scan/version binding 必须从 pin 和 previous snapshots 推导。任何 current
snapshot fallback 都是 correctness bug。通过 binding validation、lowering
window check 和 SQL mismatch case 防护。

column id / output mapping 风险：

join/UNION ALL clone plan 时容易串 column id。通过 branch-local action column、
output arity/type checks 和 dedicated unit tests 防护。

contract 风险：

branch-aware contract 可能需要 metadata schema version 增加。实现计划必须先评估
现有 `MvSchemaContract` 是否足够表达 branch 信息；如果需要扩展，必须配合 metadata
schema migration。

性能风险：

aggregate target state lookup 不能退化成无界 target 全表扫描。每个 aggregate
cutover plan 必须保留 touched group/partition 约束；除非显式 policy 允许，否则
fail-fast。

## 12. 后续计划入口

本 umbrella spec 通过后，下一步进入 implementation planning。计划应按阶段 1
开始，不直接展开所有阶段实现。阶段 1 的 implementation plan 范围限定为：

- 定义 snapshot window 与 scan binding 类型。
- 实现 `Delta(Scan)` / `Version(Scan)` binding rule。
- codegen/lowering 增加 binding consistency guard。
- 增加 targeted tests。
- refresh 主执行仍不切换，只记录和验证 outcome。
