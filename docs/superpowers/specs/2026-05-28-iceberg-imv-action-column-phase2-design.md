# Iceberg IMV Action Column Phase 2 Design

日期：2026-05-28

来源：
- `2026-05-28-iceberg-imv-rewrite-cutover-umbrella-design.md`（umbrella spec section 4.2 与 5.1）
- Phase 1 实现：`src/sql/optimizer/rewrite/imv/{scan_binding,marker,pipeline,entrypoint}.rs`

## 1. 目标

把现有 `__change_op` 字符串伪列语义提升到 optimizer 内部表达：在 IMV
rewrite pipeline 中给 Delta-bound `Scan` 注入一个 internal action column，并把它
透传到 `Project` / `Filter`。Refresh 执行不切换，rewrite outcome 只通过
validation 被观察。

终态：
- Delta-bound `Scan` 输出列里包含一个 `Int8` non-nullable 的 internal
  `__change_op` 列。
- 单表 IMV refresh 的 `Project` / `Filter` 子树保留 action column。
- Column pruning 不会把 internal column 剪掉。
- Root plan 的 user-visible output 不暴露任何 `is_internal == true` 列。
- 任何 `Join` / `UnionAll` 出现在 Delta 子树之上时，pipeline fail-fast，错误信息
  指向 Phase 5 / 6 的 delta algebra。

## 2. 非目标

- 不切换 refresh 执行（继续走现有 `iceberg_refresh.rs` SQL-string 路径）。
- 不修改 `src/exec/operators/iceberg_delta_scan.rs`、`src/engine/mv/iceberg_merge_sink.rs`
  或现有的 codegen `iceberg_metadata_pseudo_column_slots` 模型。
- 不为 `Join` / `UnionAll` 提供 action column 合并语义（Phase 5 / 6 一并处理）。
- 不引入 `Aggregate` 上的 action column 处理（Phase 4）。
- 不新增 SQL test。Phase 2 没有 user-visible 行为变化，验证靠 unit + e2e。

## 3. 当前上下文

Phase 1 提供：
- `BindIcebergScanRule` 把 `ImvDelta(Scan)` / `ImvVersion(Scan)` 解析成
  `ScanSource::IcebergDeltaTable` / `ScanSource::IcebergVersionTable` 的 bound scan。
- IMV pipeline 已有 5 个 stage：`imv-logical-normalize`、`imv-delta-marker`、
  `imv-scan-binding`、`imv-marker-cleanup`、`imv-validation`。
- `ImvExtension { mv_ctx, annotation }` 通过 `RewriteContext::extension` 暴露 MV context。

`__change_op` 现状（来自 `src/exec/change_op.rs`）：
- 常量 `CHANGE_OP_COLUMN: &str = "__change_op"`、`CHANGE_OP_INSERT: i8 = 1`、
  `CHANGE_OP_DELETE: i8 = -1`。
- 由 `IcebergDeltaScanOp` 运行时合成（每文件常量列，根据 file role 选 +1 或 -1）。
- 通过 `iceberg_refresh.rs::append_change_op_to_projection` 在 SQL 层加入 SELECT 列表。
- Merge sink (`iceberg_merge_sink.rs::partition_chunk_by_change_op`) 在 apply 时
  按 `__change_op` 值分别走 insert / position-delete 路径。

Pruning 现状（`src/sql/optimizer/rewrite/rules/column_pruning.rs`）：
- `PruneColumns` rule 自顶向下传递 `needed: Option<HashSet<ColumnId>>`。
- `ImvDelta` / `ImvVersion` 节点目前直接 `panic!`（marker 不应漏到非 IMV 路径）。

`OutputColumn` 现状（`src/sql/analysis/mod.rs`）：
- `{ column_id, name, data_type, nullable }`。没有 internal flag。

## 4. 分阶段范围

| 节点形态 | Phase 2 行为 |
|---|---|
| `Scan { source: IcebergDeltaTable, .. }` | 注入 internal action column |
| `Scan { source: IcebergVersionTable, .. }` | 不注入 |
| `Scan { source: IcebergDataFiles, .. }` | 不注入（非 IMV 路径） |
| `Project` 在 Delta 子树之上 | 透传 action column |
| `Filter` 在 Delta 子树之上 | 透传 action column |
| `Join` 在 Delta 子树之上 | fail-fast `unsupported`，提示 Phase 5 |
| `UnionAll` 在 Delta 子树之上 | fail-fast `unsupported`，提示 Phase 6 |
| `Aggregate` 在 Delta 子树之上 | fail-fast `unsupported`，提示 Phase 4（防止 action column 被静默丢弃） |

## 5. 数据模型

### 5.1 `OutputColumn` 加 `is_internal: bool`

文件：`src/sql/analysis/mod.rs`

```rust
#[derive(Clone, Debug)]
pub(crate) struct OutputColumn {
    pub column_id: ColumnId,
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub is_internal: bool,   // NEW
}
```

- 所有现有构造点默认 `is_internal: false`，零侵入。
- `is_internal: true` 的列对外不可见，对内部 operator 可见。
- 通用 flag，未来可承载 Iceberg metadata 伪列、row-id 等其他 internal 列。
  本阶段只引入 action column 的使用方。

### 5.2 `ImvActionColumn` 描述符

新文件：`src/sql/optimizer/rewrite/imv/action_column.rs`

```rust
pub(crate) struct ImvActionColumn;

impl ImvActionColumn {
    pub(crate) const NAME: &'static str = crate::exec::change_op::CHANGE_OP_COLUMN;
    pub(crate) const INSERT_VALUE: i8 = crate::exec::change_op::CHANGE_OP_INSERT;
    pub(crate) const DELETE_VALUE: i8 = crate::exec::change_op::CHANGE_OP_DELETE;

    pub(crate) fn output_column(column_id: ColumnId) -> OutputColumn {
        OutputColumn {
            column_id,
            name: Self::NAME.to_string(),
            data_type: DataType::Int8,
            nullable: false,
            is_internal: true,
        }
    }

    pub(crate) fn matches(column: &OutputColumn) -> bool {
        column.is_internal && column.name.eq_ignore_ascii_case(Self::NAME)
    }
}
```

域 `{+1, -1}` 是运行时不变量，IR 不强制；validation rule 在注释中明确语义。

### 5.3 `ColumnId` 分配

`ImvExtension` 持有一个 column factory，供 rule 申请新 `ColumnId`：

```rust
pub(crate) struct ImvExtension {
    pub mv_ctx: Arc<IcebergMvRewriteContext>,
    pub annotation: ImvPlanAnnotation,
    pub column_factory: Rc<RefCell<ColumnRefFactory>>,   // NEW
}
```

Rule 通过 `ext.column_factory.borrow_mut().next()` 申请新 column_id。
Factory 的构造点（IMV pipeline 入口）需要传入或新建一个 `ColumnRefFactory`，
建议复用 analyzer 已用的同名类型，避免引入新概念。

## 6. Pipeline 与 Rules

### 6.1 Stage 顺序

```text
imv-logical-normalize
imv-delta-marker         (WrapRootInImvDeltaRule)
imv-scan-binding         (BindIcebergScanRule)
imv-action-propagation   (NEW: InjectActionColumnRule + PropagateActionColumnRule)
imv-marker-cleanup
imv-validation           (UnresolvedMarkerCheckRule + ActionColumnValidationRule)
```

新 stage `imv-action-propagation` 注册在 `RewritePhase::SemanticRewrite`，在
`imv-scan-binding` 与 `imv-marker-cleanup` 之间。

### 6.2 `InjectActionColumnRule`

文件：`src/sql/optimizer/rewrite/imv/action_propagation.rs`

```rust
pub(crate) struct InjectActionColumnRule;

impl LogicalRewriteRule for InjectActionColumnRule {
    fn name(&self) -> &'static str { "InjectActionColumn" }
    fn phase(&self) -> RewritePhase { RewritePhase::SemanticRewrite }
    fn traversal(&self) -> RewriteTraversal { RewriteTraversal::BottomUp }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        match plan {
            LogicalPlan::Scan(scan) => {
                matches!(scan.table.source, ScanSource::IcebergDeltaTable { .. })
                    && !scan.columns.iter().any(ImvActionColumn::matches)
            }
            _ => false,
        }
    }

    fn apply(&self, plan: LogicalPlan, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let LogicalPlan::Scan(mut scan) = plan else { return Ok(RewriteResult::Unchanged); };
        let ext = ctx.extension::<ImvExtension>().ok_or_else(|| {
            "InjectActionColumn requires ImvExtension in RewriteContext".to_string()
        })?;
        let column_id = ext.column_factory.borrow_mut().next();
        scan.columns.push(ImvActionColumn::output_column(column_id));
        Ok(RewriteResult::Changed(LogicalPlan::Scan(scan)))
    }
}
```

- 只匹配 `IcebergDeltaTable`。`IcebergVersionTable` / `IcebergDataFiles` / `StarRocks` 不注入。
- `matches()` 用 `!any(matches)` 保证幂等。
- 同 stage 内 BottomUp 遍历，所有 Scan 先被注入，再触发 propagate。

### 6.3 `PropagateActionColumnRule`

```rust
pub(crate) struct PropagateActionColumnRule;

impl LogicalRewriteRule for PropagateActionColumnRule {
    fn name(&self) -> &'static str { "PropagateActionColumn" }
    fn phase(&self) -> RewritePhase { RewritePhase::SemanticRewrite }
    fn traversal(&self) -> RewriteTraversal { RewriteTraversal::BottomUp }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        match plan {
            LogicalPlan::Project(p) =>
                subtree_has_action_column(&p.input) && !output_has_action_column(plan),
            LogicalPlan::Filter(f) =>
                subtree_has_action_column(&f.input) && !output_has_action_column(plan),
            LogicalPlan::Join(_) | LogicalPlan::UnionAll(_) | LogicalPlan::Aggregate(_) =>
                subtree_has_action_column_any_child(plan),
            _ => false,
        }
    }

    fn apply(&self, plan: LogicalPlan, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        match plan {
            LogicalPlan::Project(mut p) => {
                let action = find_action_column(&p.input)
                    .ok_or_else(|| "Project missing child action column".to_string())?;
                p.columns.push(action.clone());
                p.projections.push(passthrough_projection(action.column_id));
                Ok(RewriteResult::Changed(LogicalPlan::Project(p)))
            }
            LogicalPlan::Filter(f) => propagate_through_filter(f),
            LogicalPlan::Join(_) => Err(
                "IMV action column propagation does not support Join in Phase 2; \
                 join delta algebra is scheduled for Phase 5".to_string()),
            LogicalPlan::UnionAll(_) => Err(
                "IMV action column propagation does not support UnionAll in Phase 2; \
                 union delta rewrite is scheduled for Phase 6".to_string()),
            LogicalPlan::Aggregate(_) => Err(
                "IMV action column propagation does not support Aggregate in Phase 2; \
                 aggregate state rewrite is scheduled for Phase 4".to_string()),
            _ => Ok(RewriteResult::Unchanged),
        }
    }
}
```

Filter 节点的实际 schema 形状（是否显式列 output column）在 implementation
plan 阶段需要确认。两种可能：
- Filter 复用 child schema：`apply()` 是 no-op；但 `matches()` 在 child 已经
  含 action column 后必须返回 false（即 `output_has_action_column` 视 Filter
  的 effective output 为 child output），否则同一节点会被反复 match。
- Filter 显式列 output column：`apply()` 把 action column 推到 Filter 自己的
  输出列表，下次 `matches()` 自然返回 false 达到 fixpoint。

无论哪种形状，validation V3 都从 Filter 的 effective output 上查 action column
是否存在，保证检查口径一致。

### 6.4 Pipeline 注册示例

```rust
RewriteStage::new(
    "imv-action-propagation",
    RewritePhase::SemanticRewrite,
    vec![
        Box::new(InjectActionColumnRule) as Box<dyn LogicalRewriteRule>,
        Box::new(PropagateActionColumnRule),
    ],
),
```

## 7. Validation

`imv-validation` stage 新增 `ActionColumnValidationRule`，与 `UnresolvedMarkerCheckRule`
并列。Rule 在 root 上 TopDown 跑一次完整校验。

| Invariant | 描述 | 失败诊断 |
|---|---|---|
| V1 | 每个 `Scan { source: IcebergDeltaTable }` 输出列恰好包含 1 个 `ImvActionColumn::matches` 的列；该列 `data_type == Int8`、`nullable == false`、`is_internal == true` | `"Delta-bound scan {fqn} missing action column"` / `"... has non-Int8 action column"` / `"... has nullable action column"` / `"... has duplicate action columns"` |
| V2 | 每个 `Scan { source: IcebergVersionTable }` 输出列里没有 action column | `"Version-bound scan {fqn} must not carry action column"` |
| V3 | 从带 action column 的 Scan 到 root 路径上的每个 `Project` / `Filter` 节点都透传 action column | `"action column dropped at {node_kind} above delta scan {fqn}"` |
| V4 | Root plan 的 user-visible output（`is_internal == false` 子集合）不为空 | `"root plan exposes only internal columns; action column leaked to visible output"` |
| V5 | 没有 `Join` / `UnionAll` / `Aggregate` 节点出现在 Delta 子树之上 | `"Phase 2 does not support {Join|UnionAll|Aggregate} above delta-bound scans; deferred to Phase 4/5/6"` |

V5 与 `PropagateActionColumnRule::apply()` 形成双保险。Aggregate 一并列入 V5
（而不是放任 Phase 4 处理），是为了防止 Phase 2 路径上 Aggregate 静默丢弃
action column。

## 8. Column Pruning 集成

文件：`src/sql/optimizer/rewrite/rules/column_pruning.rs`

修改点：
1. 在 `prune_inner(plan, needed)` 内，对每个 plan 节点，把其输出列表中所有
   `is_internal == true` 列的 `column_id` 强制加入 `needed` 集合。这样
   pruning 不会把 internal column 从 child 的 `required_columns` 中剪掉。
2. 把 `ImvDelta` / `ImvVersion` 的 panic 改成 passthrough（或在已 resolved 的
   plan 上正常处理）。Phase 1 之后 marker 在 validation 前就已经被 BindIcebergScanRule
   消解，pruning 不应再遇到，但保险起见把硬 panic 改成 explicit error
   (`"marker survived to pruning: {kind}"`) 而不是 panic。

`is_internal` 的"总是保留"语义在 pruning 入口集中实现，不分散到每个 plan
节点的特殊分支。

## 9. Codegen Guard

文件：`src/sql/codegen/nodes.rs`

Phase 2 不切执行，需要防止 internal action column 意外流到 codegen。在
`build_scan_node` 或 `build_exec_params_multi` 中添加 guard：

```rust
if resolved.table.columns.iter().any(|c| c.is_internal && ImvActionColumn::matches(c))
    || resolved_output_has_internal_action_column(...)
{
    return Err(format!(
        "IMV action column on scan {}.{} reached codegen before execution cutover",
        resolved.database, resolved.table.name
    ));
}
```

Guard 与 Phase 1 的 `IcebergVersionTable` codegen guard 风格一致。Phase 3
cutover 时显式取消。

注意：`ColumnDef`（catalog 层）与 `OutputColumn`（plan 层）是两种类型，
guard 应基于 `OutputColumn::is_internal`。如果 codegen 路径只从 catalog
`ColumnDef` 读列，则 guard 需要从 plan 节点的 `scan.columns` 上读。具体位置
在 implementation plan 中确认。

## 10. 测试矩阵

### 10.1 Unit tests

文件：`src/sql/optimizer/rewrite/imv/action_propagation.rs::tests`

| 测试 | 验证 |
|---|---|
| `inject_action_column_on_delta_scan` | Delta-bound Scan 注入 1 个 internal Int8 non-null 列 |
| `inject_does_not_touch_version_scan` | Version scan 不 match |
| `inject_is_idempotent` | 已有 action column 的 scan 不再注入 |
| `inject_skips_starrocks_scan` | StarRocks 不 match |
| `propagate_through_project` | Project(DeltaScan) 输出含 action column |
| `propagate_through_filter` | Filter(DeltaScan) 形态正确 |
| `propagate_rejects_join` | Join above DeltaScan → Err 含 "Phase 5" |
| `propagate_rejects_union_all` | UnionAll above DeltaScan → Err 含 "Phase 6" |
| `propagate_rejects_aggregate` | Aggregate above DeltaScan → Err 含 "Phase 4" |

### 10.2 Validation tests

文件：`src/sql/optimizer/rewrite/imv/action_column.rs::tests`

| 测试 | 验证 |
|---|---|
| `validation_passes_on_well_formed_delta_scan` | 注入 + propagate 后 root 通过 |
| `validation_rejects_missing_action_column_on_delta` | V1 失败 |
| `validation_rejects_non_int8_action_column` | V1 失败：类型错 |
| `validation_rejects_nullable_action_column` | V1 失败：nullable |
| `validation_rejects_duplicate_action_columns` | V1 失败：duplicate |
| `validation_rejects_action_column_on_version` | V2 失败 |
| `validation_rejects_dropped_action_above_project` | V3 失败 |
| `validation_rejects_dropped_action_above_filter` | V3 失败 |
| `validation_rejects_action_leaking_to_visible_root` | V4 失败 |
| `validation_rejects_join_above_delta` | V5 失败 |
| `validation_rejects_union_all_above_delta` | V5 失败 |
| `validation_rejects_aggregate_above_delta` | V5 失败 |

### 10.3 End-to-end tests

文件：`src/sql/optimizer/rewrite/imv/entrypoint.rs::tests`（复用 Phase 1
的 `iceberg_scan_plan()` 等 helper）

| 测试 | 验证 |
|---|---|
| `imv_pipeline_injects_action_on_delta_scan` | 完整跑 pipeline，root 是带 action column 的 Scan |
| `imv_pipeline_propagates_action_through_project` | Project(Scan) → outcome Project 含 action column |
| `imv_pipeline_propagates_action_through_filter` | Filter(Scan) → outcome Filter 含 action column |
| `imv_pipeline_rejects_join_in_phase2` | Project(Join(Scan, Scan)) → Err 含 "Phase 5" |
| `imv_pipeline_rejects_aggregate_in_phase2` | Aggregate(Scan) → Err 含 "Phase 4" |
| `imv_pipeline_no_action_for_version_only_plan` | 纯 Version scan 不带 action column |

### 10.4 Column pruning interaction

文件：`src/sql/optimizer/rewrite/rules/column_pruning.rs::tests`

| 测试 | 验证 |
|---|---|
| `pruning_preserves_internal_column_when_parent_does_not_request` | Project 只 select user 列，Scan.required_columns 仍含 internal action column |
| `pruning_passes_through_resolved_imv_subtree` | 把 panic 改 explicit error 后，已 resolved 的 IMV plan 跑得通 pruning |

### 10.5 Codegen guard

文件：`src/sql/codegen/nodes.rs::tests`

| 测试 | 验证 |
|---|---|
| `imv_action_column_reaches_codegen_guard` | 含 internal `__change_op` 的 ResolvedTable → codegen 路径返回 Err 包含 "before execution cutover" |

### 10.6 SQL test

不新增。Phase 2 无 user-visible 行为变化。Phase 3 projection/filter
cutover 才会加 SQL test。

### 10.7 Plan-golden（可选）

`sql-tests/optimizer/` 下可加一个 IMV pipeline trace plan-golden，断言
`imv-action-propagation` stage 存在且 Delta scan 输出含 internal column。
最终是否加由 implementation plan 决定。

## 11. 错误处理

所有错误 fail-fast，不静默回退：

- ColumnId 分配失败（factory unavailable）：报错指出 `ImvExtension::column_factory`
  缺失。
- Validation V1-V5 各自带具体诊断（见第 7 节表格），错误信息包含 base FQN
  或 node kind。
- Propagation rule 在 Join / UnionAll 上的 fail-fast 错误信息包含 Phase 5 / 6
  字样，便于上游识别。
- Codegen guard 触发时错误信息包含 `"before execution cutover"`，与 Phase 1
  IcebergVersionTable guard 风格一致。

## 12. 风险与缓解

**`OutputColumn` 加字段的影响范围风险**：
`OutputColumn` 在 analyzer、planner、optimizer、codegen、tests 大量使用。
加新字段需要更新所有构造点（数十处）。缓解：默认 `is_internal: false`，
所有现有调用点都是 trivial 改动。Implementation plan 阶段用 grep 列全所有
构造点，分批改动 + 一次性 cargo build 验证。

**Filter 节点 IR 形状不确定**：
Filter 是否显式列输出列、还是复用 child schema，决定 `PropagateActionColumnRule`
在 Filter 上是 no-op 还是要 push 列。在 implementation plan 阶段读
`src/sql/planner/plan.rs::FilterNode` 确定。

**Column pruning 与已 resolved 的 IMV plan 兼容性**：
Pruning rule 当前 panic on `ImvDelta` / `ImvVersion`。Phase 2 之后 marker 在
validation 之前被消解，pruning 不应再遇到。把硬 panic 改成 explicit error
是兜底，避免任何崩溃。

**Codegen guard 位置选择**：
`build_scan_node` 不返回 `Result`、`build_exec_params_multi` 返回 `Result`。
Phase 1 同样的设计选择把 Guard 放在 `build_exec_params_multi`。Phase 2 沿用
这个约定，避免 build_scan_node 形状大改。

**ColumnRefFactory 单例风险**：
如果 IMV pipeline 入口构造的 factory 与 analyzer 阶段的 factory 不同实例，
两套 column_id 空间可能冲突。缓解：从 `ImvRewriteInput` 入口接收 factory
或从已有 plan 推导 max column_id 后建立独立 factory；具体路径在
implementation plan 阶段读 entrypoint 代码后确定。

## 13. 后续计划入口

本 spec 通过后，进入 implementation planning。Phase 2 plan 应分以下任务：

1. 加 `OutputColumn::is_internal` 字段并更新所有构造点。
2. 创建 `imv/action_column.rs`（`ImvActionColumn` 描述符 + `ActionColumnValidationRule`）。
3. 创建 `imv/action_propagation.rs`（`InjectActionColumnRule` + `PropagateActionColumnRule`）。
4. 注册 `imv-action-propagation` stage 到 pipeline。
5. 扩展 `imv-validation` stage 注册 `ActionColumnValidationRule`。
6. 修改 `PruneColumns` rule 保留 internal column + IMV marker 处理。
7. 在 `ImvExtension` 上加 `column_factory` 字段，更新 IMV pipeline 入口构造。
8. 加 codegen guard。
9. 各层 unit + e2e tests。
10. Final verification（`cargo test --lib` / `cargo fmt --check` / `cargo build --lib`）。
