# Iceberg IMV Projection/Filter Cutover (Phase 3) Design

日期：2026-05-29

来源：
- `docs/design/specs/2026-05-28-iceberg-imv-rewrite-cutover-umbrella-design.md`（§4 阶段 3）
- `action-column-propagation.md`（Phase 2 / PR #206 已落地）
- `iceberg-scan-delta-version-binding.md`（Phase 1 / PR #203 已落地）

## 1. 目标

把**单表 projection/filter** Iceberg MV 的增量 refresh 从手写 SQL 路径
（`__nr_ivm_delta(...)` TVF + AST mutate + `__change_op` 字符串注入 + 一次性
`InMemoryCatalog`）切换为**消费 `run_imv_rewrite` 产出的可执行计划**。

终态：`refresh_iceberg_mv` 走到 `incremental_refresh_iceberg_mv` 的单表
projection/filter 分支时，不再做任何 AST mutate；而是把原始 MV SELECT +
`IcebergMvRefreshContext` 喂给统一执行入口，由 IMV rewrite pipeline 生成绑定
delta snapshot window、带内部 action column 与 apply key 的 `LogicalPlan`，再走
既有的 optimize → codegen → 执行 → merge sink 链路。

这是 umbrella spec 的阶段 3。Phase 1（scan binding）和 Phase 2（action column）
已落地但 refresh 执行尚未切换；本阶段完成"切换执行"这一步，并移除 Phase 2 留下的
codegen tripwire。

## 2. 非目标

- 不切换其它 refresh shape：单表 aggregate、join projection/filter、join
  aggregate 继续走 legacy SQL 路径（umbrella phase 4 / 5）。
- 不切换 first-refresh / rebuild / empty-base no-op / same-snapshot no-op /
  REFRESH FULL（这些不走 delta marker）。
- 不引入 fallback flag。本阶段 fail-fast，出问题靠 revert PR 回滚。
- 不改 merge sink、commit collector、staging branch 生命周期、UUID guard、
  schema contract 校验、`plan_changes` 行为、empty-delta 短路位置（保留在
  refresh 层）。
- 不改 `IcebergMvRefreshContext` 已有字段（Phase 1/2 已够用）。
- 不重命名 `__change_op`（保持字面列名，merge sink 按名读不变）。
- 不删除 `__nr_ivm_delta` TVF 及其 analyzer/planner/lowering（aggregate/join
  路径仍需）。
- 不清理 `_last_updated_sequence_number`（疑似 vestigial，与本阶段正交）。

## 3. 当前上下文（已核实）

### 3.1 执行入口

`incremental_refresh_iceberg_mv`（`src/engine/mv/iceberg_refresh.rs:6896`）是
单表 projection/filter 增量 refresh 的唯一执行点（由
`refresh_iceberg_mv`，`:1209`，在 `(Some(prev), Some(cur)) if prev != cur` 且
shape 为 `ProjectionFilter` 时进入）。其步骤 4–8（约 280 行）当前：

1. 建一次性 `InMemoryCatalog`，base 用
   `build_iceberg_table_def_for_delta_scan` 注册（暴露 4 个 v3 row-lineage 虚列）。
2. parse MV 物理 SELECT → `mutate_query_for_ivm_delta_scan` 把 base ref 改成
   `__nr_ivm_delta('cat.ns.tbl', from, to)` → `strip_catalog_from_three_part_names`。
3. `append_change_op_to_projection` 在顶层投影字符串注入 `__change_op`。
4. pre-load A9 target locator inputs（仅 delete 侧）。
5. build merge sink + 调 `execute_query_with_options(query, &one_shot_catalog,
   ..., terminal_sink: merge_sink, iceberg_catalogs: Some(...))`。

`iceberg_mv_physical_select_sql`（`src/engine/mv/iceberg_target_apply.rs:34`）
在顶层投影 append `_row_id AS __nova_base_row_id`（apply key），并校验用户列名
不得撞保留名 `__nova_base_row_id`。它被 first-refresh（`iceberg_refresh.rs:4097`）、
rebuild（`:4463`）、增量 PF（`:7096`）三处调用。

### 3.2 执行管道

`execute_query_with_options`（`src/engine/mod.rs:2685`）：
`analyze → plan_query → build_table_stats → optimize → fragment_builder::build
→ execute_plan / coordinator`。当 `terminal_sink` 或 `iceberg_catalogs` 非空时
`force_single_fragment` 为真，refresh 查询强制单 fragment。

### 3.3 已落地的 rewrite 能力

- `run_imv_rewrite`（`src/sql/optimizer/rewrite/imv/entrypoint.rs:33`）接收
  `ImvRewriteInput { plan: LogicalPlan, mv_ctx, disabled_rules, deadline,
  next_column_id }`，返回 `ImvRewriteOutcome`。
- `WrapRootInImvDelta` → `PushDeltaThroughUnary` → `BindIcebergScanRule`
  （`imv-scan-binding`）→ `InjectActionColumnRule` / `PropagateActionColumnRule`
  （`imv-action-propagation`）→ `ActionColumnValidationRule`（`imv-validation`）。
- `BindIcebergScanRule` 把 `ImvDelta(Scan)` 绑成
  `ScanSource::IcebergDeltaTable { from_snapshot_id, to_snapshot_id }`，window 来自
  `IcebergMvRewriteContext`（previous snapshot + pin + UUID 身份守护）。
- `ImvActionColumn`：`__change_op`，`Int8`，non-null，`is_internal`。
- `OutputColumn.is_internal` 通用 flag；column pruning 保留 internal 列。
- `try_run_imv_rewrite_pipeline`（`iceberg_refresh.rs:6315`）当前在 4 个 ctx
  构造点（1415 PF / 1703 / 2453 / 5490）跑 rewrite 并 swallow 结果。

### 3.4 运行时 operator 当前契约

`IcebergDeltaScanOperator`（`src/exec/operators/iceberg_delta_scan.rs`）每 batch：
scanner 产 `[<data>, _file, _pos, _row_id, _last_updated_sequence_number]`
（4 lineage 永远全产）→ `project_scanner_batch_to_contract` 按 field-id 重投影
data 列、lineage 尾按位置透传 → `inject_change_op_column` 追加 `__change_op` →
包成 Chunk（schema = `output_chunk_schema`，来自 codegen scan tuple）。

刚性假设两处：`data_slot_count = slots.len() - 5`（`:246`）与 lineage 尾按位置
透传（`:367`）；operator **从不读** trailing 5 slot 的名字。`output_chunk_schema`
本身（`src/lower/node/iceberg_delta_scan.rs:177`）= `chunk_schema_for_layout`，
已经来自 scan tuple——即"tuple 是真源"在数据上已成立，只是 operator 没按它走。

### 3.5 虚列消费图（已核实，决定注入哪几列）

base delta scan 输出 chunk 里，scan 之上真正被读的内部列只有：

- `__change_op`：merge sink `partition_chunk_by_change_op`
  （`src/engine/mv/iceberg_merge_sink.rs:298`）按名切 INSERT/DELETE。
- `_row_id`：作为 apply key（`__nova_base_row_id`），merge sink 用它在 **target**
  表里定位行做 DELETE。

`_file` / `_pos` **不在 base delta chunk 里被消费**：merge sink 用的 `_file`/`_pos`
来自单独的 **target locator scan**（`iceberg_target_apply.rs:336-378`，由
`load_target_apply_locator_inputs` 扫 target 表）。它们在全局是承重的，但**走的是
另一条 scan 路径，不是本阶段 R1 改动的 delta scan 算子**：
- standalone DELETE：iceberg-rust 原生 `TableScan` + `ArrowReaderBuilder`
  （`delete_flow.rs:853` `scan_for_position_deletes_at`），既不经 NovaRocks pipeline
  也不经 `IcebergDeltaScanOperator`。
- COW UPDATE / target locator scan：各自独立扫 target/base 表。

`IcebergDeltaScanOperator`（`ICEBERG_DELTA_SCAN_NODE`）只由 `ScanSource::IcebergDeltaTable`
产生，而该 source 只来自 `__nr_ivm_delta` TVF 或 `BindIcebergScanRule`——均为 IMV refresh
专属。故 R1 的 blast radius 收窄为"4 个 IMV refresh shape 共用的 delta scan 算子"，
standalone DELETE/UPDATE 完全不受影响。

`_last_updated_sequence_number` 在全仓库未发现 Rust 消费点（疑似 vestigial）。

**v3 `_pos` → `_row_id` 依赖（防误改）**：`_pos` 虽不在 delta scan 输出里被消费，但它是
v3 `_row_id` 推导的必要**内部输入**。Iceberg v3 允许写入时不物化 `_row_id`，读取时按
`first_row_id + _pos` 推（`synthesize_row_id`，`iceberg_delta_scan.rs:573`；规则见
`:548-553` 注释：stored 非 NULL 优先，否则 `first_row_id + position`）。NovaRocks 要求
v3 但**不要求 `_row_id` 物化**（尤其跨引擎 Spark 写的表常不物化），故 `_pos` 不可省。
"要求 v3"消不掉这个依赖。

> 结论：base delta scan 只需注入 `{_row_id, __change_op}`。这恰好 == legacy 有效
> 投影（legacy 顶层 SELECT 也只保留 `<user cols>, _row_id AS __nova_base_row_id,
> __change_op`，base 的 `_file`/`_pos`/`_last_updated_seq` 被那个 SELECT 丢掉，
> 只是旧刚性契约逼 scan tuple 先扛着）。

## 4. 架构

四块改动：(A) 执行入口接入 rewrite；(B) 运行时契约 R1；(C) rewrite 规则补全
apply key + 泛化 propagation；(D) refresh 层简化 + legacy 删除。

### 4.A 执行入口接入（替代 AST mutate 路径）

`execute_query_with_options` 增加可选参数
`mv_refresh_ctx: Option<&IcebergMvRefreshContext>`，在 `plan_query` 之后、
`optimize` 之前条件性跑 rewrite：

```text
analyze(query) → resolved + cte_registry + factory
plan_query(...)  → logical
if let Some(mv_ctx) = mv_refresh_ctx {
    let outcome = run_imv_rewrite(ImvRewriteInput {
        plan: logical,
        mv_ctx: Arc::clone(&mv_ctx.rewrite),
        disabled_rules: current_session_optimizer_settings().disabled_rules,
        deadline: None,
        next_column_id: factory.peek_next_id(),
    })?;                                  // Err 直接 fail-fast，不再 swallow
    logical = consume_outcome_into_logical_plan(outcome)?;
}
optimize(logical, ...) → physical → codegen → execute
```

- `mv_refresh_ctx == None`（普通用户 SELECT、所有非 PF 路径）时 rewrite 一行不跑，
  零回归。
- `next_column_id` 用 `factory.peek_next_id()` seed `ImvExtension` 的
  column-id allocator（PR #206 已抽好）。
- 错误语义从"swallow + 继续 legacy"改为"传出 Err → refresh 失败 →
  `handle_iceberg_mv_commit_error`"（既有错误路径，不新增基础设施）。
- `consume_outcome_into_logical_plan`：早期形态是从 outcome 取出已完全消解 marker
  的 `LogicalPlan`；marker 未消解时由 `imv-validation` 在 `run_imv_rewrite` 内部
  已 fail-fast，这里只做解包。

### 4.B 运行时契约 R1：tuple 即真源

把 `IcebergDeltaScanOperator` 的输出从"固定 5 列尾切分"改为"按名字把内部 superset
投影到 codegen tuple"：

- operator 内部生产**不变**：scanner 永远产全 4 lineage，`inject_change_op_column`
  永远追加 `__change_op`；内部 superset 恒为
  `[<data by field-id>, _file, _pos, _row_id, _last_updated_sequence_number,
  __change_op]`，列名齐全。**`_pos` 必须保留在内部生产**——它是 v3 `_row_id` 推导
  （`first_row_id + _pos`）的输入（见 §3.4）；R1 只在 `_row_id` 算出之后、于 operator
  边界丢弃 `_pos`，不影响 v3 lineage 正确性。不要因为"输出不含 `_pos`"而去 scanner
  里省掉 `_pos` 的计算。
- **新增最终投影步**：以 `output_chunk_schema.slots()` 为权威，逐 slot 按名字从
  superset 取列、按 tuple 顺序排列；tuple 未列出的虚列 drop。
- `build_data_column_projection_plan` 的 data/virtual 切分从"`len-5`"改为"按虚列名
  集合 `{_file,_pos,_row_id,_last_updated_sequence_number,__change_op}` 归类"。
- 删 `ICEBERG_DELTA_TRAILING_VIRTUAL_COLUMN_COUNT = 5`；
  `ICEBERG_DELTA_PRE_CHANGE_OP_LINEAGE_COLUMN_COUNT = 4` 留作 scanner 内部布局常量。
- 更新 `src/connector/iceberg/catalog/backend.rs` 的"keep in lockstep"注释
  （TableDef 虚列暴露与 operator 输出解耦）。

效果：tuple 要 `[k, _row_id, __change_op]` 就只出这 3 列；要全 5 列也照旧
（恒等映射 → aggregate/join legacy 路径不变）。R1 不碰 evolution-correct 的
field-id 投影逻辑——只把"猜 5 列"换成"读 tuple"。

副作用（更严格，非回归）：R1 之后 operator 开始读 trailing slot 的名字；若未来
codegen 在尾部塞了名字不在虚列集合里的 slot，R1 会正确地当 data 列做 field-id 投影。

### 4.C rewrite 规则：apply key plumbing + 泛化 propagation

base delta scan 注入 `{_row_id, __change_op}`，root 暴露 apply key。需要：

1. `__change_op`：`InjectActionColumnRule`（PR #206 已有，不改）。
2. **泛化 `PropagateActionColumnRule`**：从"`__change_op` 专属"改为"对 scan 上任意
   `is_internal` 列通用"，让 `__change_op` 和 `_row_id` 都能穿过 Project/Filter 链。
   - `output_has_action_column` / `find_action_column` / `subtree_has_action_column`
     等 helper 参数化为"按列描述符匹配"，不再 hardcode `ImvActionColumn::NAME` /
     `Int8`。
   - Aggregate/Join/Union 之上 delta 子树的 fail-fast（Phase 4/5/6 占位）保留，
     诊断信息（base FQN）不变。
   - PR #206 的全部 action-column unit test 必须继续通过（`__change_op` 行为不改）。
3. **`InjectRowIdRule`**（新）：给 `ScanSource::IcebergDeltaTable` 的 scan output
   追加 `_row_id`（`is_internal`，`Int64`，**non-null**）。non-null 依据：delta
   scanner 已在 v3 row-lineage 缺失时 fail-loudly（`iceberg_delta_scan.rs:518`
   缺 `first_row_id` 即报错），且下游 apply-key 列
   `ICEBERG_MV_APPLY_KEY_COLUMN` 本身 non-null（`iceberg_target_apply.rs:18`）。
   注册在 `imv-action-propagation` stage，与 `InjectActionColumnRule` 对称；idempotent。
4. **root apply-key 规则**（新，`InjectApplyKeyProjectRule`）：在 marker 消解后的
   plan root 外包 `Project`，append `__nova_base_row_id`（`ExprKind::ColumnRef` 指向
   `_row_id`，`is_internal`，non-null）。注册在 `imv-action-propagation` 之后、
   `imv-validation` 之前。

`_row_id` 经泛化后的 propagation 与 `InjectActionColumnRule` 的 `__change_op` 一样
被带到 root，再由 `InjectApplyKeyProjectRule` 引用。

最终：scan tuple = `[<SELECT 引用的 data 列>, _row_id, __change_op]`；root 输出 =
`[<SELECT 列>, __nova_base_row_id, __change_op]`。merge sink 按名读
`__change_op` + `__nova_base_row_id`，全部对齐，**merge sink 零改动**。

#### Validation 扩展（`imv-validation` stage）

- **V6**：root 输出含 `__nova_base_row_id`，`is_internal`，`Int64`，non-null；缺失/
  类型错 → fail-fast（带 base FQN）。
- **V7**：delta-bound scan 子树可解析出 `_row_id`（供 apply-key project 引用）；
  `InjectRowIdRule` 未命中或被 disable 时兜底报错。
- V1–V5（`__change_op`）泛化后仍成立；`_row_id` 同样走 non-null / is_internal /
  不进 user-visible 输出检查。
- 泄漏边界：`__nova_base_row_id` 是有意写入 target 的 hidden 列
  （`ICEBERG_MV_APPLY_KEY_COLUMN`）。V4 视其为"允许进 target hidden schema、不进
  用户 `SELECT *`"，按 target contract 的 hidden-columns 校验，而非 visible。

### 4.D refresh 层简化 + codegen tripwire 移除

`incremental_refresh_iceberg_mv` 步骤 4–8：

- 删一次性 `InMemoryCatalog` 构造 + `build_iceberg_table_def_for_delta_scan` 调用
  （`:7054`），改用 `state` 的正常 Iceberg catalog 视图。
- 删 `iceberg_mv_physical_select_sql` 调用（`:7096`）、
  `mutate_query_for_ivm_delta_scan` 调用（`:7134`）、
  `append_change_op_to_projection` 调用（`:7188`）。
- pre-load locator inputs（步骤 6）、merge sink 构造（步骤 7）保留。
- 调 `execute_query_with_options(query, &catalog, ..., terminal_sink: merge_sink,
  iceberg_catalogs: Some(...), mv_refresh_ctx: Some(&ctx))`，`query` 为原始 MV
  SELECT 解析结果（不再 mutate）。

净改动：步骤 4–8 从 ~280 行缩到 ~80 行；empty-delta 短路、staging branch、commit、
recovery 不动。

删 codegen tripwire：`reject_internal_action_column`
（`src/sql/codegen/fragment_builder.rs:145`）+ `visit_scan` 调用（`:497`）。

#### Legacy 删除边界（已核实）

同 PR 删除（projection/filter 专属）：
- `mutate_query_for_ivm_delta_scan` + 其 7 个 unit test（非测试调用点仅 7134）。
- `append_change_op_to_projection`（唯一调用点 7188）。
- PF 路径的 `try_run_imv_rewrite_pipeline` 调用（1415）——改为 4.A 的真实消费。

保留（其它路径在用）：
- `iceberg_mv_physical_select_sql` 函数（first-refresh 4097 / rebuild 4463 仍用）；
  仅摘除 PF 增量调用（7096）。
- `build_iceberg_table_def_for_delta_scan` 函数（6386 / 6832 仍用）；仅删 7054 调用。
- `__nr_ivm_delta` TVF + analyzer/planner/lowering、`build_nr_ivm_delta_table_factor*`、
  `iceberg_aggregate_incremental_delta_select_sql`、
  `iceberg_join_aggregate_branch_delta_sql`。
- `try_run_imv_rewrite_pipeline` 函数 + aggregate/join 三处调用（1703/2453/5490）。

保留名校验：摘除 7096 调用后，增量 PF 路径丢失
`iceberg_mv_physical_select_sql` 的"用户列名撞 `__nova_base_row_id`"校验。实现期
verify：CREATE MV 层是否已校验保留名；若无，补在建表层（不留在 refresh 路径）。

## 5. 数据流（cutover 后，单表 projection/filter 增量）

```text
refresh_iceberg_mv
  └─ incremental_refresh_iceberg_mv  (prev != cur, ProjectionFilter)
       ├─ plan_changes → batch (empty-delta 短路仍在此层)
       ├─ build merge sink + locator inputs
       └─ execute_query_with_options(原始 MV SELECT, mv_refresh_ctx=Some)
            ├─ analyze → plan_query → LogicalPlan (普通 IcebergScan)
            ├─ run_imv_rewrite:
            │    WrapRootInImvDelta → PushDeltaThroughUnary
            │    → BindIcebergScan (IcebergDeltaTable, window=(prev,cur])
            │    → InjectActionColumn (__change_op) + InjectRowId (_row_id)
            │    → PropagateActionColumn (泛化: 带 __change_op + _row_id 过 Project/Filter)
            │    → InjectApplyKeyProject (root: __nova_base_row_id = _row_id)
            │    → imv-validation (V1–V7)
            ├─ optimize → codegen (无 tripwire)
            └─ execute → IcebergDeltaScan (R1: 输出按 tuple = [data, _row_id, __change_op])
                 → ... → IcebergMergeSink (按名读 __change_op + __nova_base_row_id)
```

## 6. 失败语义

- rewrite 不完整（marker 未消解、缺 action/apply-key 列、类型错）→ `run_imv_rewrite`
  返回 Err → `execute_query_with_options` 传出 → refresh fail-fast，走
  `handle_iceberg_mv_commit_error`（abort staging、清理）。
- 无隐式 full refresh、无 best-effort。`plan_changes` 请求 full refresh 的既有
  policy 信号（rebuild）保留在 refresh 层，不受本阶段影响。
- empty-delta 仍在 refresh 层短路（不下沉到执行层）。

## 7. 测试

硬 gate：
- `iceberg-ivm` SQL suite 61/61 不回归（R1 对 aggregate/join 三路 no-op + PF
  cutover 端到端的唯一真锁）。
- `cargo test --lib` 全绿；`cargo test --lib imv` 子集。

新增 EXPLAIN plan-shape golden（`sql-tests/optimizer/imv_projection_filter_cutover_*.sql`，
`-- @explain_contains`）：
- 计划不出现 `__nr_ivm_delta`。
- 出现 `IcebergDeltaScan` 且携带绑定 snapshot window `(from, to]`。
- `__change_op` / `_row_id` / `__nova_base_row_id` 标 internal、不进 user-visible 输出。

新增 unit test：
- 泛化 propagation 同时带 `_row_id` + `__change_op` 过 Project/Filter 链。
- `InjectRowIdRule` / `InjectApplyKeyProjectRule` 的 inject + idempotent。
- V6 / V7 命中与 fail-fast。
- R1 operator name-based 投影：tuple `[k, _row_id, __change_op]` → 输出正好三列、
  丢弃 `_file`/`_pos`/`_last_updated_seq`；tuple 全 5 列 → 恒等映射（证明 legacy
  aggregate/join 不变）。

## 8. 风险与缓解

| 风险 | 缓解 |
| --- | --- |
| R1 blast radius：operator 被 4 shape 共用，Phase 3 只验 PF | iceberg-ivm 61/61 + R1 "全 5 列恒等映射" unit test |
| 泛化 `PropagateActionColumn` 动 PR #206 代码 | 保留全部 action-column unit test，泛化为参数化，`__change_op` 断言不改 |
| 无 fallback flag，出错只能 revert | option 2 决策；PR 小、同 PR 切+删；`mv_refresh_ctx==None` 时零风险 |
| 保留名校验随 7096 调用被摘 | 实现期确认 CREATE MV 层有校验，否则补在建表层 |
| `force_single_fragment` 已因 terminal_sink+iceberg_catalogs 生效 | rewrite 在 optimize 之前跑，不影响 collapse；实现期跑一条增量 refresh 确认 |
| `_last_updated_sequence_number` 疑似 vestigial | 与本阶段正交（不注入）；清理另开 task |

## 9. PR 自检对照（umbrella checklist）

1. 完善层：rewrite 执行 cutover（rewrite outcome → 执行）。
2. 新 silent fallback？无——fail-fast，移除 legacy swallow。
3. 内部 identity 依赖用户输出列？否——`_row_id` / `__change_op` /
   `__nova_base_row_id` 均 `is_internal`，apply key 进 target hidden contract。
4. optimizer 信息泄漏到 fragment/codegen？否——marker 在 logical rewrite 内消解，
   codegen tripwire 移除后内部列由 rewrite 显式管理。
5. SQL fixture 覆盖 refresh 前后等价性？是——iceberg-ivm 61/61 + 新 EXPLAIN golden。
6. 更新任务文档/索引？是——roadmap 任务 3/4/5 已标 ✅，本阶段对应 umbrella phase 3。
