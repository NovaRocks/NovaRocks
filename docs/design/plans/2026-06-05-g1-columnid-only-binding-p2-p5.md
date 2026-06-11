# G1 ColumnId-only Binding P2-P5 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 P1 已完成的 IR 地基上,把 planner / codegen / optimizer 的语义列绑定迁移到 ColumnId-only:先补齐所有 planner 产 `UNSET` / 产错 id 的结构性缺口,再删除 codegen name fallback,最后把 optimizer 统计与重写规则从按 name keying 改为按 `ColumnId` keying 并清理死代码。

**Architecture:** 单分支、分阶段、每个 task 独立红绿验证。P2 保留现有 fallback 但加非致命审计计数并把计数打到 0;P3 在计数为 0 后删除 fallback 与 name 索引,新增 fail-fast `verify_id_binding`;P4 re-key optimizer 统计和规则;P5 删除所有剩余绑定用 name helper。Repeat / Decode 的字段类型迁移与 reader 迁移一起放在 P3,延续 P1 计划中的 no-shim 决策。

**Tech Stack:** Rust;`ColumnId` / `ColumnRefFactory`;standalone planner/analyzer;optimizer rewrite + cascades;`PlanFragmentBuilder`;sql-test-runner;`dev-opt` profile。

**关联设计:** `docs/design/specs/2026-06-05-g1-columnid-only-binding-design.md` §4-§7。

---

## 当前基线

- Branch: `codex/g1-columnid-binding-p1`,ahead `origin/pr/256` 10 commits。
- P1 已完成:
  - `AggregateCall.output_column_id`
  - `WindowExpr.output_column_id`
  - `GROUPING()` marker 复用 group-by key `ColumnId`
  - exchange payload metadata 修复
- P1 验证:
  - `cargo build --profile dev-opt` PASS
  - `cargo test --profile dev-opt` PASS
  - `cargo test --profile dev-opt data_stream_sink -- --nocapture` PASS
  - `tpc-h` SQL verify PASS 22/22
  - `tpc-ds q85` 仍是单独性能/稳定性 gap:不再出现 wire metadata 缺失,但默认 120s exchange wait timeout;加长 wait 后服务端连接中断,无 panic/WARN/ERROR。

## File Structure

| 文件 | 阶段 | 动作 |
|---|---|---|
| `src/sql/planner/mod.rs` | P2 | aggregate/window/select-alias/VALUES/GenerateSeries/repeat/USING 输出 id 补齐与表达式改写 |
| `src/sql/planner/plan.rs` | P2/P3 | `GenerateSeriesNode` 输出 id;`DecodeMapping` 与 `RepeatPlanNode` 字段迁移到 `ColumnId` |
| `src/sql/analyzer/mod.rs` | P2 | USING / FULL OUTER USING id 语义修正;删除 qualifier-steering 的生产依赖 |
| `src/sql/analyzer/scope.rs` | P2/P5 | 停止使用并最终删除 `canonical_qualifier` |
| `src/sql/analyzer/resolve_from.rs` | P2 | `GenerateSeriesRelation` / derived relation 输出复用 producing id |
| `src/sql/analyzer/subquery_rewrite.rs` | P2 | subquery-alias 重曝光使用 producing id,不 fresh id |
| `src/sql/codegen/fallback_audit.rs` | P2/P5 | P2 临时审计 name fallback 命中;P5 删除 |
| `src/sql/codegen/resolve.rs` | P3/P5 | `ExprScope` 切到 id-only;新增窄 `resolve_internal_by_name`;删除 name 索引 |
| `src/sql/codegen/expr_compiler.rs` | P2/P3 | P2 记录 fallback;P3 删除 fallback/strict/display 重匹配 |
| `src/sql/codegen/id_binding_verifier.rs` | P3 | 新增 codegen 入口 fail-fast plan walker |
| `src/sql/codegen/fragment_builder.rs` | P3 | 每个 `visit_*` 按 `ColumnId` 注册 scope;Repeat/Decode reader 迁移;dict 传播按 id |
| `src/sql/codegen/nodes.rs` | P3/P4 | Decode/codegen 输出 id 与 connector stats 边界保持清晰 |
| `src/sql/optimizer/operator.rs` | P2/P3 | `LogicalGenerateSeriesOp` / `PhysicalGenerateSeriesOp` 输出 id;Repeat/Decode ColumnId 模型 |
| `src/sql/optimizer/convert.rs` | P2/P3 | logical -> operator 时保留 id 字段 |
| `src/sql/optimizer/cascades_rules/implement.rs` | P2/P3/P4 | physical op 构造保留 id;迁移 cascades 中 name-based helper |
| `src/sql/optimizer/statistics.rs` | P4 | `Statistics` / `TableStatistics` column stats re-key |
| `src/sql/optimizer/stats.rs` | P4 | 统计传播按 `ColumnId` |
| `src/sql/optimizer/logical_props.rs` | P4 | logical props column stats 改 `HashMap<ColumnId, _>` |
| `src/sql/optimizer/memo.rs` | P4 | group logical props 统计改 id-keyed |
| `src/sql/optimizer/estimate/*.rs` | P4 | NDV/selectivity/join condition 提取 `ColumnId` |
| `src/sql/optimizer/rewrite/rules/**/*.rs` | P4/P5 | predicate pushdown/UKFK/not-null/low-cardinality/column pruning helper id 化 |
| `sql-tests/optimizer/*.sql` | P2/P4 | 新增 plan-shape/cost golden,覆盖同名列和 id-only 规则 |
| `sql-tests/*/*.sql` | P2/P3 | 新增或扩展 window/grouping/subquery/USING/VALUES/GenerateSeries/Decode cases |

---

## Execution Rules

- [ ] 每个 task 开始前用 fresh subagent 做 focused implementation 或 focused review。主 agent 只合并已验证的结果。
- [ ] 每个 task 先写 failing test 或 failing grep gate,确认失败后再改生产代码。
- [ ] 每个 task 完成后至少运行该 task 指定的 targeted test 和 `cargo fmt --check`。
- [ ] 每个阶段结束时提交英文 commit。提交粒度按 task 或 phase 拆,不要把 P2-P5 压成单个大 commit。
- [ ] 不引入 id->name 临时桥。P2 可保留旧 fallback 作为网,但不能新增新的 semantic name binding。
- [ ] IMV `AggregateStateMergeOp` 暂不改模型;只允许通过 P3 的 `resolve_internal_by_name` 窄通道读取内部机器列。

---

## Task 1 - P2: 加 name fallback 非致命审计计数

**目的:** 在删除 fallback 前获得可运行证据。P2 的目标不是马上 fail-fast,而是把 fallback 命中计数降到 0。

**Files:**
- Add: `src/sql/codegen/fallback_audit.rs`
- Modify: `src/sql/codegen/mod.rs`
- Modify: `src/sql/codegen/expr_compiler.rs`
- Test: `src/sql/codegen/expr_compiler.rs` 或 `src/sql/codegen/fallback_audit.rs`

- [x] Step 1: 写红测 `p2_name_fallback_audit_records_columnref_fallback`
  - 构造一个只有 name binding、没有 id binding 的 `ExprScope`。
  - 编译 `ColumnRef { column_id: ColumnId::UNSET, column: "a" }`。
  - 断言编译成功且 `fallback_audit::snapshot().column_ref_name_fallbacks == 1`。
  - Run: `cargo test --profile dev-opt p2_name_fallback_audit_records_columnref_fallback -- --nocapture`
  - Expected: FAIL,因为审计模块不存在。

- [x] Step 2: 实现审计模块
  - `fallback_audit.rs` 定义:
    - `pub(crate) struct FallbackAuditSnapshot { column_ref_name_fallbacks: u64, display_expr_name_fallbacks: u64, aggregate_display_name_fallbacks: u64 }`
    - `AtomicU64` 计数器
    - `record_column_ref_name_fallback()`
    - `record_display_expr_name_fallback()`
    - `record_aggregate_display_name_fallback()`
    - `snapshot()`
    - `#[cfg(test)] reset()`
  - `expr_compiler.rs` 三处 fallback 命中时记录:
    - `ColumnRef` arm 的 `resolve_column` fallback
    - `FunctionCall` display name 重匹配
    - `AggregateCall` display name 重匹配

- [x] Step 3: 加一个审计归零测试入口
  - 增加 helper `assert_no_codegen_name_fallbacks_after<F>(f: F)` 只用于测试。
  - 后续 P2 cases 都用该 helper 包住 planner -> optimizer -> codegen 路径。

- [x] Step 4: 验证
  - Run: `cargo test --profile dev-opt p2_name_fallback_audit_records_columnref_fallback -- --nocapture`
  - Run: `cargo fmt --check`

- [x] Step 5: Commit
  - `git commit -m "codegen: audit name fallback hits before ColumnId-only binding"`

---

## Task 2 - P2: aggregate 结果和计算型 group key 改写为 ColumnRef(id)

**目的:** 删除 project-over-aggregate 依赖 display 字符串重匹配的主因。

**Files:**
- Modify: `src/sql/planner/mod.rs`
- Test: `src/sql/planner/mod.rs`
- SQL Test: `sql-tests/optimizer/column_id_binding_aggregate.sql`

- [x] Step 1: 写红测 `p2_aggregate_projection_rewrites_agg_call_to_output_id_ref`
  - SQL: `SELECT sum(b) + 1 AS s1 FROM t`
  - 断言上层 Project 的 `sum(b)+1` 中,`AggregateCall(sum)` 被替换成 `ExprKind::ColumnRef { column_id: aggregate.output_column_id, .. }`。
  - 断言 codegen fallback audit 为 0。
  - Run: `cargo test --profile dev-opt p2_aggregate_projection_rewrites_agg_call_to_output_id_ref -- --nocapture`

- [x] Step 2: 写红测 `p2_computed_group_key_rewrites_by_structural_expr`
  - SQL: `SELECT a + 1 AS k, sum(b) FROM t GROUP BY a + 1`
  - 断言 projection 中的 `a + 1` 被改写为 `ColumnRef(group_key_output_id)`。
  - 断言没有调用 `typed_expr_display_name` 做语义匹配;可用 grep gate:
    - Run: `rg -n "typed_expr_display_name\\(gb\\)|typed_expr_display_name\\(expr\\)" src/sql/planner/mod.rs`
    - Expected after green:不能命中 group-key rewrite 路径。

- [x] Step 3: 实现 aggregate call 替换
  - 在 `split_projection_for_aggregate` 内,`collect_aggregates` 后建立 aggregate signature -> `output_column_id` 映射。
  - 新增 `rewrite_agg_calls_to_refs(expr, aggregate_calls)`。
  - 命中 `ExprKind::AggregateCall` 时返回:
    - `ExprKind::ColumnRef { qualifier: None, column: aggregate_call.name.clone(), column_id: aggregate_call.output_column_id }`
    - `data_type` / `nullable` 保持原 expr。
  - HAVING 同样走该改写,避免 HAVING aggregate display fallback。

- [x] Step 4: 实现 group key 结构化匹配
  - 新增 `typed_expr_semantically_eq(left, right)` 递归比较:
    - `ColumnRef`:优先比较非 `UNSET` `column_id`;两边都 `UNSET` 时比较 qualifier/name 仅用于 analyzer 入口前的测试 fixture。
    - Literal/Binary/Unary/Function/Cast/Nested/Case/Between/Like/IsNull/Window/Aggregate:递归比较结构和函数名。
  - `rewrite_exact_group_by_expr_ref` 改成接受 `(group_expr, output_column_id)` 列表。
  - 命中后返回 `ColumnRef(group_output_id)`。
  - `typed_expr_display_name` 只保留为输出 label,不作为匹配 key。

- [ ] Step 5: SQL golden
  - 新增 `sql-tests/optimizer/column_id_binding_aggregate.sql`:
    - `SELECT sum(b) + 1 FROM t`
    - `SELECT a + 1, sum(b) FROM t GROUP BY a + 1`
    - `SELECT mod(a, 2), count(*) FROM t GROUP BY mod(a, 2)`
  - 每个 case 增加 `-- @explain_contains=stats={rows=` 或现有 plan-shape 断言,并在 rust unit test 里检查 audit 为 0。

- [ ] Step 6: 验证
  - Run: `cargo test --profile dev-opt p2_aggregate_projection_rewrites_ -- --nocapture`
  - Run: `cargo test --profile dev-opt p2_computed_group_key_rewrites_by_structural_expr -- --nocapture`
  - Run: `cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --suite optimizer --only column_id_binding_aggregate --mode verify`
  - Run: `cargo fmt --check`

- [x] Step 7: Commit
  - `git commit -m "planner: rewrite aggregate outputs and computed group keys by ColumnId"`

---

## Task 3 - P2: window、select alias、VALUES、GenerateSeries 输出 id 补齐

**目的:** 消除 P2 缺口 C/D/E/F,确保这些合成输出不再产生 `UNSET` executable `ColumnRef`。

**Files:**
- Modify: `src/sql/planner/mod.rs`
- Modify: `src/sql/planner/plan.rs`
- Modify: `src/sql/analyzer/resolve_from.rs`
- Modify: `src/sql/analyzer/subquery_rewrite.rs`
- Modify: `src/sql/optimizer/operator.rs`
- Modify: `src/sql/optimizer/convert.rs`
- Modify: `src/sql/optimizer/cascades_rules/implement.rs`
- Test: `src/sql/planner/mod.rs`, `src/sql/analyzer/subquery_rewrite.rs`, `src/sql/codegen/fragment_builder.rs`

- [x] Step 1: 写红测 `p2_window_call_rewrites_to_window_output_id`
  - SQL: `SELECT row_number() OVER (PARTITION BY a ORDER BY b) + 1 FROM t`
  - 断言 Project 中的 window call 被替换成 `ColumnRef(window_expr.output_column_id)`。
  - Run: `cargo test --profile dev-opt p2_window_call_rewrites_to_window_output_id -- --nocapture`

- [x] Step 2: 写红测 `p2_select_alias_remap_preserves_inner_output_id`
  - SQL: `SELECT a AS x FROM t ORDER BY x`
  - 定位 `__nr_sel_0` 的 `ColumnRef`,断言其 `column_id` 等于内层 project item 的 `output_column_id`,且非 `UNSET`。

- [x] Step 3: 写红测 `p2_values_output_uses_single_column_id`
  - SQL: `VALUES (1, 2), (3, 4)`
  - 断言 `ValuesNode.columns` 与 query output columns 使用同一组 id。

- [x] Step 4: 写红测 `p2_generate_series_output_has_column_id_through_codegen`
  - SQL: `SELECT * FROM TABLE(generate_series(1, 3, 1)) AS gs(v)`
  - 断言 `GenerateSeriesNode` / `LogicalGenerateSeriesOp` / `PhysicalGenerateSeriesOp` 都携带同一个 `output_column_id`。

- [x] Step 5: 实现 window id rewrite
  - `rewrite_window_calls` 使用 P1 已填充的 `WindowExpr.output_column_id`。
  - 返回 `ColumnRef(window_output_id)`,不再用 `ColumnId::UNSET` + `output_name`。

- [x] Step 6: 实现 select alias id remap
  - `remap_select_alias_refs` 的入参从 `name_to_idx` 扩展为 `name_to_output: HashMap<String, (usize, ColumnId)>`。
  - 生成 `__nr_sel_<idx>` 时保留对应 inner output id。
  - 只把 `__nr_sel_<idx>` 当 display label,不把它当 semantic binding key。

- [x] Step 7: 实现 VALUES id 一致性
  - `plan_values` 创建 `OutputColumn` 时一次性铸 id。
  - root query output 直接复用 `ValuesNode.columns` 的 id,不重新铸造。

- [x] Step 8: 实现 GenerateSeries id
  - 给 `GenerateSeriesNode` 增加 `output_column_id: ColumnId`。
  - `resolve_from.rs` 构造 `GenerateSeriesRelation` 时使用 factory 铸造并保存 id。
  - `plan_output_columns(LogicalPlan::GenerateSeries)` 返回该 id。
  - `LogicalGenerateSeriesOp` / `PhysicalGenerateSeriesOp` 增加 `output_column_id` 并在 convert/implement 中透传。
  - `visit_generate_series` 注册 output scope 时用 `add_column_with_id(output_column_id, alias, column_name, binding)`。

- [x] Step 9: 验证
  - Run: `cargo test --profile dev-opt p2_window_call_rewrites_to_window_output_id -- --nocapture`
  - Run: `cargo test --profile dev-opt p2_select_alias_remap_preserves_inner_output_id -- --nocapture`
  - Run: `cargo test --profile dev-opt p2_values_output_uses_single_column_id -- --nocapture`
  - Run: `cargo test --profile dev-opt p2_generate_series_output_has_column_id_through_codegen -- --nocapture`
  - Run: `cargo fmt --check`

- [x] Step 10: Commit
  - `git commit -m "planner: preserve ColumnId for window aliases values and generate_series"`

---

## Task 4 - P2: Repeat / USING / subquery alias 的 producing id 连续性

**目的:** 消除 P2 缺口 G/H/I/J。Repeat 字段类型迁移仍放 P3,但 P2 必须停止产出依赖 name fallback 的 executable `ColumnRef`。

**Files:**
- Modify: `src/sql/planner/mod.rs`
- Modify: `src/sql/analyzer/mod.rs`
- Modify: `src/sql/analyzer/scope.rs`
- Modify: `src/sql/analyzer/resolve_expr.rs`
- Modify: `src/sql/analyzer/subquery_rewrite.rs`
- Modify: `src/sql/optimizer/rewrite/required_columns.rs`
- Test: `src/sql/planner/mod.rs`, `src/sql/analyzer/mod.rs`, `src/sql/analyzer/subquery_rewrite.rs`
- SQL Test: `sql-tests/optimizer/column_id_binding_repeat_using.sql`

- [x] Step 1: 写红测 `p2_rollup_materialized_key_has_real_id`
  - SQL: `SELECT grouping(a + 1), a + 1 FROM t GROUP BY ROLLUP(a + 1)`
  - 断言 `prepare_repeat_input` 物化的 `__repeat_group_key_0` Project item 有真实 id,后续 repeat/grouping refs 复用该 id 或对应 repeat output id。

- [x] Step 2: 写红测 `p2_using_reference_keeps_analyzer_selected_id`
  - SQL: `SELECT k FROM l JOIN r USING(k)`
  - 断言 unqualified `k` 的 `ColumnRef.column_id` 等于 analyzer 选定侧输出 id,而不是通过 `canonical_qualifier` 改 qualifier。

- [x] Step 3: 写红测 `p2_full_outer_using_coalesce_has_project_output_id`
  - SQL: `SELECT k FROM l FULL OUTER JOIN r USING(k)`
  - 断言 `COALESCE(l.k, r.k)` 作为计算型 Project 输出有真实 id,上层引用该 id。

- [x] Step 4: 写红测 `p2_subquery_alias_reexposes_producing_id`
  - SQL: `SELECT x FROM (SELECT a AS x FROM t) s WHERE x > 1`
  - 断言外层 `x` 与内层 `a AS x` producing id 一致。

- [x] Step 5: 实现 Repeat 非 ColumnRef key 物化 id
  - `prepare_repeat_input` 中每个非 `ColumnRef` rollup key 物化成 Project item 时铸造 `materialized_id`。
  - 对应 repeat 输出再铸造 `repeat_output_id`。
  - 在 P2 内保留旧 `grouping_key_aliases: Vec<(String, String)>` 供现有 reader 使用,但新增 side table 或局部 map `(materialized_id, repeat_output_id)` 供 expression rewrite 使用。
  - `rewrite_grouping_key_refs` 不再写 `qualifier = "__repeat_group"`;它返回带 `repeat_output_id` 的 `ColumnRef`。

- [x] Step 6: 修 USING
  - INNER/LEFT/RIGHT USING:解析出的 unqualified USING column 直接保留 analyzer 选定侧 id。
  - FULL OUTER USING:生成 `COALESCE` project output id,后续 unqualified 引用绑定到该 output id。
  - `canonical_qualifier` 仅临时保留给旧测试或 dead code,生产路径不再依赖它。

- [x] Step 7: 修 subquery alias 重曝光
  - `subquery_rewrite.rs` / derived relation scope 注册列时,从 producer `OutputColumn.column_id` 调用 `add_column_with_id` 或 analyzer 等价 API。
  - 禁止为 alias wrapper fresh 一个新 id,除非该 alias 是新的计算表达式输出。

- [x] Step 8: SQL golden
  - 新增 `sql-tests/optimizer/column_id_binding_repeat_using.sql`:
    - rollup computed key
    - cube simple key
    - inner/left/right/full outer using
    - subquery alias with WHERE and ORDER BY
  - 每个 case 验证结果;可加 `-- @explain_contains=Repeat` / `HashJoin`。

- [x] Step 9: 验证 P2 fallback 归零
  - Run: `cargo test --profile dev-opt p2_rollup_materialized_key_has_real_id -- --nocapture`
  - Run: `cargo test --profile dev-opt p2_using_reference_keeps_analyzer_selected_id -- --nocapture`
  - Run: `cargo test --profile dev-opt p2_full_outer_using_coalesce_has_project_output_id -- --nocapture`
  - Run: `cargo test --profile dev-opt p2_subquery_alias_reexposes_producing_id -- --nocapture`
  - Run: `cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --suite optimizer --only column_id_binding_repeat_using --mode verify`
  - Run: `cargo fmt --check`

- [x] Step 10: Commit
  - `git commit -m "planner: preserve ColumnId across repeat using and subquery aliases"`

---

## Task 5 - P2 Gate: targeted SQL surfaces fallback count 为 0

**目的:** 用可重复 evidence 证明 P3 删除 fallback 是安全操作。

**Files:**
- Modify: `src/sql/codegen/fallback_audit.rs`
- Test: `src/sql/codegen/fragment_builder.rs` 或 `src/sql/planner/mod.rs`
- SQL Test: existing suites only

- [x] Step 1: 新增集成式 rust test `p2_targeted_surfaces_do_not_use_name_fallback`
  - 覆盖 queries:
    - aggregate: `SELECT sum(b)+1 FROM t`
    - computed group key: `SELECT a+1, count(*) FROM t GROUP BY a+1`
    - window: `SELECT row_number() OVER (PARTITION BY a ORDER BY b)+1 FROM t`
    - select alias order: `SELECT a AS x FROM t ORDER BY x`
    - values: `SELECT * FROM (VALUES (1, 2)) v(a, b)`
    - generate_series: `SELECT * FROM TABLE(generate_series(1,3,1)) AS gs(v)`
    - rollup: `SELECT grouping(a), a FROM t GROUP BY ROLLUP(a)`
    - using: `SELECT k FROM l JOIN r USING(k)`
    - subquery alias: `SELECT x FROM (SELECT a AS x FROM t) s WHERE x > 0`
  - 每个 query:reset audit -> plan/optimize/codegen -> assert snapshot 全 0。
  - 2026-06-06 evidence:
    - RED: aggregate case hit `column_ref_name_fallbacks=1`.
    - GREEN: `cargo test --profile dev-opt p2_targeted_surfaces_do_not_use_name_fallback -- --nocapture` PASS.
    - Fixes included aggregate group/output id registration, window output id registration, and Repeat `GROUPING()` virtual output id propagation.

- [x] Step 2: 跑 SQL targeted suites
  - Run: `cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --suite optimizer --mode verify`
  - Run: `cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --suite join --mode verify`
  - Run: `cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --suite cte --mode verify`
  - 2026-06-06 evidence:
    - `optimizer --mode verify` PASS 39/39 after updating five `count(*)` EXPLAIN goldens from old fallback display `count() AS count(*)` to direct semantic display `count(*)`.
    - `join --mode verify` PASS 60/60.
    - `cte --mode verify` PASS 3/3.

- [x] Step 3: grep gate
  - Run: `rg -n "ColumnId::UNSET|resolve_column\\(|typed_expr_display_name\\(" src/sql/planner src/sql/analyzer src/sql/codegen`
  - 人审分类:
    - allow: display/output labels, SQL parse/analyzer入口, connector boundary, tests
    - block: executable `ColumnRef` construction, codegen semantic binding, optimizer matching
  - 2026-06-06 classification:
    - allow: display/output labels, audit/fallback implementation still present for P2, tests/fixtures, analyzer parse/default literal sentinels, connector/dict/internal boundary lookups.
    - P3 block list remains intentional: `ExprCompiler` name fallback paths, general `ExprScope::resolve_column`, Repeat/Decode/name lookup migration points.

- [x] Step 4: Commit
  - `git commit -m "test: prove targeted ColumnId binding surfaces avoid name fallback"`

---

## Task 6 - P3: `ExprScope` 改 id-only,保留内部机器列窄通道

**目的:** 让 scope 类型层面无法做普通 name semantic binding。

**Files:**
- Modify: `src/sql/codegen/resolve.rs`
- Modify: `src/sql/codegen/fragment_builder.rs`
- Modify: `src/sql/codegen/expr_compiler.rs`
- Test: `src/sql/codegen/resolve.rs`

- [x] Step 1: 写红测 `p3_expr_scope_does_not_resolve_semantic_columns_by_name`
  - `ExprScope::add_column_with_id(real_id, None, "a", binding)`
  - 断言 `resolve_by_id(real_id)` 成功。
  - 断言不存在普通 name resolver;若测试无法直接表达,先改调用点使 `resolve_column` 删除后编译失败作为红。

- [x] Step 2: 写红测 `p3_internal_name_channel_is_explicit`
  - `scope.add_internal_column("__change_op", binding)`
  - `resolve_internal_by_name("__change_op")` 成功。
  - `resolve_internal_by_name("a")` 失败,除非显式 add internal。

- [x] Step 3: 修改 `ExprScope`
  - 删除字段:
    - `qualified`
    - `unqualified`
  - 删除方法:
    - `add_qualified_alias`
    - `add_unqualified_alias`
    - `resolve_column`
    - `has_id_bindings`
    - `binding_has_id_index`
  - 保留:
    - `ordered: Vec<(String, ColumnBinding)>` 仅供 `SELECT *` / output schema display
    - `by_id: HashMap<ColumnId, ColumnBinding>`
  - 新增:
    - `internal_by_name: HashMap<String, ColumnBinding>`
    - `add_internal_column(name, binding)`
    - `resolve_internal_by_name(name)`

- [x] Step 4: 修改所有 scope 注册
  - 普通输出全部调用 `add_column_with_id` 或 `add_id_alias`。
  - `add_column` 若保留,签名必须要求 `ColumnId` 或只追加 `ordered`,不能建立 name binding。
  - `AggregateStateMerge` 内部列用 `add_internal_column("__change_op", ...)` 等显式注册。

- [x] Step 5: 验证
  - Run: `cargo test --profile dev-opt p3_expr_scope_ -- --nocapture`
  - Run: `cargo build --profile dev-opt`
  - Run: `cargo fmt --check`
  - 2026-06-06 evidence:
    - `cargo test --profile dev-opt p3_ -- --nocapture` PASS for 6 P3 scope/compiler tests.
    - `cargo build --profile dev-opt` PASS.
    - `cargo fmt` applied; later `cargo fmt --check` is part of phase verification.

- [x] Step 6: Commit
  - `git commit -m "codegen: make ExprScope semantic binding ColumnId-only"`

---

## Task 7 - P3: 删除 expr compiler fallback / strict / display 重匹配

**目的:** 真正切断普通 `ColumnRef` 的 name fallback。

**Files:**
- Modify: `src/sql/codegen/expr_compiler.rs`
- Modify: `src/sql/codegen/fragment_builder.rs`
- Test: `src/sql/codegen/expr_compiler.rs`

- [x] Step 1: 写红测 `p3_columnref_without_id_fails_fast`
  - 构造 `ColumnRef { column_id: ColumnId::UNSET, column: "a" }`。
  - scope 即使有 display `"a"` 的 ordered column,编译也必须失败。
  - 期望错误包含 `ColumnRef 'a' has no ColumnId`。

- [x] Step 2: 写红测 `p3_aggregate_call_is_not_display_rebound_in_expr_compiler`
  - 构造 project-over-aggregate scope 中带同名 output 的场景。
  - 直接编译 `ExprKind::AggregateCall` 应进入函数编译或报错,不能通过 display name 返回 slot。
  - 正常 planner 路径应已在 P2 改写为 `ColumnRef(output_id)`。

- [x] Step 3: 修改 `ExprKind::ColumnRef` arm
  - `column_id == ColumnId::UNSET` 直接 `Err("ColumnRef '<name>' has no ColumnId in executable plan")`。
  - `scope.resolve_by_id(column_id)` miss 直接 `Err("ColumnId(<id>) for column '<name>' cannot be resolved in current scope")`。
  - 删除 `strict_missing_id` 字段、`for_join_side_binding`、`has_id_bindings` 和 `binding_has_id_index` 分支。

- [x] Step 4: 删除 display 重匹配
  - `FunctionCall` arm 删除 `typed_expr_display_name(expr)` + `scope.resolve_column`。
  - `AggregateCall` arm 删除 `agg_call_display_name_from_parts` + `scope.resolve_column`。
  - compiler 只编译真实函数/聚合函数;project-over-aggregate 由 P2 `ColumnRef(id)` 表达。

- [x] Step 5: 验证
  - Run: `cargo test --profile dev-opt p3_columnref_without_id_fails_fast -- --nocapture`
  - Run: `cargo test --profile dev-opt p3_aggregate_call_is_not_display_rebound_in_expr_compiler -- --nocapture`
  - Run: `cargo build --profile dev-opt`
  - Run: `cargo fmt --check`
  - 2026-06-06 evidence:
    - `cargo test --profile dev-opt p3_ -- --nocapture` PASS for `p3_columnref_without_id_fails_fast`, `p3_column_id_mismatch_does_not_fall_back_to_name`, `p3_internal_name_channel_is_explicit`, and `p3_aggregate_call_is_not_display_rebound_in_expr_compiler`.
    - `cargo build --profile dev-opt` PASS.

- [x] Step 6: Commit
  - `git commit -m "codegen: remove name fallback from expression compilation"`

---

## Task 8 - P3: 新增 `verify_id_binding` fail-fast plan walker

**目的:** 让未来任何漏网 `UNSET` / scope miss 在 codegen 入口响亮失败。

**Files:**
- Add: `src/sql/codegen/id_binding_verifier.rs`
- Modify: `src/sql/codegen/mod.rs`
- Modify: `src/sql/codegen/fragment_builder.rs`
- Test: `src/sql/codegen/id_binding_verifier.rs`

- [x] Step 1: 写红测 `p3_verify_id_binding_rejects_unset_columnref`
  - 构造最小 `PhysicalProject` over `PhysicalValues`,Project expr 含 `ColumnRef(UNSET)`。
  - 调用 `verify_id_binding(&plan)`。
  - 断言返回 Err 且包含 `UNSET ColumnRef`。

- [x] Step 2: 写红测 `p3_verify_id_binding_rejects_missing_input_binding`
  - child output 只含 id A,Project expr 引用 id B。
  - 断言 Err 包含 `not produced by child scope`。

- [x] Step 3: 实现 verifier
  - walker 输入 `&PhysicalPlanNode`。
  - 每个节点返回 `HashSet<ColumnId>` 表示该节点输出 ids。
  - 对每个节点表达式中的 executable `ColumnRef`:
    - id 必须非 `UNSET`
    - id 必须在该表达式对应输入 child 输出集合中
  - 对 leaf:
    - `PhysicalScan.columns`
    - `PhysicalValues.columns`
    - `PhysicalGenerateSeries.output_column_id`
    - `PhysicalCTEConsume.output_columns`
  - 对 pass-through:
    - Filter/Sort/Limit/Distribution 继承 child ids
  - 对 Project/Aggregate/Window/Repeat/Decode/SetOp/Join:
    - 用各自 output columns / op 字段计算输出集合。
  - IMV `AggregateStateMerge` 内部机器列不通过普通 `ColumnRef`;若当前实现确有内部 name refs,必须在 verifier 中只允许 `AggregateStateMergeOp` 专用白名单并注释 `IMV refactor: migrate to ColumnId`。

- [x] Step 4: 接入 codegen 入口
  - 在 `PlanFragmentBuilder::build` 和 `build_with_mv_refresh_ctx` 进入 `visit(plan)` 前调用 verifier。
  - debug build 使用 `debug_assert!(verify_id_binding(plan).is_ok())` 之外仍返回 Err,避免 release 静默。

- [x] Step 5: 验证
  - Run: `cargo test --profile dev-opt p3_verify_id_binding_ -- --nocapture`
  - Run: `cargo build --profile dev-opt`
  - Run: `cargo fmt --check`
  - 2026-06-06 evidence:
    - `cargo test --profile dev-opt p3_ -- --nocapture` PASS for verifier red tests plus aggregate/repeat output-id edge cases.
    - `cargo test --profile dev-opt p2_targeted_surfaces_do_not_use_name_fallback -- --nocapture` PASS.
    - `cargo build --profile dev-opt` PASS.

- [x] Step 6: Commit
  - `git commit -m "codegen: verify ColumnId bindings before fragment build"`

---

## Task 9 - P3: Repeat / Decode ColumnId 模型与 `fragment_builder` reader 迁移

**目的:** 完成 P1 延后的 no-shim 类型迁移,删除 `__repeat_group` runtime alias 依赖和 Decode name lookup。

**Files:**
- Modify: `src/sql/planner/plan.rs`
- Modify: `src/sql/planner/mod.rs`
- Modify: `src/sql/optimizer/operator.rs`
- Modify: `src/sql/optimizer/convert.rs`
- Modify: `src/sql/optimizer/cascades_rules/implement.rs`
- Modify: `src/sql/optimizer/rewrite/required_columns.rs`
- Modify: `src/sql/optimizer/rewrite/rules/column_pruning/prune_decode.rs`
- Modify: `src/sql/optimizer/rewrite/rules/low_cardinality_dict/rewriter.rs`
- Modify: `src/sql/codegen/fragment_builder.rs`
- Modify: `src/sql/codegen/nodes.rs`
- Test: `src/sql/codegen/fragment_builder.rs`, `src/sql/optimizer/rewrite/rules/low_cardinality_dict/rewriter.rs`

- [x] Step 1: 写红测 `p3_repeat_codegen_uses_column_ids_not_repeat_group_name`
  - 构造 `PhysicalRepeatOp` 使用 `ColumnId` lists。
  - 断言 `visit_repeat` 输出 scope 能用 repeat output id resolve。
  - grep gate: `rg -n "__repeat_group|grouping_key_aliases: Vec<\\(String, String\\)>|repeat_column_ref_list: Vec<Vec<String>>" src/sql`
  - Expected after green:不在生产代码命中。

- [x] Step 2: 写红测 `p3_decode_codegen_uses_source_and_output_ids`
  - 构造 `DecodeMapping { source_column_id, output_column_id }`。
  - child scope 中有 source id。
  - `visit_decode` 不调用 `resolve_column`,直接 `resolve_by_id(source_column_id)` 并注册 `output_column_id`。

- [ ] Step 3: 迁移 Repeat structs
  - `RepeatPlanNode` / `LogicalRepeatOp` / `PhysicalRepeatOp` 字段:
    - `repeat_column_ref_list: Vec<Vec<ColumnId>>`
    - `all_rollup_columns: Vec<ColumnId>`
    - `grouping_key_aliases: Vec<(ColumnId, ColumnId)>`
    - `grouping_fn_args: Vec<(ColumnId, Vec<ColumnId>)>`
  - display name 只保留在 `OutputColumn.name`。

- [x] Step 4: 迁移 Decode structs
  - `DecodeMapping` 改为:
    - `source_column_id: ColumnId`
    - `output_column_id: ColumnId`
  - low-cardinality rewrite 仍可在 catalog/dict metadata 边界按真实列名判断资格,但产出的 mapping 必须是 ids。
  - column pruning decode rule 按 output/source ids 判断 required set。

- [x] Step 5: 修改 `fragment_builder`
  - `visit_repeat`:
    - child binding lookup 全部 `resolve_by_id`
    - grouping fn / grouping id 虚拟槽按 ids 注册
    - 删除 `add_qualified_alias("__repeat_group", ...)`
  - `visit_decode`:
    - 继承 child `by_id`
    - 对每个 mapping 从 source id 找 slot,生成 decode output slot,注册 output id
    - dict 传播按 source slot id -> output slot id

- [x] Step 6: 验证
  - Run: `cargo test --profile dev-opt p3_repeat_codegen_uses_column_ids_not_repeat_group_name -- --nocapture`
  - Run: `cargo test --profile dev-opt p3_decode_codegen_uses_source_and_output_ids -- --nocapture`
  - Run: `cargo test --profile dev-opt low_cardinality -- --nocapture`
  - Run: `cargo build --profile dev-opt`
  - Run: `cargo fmt --check`
  - 2026-06-06 evidence:
    - `cargo test --profile dev-opt p3_ -- --nocapture` PASS, including Repeat output-id and direct fragment scope tests.
    - `cargo test --profile dev-opt low_cardinality -- --nocapture` PASS.
    - `cargo build --profile dev-opt` PASS.

- [ ] Step 7: Commit
  - `git commit -m "optimizer: model repeat and decode columns by ColumnId"`

---

## Task 10 - P3: 所有 `visit_*` 输出 scope 和 dict 传播按 id 注册

**目的:** 补齐 P3 codegen 终态不变量:每个算子每个输出都有 `by_id` binding。

**Files:**
- Modify: `src/sql/codegen/fragment_builder.rs`
- Test: `src/sql/codegen/fragment_builder.rs`

- [x] Step 1: 写红测 `p3_every_fragment_builder_output_has_id_binding`
  - 构造覆盖 Scan/Project/Aggregate/Window/Values/GenerateSeries/Repeat/Decode/Join/SetOp/CTE 的小 physical plans。
  - 对每个 visit result,遍历 expected output ids,断言 `scope.resolve_by_id(id).is_some()`。

- [x] Step 2: 修改 output scope 注册
  - `visit_scan`: scan columns by id。
  - `visit_project`: project item output id 注册;ColumnRef passthrough dict 传播按 input id 查 binding。
  - `visit_hash_aggregate`:group-by output ids + aggregate output ids 按 `op.output_columns` 注册,停止用 display name 重建 key。
  - `visit_window`:window output 用 `WindowExpr.output_column_id`。
  - `visit_values`:values columns by id。
  - `visit_generate_series`:generated column by id。
  - `visit_join`:merge child scopes by id;同名列不冲突。
  - `visit_union` / `visit_intersect` / `visit_except`:输出 id 来自 op output columns,child refs 已由 planner/optimizer 对齐。
  - `visit_cte_produce` / `visit_cte_consume`:produce/consume ids 对齐。

- [x] Step 3: dict 传播改 id
  - 替换 `fragment_builder.rs` 中 `resolve_column(None, column)` 的 dict lookup:
    - Project:从 `ExprKind::ColumnRef { column_id, .. }` 用 `resolve_by_id`
    - Aggregate:group-by expr 从 `ColumnRef.column_id` 或 expression output id 查 source binding
    - Decode:source id -> output id

- [x] Step 4: 验证
  - Run: `cargo test --profile dev-opt p3_every_fragment_builder_output_has_id_binding -- --nocapture`
  - Run: `cargo test --profile dev-opt data_stream_sink -- --nocapture`
  - Run: `cargo build --profile dev-opt`
  - Run: `cargo fmt --check`
  - 2026-06-06 evidence:
    - `cargo test --profile dev-opt p3_every_fragment_builder_output_has_id_binding -- --nocapture` PASS.
    - `cargo test --profile dev-opt p2_targeted_surfaces_do_not_use_name_fallback -- --nocapture` PASS.
    - `cargo build --profile dev-opt` PASS.

- [x] Step 5: Commit
  - `git commit -m "codegen: register all fragment output bindings by ColumnId"`

---

## Task 11 - P4: optimizer statistics re-key 到 `ColumnId`

**目的:** name-keyed stats 会在自连接/同名列处碰撞;P4 把 optimizer 内部统计统一改为 id-keyed,仅 catalog/connector 边界保留真实列名。

**Files:**
- Modify: `src/sql/optimizer/statistics.rs`
- Modify: `src/sql/optimizer/stats.rs`
- Modify: `src/sql/optimizer/logical_props.rs`
- Modify: `src/sql/optimizer/memo.rs`
- Modify: `src/sql/optimizer/search.rs`
- Modify: `src/sql/optimizer/cost.rs`
- Modify: `src/sql/optimizer/physical_plan.rs`
- Modify: `src/sql/optimizer/extract.rs`
- Modify: `src/sql/optimizer/estimate/ndv.rs`
- Modify: `src/sql/optimizer/estimate/selectivity.rs`
- Modify: `src/sql/optimizer/estimate/join_condition.rs`
- Modify: `src/sql/optimizer/rewrite/rules/join_reorder/cardinality.rs`
- Modify: `src/sql/optimizer/rewrite/rules/aggregate_pushdown/cost.rs`
- Test: optimizer stats unit tests

- [ ] Step 1: 写红测 `p4_self_join_same_name_columns_keep_distinct_statistics`
  - 构造 left scan id A name `k`,right scan id B name `k`。
  - stats 中 A/B 使用不同 NDV。
  - join/cardinality 估算必须分别读取 A/B,不能按 `"k"` 覆盖。

- [ ] Step 2: 写红测 `p4_scan_maps_catalog_name_stats_to_output_ids_once`
  - catalog `TableStatistics.column_stats` 仍可从真实列名输入。
  - scan stats 输出必须是 `HashMap<ColumnId, ColumnStatistic>`。
  - 输出 id 与 `scan.columns[i].column_id` 一致。

- [ ] Step 3: 改核心类型
  - `Statistics.column_statistics: HashMap<ColumnId, ColumnStatistic>`
  - `TableStatistics.column_stats: HashMap<ColumnId, ColumnStatistic>` 对 optimizer 内部表统计使用。
  - 如果需要保留 catalog-name 输入,新增边界 helper:
    - `map_catalog_column_stats_to_ids(scan.columns, catalog_name_stats) -> HashMap<ColumnId, ColumnStatistic>`
  - `LogicalProperties.column_statistics` / `memo` 同步改 id-keyed。

- [ ] Step 4: 改 estimator helpers
  - `extract_column_name` -> `extract_column_id`。
  - `get_expr_ndv`, `get_join_key_ndv_with_confidence`, `estimate_selectivity`, `is_unknown_column_literal_eq` 都接收 `HashMap<ColumnId, ColumnStatistic>`。
  - `ColumnRef(UNSET)` 在 optimizer stats 中返回 fallback confidence,不查 name。

- [ ] Step 5: 改 stats propagation
  - Project:output id -> expression stats。
  - Aggregate:group output id -> group expr stats;aggregate result id -> unknown/derived stats。
  - Join:merge left/right stats by id;join key NDV 按 id。
  - SetOp:按 output column id merge child stats,不是按 output name。
  - GenerateSeries:使用 `PhysicalGenerateSeriesOp.output_column_id`。
  - Decode:source id stats 迁移到 output id。

- [ ] Step 6: 验证
  - Run: `cargo test --profile dev-opt p4_self_join_same_name_columns_keep_distinct_statistics -- --nocapture`
  - Run: `cargo test --profile dev-opt p4_scan_maps_catalog_name_stats_to_output_ids_once -- --nocapture`
  - Run: `cargo test --profile dev-opt optimizer::stats -- --nocapture`
  - Run: `cargo test --profile dev-opt optimizer::estimate -- --nocapture`
  - Run: `cargo build --profile dev-opt`
  - Run: `cargo fmt --check`

- [ ] Step 7: Commit
  - `git commit -m "optimizer: key column statistics by ColumnId"`

---

## Task 12 - P4: optimizer rewrite rules 改 ColumnId 匹配

**目的:** 删除 predicate pushdown / not-null / UKFK / low-cardinality / cascades 中的 semantic name matching。

**Files:**
- Modify: `src/sql/optimizer/rewrite/rules/utils.rs`
- Modify: `src/sql/optimizer/rewrite/rules/predicate_pushdown/push_to_scan.rs`
- Modify: `src/sql/optimizer/rewrite/rules/predicate_pushdown/push_to_join.rs`
- Modify: `src/sql/optimizer/rewrite/rules/predicate_pushdown/push_to_aggregate.rs`
- Modify: `src/sql/optimizer/rewrite/rules/predicate_pushdown/semi_anti_condition.rs`
- Modify: `src/sql/optimizer/rewrite/rules/derive_join_not_null.rs`
- Modify: `src/sql/optimizer/rewrite/rules/ukfk.rs` 或实际 UKFK 文件
- Modify: `src/sql/optimizer/rewrite/rules/low_cardinality_dict/*.rs`
- Modify: `src/sql/optimizer/rewrite/rules/column_pruning/*.rs`
- Modify: `src/sql/optimizer/cascades_rules/implement.rs`
- Modify: `src/sql/optimizer/cascades_rules/join_associativity.rs`
- Test: rule unit tests + `sql-tests/optimizer/column_id_binding_optimizer.sql`

- [ ] Step 1: 写红测 `p4_pushdown_scan_uses_column_ids_for_same_name_columns`
  - scan output ids A/B,同名或大小写变化。
  - predicate 引用 id A 时只按 A 判断可 push。

- [ ] Step 2: 写红测 `p4_pushdown_join_deletes_name_fallback`
  - left/right 都有 `k`。
  - predicate 引用 left id 时只下推 left,不能因 name 相同误判 both。

- [ ] Step 3: 写红测 `p4_derive_join_not_null_collects_ids_only`
  - join eq condition 两侧同名不同 id。
  - 派生 not-null predicate 引用对应侧 id。

- [ ] Step 4: 迁移 utils
  - 新增或保留 `collect_column_id_refs(expr) -> Vec<ColumnId>`。
  - 所有 semantic rule 调用迁移到 id helper。
  - `collect_column_refs` / `collect_qualified_column_refs` 只允许留给 debug、error、connector name boundary;否则 P5 删除。

- [ ] Step 5: 迁移 predicate pushdown
  - `push_to_scan`:scan column set 改 `HashSet<ColumnId>`。
  - `push_to_join`:删除 `collect_column_refs` fallback branch;`classify_sides_by_column_ids` miss 直接 residual。
  - `push_to_aggregate`:group-by columns 改 group-by output ids。
  - `semi_anti_condition`:同步 id 分类。

- [ ] Step 6: 迁移 UKFK / not-null / low-cardinality
  - UK/FK catalog 列名只在 scan 边界解析一次为 ids,rule 匹配 ids。
  - `derive_join_not_null` 删除 `(HashSet<ColumnId>, HashSet<String>)` 双轨,只返回 ids。
  - low-cardinality dict 资格检查可读 catalog name,但 rewrite output `DecodeMapping` 必须是 ids。

- [ ] Step 7: 迁移 cascades helper
  - `collect_column_refs_lowercase` 改 `collect_column_ids`。
  - `join_associativity` / `get_group_column_names` 只在 display/golden 输出使用 name;决策走 id。

- [ ] Step 8: SQL golden
  - 新增 `sql-tests/optimizer/column_id_binding_optimizer.sql`:
    - self join same column names with filter pushdown
    - aggregate pushdown with computed group key id
    - low-cardinality decode through aggregate
    - derive join not null on same-name keys
  - 记录任何 cost/plan golden delta,在 case 注释说明是 id-keying 修正。

- [ ] Step 9: 验证
  - Run: `cargo test --profile dev-opt p4_pushdown_ -- --nocapture`
  - Run: `cargo test --profile dev-opt p4_derive_join_not_null_collects_ids_only -- --nocapture`
  - Run: `cargo test --profile dev-opt low_cardinality -- --nocapture`
  - Run: `cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --suite optimizer --only column_id_binding_optimizer --mode verify`
  - Run: `cargo build --profile dev-opt`
  - Run: `cargo fmt --check`

- [ ] Step 10: Commit
  - `git commit -m "optimizer: match rewrite rules by ColumnId"`

---

## Task 13 - P5: 删除死代码和 semantic name binding helper

**目的:** 收尾,让 grep gate 证明终态没有普通语义 name binding。

**Files:**
- Modify: `src/sql/analyzer/scope.rs`
- Modify: `src/sql/analyzer/resolve_expr.rs`
- Modify: `src/sql/planner/mod.rs`
- Modify: `src/sql/codegen/resolve.rs`
- Modify: `src/sql/codegen/expr_compiler.rs`
- Modify: `src/sql/codegen/fragment_builder.rs`
- Modify: `src/sql/codegen/mod.rs`
- Delete: `src/sql/codegen/fallback_audit.rs`
- Modify: `src/sql/optimizer/rewrite/rules/utils.rs`
- Modify: any file hit by grep gates below

- [ ] Step 1: 写 grep 红门禁
  - Run: `rg -n "canonical_qualifier|__repeat_group|resolve_column\\(|add_qualified_alias|add_unqualified_alias|strict_missing_id|binding_has_id_index|has_id_bindings|fallback_audit" src/sql`
  - Expected after green:0 production hits。

- [ ] Step 2: 写 display helper 门禁
  - Run: `rg -n "typed_expr_display_name" src/sql/planner src/sql/codegen src/sql/optimizer`
  - 人审允许:
    - output column display name
    - EXPLAIN/debug/error
    - sql-test golden formatting
  - block:
    - matching, binding, dedup, stats keying, rule classification

- [ ] Step 3: 删除 analyzer qualifier steering
  - 删除 `AnalyzerScope.canonical_qualifier` 字段和方法。
  - 删除 `resolve_expr.rs` 中通过 canonical qualifier 重写 ColumnRef qualifier 的逻辑。
  - 确保 USING/FULL OUTER USING tests 仍依赖 id。

- [ ] Step 4: 删除 Repeat name alias
  - 删除 `REPEAT_GROUP_QUALIFIER`。
  - 删除任何 `__repeat_group` 注册和 rewrite。

- [ ] Step 5: 删除 fallback audit
  - 删除 module 和所有 record/snapshot 调用。
  - P2 的 audit tests 改为 P3/P5 fail-fast tests 或删除。

- [ ] Step 6: 删除 optimizer name helper
  - 删除不再使用的 `collect_column_refs` / `collect_qualified_column_refs` / `collect_column_refs_lowercase`。
  - 保留 connector/output display 所需 helper 时改名为 `collect_column_display_names_for_explain` 或更窄名字,避免误用。

- [ ] Step 7: 验证
  - Run: `rg -n "canonical_qualifier|__repeat_group|resolve_column\\(|add_qualified_alias|add_unqualified_alias|strict_missing_id|binding_has_id_index|has_id_bindings|fallback_audit" src/sql`
  - Run: `rg -n "typed_expr_display_name" src/sql/planner src/sql/codegen src/sql/optimizer`
  - Run: `cargo build --profile dev-opt`
  - Run: `cargo test --profile dev-opt`
  - Run: `cargo fmt --check`

- [ ] Step 8: Commit
  - `git commit -m "cleanup: remove semantic name binding leftovers"`

---

## Task 14 - Final Verification

**目的:** 给 PR #256 更新提供完整 evidence。P2-P5 终态必须 green;`tpc-ds q85` 按 P1 记录作为独立性能/稳定性 gap,不能掩盖 ColumnId binding correctness。

- [ ] Step 1: 基础验证
  - Run: `cargo fmt --check`
  - Run: `cargo build --profile dev-opt`
  - Run: `cargo test --profile dev-opt`

- [ ] Step 2: SQL targeted suites
  - Run: `cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --suite optimizer --mode verify`
  - Run: `cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --suite join --mode verify`
  - Run: `cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --suite cte --mode verify`
  - Run: `cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --suite tpc-h --mode verify`

- [ ] Step 3: Iceberg/IMV smoke,使用当前 worktree runtime
  - Run:
    ```bash
    source docker/iceberg-rest/runtime/current/env.sh
    docker/iceberg-rest/up.sh
    cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
      --config "$NOVAROCKS_SQL_TEST_CONFIG" \
      --suite iceberg --mode verify
    ```
  - Run IMV/refresh 相关 suite 时使用相同 generated config,不硬编码 port。

- [ ] Step 4: G1 grep gates
  - Run: `rg -n "canonical_qualifier|__repeat_group|resolve_column\\(|add_qualified_alias|add_unqualified_alias|strict_missing_id|binding_has_id_index|has_id_bindings|fallback_audit" src/sql`
  - Expected: no production hits。
  - Run: `rg -n "collect_column_refs|collect_qualified_column_refs|collect_column_refs_lowercase|extract_column_name" src/sql/optimizer`
  - Expected: no semantic optimizer hits;若保留 display/helper,文件名和函数名必须说明 display-only。
  - Run: `rg -n "ColumnId::UNSET" src/sql/planner src/sql/optimizer src/sql/codegen`
  - Expected: only tests, explicit sentinel handling, or connector/display boundary;no executable `ColumnRef` construction.

- [ ] Step 5: q85 复核
  - 默认 config 跑 `tpc-ds --only q85 --mode verify`,记录是否仍是 exchange wait timeout。
  - 若仍失败,在最终说明里标为既有 validation gap;不得把它算作 ColumnId binding regression。

- [ ] Step 6: Commit final test/golden updates
  - `git commit -m "test: cover ColumnId-only binding migration"`

---

## Review Checklist

- [ ] 每个普通 executable `ColumnRef` 都有非 `UNSET` `ColumnId`。
- [ ] 每个 physical node 输出 id 都能在 `ExprScope.by_id` resolve。
- [ ] `ExprCompiler` 普通列解析只走 `resolve_by_id`。
- [ ] `resolve_internal_by_name` 只由 IMV/AggregateStateMerge 内部机器列使用。
- [ ] optimizer stats 内部 `column_statistics` 是 `HashMap<ColumnId, ColumnStatistic>`。
- [ ] optimizer rule classification 用 id set,不是 lowercase name set。
- [ ] name 只存在于 parse/analyzer入口、output schema/display、error/EXPLAIN/debug、descriptor label、connector schema boundary。
- [ ] 无新增 id->name 临时桥。
