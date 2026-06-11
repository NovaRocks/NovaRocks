# IMV join-delta 组合化(纯 `Delta(Join)` 规则)实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让顶层为 aggregate 的 Iceberg IMV 在 agg 与 join 之间夹透传节点(`Aggregate(Filter(Join))`)以及多层 inner/cross join 嵌套(`Aggregate(Join(Join(A,B),C))`)时支持增量刷新。

**Architecture:** 把现有复合规则 `RewriteJoinAggregateDeltaRule`(只匹配 `ImvDelta(root, Aggregate(Join))`)拆成纯 `RewriteJoinDeltaRule`(匹配任意 `ImvDelta(Join)`),与 `PushDeltaThroughUnaryRule` 并入同一个 `imv-delta-pushdown` stage 共享 fixpoint;delta 经 aggregate-state 下推到 agg 输入、穿过 Filter/Project 后落到裸 `Join` 由纯规则展开;`mark_scan` 放宽支持嵌套 join。target apply key 恒为 `GroupRowId`,不碰 apply-key 物理表示。

**Tech Stack:** Rust(`src/sql/optimizer/rewrite/imv/`);SQL 回归测试(`sql-tests/iceberg-ivm/`、`sql-tests/optimizer/`,需 Docker MinIO + standalone-server)。

**Spec:** [docs/design/specs/2026-06-06-imv-join-delta-decomposition-design.md](../specs/2026-06-06-imv-join-delta-decomposition-design.md)

**边界(本计划不做,显式留作独立 spec):** join 一侧是 aggregate(`Agg(Join(Agg(X),B))`,§12.3 nested-agg)、顶层 join-projection 的多表嵌套(§12.2 apply-key arity)。这两者在 Task 6 以显式 reject 守住。join-projection MV(无顶层 agg)走独立的 `incremental_refresh_iceberg_join_mv` + `iceberg_join_coalesce` 路径,**不经过本计划改动的 imv rewrite pipeline**,不受影响。

---

## File Structure

| 文件 | 职责 | 改动 |
|---|---|---|
| `src/sql/optimizer/rewrite/imv/join_delta.rs` | join delta 展开规则 + `mark_scan` | 重写规则为纯 `Delta(Join)`、rename、`mark_scan` 加 Join 分支 |
| `src/sql/optimizer/rewrite/imv/delta_pushdown.rs` | delta 穿过 unary 节点 | Join 分支由 fail-fast 改为 `Unchanged` |
| `src/sql/optimizer/rewrite/imv/pipeline.rs` | imv rewrite stage 编排 | 删 `imv-join-delta` stage,纯规则并入 `imv-delta-pushdown` |
| `sql-tests/iceberg-ivm/sql/iceberg_ivm_aggregate_filter_join.sql` (+`.result`) | 正向 e2e:带 WHERE 的 join-agg | 新建 |
| `sql-tests/iceberg-ivm/sql/iceberg_ivm_aggregate_nested_join.sql` (+`.result`) | 正向 e2e:三表嵌套 join-agg | 新建 |
| `sql-tests/iceberg-ivm/sql/iceberg_ivm_union_shape_rejects_unsupported.sql` | 边界 reject | 迁出 filter-join、加 join-of-aggregate reject |
| `sql-tests/optimizer/sql/imv_aggregate_filter_join_logical.sql` | plan-shape 断言 | 新建 |
| `sql-tests/optimizer/sql/imv_aggregate_nested_join_logical.sql` | plan-shape 断言 | 新建 |

---

## Task 1: `mark_scan` 放宽支持 Join 侧(嵌套展开的基石)

**Files:**
- Modify: `src/sql/optimizer/rewrite/imv/join_delta.rs:170-188`(`mark_scan`)
- Test: 同文件 `#[cfg(test)] mod tests`

现状 `mark_scan` 只接受 `Scan/Project/Filter`,遇 `Join`/`Aggregate` 等走 `other => Err(...)`。本任务给 `Join` 加分支:**Delta marker 把整个 join 包成 `ImvDelta(Join)`(交给后续 join-delta 递归展开);Version marker 递归下推到两侧、保持同一 role**。

- [ ] **Step 1: 写失败测试**

在 `join_delta.rs` 的 `mod tests` 末尾加(复用已有 helper `join_of`、`scan`、`mark_delta_scan`、`mark_version_scan`、`ImvVersionRef`、`ImvVersionRole`):

```rust
    #[test]
    fn mark_delta_scan_wraps_nested_join_whole() {
        // Delta marker over a Join must wrap the entire join (待 join-delta 递归展开),
        // NOT push into the two sides.
        let join = join_of(scan("a", 1), scan("b", 10));
        let marked = mark_delta_scan(join, ColumnId(100)).expect("mark delta over join");
        let LogicalPlan::ImvDelta(delta) = marked else {
            panic!("expected ImvDelta wrapping the whole join, got {marked:?}");
        };
        assert!(!delta.is_root, "nested join delta marker is not root");
        assert_eq!(delta.action_column, Some(ColumnId(100)));
        assert!(matches!(delta.input.as_ref(), LogicalPlan::Join(_)));
    }

    #[test]
    fn mark_version_scan_pushes_same_role_down_both_join_sides() {
        // Version marker over a Join distributes over the join:
        // Version(Join(a,b), from) == Join(Version(a, from), Version(b, from)).
        let join = join_of(scan("a", 1), scan("b", 10));
        let marked = mark_version_scan(join, ImvVersionRef::from_snapshot())
            .expect("mark version over join");
        let LogicalPlan::Join(j) = marked else {
            panic!("expected Join with both sides version-marked, got {marked:?}");
        };
        let left_v = assert_version_side(j.left.as_ref());
        let right_v = assert_version_side(j.right.as_ref());
        assert_eq!(left_v.version_ref, ImvVersionRef { role: ImvVersionRole::From });
        assert_eq!(right_v.version_ref, ImvVersionRef { role: ImvVersionRole::From });
    }

    fn assert_version_side(plan: &LogicalPlan) -> &ImvVersionNode {
        match plan {
            LogicalPlan::ImvVersion(v) => v,
            // version side may be wrapped via Project/Filter recursion; for a bare scan it is direct
            other => panic!("expected ImvVersion on join side, got {other:?}"),
        }
    }
```

在 tests `use` 区补 `ImvVersionNode`(已 import `ImvVersionRef`、`ImvVersionRole`;若缺则加 `use crate::sql::optimizer::rewrite::imv::marker::ImvVersionNode;`)。

- [ ] **Step 2: 运行,确认失败**

Run: `cargo test --lib mark_delta_scan_wraps_nested_join_whole mark_version_scan_pushes_same_role_down_both_join_sides -- --nocapture`
Expected: FAIL —— 现 `mark_scan` 遇 Join 返回 `Err("...supports only Scan/Project/Filter join sides...")`。

- [ ] **Step 3: 实现 Join 分支**

把 `mark_scan`(:170-188)改为(在 `Filter` 分支后、`other` 前插入 `Join` 分支):

```rust
fn mark_scan(plan: LogicalPlan, marker: MarkerKind) -> Result<LogicalPlan, String> {
    Ok(match plan {
        LogicalPlan::Scan(_) => wrap_scan_marker(plan, marker),
        LogicalPlan::Project(mut project) => {
            project.input = Box::new(mark_scan(*project.input, marker)?);
            LogicalPlan::Project(project)
        }
        LogicalPlan::Filter(mut filter) => {
            filter.input = Box::new(mark_scan(*filter.input, marker)?);
            LogicalPlan::Filter(filter)
        }
        LogicalPlan::Join(join) => match marker {
            // Delta over a Join: wrap the whole join as a (non-root) delta sub-problem;
            // RewriteJoinDeltaRule expands it in a later fixpoint iteration (nested join).
            MarkerKind::Delta(_) => wrap_scan_marker(LogicalPlan::Join(join), marker),
            // Version over a Join: a full snapshot distributes over the join — push the
            // SAME role down both sides.
            MarkerKind::Version(version_ref) => {
                let JoinNode {
                    left,
                    right,
                    join_type,
                    condition,
                    required_output_columns,
                } = join;
                LogicalPlan::Join(JoinNode {
                    left: Box::new(mark_scan(*left, MarkerKind::Version(version_ref))?),
                    right: Box::new(mark_scan(*right, MarkerKind::Version(version_ref))?),
                    join_type,
                    condition,
                    required_output_columns,
                })
            }
        },
        other => {
            return Err(format!(
                "Iceberg IMV join delta rewrite supports only Scan/Project/Filter/Join join sides, got {}",
                plan_kind(&other)
            ));
        }
    })
}
```

`MarkerKind`(:165-168)需可在 `Version` 分支复用 `version_ref` 两次。`ImvVersionRef` 是 Copy(role 枚举)。若编译报 move,给 `enum MarkerKind { Delta(ColumnId), Version(ImvVersionRef) }` 上方加 `#[derive(Clone, Copy)]`,并在 `Version(version_ref)` 两处用 `MarkerKind::Version(version_ref)`(Copy 后无需 clone)。`wrap_scan_marker`(:190-203)已对任意 `LogicalPlan` 用 `Box::new(scan)` 包 marker,直接接受 `LogicalPlan::Join`,无需改。

- [ ] **Step 4: 运行,确认通过**

Run: `cargo test --lib --package <crate> mark_delta_scan_wraps_nested_join_whole mark_version_scan_pushes_same_role_down_both_join_sides`
Expected: PASS。再跑 `cargo test --lib join_delta` 确认旧 mark_scan 测试不回归。

- [ ] **Step 5: fmt/clippy/commit**

```bash
cargo fmt
cargo clippy --lib 2>&1 | tail -5
git add src/sql/optimizer/rewrite/imv/join_delta.rs
git commit -m "feat(imv): mark_scan supports Join sides (delta wraps whole join, version distributes)"
```

---

## Task 2: 核心重构 —— 纯 `RewriteJoinDeltaRule` + pipeline 重排 + delta_pushdown Join→Unchanged

**Files:**
- Modify: `src/sql/optimizer/rewrite/imv/join_delta.rs`(struct rename + `matches` + `apply` + 单测)
- Modify: `src/sql/optimizer/rewrite/imv/pipeline.rs`(import + stage 编排 + stage 测试)
- Modify: `src/sql/optimizer/rewrite/imv/delta_pushdown.rs`(Join 分支 + reject 测试)

这是一个**原子重构**:完成后单层 `Aggregate(Join)`(现有)走新路径仍 working,`Aggregate(Filter(Join))` 开始 working。中间不拆开,避免出现"单层 join-agg 暂时坏"的中间态。

- [ ] **Step 1: 写新规则的失败单测**

替换 `join_delta.rs` 中现有的规则级测试(`supported_join_delta_kinds_are_inner_and_cross_only` 保留;删除/改写 `rewrite_inner_join_aggregate_delta_expands_two_stable_branches`、`rewrite_cross_join_aggregate_delta_expands_two_stable_branches`、`rewrite_join_aggregate_delta_rejects_outer_join`、`join_delta_preserves_branch_scope`、`assert_supported_join_rewrite` 等针对旧复合形状的断言)为面向纯规则的新断言:

```rust
    #[test]
    fn pure_join_delta_matches_imv_delta_over_join_any_root() {
        let rule = RewriteJoinDeltaRule;
        let ctx = build_ctx();
        // 非 root delta over join —— 这是 aggregate-state 下推后的形态
        let non_root = LogicalPlan::ImvDelta(ImvDeltaNode {
            input: Box::new(join_of(scan("l", 1), scan("r", 10))),
            is_root: false,
            action_column: Some(ColumnId(100)),
            branch_scope: None,
        });
        assert!(rule.matches(&non_root, &ctx));
        // 不再匹配 ImvDelta(Aggregate(...)) —— 那是 aggregate-state 的职责
        let over_agg = delta(aggregate_over(join_over(JoinKind::Inner)));
        assert!(!rule.matches(&over_agg, &ctx));
    }

    #[test]
    fn pure_join_delta_expands_into_union_without_outer_aggregate() {
        let rule = RewriteJoinDeltaRule;
        let mut ctx = build_ctx();
        let plan = LogicalPlan::ImvDelta(ImvDeltaNode {
            input: Box::new(join_over(JoinKind::Inner)),
            is_root: false,
            action_column: Some(ColumnId(100)),
            branch_scope: None,
        });
        let RewriteResult::Changed(LogicalPlan::Union(union)) =
            rule.apply(plan, &mut ctx).expect("expand")
        else {
            panic!("pure join-delta must expand ImvDelta(Join) directly into a Union");
        };
        assert!(union.all);
        assert_eq!(union.inputs.len(), 2);
        // 左支:Δleft ⋈ Version(right, from)
        let left = assert_normalized_branch(&union.inputs[0], ColumnId(100));
        assert_delta(left.left.as_ref(), "left", ColumnId(100));
        assert_version(left.right.as_ref(), "right", ImvVersionRole::From);
        // 右支:Version(left, to) ⋈ Δright
        let right = assert_normalized_branch(&union.inputs[1], ColumnId(100));
        assert_version(right.left.as_ref(), "left", ImvVersionRole::To);
        assert_delta(right.right.as_ref(), "right", ColumnId(100));
    }

    #[test]
    fn pure_join_delta_rejects_outer_join() {
        let rule = RewriteJoinDeltaRule;
        let mut ctx = build_ctx();
        let plan = LogicalPlan::ImvDelta(ImvDeltaNode {
            input: Box::new(join_over(JoinKind::LeftOuter)),
            is_root: false,
            action_column: Some(ColumnId(100)),
            branch_scope: None,
        });
        let err = rule.apply(plan, &mut ctx).expect_err("outer must reject");
        assert!(err.contains("inner/cross"), "unexpected: {err}");
    }

    #[test]
    fn pure_join_delta_nested_leaves_inner_join_delta_for_next_iteration() {
        // ImvDelta(Join(Join(a,b), c)) → 外层展开后,左支的 delta 侧应是
        // ImvDelta(Join(a,b)),留待下一轮 fixpoint。
        let rule = RewriteJoinDeltaRule;
        let mut ctx = build_ctx();
        let inner = join_of(scan("a", 1), scan("b", 10));
        let outer = join_of_with_left(inner, scan("c", 20));
        let plan = LogicalPlan::ImvDelta(ImvDeltaNode {
            input: Box::new(outer),
            is_root: false,
            action_column: Some(ColumnId(100)),
            branch_scope: None,
        });
        let RewriteResult::Changed(LogicalPlan::Union(union)) =
            rule.apply(plan, &mut ctx).expect("expand outer")
        else {
            panic!("expected Union");
        };
        // 左支 = ImvDelta(Join(a,b)) ⋈ Version(c, from);delta 侧仍是 ImvDelta(Join)
        let left = assert_normalized_branch(&union.inputs[0], ColumnId(100));
        let LogicalPlan::Project(p) = left.left.as_ref() else {
            // mark_delta_scan(Join) 直接产 ImvDelta(Join);normalize 后在 Project 下
            panic!("expected project over delta side");
        };
        assert!(matches!(p.input.as_ref(), LogicalPlan::ImvDelta(d) if matches!(d.input.as_ref(), LogicalPlan::Join(_))));
    }
```

需要一个新 helper `join_of_with_left`(内层 join 作为外层左孩子)和确保 `assert_normalized_branch`/`assert_delta`/`assert_version` 仍在(它们已存在于现有测试)。`join_of_with_left`:

```rust
    fn join_of_with_left(left: LogicalPlan, right: LogicalPlan) -> LogicalPlan {
        let right_cols = plan_output_columns(&right).expect("right cols");
        let left_cols = plan_output_columns(&left).expect("left cols");
        LogicalPlan::Join(JoinNode {
            left: Box::new(left),
            right: Box::new(right),
            join_type: JoinKind::Inner,
            condition: Some(TypedExpr {
                kind: ExprKind::BinaryOp {
                    left: Box::new(col_expr(left_cols[0].column_id.0, &left_cols[0].name)),
                    op: BinOp::Eq,
                    right: Box::new(col_expr(right_cols[0].column_id.0, &right_cols[0].name)),
                },
                data_type: DataType::Boolean,
                nullable: false,
            }),
            required_output_columns: None,
        })
    }
```

- [ ] **Step 2: 运行,确认失败(编译失败:`RewriteJoinDeltaRule` 未定义)**

Run: `cargo test --lib pure_join_delta_ 2>&1 | tail -20`
Expected: 编译错误 `cannot find type RewriteJoinDeltaRule`。

- [ ] **Step 3: 重写规则(rename + matches + apply)**

在 `join_delta.rs` 把 `RewriteJoinAggregateDeltaRule`(:14)及其 impl 改为:

```rust
pub(crate) struct RewriteJoinDeltaRule;
```

`name()`:`"RewriteJoinAggregateDelta"` → `"RewriteJoinDelta"`。
`phase()`/`traversal()` 不变(`StructuralRewrite` / `TopDown`)。

`matches`(:36-47)替换为:

```rust
    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        matches!(
            plan,
            LogicalPlan::ImvDelta(delta)
                if matches!(delta.input.as_ref(), LogicalPlan::Join(_))
        )
    }
```

`apply`(:49-133)替换为(直接展开 `ImvDelta(Join)` 成 `Union`,**不再**包外层 aggregate 或 root delta):

```rust
    fn apply(&self, plan: LogicalPlan, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let LogicalPlan::ImvDelta(delta) = plan else {
            return Ok(RewriteResult::Unchanged);
        };
        let LogicalPlan::Join(join) = *delta.input else {
            return Ok(RewriteResult::Unchanged);
        };

        if !join_delta_kind_supported(join.join_type) {
            return Err(format!(
                "Iceberg IMV join delta rewrite supports inner/cross joins only, got {:?}",
                join.join_type
            ));
        }

        // branch_scope is consumed by aggregate-state on old_input; a non-root
        // join delta never carries it (see spec §4.4). Drop defensively.
        let action_column = match delta.action_column {
            Some(action_column) => action_column,
            None => ctx
                .extension::<ImvExtension>()
                .ok_or_else(|| {
                    "RewriteJoinDelta requires ImvExtension in RewriteContext".to_string()
                })?
                .allocate_column_id(),
        };

        let JoinNode {
            left,
            right,
            join_type,
            condition,
            required_output_columns,
        } = join;
        let left = *left;
        let right = *right;
        let mut output_columns = join_output_columns(join_type, &left, &right)?;
        output_columns.push(ImvActionColumn::output_column(action_column));

        let left_delta_branch = normalize_branch_output(
            LogicalPlan::Join(JoinNode {
                left: Box::new(mark_delta_scan(left.clone(), action_column)?),
                right: Box::new(mark_version_scan(
                    right.clone(),
                    ImvVersionRef::from_snapshot(),
                )?),
                join_type,
                condition: condition.clone(),
                required_output_columns: required_output_columns.clone(),
            }),
            &output_columns,
        );

        let right_delta_branch = normalize_branch_output(
            LogicalPlan::Join(JoinNode {
                left: Box::new(mark_version_scan(left, ImvVersionRef::to_snapshot())?),
                right: Box::new(mark_delta_scan(right, action_column)?),
                join_type,
                condition,
                required_output_columns: required_output_columns.clone(),
            }),
            &output_columns,
        );

        Ok(RewriteResult::Changed(LogicalPlan::Union(UnionNode {
            inputs: vec![left_delta_branch, right_delta_branch],
            all: true,
            output_columns,
            required_output_columns,
        })))
    }
```

(`ImvVersionNode`/`ImvDeltaNode` 的 import 已在文件头;`branch_scope` 不再读取,移除其局部绑定。)

- [ ] **Step 4: pipeline 重排**

`pipeline.rs`:import(:16)`RewriteJoinAggregateDeltaRule` → `RewriteJoinDeltaRule`。删除 `imv-join-delta` stage(:46-50)整段。把 `imv-delta-pushdown` stage(:64-68)改为同时含两条规则:

```rust
        RewriteStage::new(
            "imv-delta-pushdown",
            RewritePhase::StructuralRewrite,
            vec![
                Box::new(PushDeltaThroughUnaryRule) as Box<dyn LogicalRewriteRule>,
                Box::new(RewriteJoinDeltaRule) as Box<dyn LogicalRewriteRule>,
            ],
        ),
```

更新 pipeline 测试 `pipeline_runs_join_and_aggregate_rewrite_before_generic_delta_pushdown`(:124-153):删除对 `imv-join-delta` 的查找;改为断言 `imv-join-delta` stage 不存在、且 `imv-delta-pushdown` 在 `imv-aggregate-state` 之后:

```rust
    #[test]
    fn pipeline_runs_join_delta_inside_pushdown_after_aggregate_state() {
        let p = build_imv_pipeline();
        let names = p.stage_names();
        assert!(!names.iter().any(|n| *n == "imv-join-delta"),
            "imv-join-delta stage must be removed: {names:?}");
        let agg = names.iter().position(|n| *n == "imv-aggregate-state").unwrap();
        let pushdown = names.iter().position(|n| *n == "imv-delta-pushdown").unwrap();
        assert!(agg < pushdown, "stage order: {names:?}");
        // 纯 join-delta 与 pushdown 同 stage
        assert!(p.rule_names().iter().any(|n| *n == "RewriteJoinDelta"));
    }
```

- [ ] **Step 5: delta_pushdown 的 Join 分支改 Unchanged**

`delta_pushdown.rs` 的 decision match(:58-72),把 `Join` 分支由 `Err` 改为返回 `Unchanged`(让同 stage 的 `RewriteJoinDeltaRule` 接手),`Aggregate`/`Union` 仍 fail-fast:

```rust
        match delta.input.as_ref() {
            LogicalPlan::Project(_) | LogicalPlan::Filter(_) => { /* fall through to push */ }
            LogicalPlan::Aggregate(_) => {
                return Err("Iceberg IMV rewrite does not support this aggregate shape".to_string());
            }
            LogicalPlan::Join(_) => {
                // Left for RewriteJoinDeltaRule in the same stage's fixpoint.
                return Ok(RewriteResult::Unchanged);
            }
            LogicalPlan::Union(_) => {
                return Err("Iceberg IMV rewrite does not support this union shape".to_string());
            }
            _ => return Ok(RewriteResult::Unchanged),
        }
```

更新 `delta_pushdown.rs` 的 `rejects_delta_over_join` 测试(:316-326)为反向断言:

```rust
    #[test]
    fn leaves_delta_over_join_for_join_delta_rule() {
        let rule = PushDeltaThroughUnaryRule;
        let mut ctx = ctx();
        let plan = delta(join_over(leaf_scan(), leaf_scan()));
        let result = rule.apply(plan, &mut ctx).expect("join must be a no-op, not fail");
        assert!(matches!(result, RewriteResult::Unchanged),
            "delta over join is left for RewriteJoinDeltaRule");
    }
```

(`rejects_delta_over_aggregate`、`rejects_delta_over_union` 保留不变。)

- [ ] **Step 6: 运行全部 imv rewrite 单测,确认通过**

Run: `cargo test --lib sql::optimizer::rewrite::imv -- --nocapture 2>&1 | tail -30`
Expected: PASS（含 Task 1 + Task 2 新测、`branch_union.rs` 的 `pipeline_branch_union_of_aggregate_over_join_composes` 仍绿）。若 `branch_union.rs` 测试断言了旧中间形态,按其报错更新断言(语义不变,只更新形状期望)。

- [ ] **Step 7: fmt/clippy/commit**

```bash
cargo fmt && cargo clippy --lib 2>&1 | tail -5
git add src/sql/optimizer/rewrite/imv/{join_delta.rs,pipeline.rs,delta_pushdown.rs}
git commit -m "refactor(imv): decompose join-delta into pure Delta(Join) rule sharing the pushdown fixpoint"
```

---

## Task 3: pipeline 级集成单测(`Aggregate(Filter(Join))` / 嵌套)

**Files:**
- Test: `src/sql/optimizer/rewrite/imv/branch_union.rs` 的 `mod tests`(已有 `build_ctx` + `build_imv_pipeline` 集成测试范式与 helper),或 `join_delta.rs` —— 选 `branch_union.rs`,因其 `build_ctx` 已注册 `ice.db.b` 基表且有 schema contract。

证明完整 pipeline 对带 Filter 的 join-agg 与嵌套 join-agg 跑通、无 marker 残留。

- [ ] **Step 1: 写失败测试**

在 `branch_union.rs` 的 `mod tests` 加(复用其 `build_ctx`、`scan`、`aggregate_over`、`join_of`、`filter_over`、`plan_contains_imv_marker`、`build_imv_pipeline`):

```rust
    #[test]
    fn pipeline_aggregate_over_filtered_join_composes() {
        use crate::sql::optimizer::rewrite::imv::marker::plan_contains_imv_marker;
        use crate::sql::optimizer::rewrite::imv::pipeline::build_imv_pipeline;
        let mut ctx = build_ctx();
        // Aggregate( Filter( Join(b, b) ) ) —— filter 夹在 agg 与 join 之间
        let join = join_of(scan("b", 1), scan("b", 10));
        let filtered = filter_over(join, 1, "region");
        let plan = aggregate_over(filtered);
        let out = build_imv_pipeline().rewrite(plan, &mut ctx)
            .expect("aggregate over filtered join must compose");
        assert!(!plan_contains_imv_marker(&out),
            "no IMV marker may survive: {out:?}");
    }

    #[test]
    fn pipeline_aggregate_over_nested_join_composes() {
        use crate::sql::optimizer::rewrite::imv::marker::plan_contains_imv_marker;
        use crate::sql::optimizer::rewrite::imv::pipeline::build_imv_pipeline;
        let mut ctx = build_ctx();
        // Aggregate( Join( Join(b,b), b ) ) —— 三层嵌套(homogeneous base b)
        let inner = join_of(scan("b", 1), scan("b", 10));
        let outer = join_of(inner, scan("b", 20));
        let plan = aggregate_over(outer);
        let out = build_imv_pipeline().rewrite(plan, &mut ctx)
            .expect("aggregate over nested join must compose");
        assert!(!plan_contains_imv_marker(&out),
            "no IMV marker may survive: {out:?}");
    }
```

注:`join_of` 需接受任意 `LogicalPlan` 左右孩子(现有 `join_of(left, right)` 已是)。`filter_over(input, column_id, column)` 已存在于 `branch_union.rs` tests。若 `aggregate_over` 的 group key 列 id 与嵌套 join 输出不一致导致 group-key 解析失败,调整 `aggregate_over` 的 group_by 指向 join 输出的 region 列 id。

- [ ] **Step 2: 运行,确认失败 / 通过**

Run: `cargo test --lib pipeline_aggregate_over_filtered_join_composes pipeline_aggregate_over_nested_join_composes -- --nocapture`
Expected: 在 Task 2 完成后**应直接 PASS**(组合路径已通)。若 FAIL,按报错定位(常见:group-key 列 id 映射或 mark_scan 边界),修到 PASS。这是 Task 1+2 的集成验证关。

- [ ] **Step 3: commit**

```bash
git add src/sql/optimizer/rewrite/imv/branch_union.rs
git commit -m "test(imv): pipeline integration for aggregate over filtered/nested join"
```

---

## Task 4: 正向 e2e —— `iceberg_ivm_aggregate_filter_join.sql`

**Files:**
- Create: `sql-tests/iceberg-ivm/sql/iceberg_ivm_aggregate_filter_join.sql`
- Create(record 生成): `sql-tests/iceberg-ivm/result/iceberg_ivm_aggregate_filter_join.result`

参照现有 `iceberg_ivm_union_of_aggregate_over_join.sql` 的结构(同目录),走 `MV == 全量重算` cross-check。

- [ ] **Step 1: 写 fixture**

```sql
-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,join,aggregate,filter,target_state
-- Test Point: aggregate over a FILTERED inner join (Aggregate(Filter(Join)))
-- refreshes incrementally. The WHERE sits between the aggregate and the join,
-- exercising the decomposed pure Delta(Join) rule + delta pushdown through Filter.
-- Method: fact JOIN dim, WHERE f.amount > 0, GROUP BY d.region. Initial REFRESH,
-- then INSERT into both bases and DELETE from fact; cross-check MV == full recompute.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_fjoin_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/ice_ivm_fjoin_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_ivm_fjoin_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_fjoin_${uuid0}.ns_${uuid0}.fact (
  id BIGINT NOT NULL, dim_id BIGINT, amount BIGINT
) TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");
CREATE TABLE ice_ivm_fjoin_${uuid0}.ns_${uuid0}.dim (
  id BIGINT NOT NULL, region STRING
) TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");
SET CATALOG ice_ivm_fjoin_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW fjoin_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT d.region, COUNT(*) AS c, SUM(f.amount) AS s
FROM ice_ivm_fjoin_${uuid0}.ns_${uuid0}.fact AS f
JOIN ice_ivm_fjoin_${uuid0}.ns_${uuid0}.dim AS d ON f.dim_id = d.id
WHERE f.amount > 0
GROUP BY d.region;

-- query 2
-- @skip_result_check=true
INSERT INTO ice_ivm_fjoin_${uuid0}.ns_${uuid0}.dim VALUES (10,'east'),(20,'west'),(30,'south');
INSERT INTO ice_ivm_fjoin_${uuid0}.ns_${uuid0}.fact VALUES
  (1,10,100),(2,10,200),(3,20,50),(4,30,-5),(5,30,70);
REFRESH MATERIALIZED VIEW fjoin_mv_${uuid0};

-- query 3
SELECT region, c, s FROM fjoin_mv_${uuid0} ORDER BY region;

-- query 4
SELECT d.region, COUNT(*) AS c, SUM(f.amount) AS s
FROM ice_ivm_fjoin_${uuid0}.ns_${uuid0}.fact AS f
JOIN ice_ivm_fjoin_${uuid0}.ns_${uuid0}.dim AS d ON f.dim_id = d.id
WHERE f.amount > 0 GROUP BY d.region ORDER BY region;

-- query 5
-- @skip_result_check=true
INSERT INTO ice_ivm_fjoin_${uuid0}.ns_${uuid0}.dim VALUES (40,'north');
INSERT INTO ice_ivm_fjoin_${uuid0}.ns_${uuid0}.fact VALUES (6,20,80),(7,40,60),(8,10,-9);

-- query 6
-- @skip_result_check=true
-- @explain_contains=AggregateStateMerge
-- @explain_contains=Filter
-- @explain_contains=UNION
-- @explain_contains=sum_state_signed
-- @explain_contains=IcebergMvTargetState
REFRESH MATERIALIZED VIEW fjoin_mv_${uuid0};

-- query 7
SELECT region, c, s FROM fjoin_mv_${uuid0} ORDER BY region;

-- query 8
SELECT d.region, COUNT(*) AS c, SUM(f.amount) AS s
FROM ice_ivm_fjoin_${uuid0}.ns_${uuid0}.fact AS f
JOIN ice_ivm_fjoin_${uuid0}.ns_${uuid0}.dim AS d ON f.dim_id = d.id
WHERE f.amount > 0 GROUP BY d.region ORDER BY region;

-- query 9
-- @skip_result_check=true
DELETE FROM ice_ivm_fjoin_${uuid0}.ns_${uuid0}.fact WHERE id = 1;

-- query 10
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW fjoin_mv_${uuid0};

-- query 11
SELECT region, c, s FROM fjoin_mv_${uuid0} ORDER BY region;

-- query 12
SELECT d.region, COUNT(*) AS c, SUM(f.amount) AS s
FROM ice_ivm_fjoin_${uuid0}.ns_${uuid0}.fact AS f
JOIN ice_ivm_fjoin_${uuid0}.ns_${uuid0}.dim AS d ON f.dim_id = d.id
WHERE f.amount > 0 GROUP BY d.region ORDER BY region;

-- query 13
-- @skip_result_check=true
DROP MATERIALIZED VIEW fjoin_mv_${uuid0};
DROP TABLE ice_ivm_fjoin_${uuid0}.ns_${uuid0}.fact FORCE;
DROP TABLE ice_ivm_fjoin_${uuid0}.ns_${uuid0}.dim FORCE;
DROP DATABASE ice_ivm_fjoin_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_fjoin_${uuid0};
```

- [ ] **Step 2: 起环境 + 构建 + 录制 golden**

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo build --profile dev-opt
# 起 server(后台,等 NOVAROCKS_READY,见 CLAUDE.md §7.3)
LOG=/tmp/nova-fjoin.log
NO_PROXY=127.0.0.1,localhost target/dev-opt/novarocks standalone-server \
  --config "$NOVAROCKS_STANDALONE_CONFIG" >"$LOG" 2>&1 &
SRV=$!; for i in $(seq 1 60); do grep -q '^NOVAROCKS_READY ' "$LOG" && break; kill -0 $SRV 2>/dev/null || { tail -20 "$LOG"; exit 1; }; sleep 1; done
# 录制本 case 的 golden(NovaRocks-only ref)
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-ivm \
  --only iceberg_ivm_aggregate_filter_join --mode record --record-from target
```

- [ ] **Step 3: verify 通过**

Run:
```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-ivm \
  --only iceberg_ivm_aggregate_filter_join --mode verify
```
Expected: PASS —— query 3==4、7==8、11==12(MV == 全量重算),且 `@explain_contains` 全部命中。**人工核对**:打开生成的 `.result` 确认 MV 行与重算行逐行一致(非空、region 聚合正确)。

- [ ] **Step 4: commit**

```bash
git add sql-tests/iceberg-ivm/sql/iceberg_ivm_aggregate_filter_join.sql \
        sql-tests/iceberg-ivm/result/iceberg_ivm_aggregate_filter_join.result
git commit -m "test(imv): e2e aggregate over filtered join incremental refresh"
```

---

## Task 5: 正向 e2e —— `iceberg_ivm_aggregate_nested_join.sql`(三表嵌套)

**Files:**
- Create: `sql-tests/iceberg-ivm/sql/iceberg_ivm_aggregate_nested_join.sql` + `.result`

MV:`fact JOIN dim ON … JOIN dim2 ON … GROUP BY dim2.region`(三表 inner join 嵌套,顶层 agg)。

- [ ] **Step 1: 写 fixture**

结构同 Task 4(catalog/db、三张表 `fact(id,dim_id,amount)`、`dim(id,region_id)`、`dim2(id,region)`,均 format-v3 + row-lineage)。MV:

```sql
CREATE MATERIALIZED VIEW njoin_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT d2.region, COUNT(*) AS c, SUM(f.amount) AS s
FROM ice_ivm_njoin_${uuid0}.ns_${uuid0}.fact AS f
JOIN ice_ivm_njoin_${uuid0}.ns_${uuid0}.dim  AS d  ON f.dim_id = d.id
JOIN ice_ivm_njoin_${uuid0}.ns_${uuid0}.dim2 AS d2 ON d.region_id = d2.id
GROUP BY d2.region;
```

矩阵:initial INSERT(三表)+ REFRESH + cross-check;再各表 INSERT + REFRESH + cross-check;再 DELETE(fact 一行)+ REFRESH + cross-check。query 6 REFRESH 加:

```sql
-- @explain_contains=AggregateStateMerge
-- @explain_contains=UNION
-- @explain_contains=IcebergVersionTable
-- @explain_contains=sum_state_signed
```

(注:`@explain_contains` 是子串包含、不计次数;"外内两层各一组 UNION" 由 Task 7 的 EXPLAIN 人工核对兜底。)

cross-check 查询 = MV 定义的全量 SELECT(同 query),`ORDER BY region`。

- [ ] **Step 2: 录制 + verify**(同 Task 4 Step 2-3,`--only iceberg_ivm_aggregate_nested_join`)

Expected: PASS,MV == 全量重算贯穿 insert/delete;`.result` 人工核对非空且与重算一致。

- [ ] **Step 3: commit**

```bash
git add sql-tests/iceberg-ivm/sql/iceberg_ivm_aggregate_nested_join.sql \
        sql-tests/iceberg-ivm/result/iceberg_ivm_aggregate_nested_join.result
git commit -m "test(imv): e2e aggregate over three-table nested join incremental refresh"
```

---

## Task 6: 更新 `iceberg_ivm_union_shape_rejects_unsupported.sql` —— 守住第二档边界

**Files:**
- Modify: `sql-tests/iceberg-ivm/sql/iceberg_ivm_union_shape_rejects_unsupported.sql`(+ `.result` 重录)

- [ ] **Step 1: 先读现有文件**

Run: `sed -n '1,200p' sql-tests/iceberg-ivm/sql/iceberg_ivm_union_shape_rejects_unsupported.sql`
确认其中是否有 `Aggregate(Filter(Join))` 或"composed-over-join 不支持"的 reject case(Phase 4 文档提到 `iceberg_ivm_union_shape_rejects_unsupported` 改过)。

- [ ] **Step 2: 改造**

- 若存在"带 filter 的 join-agg 不支持"reject query:**移除**它(该形态现已支持,正向覆盖在 Task 4)。
- **新增/保留**一个 **join 一侧是 aggregate** 的 reject,守住第二档边界(以 `@expect_error` 断言拒绝)。形如:

```sql
-- query N
-- @expect_error=does not support
-- join 一侧是聚合子查询(join-of-aggregate)—— 第二档,显式不支持
CREATE MATERIALIZED VIEW joa_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT d.region, SUM(g.sv) AS total
FROM ( SELECT dim_id, SUM(amount) AS sv
       FROM ice_ivm_rej_${uuid0}.ns_${uuid0}.fact GROUP BY dim_id ) AS g
JOIN ice_ivm_rej_${uuid0}.ns_${uuid0}.dim AS d ON g.dim_id = d.id
GROUP BY d.region;
```

`@expect_error` 的子串取实际报错(CREATE 期或首次 REFRESH 期)。先用 verify 跑出实际报错文本,再回填断言子串(若 CREATE 即拒绝,断言 CREATE 的错;若 CREATE 通过、REFRESH 拒绝,把 reject 放在 REFRESH query 上)。

- [ ] **Step 3: 重录 + verify**(`--only iceberg_ivm_union_shape_rejects_unsupported`)

Expected: PASS —— filter-join 不再出现于此 reject 套件;join-of-aggregate 按 `@expect_error` 被拒。

- [ ] **Step 4: commit**

```bash
git add sql-tests/iceberg-ivm/sql/iceberg_ivm_union_shape_rejects_unsupported.sql \
        sql-tests/iceberg-ivm/result/iceberg_ivm_union_shape_rejects_unsupported.result
git commit -m "test(imv): filtered join-agg now supported; pin join-of-aggregate as the rejected boundary"
```

---

## Task 7: optimizer plan-shape 断言

**Files:**
- Create: `sql-tests/optimizer/sql/imv_aggregate_filter_join_logical.sql`
- Create: `sql-tests/optimizer/sql/imv_aggregate_nested_join_logical.sql`

参照现有 `sql-tests/optimizer/sql/imv_join_aggregate_logical_cutover.sql`(`@skip_result_check=true` + `EXPLAIN REFRESH` + `@explain_contains`),建真实 previous snapshot 后 append、再 `EXPLAIN REFRESH`。

- [ ] **Step 1: 写两个 fixture**

`imv_aggregate_filter_join_logical.sql`:建 fact/dim、MV = 带 `WHERE f.amount>0` 的 join-agg、REFRESH(建 previous snapshot)、INSERT fact、`EXPLAIN REFRESH` 断言:

```sql
-- @explain_contains=AggregateStateMerge
-- @explain_contains=Filter
-- @explain_contains=UNION
-- @explain_contains=sum_state_signed
-- @explain_contains=IcebergVersionTable
EXPLAIN REFRESH MATERIALIZED VIEW <mv>;
```

`imv_aggregate_nested_join_logical.sql`:三表嵌套 join-agg,`EXPLAIN REFRESH` 断言 `AggregateStateMerge / UNION / IcebergVersionTable`。

- [ ] **Step 2: verify + 人工核对嵌套展开**

Run: `... --suite optimizer --only imv_aggregate_filter_join_logical,imv_aggregate_nested_join_logical --mode verify`
Expected: PASS。**人工核对一次** `imv_aggregate_nested_join_logical` 的 EXPLAIN 输出:确认 **外层与内层 join 各产生一组 `UNION` + `IcebergVersionTable`**(嵌套被逐层展开)——这是计数断言无法表达、需肉眼确认的 plan-shape 凭证。

- [ ] **Step 3: commit**

```bash
git add sql-tests/optimizer/sql/imv_aggregate_filter_join_logical.sql \
        sql-tests/optimizer/sql/imv_aggregate_nested_join_logical.sql \
        sql-tests/optimizer/result/imv_aggregate_filter_join_logical.result \
        sql-tests/optimizer/result/imv_aggregate_nested_join_logical.result
git commit -m "test(imv): optimizer plan-shape for aggregate over filtered/nested join refresh"
```

---

## Task 8: 全量回归 + 文档收尾

**Files:**
- Modify: `docs/design/plans/2026-06-05-imv-phase4-retire-incremental-mv-shape.md`(Known limitation 标记已解)
- (可选)`NovaRocks Roadmap.md`(在用户 Obsidian 仓,按需)

- [ ] **Step 1: 跑全 iceberg-ivm 套件,确认不回归**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-ivm --mode verify
```
Expected: 全绿(含原有 67 + 新增的 filter-join / nested-join,reject 套件按新边界)。

- [ ] **Step 2: 跑 optimizer imv plan-shape,确认不回归**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite optimizer \
  --only imv_join_aggregate_logical_cutover,imv_aggregate_logical_cutover,imv_aggregate_filter_join_logical,imv_aggregate_nested_join_logical --mode verify
```
Expected: 全绿(现有 cutover case 的 `@explain_contains` 仍命中 → 单层 `Aggregate(Join)` 等价路径未退步)。

- [ ] **Step 3: 全量 lib 测试 + 构建检查**

```bash
cargo test --lib sql::optimizer::rewrite::imv 2>&1 | tail -5
cargo build 2>&1 | tail -3
cargo clippy 2>&1 | tail -5
```
Expected: 全绿、无 warning 回归。

- [ ] **Step 4: 标记 Phase 4 Known limitation 已解**

在 `docs/design/plans/2026-06-05-imv-phase4-retire-incremental-mv-shape.md` 的 "Known limitation" 段尾追加一行:

```markdown
**Update (2026-06-06):** Resolved by the join-delta decomposition
(docs/design/specs/2026-06-06-imv-join-delta-decomposition-design.md):
`Aggregate(Filter(Join))` and multi-level inner/cross join nesting now refresh
incrementally. join-of-aggregate and nested join-projection remain deferred.
```

- [ ] **Step 5: commit**

```bash
git add docs/design/plans/2026-06-05-imv-phase4-retire-incremental-mv-shape.md
git commit -m "docs(imv): mark Aggregate(Filter(Join)) limitation resolved by join-delta decomposition"
```

---

## Self-Review

**1. Spec coverage（逐节核对 spec → task）：**
- §4.1 纯规则 + 删旧 + pushdown Join→Unchanged + pipeline 并 stage → Task 2 ✓
- §4.3 `mark_scan` 放宽（delta 包整 join / version 下推）→ Task 1 ✓
- §4.4 防双计 / 嵌套 / self-join → Task 2(展开复用现有 from/to 语义)+ Task 3(嵌套集成)+ Task 5(self-join 由 `b JOIN b` 覆盖)✓
- §4.5 marker 传播 / 终止性 → Task 2 单测(嵌套留内层 delta)✓
- §5.1 回归护栏 → Task 8 ✓；§5.2 单测连带改写 → Task 2 Step 1/4/5 ✓
- §5.3 正向 e2e（filter-join / nested / reject 边界）→ Task 4 / 5 / 6 ✓
- §5.4 optimizer plan-shape → Task 7 ✓
- §7 验收 → Task 8 ✓

**2. Placeholder scan:** 无 "TBD/TODO/类似 Task N"；Rust 步骤均含完整代码;e2e 步骤含完整 SQL 或明确的"参照 Task 4 结构 + 列出差异"。`@expect_error` 子串在 Task 6 Step 2 明确"先 verify 跑出实际文本再回填"——这是有意的运行期取值,非占位。

**3. Type consistency:** `RewriteJoinDeltaRule`(struct)/ `"RewriteJoinDelta"`(name)/ `RewriteJoinDeltaRule`(pipeline import 与 vec)三处一致;`mark_delta_scan(plan, ColumnId)`、`mark_version_scan(plan, ImvVersionRef)`、`ImvVersionRef::{from,to}_snapshot()`、`ImvVersionRole::{From,To}`、`normalize_branch_output(plan, &[OutputColumn])`、`join_output_columns(JoinKind,&L,&R)`、`RewriteResult::{Changed,Unchanged}`、`UnionNode{inputs,all,output_columns,required_output_columns}`、`ImvDeltaNode{input,is_root,action_column,branch_scope}` 均与现有 `join_delta.rs` 签名一致。

---

## Execution Handoff

见 writing-plans 收尾:实现时按 Task 1→8 顺序,推荐 subagent-driven(每 task 一个 fresh subagent + 两段式 review)。Task 4-8 需 Docker(`docker/iceberg-rest/up.sh`)+ standalone-server(等 `NOVAROCKS_READY`),由 controller 编排。
