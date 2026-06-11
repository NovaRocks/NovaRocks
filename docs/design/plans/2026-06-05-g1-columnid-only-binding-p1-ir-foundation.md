# G1 ColumnId-only 绑定 — Phase 1:IR 地基(additive id 接入)实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 给计算型算子输出(aggregate / window / GROUPING() 结果)赋予真实 `ColumnId` 身份字段并由 planner 填充,为后续阶段把 `ColumnRef→slot` 绑定切到 id-only 打地基。

**Architecture:** 纯 **additive** 改动——只新增/填充 `output_column_id`,不改任何 reader,codegen 仍走现有 name fallback,**行为零变化、零 shim**。每个新字段配一个断言「该字段被填成非 `UNSET`」的单测;回归 gate 为核心 SQL 套件全绿。

**Tech Stack:** Rust;`ColumnRefFactory`(`src/sql/column_id.rs`);`analysis::TypedExpr`;sql-test-runner。

**关联设计:** `docs/design/specs/2026-06-05-g1-columnid-only-binding-design.md`(§3 IR 地基)。

---

## 与 spec §7 的偏差(已审定)

spec §7 把 `Repeat` / `Decode` 的**字段类型迁移**(`Vec<String>`→`Vec<ColumnId>`、`DecodeMapping` `String`→`ColumnId`)也放在 P1。**本计划把这两项移到 P3**,与 `visit_repeat` / `visit_decode` 的 codegen reader 迁移**同步落地**。原因:在 P1 改类型而 reader 仍按名工作,会逼出一个 `id→name`(`factory.column_name(id)`)的**临时桥**——这正是「不要 shim」要避免的。把类型变更与 reader 变更绑在同一步,杜绝过渡桥。Phase 1 因此只做**纯 additive** 的三件事。

---

## File Structure(Phase 1 触及)

| 文件 | 动作 | 职责 |
|---|---|---|
| `src/sql/planner/plan.rs` | Modify | `AggregateCall` / `WindowExpr` 加 `output_column_id` 字段 |
| `src/sql/planner/mod.rs` | Modify | `collect_aggregates` 铸造 agg output id;`build_window_and_project` 填 window output id;grouping marker 填真 id |
| `src/sql/analyzer/mod.rs` | Modify | 把 grouping-fn 的 group-by id 透传给 marker 替换,消除 `mod.rs:2340` 的 `UNSET` |
| 各 `AggregateCall` / `WindowExpr` 构造点(测试 / IMV / optimizer) | Modify | 编译器驱动补字段(测试用 `UNSET`,IMV/optimizer 用已有 id 或铸造) |
| `src/sql/planner/mod.rs`(`#[cfg(test)]`) | Modify | 新增三个断言「id 被填充」的单测 |

**`output_column_id` 全部构造点(`grep` 已确认,编译器会再次全列):**
- `AggregateCall {`(plan.rs 的 struct,**非** `ExprKind::AggregateCall`):`optimizer/mod.rs:481`、`optimizer/operator.rs:576`(test)、`optimizer/stats.rs:1795`(test)、`optimizer/rewrite/imv/action_column.rs:946/1173/1213`、`optimizer/rewrite/required_columns.rs:1019`(test)、生产路径 `planner/mod.rs:1713`(`collect_aggregates`)。
- `WindowExpr {`:`optimizer/search.rs:1041`(test)、`optimizer/rewrite/required_columns.rs:1442/1490`(test)、`optimizer/stats.rs:3275`(test)、`optimizer/cascades_rules/implement.rs:1880/1935`(test)、`optimizer/derive/window.rs:105/132`(test)、生产路径 `planner/mod.rs` 的 `build_window_and_project`。

---

## Task 1:`AggregateCall` 加 `output_column_id` 并在 planner 铸造

**Files:**
- Modify: `src/sql/planner/plan.rs:283-289`
- Modify: `src/sql/planner/mod.rs:1623-1657`(`split_projection_for_aggregate`)、`:1686-1720`(`collect_aggregates`)
- Modify(编译器驱动):上面列出的全部 `AggregateCall {` 构造点
- Test: `src/sql/planner/mod.rs`(`#[cfg(test)]` mod,2421 起)

- [ ] **Step 1: 写失败的单测**

在 `src/sql/planner/mod.rs` 的 `#[cfg(test)] mod tests` 末尾追加(紧邻 `test_plan_query_wraps_single_cte_in_anchor` 风格):

```rust
#[test]
fn p1_aggregate_call_gets_output_column_id() {
    // SELECT a, sum(b) FROM t GROUP BY a
    // 经 planner 后,Aggregate 的每个 AggregateCall 必须带非 UNSET 的 output_column_id。
    let plan = plan_test_query("SELECT a, sum(b) AS s FROM t GROUP BY a");
    let aggs = first_aggregate_calls(&plan);
    assert!(!aggs.is_empty(), "expected at least one AggregateCall");
    for call in &aggs {
        assert_ne!(
            call.output_column_id,
            crate::sql::column_id::ColumnId::UNSET,
            "AggregateCall {} must carry a real output_column_id",
            call.name
        );
    }
}
```

> 若 `plan_test_query` / `first_aggregate_calls` 测试辅助不存在,在同一 `mod tests` 内先加最小实现:`plan_test_query` 复用本文件既有测试建表/规划辅助(参考 2421 起的现有用例如何构造 `StandaloneState` + `plan_query`);`first_aggregate_calls` 深度优先遍历 `LogicalPlan` 找到第一个 `LogicalPlan::Aggregate`(或 `HashAggregate`)节点并返回其 `aggregates.clone()`。

- [ ] **Step 2: 运行,确认编译失败(字段不存在)**

Run: `cargo build --profile dev-opt 2>&1 | head -40`
Expected: FAIL — `no field 'output_column_id' on type 'AggregateCall'`(测试引用了尚未存在的字段)。

- [ ] **Step 3: 加字段**

`src/sql/planner/plan.rs:283`:

```rust
#[derive(Clone, Debug)]
pub(crate) struct AggregateCall {
    pub name: String,
    pub args: Vec<TypedExpr>,
    pub distinct: bool,
    pub result_type: DataType,
    pub order_by: Vec<SortItem>,
    /// G1: globally-unique id of THIS aggregate's output column. Minted by
    /// `collect_aggregates` during projection splitting. Phase 1 only populates
    /// it; Phase 2 rewrites upper-projection refs to `ColumnRef(this id)` and
    /// Phase 3 binds the agg-result slot by it.
    pub output_column_id: crate::sql::column_id::ColumnId,
}
```

- [ ] **Step 4: 生产路径填充——`collect_aggregates` 铸造 id**

`collect_aggregates` 需要拿到 `&mut ColumnRefFactory`。改签名并在 push 处铸造:

`src/sql/planner/mod.rs:1686` 签名:

```rust
fn collect_aggregates(
    expr: &TypedExpr,
    out: &mut Vec<AggregateCall>,
    factory: &mut ColumnRefFactory,
) {
```

`:1712-1720` push 处:

```rust
            if !already {
                let display = crate::sql::codegen::helpers::agg_call_display_name_from_parts(
                    name, args, *distinct, order_by,
                );
                let output_column_id =
                    factory.create(None, display, expr.data_type.clone(), expr.nullable);
                out.push(AggregateCall {
                    name: name.clone(),
                    args: args.clone(),
                    distinct: *distinct,
                    result_type: expr.data_type.clone(),
                    order_by: order_by.clone(),
                    output_column_id,
                });
            }
```

更新 `collect_aggregates` 的两个调用点(`split_projection_for_aggregate` 内 `:1635` 项循环、`:1653` HAVING)传入 `factory`。`split_projection_for_aggregate` 签名已带 `factory: &mut ColumnRefFactory`(`:1627`),直接透传。

- [ ] **Step 5: 编译器驱动修其余构造点**

Run: `cargo build --profile dev-opt 2>&1 | grep -A3 "missing field \`output_column_id\`" | head -60`

对每个被标记的 `AggregateCall {` 构造点补字段,按规则:
- **测试** 构造点(`operator.rs:576`、`stats.rs:1795`、`required_columns.rs:1019`):加 `output_column_id: crate::sql::column_id::ColumnId::UNSET,`。
- **IMV** 构造点(`action_column.rs:946/1173/1213`):这些在 IMV 重写里已能访问 `ColumnRefFactory`,加 `output_column_id: factory.create(None, <agg 显示名>, <result_type>, true),`;若该处无 factory 句柄,先铸造再传入(与相邻 OutputColumn 的 id 铸造保持一致写法)。
- **optimizer** 构造点(`optimizer/mod.rs:481`,若为测试 fixture):用 `ColumnId::UNSET`;若为生产路径则铸造。

- [ ] **Step 6: 运行单测,确认通过**

Run: `cargo test --profile dev-opt p1_aggregate_call_gets_output_column_id 2>&1 | tail -20`
Expected: PASS。

- [ ] **Step 7: 提交**

```bash
git add src/sql/planner/plan.rs src/sql/planner/mod.rs \
        src/sql/optimizer/ # 被编译器驱动改动的构造点
git commit -m "planner: add output_column_id to AggregateCall, mint in collect_aggregates (G1 P1)"
```

---

## Task 2:`WindowExpr` 加 `output_column_id` 并在 planner 填充

**Files:**
- Modify: `src/sql/planner/plan.rs:158-170`
- Modify: `src/sql/planner/mod.rs`(`build_window_and_project`,~1040-1075)
- Modify(编译器驱动):上面列出的全部 `WindowExpr {` 构造点
- Test: `src/sql/planner/mod.rs`(`#[cfg(test)]`)

- [ ] **Step 1: 写失败的单测**

```rust
#[test]
fn p1_window_expr_gets_output_column_id() {
    // SELECT a, row_number() OVER (PARTITION BY a ORDER BY b) FROM t
    let plan = plan_test_query(
        "SELECT a, row_number() OVER (PARTITION BY a ORDER BY b) AS rn FROM t",
    );
    let wins = first_window_exprs(&plan);
    assert!(!wins.is_empty(), "expected at least one WindowExpr");
    for w in &wins {
        assert_ne!(
            w.output_column_id,
            crate::sql::column_id::ColumnId::UNSET,
            "WindowExpr {} must carry a real output_column_id",
            w.output_name
        );
    }
}
```

> `first_window_exprs`:DFS 找第一个 `LogicalPlan::Window` 节点返回其 `window_exprs.clone()`(与 Task 1 的 `first_aggregate_calls` 同风格)。

- [ ] **Step 2: 运行,确认编译失败**

Run: `cargo build --profile dev-opt 2>&1 | head -40`
Expected: FAIL — `no field 'output_column_id' on type 'WindowExpr'`。

- [ ] **Step 3: 加字段**

`src/sql/planner/plan.rs:158`:

```rust
#[derive(Clone, Debug)]
pub(crate) struct WindowExpr {
    pub name: String,
    pub args: Vec<TypedExpr>,
    pub distinct: bool,
    pub partition_by: Vec<TypedExpr>,
    pub order_by: Vec<SortItem>,
    pub window_frame: Option<crate::sql::analysis::WindowFrame>,
    pub result_type: DataType,
    /// Display label only (EXPLAIN / output schema). Identity is now
    /// `output_column_id`. (G1: `output_name` downgraded from a binding key.)
    pub output_name: String,
    /// G1: globally-unique id of this window function's output column.
    pub output_column_id: crate::sql::column_id::ColumnId,
    pub ignore_nulls: bool,
}
```

- [ ] **Step 4: 生产路径填充**

在 `build_window_and_project`(`src/sql/planner/mod.rs`,~1058-1071)处:该函数已为每个 window 输出 `factory.create(...)` 铸造了一个 id 放进 `output_columns`。把**同一个 id** 也写进对应的 `WindowExpr.output_column_id`(让 `WindowExpr` 与其 `output_columns` 条目共享 id,不要再铸第二个)。具体:在构造 `WindowExpr { ... }` 的字面量里加 `output_column_id: <该 window 输出已铸造的 id>,`。

- [ ] **Step 5: 编译器驱动修其余构造点**

Run: `cargo build --profile dev-opt 2>&1 | grep -A3 "missing field \`output_column_id\`" | head -60`

全部 `WindowExpr {` 测试构造点(`search.rs:1041`、`required_columns.rs:1442/1490`、`stats.rs:3275`、`implement.rs:1880/1935`、`derive/window.rs:105/132`)加 `output_column_id: crate::sql::column_id::ColumnId::UNSET,`。

- [ ] **Step 6: 运行单测,确认通过**

Run: `cargo test --profile dev-opt p1_window_expr_gets_output_column_id 2>&1 | tail -20`
Expected: PASS。

- [ ] **Step 7: 提交**

```bash
git add src/sql/planner/plan.rs src/sql/planner/mod.rs src/sql/optimizer/
git commit -m "planner: add output_column_id to WindowExpr, share window output id (G1 P1)"
```

---

## Task 3:`GROUPING()/GROUPING_ID()` marker 携带真实 id

**Files:**
- Modify: `src/sql/analyzer/mod.rs`(grouping-fn group-by 铸造处 ~1344;marker 替换 `replace_grouping_markers_in_typed_expr` ~2332-2346 及其调用点)
- Test: `src/sql/analyzer/mod.rs`(`#[cfg(test)]`)或 `src/sql/planner/mod.rs`

**背景:** group-by 侧的 grouping-fn 列在 `mod.rs:1344` 已铸造真 id;但投影侧的 marker 替换(`mod.rs:2340`)产 `ColumnId::UNSET`。把前者的 id 透传到后者。

- [ ] **Step 1: 写失败的单测**

```rust
#[test]
fn p1_grouping_marker_carries_group_by_id() {
    // SELECT a, grouping(a) FROM t GROUP BY ROLLUP(a)
    // 投影里的 grouping(a) 解析后必须是携带真实 id 的 ColumnRef,
    // 且该 id == group-by 侧 __grouping_fn_0 的 id。
    let resolved = analyze_test_query("SELECT a, grouping(a) AS g FROM t GROUP BY ROLLUP(a)");
    let (proj_id, gb_id) = grouping_fn_ids(&resolved); // 见下
    assert_ne!(proj_id, crate::sql::column_id::ColumnId::UNSET);
    assert_eq!(proj_id, gb_id, "projection grouping ref must reuse the group-by key id");
}
```

> `grouping_fn_ids`:从分析结果里取投影中 `__grouping_fn_0` 的 `ColumnRef.column_id`(proj_id)与 `group_by` 中同名键的 `column_id`(gb_id)。用现有 analyzer 测试辅助构造 `analyze_test_query`(参考 analyzer mod 内既有测试)。

- [ ] **Step 2: 运行,确认失败**

Run: `cargo test --profile dev-opt p1_grouping_marker_carries_group_by_id 2>&1 | tail -20`
Expected: FAIL — `proj_id == UNSET` 或 `proj_id != gb_id`。

- [ ] **Step 3: 透传 group-by 铸造的 id**

在 `mod.rs:1343-1352` 铸造 grouping-fn group-by 列时,把 `(fn_name, column_id)` 记到一个局部 `grouping_fn_ids: Vec<(String, ColumnId)>`(与现有 `grouping_fn_args` 平行)。给 `replace_grouping_markers_in_typed_expr` 增加参数 `grouping_fn_ids: &[(String, ColumnId)]`,并把 `:2340` 的:

```rust
column_id: crate::sql::column_id::ColumnId::UNSET,
```

改为按 `idx` 取对应 id:

```rust
column_id: grouping_fn_ids
    .get(idx)
    .map(|(_, id)| *id)
    .expect("grouping_fn id must exist for marker index"),
```

更新 `replace_grouping_markers_in_typed_expr` 的所有调用点,传入新建的 `grouping_fn_ids`。

- [ ] **Step 4: 运行单测,确认通过**

Run: `cargo test --profile dev-opt p1_grouping_marker_carries_group_by_id 2>&1 | tail -20`
Expected: PASS。

- [ ] **Step 5: 提交**

```bash
git add src/sql/analyzer/mod.rs
git commit -m "analyzer: grouping() marker reuses group-by key ColumnId, not UNSET (G1 P1)"
```

---

## Task 4:Phase 1 回归 gate(行为零变化验证)

**目的:** 证明 additive 改动未改变任何执行行为。无需 docker(仅核心 SQL 套件 + 单测)。

- [ ] **Step 1: 全量构建 + 单测**

Run:
```bash
cargo build --profile dev-opt 2>&1 | tail -5
cargo test --profile dev-opt 2>&1 | tail -30
```
Expected: 构建无错;`cargo test` 全绿(含 Task 1–3 三个新单测)。

- [ ] **Step 2: 起 standalone-server(后台,等就绪标记)**

Run(若本 worktree 有生成环境则先 `source`):
```bash
[ -f docker/iceberg-rest/runtime/current/env.sh ] && source docker/iceberg-rest/runtime/current/env.sh
LOG=/tmp/nr-p1.log
NO_PROXY=127.0.0.1,localhost target/dev-opt/novarocks standalone-server \
  ${NOVAROCKS_STANDALONE_CONFIG:+--config "$NOVAROCKS_STANDALONE_CONFIG"} \
  ${NOVAROCKS_STANDALONE_CONFIG:---port 9030} >"$LOG" 2>&1 &
for i in $(seq 1 60); do grep -q '^NOVAROCKS_READY ' "$LOG" && break; sleep 1; done
grep -q '^NOVAROCKS_READY ' "$LOG" || { echo "server failed"; tail -20 "$LOG"; exit 1; }
```
Expected: 出现 `NOVAROCKS_READY mysql_port=... pid=...`。

> `target/dev-opt/novarocks` 为 `--profile dev-opt` 产物路径。

- [ ] **Step 3: 跑 load-bearing 核心套件(非 docker)**

Run(套件名以本仓库实际可用为准,逐个 verify):
```bash
RUNNER="cargo run --profile dev-opt --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests --"
CFG="${NOVAROCKS_SQL_TEST_CONFIG:+--config $NOVAROCKS_SQL_TEST_CONFIG}"
for s in cte join filter sort tpc-h tpc-ds; do
  echo "== suite $s =="; $RUNNER $CFG --suite "$s" --mode verify || { echo "FAIL $s"; break; }
done
```
Expected: 每个套件全绿(尤其含 GROUP BY / window / grouping sets 的 `tpc-ds` 查询)。

> 若仓库另有独立的 `aggregate` / `window` / `grouping` 套件,一并纳入。docker 相关(`iceberg*`)套件留待 P3/P4 整体回归。

- [ ] **Step 4: 关 server**

Run: `kill "$(pgrep -f 'novarocks standalone-server')" 2>/dev/null; true`

- [ ] **Step 5: 收尾提交(若 Step 1–4 有夹带改动)**

```bash
git add -A && git commit -m "test: Phase 1 regression gate green (G1 P1)" || true
```

---

## Phase 1 验收

- `cargo build --profile dev-opt` 无错;`cargo test` 全绿,含三个新断言单测。
- 核心 SQL 套件(`cte`/`join`/`filter`/`sort`/`tpc-h`/`tpc-ds`)`verify` 全绿——**行为零变化**。
- `AggregateCall.output_column_id` / `WindowExpr.output_column_id` 在生产路径均为非 `UNSET`;`grouping()` 投影引用复用 group-by 键 id。
- **未引入任何 reader 改动或 id→name 桥**(`Repeat`/`Decode` 类型迁移留 P3)。

---

## 后续阶段(JIT 规划)

P2–P5 各自在前一阶段落地后单独出计划(`writing-plans` 重新调用),因为后阶段的逐行代码依赖前阶段建立的具体类型/状态:

- **P2 Planner 补缺口**(spec §4 A–J):`rewrite_agg_calls_to_refs`、window/grouping 引用改写、`__nr_sel`/GenerateSeries/VALUES/非 ColumnRef rollup key id、USING id 解析、subquery-alias id 一致;加**非致命 fallback 审计计数**,目标趋 0。
- **P3 Codegen id-only**(spec §5 + 本计划下放的 Repeat/Decode 类型迁移):删 `ExprScope` name 索引/`resolve_column`/fallback/`strict_missing_id`;`visit_*` 全按 id 注册;`visit_repeat`/`visit_decode` 与其 ColumnId 模型同步迁移;加 `resolve_internal_by_name` 隔离通道 + `verify_id_binding` fail-fast plan-walker。**Gate:P2 审计计数必须为 0。**
- **P4 Optimizer**(spec §6):统计子系统 re-key 到 `ColumnId`(scan 边界 name→id 桥);`push_to_scan`/`ukfk`/`derive_join_not_null` 改 id;记录自连接/同名 cost golden delta。
- **P5 死代码清扫**:删 `canonical_qualifier`、`__repeat_group`、`utils` 名字 helper、用于绑定的 `typed_expr_display_name`。
