# G1 收尾:planner/optimizer/codegen 列绑定迁移到 ColumnId-only 语义

**日期**:2026-06-05
**状态**:设计已确认,待出实施计划
**关联**:`docs/design/specs/2026-05-20-sql-layer-roadmap-design.md` §2.1 **G1 — ColumnId (P0)**

---

## 1. 背景与目标

### 1.1 G1 是半成品

roadmap §2.1 的 G1 目标是「用全局唯一 `ColumnId(u32)` 替换字符串式 `ColumnRef`,所有
distribution/sort/equivalence/输出 schema 按 id 引用,展示名走按 id 的侧表」。实际落地
只完成了一半:

- ✅ `ColumnRefFactory` / `ColumnId` 已存在(`src/sql/column_id.rs`)。
- ✅ optimizer 的**物理决策核心**已纯 id:distribution / sort key / equivalence class /
  column pruning / winner cache(`src/sql/optimizer/property.rs`、`logical_props.rs`、
  `search.rs`)。
- ✅ codegen 的 `ExprScope` 引入了 `by_id` 索引,leaf `ColumnRef` 解析 id-first。
- ❌ codegen 仍保留 **name fallback**(`expr_compiler.rs:424`)、`strict_missing_id`、
  project-over-aggregate 的 **display 字符串重匹配**(`expr_compiler.rs:763/812`)。
- ❌ 多处**结构性 id 缺口**逼着 fallback 必须存在(见 §4 现状列)。
- ❌ optimizer 统计子系统按 name keying;若干重写规则按名匹配;`Repeat` / `Decode` /
  `AggregateStateMerge` 用名字建模列。

roadmap 原始验收标准「`grep -r 'ColumnRef {' src/sql/optimizer/` 返回零」从未满足:
optimizer 全程复用 `analysis::TypedExpr`,其 `ExprKind::ColumnRef { column_id, qualifier,
column }` 一直带着 name 字段。

### 1.2 本次目标(精化 G1 验收)

把 planner/optimizer/codegen 内部列绑定一次性迁到 **ColumnId-only 语义**。**不做兼容过渡
方案,不保留「按名 fallback 绑定语义列」的生产逻辑。**

精化点:不要求「删掉 ColumnRef 的 name 字段」,而是「**禁止用 name 做语义列绑定**」。name
作为字段保留供展示/调试,但绑定层(scope)在构造上就无法用 name 解析——这是用户确认的
**方案 A**。

### 1.3 范围(用户确认 = 最大范围)

本次纳入:
1. **核心绑定路径**:ColumnRef→slot/tuple 全程只靠 ColumnId;补齐所有逼出 name fallback
   的结构性 id 缺口;`Repeat`/`Decode` 改 ColumnId 模型;USING 改 id 解析;最终删掉
   `ExprScope` 的 name 语义索引、name fallback、`strict_missing_id`。
2. **optimizer 逻辑重写规则**的按名匹配(`push_to_scan` / `push_to_join` fallback /
   `ukfk` / `derive_join_not_null`)改按 ColumnId。
3. **optimizer 统计/基数子系统**从按 name keying 改为按 ColumnId keying。

### 1.4 非目标 / 明确边界

- **`AggregateStateMergeOp`(IMV 专用)不在本次范围**。等进行中的 IMV refresh property
  framework 重构(见 memory `project_imv_refresh_property_framework`)做完后再改。本次为其
  内部机器列(`__change_op`、aggregate-state 列)留**显式隔离的名字查找通道**
  `resolve_internal_by_name`,而非泛化 fallback(见 §4)。
- 不引入独立的 id-only ColumnRef 类型(否决了「彻底剥离 name 字段」的方案 B)。
- 不动 analyzer 的入口名解析(`AnalyzerScope` 按名→id 是合法入口)。

---

## 2. 目标契约:name / id 边界

### 2.1 name 仅允许出现在这 5 个边界(穷举)

| # | 边界 | 载体 |
|---|---|---|
| 1 | **SQL 解析入口** | `AnalyzerScope` 按名解析产出 id(analyzer 内部) |
| 2 | **输出列展示 / 输出 schema** | `OutputColumn.name`、MySQL wire schema、`SELECT *` 展开;`ExprScope.ordered` **仅**此用途 |
| 3 | **错误 / EXPLAIN / debug / log** | `ColumnRefFactory::display_name(id)`;`ColumnRef.qualifier/column` 字段(**只读展示**) |
| 4 | **descriptor 可读标签** | tuple/slot descriptor 的 name(标签,不参与决策) |
| 5 | **connector schema 边界** | 真实 catalog 列名(scan/sink 与外部存储、catalog 元数据对接) |

任何其他对 name 的语义使用(绑定、匹配、dedup、keying)都视为违规,必须改 ColumnId。

### 2.2 绑定契约(终态硬性不变量)

1. 每个到达 codegen 的**可执行 `ColumnRef` 必带真实(非 `ColumnId::UNSET`)`column_id`**。
2. 每个算子的**每个输出列**在其输出 scope 里都有 `by_id` binding。
3. codegen 解析 `ColumnRef` **只走 `resolve_by_id`**;未命中 = **硬错误**(无 name fallback)。
4. **fail-fast 校验**:codegen 入口跑 `verify_id_binding(plan)` plan-walker,断言 1+2;
   debug build `panic`/`debug_assert`,release 返回明确 `Err`。隔离的内部列(§4)按可
   grep 的白名单跳过。

---

## 3. IR / 计划结构改造(给「计算型输出」赋予 id 身份)

这是地基:没有 id 字段,上层就只能按 display 字符串重匹配。

| 结构(文件:行) | 现状 | 改造 |
|---|---|---|
| `AggregateCall`(`plan.rs:283`) | 无 output id | **加 `output_column_id: ColumnId`** |
| `WindowExpr`(`plan.rs:158`) | 仅 `output_name` | **加 `output_column_id: ColumnId`**;`output_name` 降级为展示标签 |
| `GROUPING()`/`GROUPING_ID()` 虚拟列 | analyzer `mod.rs:2340` 产 `UNSET` | analyzer 给**真实 id**;Repeat 按 id 注册虚拟槽 |
| `LogicalRepeatOp`/`PhysicalRepeatOp`(`operator.rs:233`) | `repeat_column_ref_list: Vec<Vec<String>>`、`all_rollup_columns: Vec<String>`、`grouping_key_aliases: Vec<(String,String)>`、`grouping_fn_args: Vec<(String, Vec<String>)>` | 改 `Vec<Vec<ColumnId>>`、`Vec<ColumnId>`、`Vec<(ColumnId, ColumnId)>`(原列 id→重复组输出 id)、`Vec<(ColumnId, Vec<ColumnId>)>`;**删除 `__repeat_group` 名字别名整套机制** |
| `DecodeMapping` / `LogicalDecodeOp`(`operator.rs:271`) | `dict_column`/`string_column` 为 `String` | 改 `source_column_id`/`output_column_id: ColumnId`;Decode 输出 scope **继承子节点 `by_id`** |
| `AggregateStateMergeOp`(`operator.rs:144`) | name 建模 | **本次不动**(IMV 重构后再改),保留隔离通道 |

`AggregateCall`/`WindowExpr` 拿到 output id 是 §4 A/C 改写与 §5.2 删除 display 重匹配的前提。

---

## 4. Planner:逐个补齐 id 缺口

目标:让**每个**到达 codegen 的可执行 `ColumnRef` 都带真 id,且每个算子输出都有对应 id。

| # | 缺口(现状) | 改造 |
|---|---|---|
| A | **aggregate 结果**:`AggregateCall` inline 在上层 projection,codegen 按 display 重匹配 | `split_projection_for_aggregate`(`mod.rs:1623`)给每个 `AggregateCall` mint `output_column_id`;新增 `rewrite_agg_calls_to_refs`,把 projection 里每个 `AggregateCall` 子表达式替换为 `ColumnRef(agg.output_column_id)`。`sum(b)+1` → `ColumnRef(id)+1` |
| B | **计算型 group key**:`rewrite_exact_group_by_expr_ref`(`mod.rs:1659`)按 `typed_expr_display_name` 字符串匹配,非 ColumnRef 产 `UNSET` | 改用**结构化 TypedExpr 相等**(归一化比较,非 display 字符串)识别 projection 中等于某 group-by 的子表达式;每个 group-by 输出在建 Aggregate `output_columns` 时分配真 id;命中子表达式改写为 `ColumnRef(group_key_id)`。**定性:这是 planner 变换(表达式树相等),不是列绑定** |
| C | **window 结果**:`rewrite_window_calls`(`mod.rs:1375`)产 `ColumnRef{UNSET, output_name}` | 把已 mint 的 window output id 贯通进改写,产 `ColumnRef(window_output_id)` |
| D | **`__nr_sel_<idx>`**:`remap_select_alias_refs`(`mod.rs:346`)产 `UNSET` | 为该合成 select-alias 列 mint/复用真 id 并携带 |
| E | **GenerateSeries**:`plan_output_columns`(`mod.rs:465`)产 `UNSET` OutputColumn | mint 输出列 id;经 derived/CTE adapter 时 `source.column_id` 不再 `UNSET` |
| F | **top-level VALUES**:`plan_values`(`mod.rs:2406`)的 node id 与 query output id 分叉 | 每个 VALUES 输出列单一 id,node 与 query output 一致 |
| G | **非 ColumnRef rollup key**(ROLLUP 下 FULL OUTER USING COALESCE):`prepare_repeat_input`(`mod.rs:761`)产 `UNSET` | 物化 `__repeat_group_key_N` Project 槽 mint 真 id;rollup-key 引用携带该 id;Repeat `grouping_key_aliases` 变成 `(materialized_id → repeat_output_id)` |
| H | **USING / FULL OUTER USING**:靠 `canonical_qualifier` 名字操纵 + COALESCE 替换 | INNER/LEFT/RIGHT:unqualified USING 引用直接携带 analyzer 选定那侧的真 id,**删除 qualifier-steering**;FULL OUTER:COALESCE 作为计算型 Project 输出,照常拿 `output_column_id` |
| I | **subquery-alias 重曝光**:`subquery_rewrite.rs` 用 `scope.add_column`(fresh id)与 producing id 分叉(当前靠 name fallback 掩盖) | 改 `add_column_with_id` 用 producing id 重曝光,后续解析得同一 id |
| J | **ColumnRef rollup key**:已保 id,但有 `__repeat_group` qualifier 改写 | 删除 `__repeat_group` 改写(id-only 后不需要),物化槽 id 直接流过 |

---

## 5. Codegen:`ExprScope` → id-only 绑定

### 5.1 `src/sql/codegen/resolve.rs`

- **删除** `qualified`、`unqualified`(name→binding 语义索引)、`binding_has_id_index`、
  `resolve_column`(语义名解析器)。
- **保留** `by_id`(唯一绑定索引);`ordered` **仅**留给 `SELECT *`/输出 schema(展示),
  概念上不再是绑定索引(对应 §2.1 边界 #2)。
- **新增** `resolve_internal_by_name`:**只**给 IMV/AggregateStateMerge 的内部机器列
  (`__change_op` 等)使用,带 `// TODO(IMV refactor): migrate to ColumnId` 标注。普通
  `ColumnRef` **永不**触及——这是可 grep 的窄通道,不是泛化 fallback。

### 5.2 `src/sql/codegen/expr_compiler.rs`

- `ColumnRef` arm(382-432)塌缩为
  `let b = self.scope.resolve_by_id(column_id).ok_or_else(|| <hard error>)?;`。
- **删除** `strict_missing_id` 字段与 strict/lenient 分叉。
- **删除** `FunctionCall`(763)/`AggregateCall`(812)的 display 字符串重匹配 arm(planner
  已把它们改写成 `ColumnRef`,compiler 只会看到 `ColumnRef`)。

### 5.3 `src/sql/codegen/fragment_builder.rs`

每个 `visit_*` 按 `op.output_columns[i].column_id` 用 `add_column_with_id` 注册输出:

- Scan / Project:已 id(保留)。
- **Aggregate**:group-by 槽 + agg-结果槽都按新 output id 注册(读 `op.output_columns`,
  停止重建 display name)。
- **Window**:按 `output_column_id` 注册(停用 `output_name`)。
- **Repeat**:repeat 输出 + `grouping_id` + grouping-fn 虚拟槽全按 id 注册(新 ColumnId 模型);
  删 `__repeat_group` 别名。
- **Decode**:继承子 `by_id`,按 `source_column_id→output_column_id` 映射槽(停止丢弃重建 +
  name resolve)。
- Values / GenerateSeries / TableFunction 新输出:按已 mint 的 id 注册。
- Join / Filter / Sort / Limit / Exchange / SetOp / CTE:已按 id 继承/merge(保留;`merge`
  仍 by_id **左胜**以保自连接正确性)。
- **dict 传播**(`visit_project`/`visit_hash_aggregate` 复制 `TGlobalDict` 的 name lookup
  1962/2509)→ 改按 id 查源槽。
- **tuple/layout**:descriptor 标签保 name(§2.1 #4);但 `eq_ignore_ascii_case` 名字匹配做
  的**槽映射决策**(155-169、1026-1040)改 id(具体落点 plan 阶段核实)。

---

## 6. Optimizer:统计 + 重写规则 → ColumnId

**核心模式:name 只活在 catalog/connector 读取边界(§2.1 #5),在 scan 处用
`output_columns` 的 (name, id) 一次性映射成 ColumnId,往上全是 id。**

### 6.1 统计子系统(name-keyed → id-keyed)

- `Statistics.column_statistics`、`TableStatistics.column_stats`(`statistics.rs:69/136`)、
  `LogicalProperties.column_statistics`(`memo.rs:111`)→ `HashMap<ColumnId, ColumnStatistic>`。
- `extract_column_name`(`selectivity.rs:208`,读 `ColumnRef.column` 忽略 id)→
  `extract_column_id`(读 `column_id`)。波及 NDV(`estimate/ndv.rs`)、selectivity
  (`selectivity.rs`)、join-key NDV(`stats.rs:341/588/976`)、agg-project 传播
  (`stats.rs:106/285/606`)、scan stats(`stats.rs:1067`)、set-op merge(`stats.rs:782`)。
- **唯一 name 桥**:scan 级统计来自 catalog 按真实列名取——在 scan 边界用该 scan 的
  `output_columns` 把 catalog-name → ColumnId 映射一次,产出 id-keyed `Statistics`。set-op
  merge 改按输出列 id。

### 6.2 重写规则(按名匹配 → id)

- `push_to_scan.rs:42-61`:conjunct 引用列与 `scan.columns` **名字**比对 → 比对 **ColumnId
  集合**(scan output_columns 带 id)。
- `push_to_join.rs:83` / `push_to_aggregate.rs:58`:已优先 id,**删掉 name fallback 分支**。
- `ukfk.rs`:UK/FK 约束列名来自 catalog → 在 scan 边界解析成 ColumnId 一次,再按 id 匹配。
- `derive_join_not_null.rs:114/204`:收集名字 → 收集 ColumnId。
- `low_cardinality_dict`:dict 资格检查读 catalog dict 元数据(按名,边界允许),但产出的
  `DecodeMapping` 按 id(§3);已有的 `col.column_id = source_column_id` id 连续性保留。
- `rewrite/rules/utils.rs`:`collect_column_refs`/`*_output_column_names` 等 → 增 id 版
  (`collect_column_ids`)并迁移调用方。

### 6.3 预期 golden 变化

name-keying 会在**同名列**(自连接两侧都叫 `k1`)处意外共享/碰撞统计;id-keying 修正后,
自连接/同名场景的 cost 可能**合理变化**。§7 标记为有意 golden delta。

---

## 7. 交付分阶段 + 验证

单分支、依赖序、每阶段可编译过 suite;终态零 fallback、零 compat shim。

| Phase | 内容 | Gate |
|---|---|---|
| **P1 IR 地基** | §3 加字段 / ColumnId 模型 + planner 填充;**fallback 仍在,行为不变** | 全 suite green |
| **P2 Planner 补缺口** | §4 A–J;每个 `ColumnRef` 带真 id,fallback 变 dead 但留作网 | suite green + 加**非致命审计计数**统计 fallback 命中,应趋 0 |
| **P3 Codegen id-only** | §5 全量;删 name 索引/fallback/strict;加 `resolve_internal_by_name`;**加 fail-fast plan-walker** | suite green + **P2 审计计数必须为 0**(证明删 fallback 安全) |
| **P4 Optimizer** | §6 统计 re-key + 规则 id 化 | suite green + 记录自连接/同名 cost golden delta |
| **P5 死代码清扫** | 删 `canonical_qualifier`、`__repeat_group`、`utils` 名字 helper、用于绑定的 `typed_expr_display_name` | `cargo build` 干净、无 dead_code allow;suite green |

### 7.1 验证面(load-bearing 用例)

`window_*`、`grouping_sets`/rollup/cube、`subquery_alias`、自连接、`join`(USING/NATURAL)、
set-ops(`union`/`intersect`/`except`)、dict/low-cardinality、CTE、`tpc-ds`/`tpc-h`/`ssb`;
IMV suites 经隔离通道仍须过(`AggregateStateMerge` 不动)。

### 7.2 强制不变量

codegen 入口跑 `verify_id_binding(plan)`:遍历每个算子表达式,对每个可执行 `ColumnRef`
断言 `column_id != UNSET` 且能在输入 scope 的 `by_id` 命中;隔离的内部列按可 grep 的白名单
跳过。debug `panic` / release `Err`。

### 7.3 关键安全论证

- P2→P3 之间「审计计数 = 0」是删 fallback 安全的**证据**。
- fail-fast 不变量把任何漏网缺口变成**响亮失败**而非静默错值(对齐 CLAUDE.md「fail fast on
  ambiguous semantics」)。

### 7.4 roadmap G1 验收(精化)

- 全 suite green(尤重 `subquery_alias`、`window_*`)。
- 新单测:重命名/重曝光列共享同一 `ColumnId`。
- 新不变量:无 `UNSET` `ColumnRef` 到达 codegen。

---

## 8. 风险

| 风险 | 缓解 |
|---|---|
| 漏掉某个产 `UNSET` 的构造点 | P2 审计计数 + P3 fail-fast plan-walker 双保险;§4 已穷举已知缺口 |
| 自连接 / 同名列绑定回归 | `ExprScope.merge` 保留 by_id 左胜;保留自连接 sql-test 用例 |
| 统计 re-key 改动 cost 致计划漂移 | §6.3 预期内 golden delta,P4 单独记录、人审 |
| IMV 路径因删 fallback 崩 | `resolve_internal_by_name` 隔离通道 + IMV suites gate |
| 巨型改动难 review | 5 阶段单分支,每阶段独立 gate,可分 PR |
