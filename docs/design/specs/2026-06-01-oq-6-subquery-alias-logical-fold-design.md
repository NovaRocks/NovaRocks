# OQ-6: SubqueryAlias logical fold — 设计

Date: 2026-06-01
Task: OQ-6 in Optimizer Plan Quality Roadmap
Predecessor: 无硬依赖；OQ-1 已让列裁剪可穿透 `SubqueryAlias`
Scope: 按 StarRocks transformer parity 方向，完整移除 NovaRocks 计划层 `SubqueryAlias` operator

---

## 1. 目标

`SubqueryAlias` 只表达 SQL 作用域中的限定名，例如 `FROM (SELECT ...) s` 的 `s`，或 `WITH w AS (...) FROM w t2` 的 `t2`。它不改变行、列值、排序、分布、统计信息，也不应该成为 optimizer 或 executor 里的真实算子。

本任务把 alias 从“计划节点”降级为“分析期作用域 + 输出元数据”：

- analyzer 继续完整处理 derived table alias、CTE consume alias 和 `s(col_alias...)` 列别名列表。
- planner 不再生成 `LogicalPlan::SubqueryAlias`。
- optimizer memo 不再包含 `LogicalSubqueryAliasOp` 或 `PhysicalSubqueryAliasOp`。
- codegen 不再依赖 `PhysicalSubqueryAlias` 给 scope 补充限定名。
- `EXPLAIN` 中不再出现 `SUBQUERY ALIAS [...]`，只展示真实关系代数节点。

完成后，NovaRocks 的 standalone optimizer 在该维度上向 StarRocks 对齐：StarRocks `RelationTransformer.visitSubqueryRelation` 直接复用 subquery root 和 output mapping，不引入 optimizer alias operator。

## 2. 当前问题

当前 NovaRocks 的 derived table 路径是：

```text
Relation::Subquery
  -> plan inner query
  -> LogicalPlan::SubqueryAlias(inner)
  -> Operator::LogicalSubqueryAlias
  -> Operator::PhysicalSubqueryAlias
  -> fragment_builder.visit_subquery_alias
```

这带来几个问题：

1. **无语义节点污染 optimizer。** stats、cost、logical props、property derive、rewrite traversal、EXPLAIN 都需要特殊处理 alias，虽然它没有执行语义。
2. **其它规则需要绕过 alias。** OQ-1 已经通过 ColumnId 让列裁剪穿透 alias，但后续规则仍可能继续出现“多一层透明节点”的特殊分支。
3. **CTE inline 重新引入 alias。** 单引用 CTE 在 `replace_cte_consume` 中会用 `SubqueryAlias(replacement)` 表达 consume 侧 alias 和 output columns，即使 CTE 已经 inline。
4. **physical codegen 承担 SQL 名字解析残留。** `visit_subquery_alias` 当前给 scope 补充 `alias.column` lookup；长期应由 analyzer 固化 `ColumnId`，codegen 只按 id 和真实输出槽解析。

OQ-6 的收益不主要来自少执行一个昂贵算子，而是清理计划表示，让 derived table / CTE / inline view case 的 plan shape 更接近 StarRocks，也减少后续 optimizer 规则的特殊处理。

## 3. 非目标

- 不改变 FE-compatible thrift plan lowering。
- 不改变多消费 CTE 的 `CTEProduce` / `CTEConsume` / multicast exchange 策略。
- 不实现新的 optimizer 语义规则；本任务是计划表示清理。
- 不做 OQ-7 的 join / tpc-h / tpc-ds 全量 baseline 锁定。
- 不保留新的 `SubqueryAlias` fallback。复杂 case 必须用 analyzer output metadata 或普通 `Project` adapter 表达。

## 4. 架构设计

### 4.1 分层边界

alias 的职责拆成三层：

| 职责 | 归属 | 说明 |
| --- | --- | --- |
| SQL 名字解析 | analyzer | 解析 `s.col`、`t2.col`、`s(x, y)`；生成带 `ColumnId` 的表达式 |
| 输出元数据 | `OutputColumn` / `ProjectItem` | 表达外层可见列名、类型、nullable、ColumnId |
| 执行语义 | 无 | alias 不产生行、不过滤、不排序、不聚合 |

因此，`LogicalPlan` 和 optimizer `Operator` 不再拥有 alias variant。

### 4.2 输出适配器

planner 增加内部 helper，概念上称为 `adapt_plan_output(input, target_output_columns)`。

输入：

- `input: LogicalPlan`
- `target_output_columns: Vec<OutputColumn>`，来自 analysis 层的 `Relation::Subquery.output_columns` 或 `Relation::CTEConsume.output_columns`

行为：

1. 读取 `input` 当前按位置输出的 `OutputColumn`。
2. 如果长度、`ColumnId`、列名、类型、nullable 均与 `target_output_columns` 一致，直接返回 `input`。
3. 如果不一致，生成普通 `ProjectNode`：
   - 每个 projection item 按位置引用 child output。
   - `ProjectItem.output_name` 使用 target output name。
   - `ProjectItem.output_column_id` 使用 target output `ColumnId`。
   - `expr` 使用 child output 的 `ColumnId` 和类型，保证读取 child 槽正确。

这个 adapter 是真实输出 schema 适配，不是新的 alias operator。它只在需要改名或重绑定 `ColumnId` 时插入。

### 4.3 Derived table 路径

当前：

```text
Relation::Subquery { query, alias, output_columns }
  -> plan_scoped_query(query)
  -> LogicalPlan::SubqueryAlias { input, alias, output_columns }
```

改为：

```text
Relation::Subquery { query, output_columns, .. }
  -> plan_scoped_query(query)
  -> adapt_plan_output(input, output_columns)
```

`alias` 字段仍保留在 analysis 层，服务 scope 和 MV lineage 等分析用途，但不会进入 logical plan。

### 4.4 CTE inline 路径

多消费 CTE 保留 `CTEProduce` / `CTEConsume`，因为它们表达实际复用边界。

单引用 CTE inline 当前：

```text
CTEConsume(node)
  -> SubqueryAlias {
       input: replacement,
       alias: node.alias,
       output_columns: node.output_columns
     }
```

改为：

```text
CTEConsume(node)
  -> adapt_plan_output(replacement, node.output_columns)
```

这样 inline 后既不保留 alias node，又能保持 consume 侧 fresh `ColumnId`。这点很重要：CTE producer 的 ColumnId 属于定义体，多次 consume 必须能区分不同 alias，即使单引用 inline 也应保留 consumer output id 语义。

### 4.5 Set operation derived table

`Union` / `Intersect` / `Except` 已经有显式 `output_columns`。删除 alias 后，set-op derived table 不能再依赖 `SubqueryAlias.output_columns` 修正外层可见 output ids。

设计要求：

- `plan_scoped_query` 继续把 query-level `output_columns` stamped 到 set-op node。
- `adapt_plan_output` 对 set-op child 做按位置验证，必要时生成 Project adapter。
- 现有关于 set-op output id 的 planner regression test 改写为“不需要 SubqueryAlias 也能保持 set-op output ids 与 derived table output ids 一致”。

### 4.6 Window ordering reuse

当前 `logical_plan_satisfies_window_ordering` 有 alias 透明特判。删除 alias 后，该特判直接移除。对应测试改为验证 derived table 不生成 alias 后，排序复用仍然成立：

```sql
SELECT sum(o_custkey) OVER (ORDER BY o_orderkey)
FROM (SELECT o_orderkey, o_custkey FROM orders ORDER BY o_orderkey) s
```

计划中只应有 child sort，不应出现 `SUBQUERY ALIAS`，也不应多出第二个 sort。

## 5. 删除范围

本任务直接删除计划层 alias operator，不保留死代码：

- `LogicalPlan::SubqueryAlias` 和 `SubqueryAliasNode`
- `Operator::LogicalSubqueryAlias` / `Operator::PhysicalSubqueryAlias`
- `SubqueryAliasToPhysical`
- `stats` / `logical_props` / `derive` / `cost` / `extract` / `explain` 中的 alias 分支
- `fragment_builder.visit_subquery_alias`
- `PruneSubqueryAliasColumns` 规则、模块和 registry 条目
- `required_columns`、`cte_rewrite`、IMV marker/action propagation、low-cardinality、join reorder 等 traversal 中的 alias 分支

Analysis 层保留：

- `Relation::Subquery { alias, output_columns, .. }`
- `Relation::CTEConsume { alias, output_columns, .. }`

这些字段表达 SQL scope 或 CTE consume 语义，不是 plan operator。

## 6. 错误处理与正确性边界

- `adapt_plan_output` 遇到 child output 和 target output 长度不一致时 fail fast，返回明确错误。
- 类型或 nullable 不一致时 fail fast，避免隐式类型降级。
- 如果 target output 使用了 child 不存在的 `ColumnId`，adapter 不按 id 查找 child，而按位置读取 child output，再显式产出 target id。这是 CTE inline 和 derived table column alias 的预期行为。
- `Project` adapter 只负责按位置输出适配，不做表达式重写或 predicate pushdown。
- analyzer 仍负责 `s(col_alias...)` 数量校验；planner 不重复解析 SQL alias list。

## 7. 测试计划

### 7.1 单元测试

Planner/analyzer 覆盖：

- `FROM (SELECT o_orderkey FROM orders) s` planned tree 不含 `SubqueryAlias`。
- `FROM (SELECT o_orderkey FROM orders) s(ok)` 中外层 `s.ok` 能解析，planner 输出列名为 `ok`。
- 单引用 CTE inline 后不含 `SubqueryAlias`，并保持 consumer output `ColumnId`。
- 多消费 CTE 仍保留 `CTEConsume`，不被错误 inline。
- set-op derived table 不需要 alias node 也保持 set-op output ids 正确。
- window ordering reuse 通过 derived table 后仍只保留一个 Sort。

### 7.2 SQL golden

在 `sql-tests/optimizer/` 增加 OQ-6 cases：

- derived table `EXPLAIN` 不含 `SUBQUERY ALIAS`。
- CTE inline `EXPLAIN` 不含 `SUBQUERY ALIAS`。
- 带列别名列表的 derived table 能查询成功，结果列名正确。
- roadmap 标杆 q22 shape 不含 `SUBQUERY ALIAS`，真实 join / scan / aggregate 节点仍存在。

### 7.3 回归验证

最低验证集：

```bash
cargo fmt --check
cargo test --lib
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite optimizer --mode verify
```

有本地 standalone server 时追加 targeted smoke：

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite cte --mode verify

cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite join --mode verify -j 1
```

## 8. PR 验收

PR 描述必须包含：

1. OQ-6 对应 StarRocks 参考点：`RelationTransformer.visitSubqueryRelation` 不生成 alias optimizer operator。
2. NovaRocks before/after `EXPLAIN` diff，显示 `SUBQUERY ALIAS` 消失。
3. q22 / derived table / CTE inline 三类 case 的 plan shape。
4. `sql-tests/optimizer` golden 新增或更新说明。
5. 实际运行的验证命令与结果。

Roadmap 更新：

- 将 OQ-6 标记为完成。
- 说明实现级别是 planner/transformer parity：计划层 alias operator 已移除，而不是仅隐藏 physical EXPLAIN。

## 9. 实施顺序建议

1. 增加 `plan_output_columns` / `adapt_plan_output` helper，并先覆盖普通 Project adapter 单元测试。
2. 修改 `Relation::Subquery` planning，不再生成 `SubqueryAlias`。
3. 修改 CTE inline `replace_cte_consume`，用 adapter 替代 alias wrapper。
4. 删除 logical/physical alias operator 和 implementation rule。
5. 清理所有 traversal、stats、cost、derive、explain、codegen 分支。
6. 删除 `PruneSubqueryAliasColumns` 及 registry 条目，更新 rule count 测试。
7. 更新 planner 单元测试和 optimizer SQL golden。
8. 跑验证命令，更新 roadmap 进度。
