# Iceberg target MV base evolution hardening design

- 状态：待用户 review
- 日期：2026-05-18
- 范围：`storage_engine='iceberg'` 的 Iceberg MV target；base table schema evolution；base table partition evolution；single-base aggregate；two-base join aggregate
- 非范围：managed-lake target；target partition-pruned touched-group lookup；target repartition；`MIN/MAX` 的 partition full recompute fallback

## 1. 背景

Iceberg target MV 已经具备 projection/filter、join projection/filter、single-base aggregate、two-base join aggregate 的增量刷新能力。A11 也已经为 Iceberg target MV 引入 schema / field-id contract，用于在 refresh 前判断 base/target schema drift 是否仍然安全。

当前实现仍有一个明显缺口：contract 与 rebind 能力最早为 single-base projection/filter 设计，后续 aggregate / join aggregate 复用了部分 guard，但没有把 base schema evolution 与 base partition evolution 的支持矩阵完整扩展到新的 MV 形态。

典型风险包括：

- aggregate group key rename 后，stored SELECT 的 `GROUP BY` 仍引用旧列名；
- aggregate input rename 后，部分 aggregate expression 可以被 projection rewrite 覆盖，但 `HAVING` / `ORDER BY` / group expression 不完整；
- join aggregate 中 join key、group key、measure input 可能来自不同 base，当前 join contract 对 referenced rename 直接 fail fast；
- base table partition spec evolution 不是 MV schema contract 的一部分，但缺少明确 SQL 覆盖来证明 aggregate / join aggregate refresh 对它透明。

本任务是 A11 的后续 hardening：把 Iceberg target aggregate / join aggregate 的 base evolution 行为从“部分复用 projection/filter contract”升级为显式支持矩阵，并把缺失的 rebind 能力和 regression coverage 一起补齐。

## 2. 目标

1. 明确 Iceberg target MV 在 base schema evolution 下的支持矩阵，覆盖 projection/filter、join projection/filter、single-base aggregate、join aggregate。
2. 支持 aggregate / join aggregate 中 referenced base column rename 后继续 refresh，前提是 Iceberg field id、type、nullability、base table identity 都保持兼容。
3. 扩展 rebind rewrite，使 stored SELECT 中的 projection、WHERE、JOIN ON、GROUP BY、HAVING、ORDER BY、aggregate arguments 都能按当前 base column name 重写。
4. 对 base table partition evolution 建立 MV 层语义：不把 base partition spec 写入 MV contract；只要 schema contract 兼容，refresh 应继续正确。
5. 保持 target partition contract 的 fail-fast 语义。target Iceberg MV table 的 partition spec 被外部改动时仍然报错，不自动修复。

## 3. 非目标

- 不给 managed-lake target 增加任何支持。
- 不实现 partition-pruned touched-group state lookup；该优化属于独立 P1 后续任务。
- 不支持 target repartition。未来如果要做，应该通过显式 `ALTER MATERIALIZED VIEW ... REPARTITION` 或重建流程重新生成 target contract。
- 不把 unsafe base schema drift 自动降级成 full refresh。Iceberg-backed full refresh 当前不是这个任务的恢复策略。
- 不放宽 join 形态限制。仍然只覆盖已经支持的 two-table inner equi-join aggregate。
- 不为 `MIN/MAX` 引入 delete-bearing refresh fallback。

## 4. 当前实现结论

代码路径：

- `src/engine/mv/schema_contract.rs`
  - `validate_schema_contract` 检查 base/target UUID、format v3、row-lineage、target partition contract、referenced base field id、target visible/state columns。
  - single-base `check_base_referenced_fields` 当前检查 field id 存在与 type signature，不检查 `required` nullability drift。
  - single-base referenced rename 会返回 `CompatibleSafeWithRebind`。
- `src/engine/mv/iceberg_refresh.rs`
  - single-base projection/filter 与 aggregate refresh 会在 `CompatibleSafeWithRebind` 时调用 `rewrite_select_sql_for_rebind`。
  - `rewrite_select_sql_for_rebind` 当前只重写 projection 与 WHERE；注释仍标明 A11 phase 1 不覆盖 GROUP BY / HAVING / ORDER BY。
  - `validate_join_schema_contract` 对 join-family MV 的 referenced base rename 直接报错；它不会返回 rebind decision。
- `src/meta/repository/mv_contract.rs`
  - target partition contract 已持久化并在 refresh guard 中校验。
  - 没有 base partition contract，这符合本任务目标：base partition spec 不应成为 MV 语义 identity。

## 5. 支持矩阵

### 5.1 Base schema evolution

| Base 变化 | Projection/filter | Join projection/filter | Single aggregate | Join aggregate | 处理 |
| --- | --- | --- | --- | --- | --- |
| add unreferenced column | 支持 | 支持 | 支持 | 支持 | contract 不引用该 field id |
| drop unreferenced column | 支持 | 支持 | 支持 | 支持 | contract 不引用该 field id |
| rename unreferenced column | 支持 | 支持 | 支持 | 支持 | contract 不引用该 field id |
| reorder columns | 支持 | 支持 | 支持 | 支持 | 以 Iceberg field id 为准 |
| rename referenced projection column | 已支持 | 本任务补齐 | 不适用 | 不适用 | rebind stored SELECT |
| rename referenced group key | 不适用 | 不适用 | 本任务补齐 | 本任务补齐 | rebind GROUP BY 与 projection |
| rename referenced aggregate input | 不适用 | 不适用 | 本任务补齐 | 本任务补齐 | rebind aggregate arguments |
| rename referenced join key | 不适用 | 本任务补齐 | 不适用 | 本任务补齐 | rebind JOIN ON |
| drop referenced column | fail fast | fail fast | fail fast | fail fast | field id missing |
| type change referenced column | fail fast | fail fast | fail fast | fail fast | type signature drift |
| nullability change referenced column | 本任务补齐 fail fast | 已 fail fast | 本任务补齐 fail fast | 已 fail fast | 统一 contract 语义 |
| drop + add same name | fail fast | fail fast | fail fast | fail fast | old field id missing |
| base table replaced / UUID changed | fail fast | fail fast | fail fast | fail fast | table identity drift |

Nullability drift 第一阶段按 unsafe 处理。原因是 `COUNT(expr)`、filter predicate、join predicate、target output nullable contract 都可能受影响；如果未来要放宽 required -> optional，需要单独证明表达式语义、target schema 与旧数据兼容。

### 5.2 Base partition evolution

| Base partition 变化 | Single aggregate | Join aggregate | 处理 |
| --- | --- | --- | --- |
| unpartitioned -> partitioned | 支持，需 SQL 覆盖 | 支持，需 SQL 覆盖 | schema contract 兼容时继续 refresh |
| partitioned -> evolved spec | 支持，需 SQL 覆盖 | 支持，需 SQL 覆盖 | row-lineage/change scan 读取多 spec 文件 |
| drop partition field / replace transform | 支持，需 SQL 覆盖 | 支持，需 SQL 覆盖 | 不写入 MV contract |
| target MV partition spec drift | fail fast | fail fast | target contract guard |

Base partition spec 不进入 MV contract。MV 的语义依赖 base table identity、row-lineage、referenced field id 与 snapshot range；base 数据文件属于哪个 partition spec 是 Iceberg scan/change planning 的输入，不应让 MV refresh 因 `metadata.specs().len() > 1` 这种条件直接失败。

如果底层 Iceberg change planning 发现某类文件变化仍不支持，例如不满足 row-lineage 的 rewrite 或删除语义，应继续通过现有 change planning 错误 fail fast。这个错误属于 change source unsupported，不属于 partition evolution 本身 unsupported。

## 6. 设计

### 6.1 Contract decision 扩展

把当前 single-base 的 `(field_id, old_name, current_name)` rebind 信息升级为可表达 multi-base 的结构：

```text
RebindColumn {
  base_table_fqn: String,
  field_id: i32,
  name_at_create: String,
  current_name: String,
}
```

single-base validator 可以继续返回一个 base 的 `RebindColumn`。join-family validator 需要从 `contract.bases` 逐个 base 生成 rebind plan，而不是遇到 rename 直接报错。

判断规则：

- field id missing、type drift、nullability drift、UUID drift、row-lineage drift：`Incompatible`。
- 只有 name drift：`CompatibleSafeWithRebind`。
- 没有 drift：`CompatibleSafe`。

### 6.2 Single-base nullability guard

`check_base_referenced_fields` 需要补齐 `field.required != record.required` 检查，并返回新的 `BaseFieldNullabilityChanged` 错误。

这会让 single-base projection/filter 和 aggregate 与当前 join-family guard 保持一致。错误信息应保持 fail-fast 风格，提示 recreate 或 rebuild，而不是尝试 loose cast。

### 6.3 Join-family schema validator

`validate_join_schema_contract` 当前只返回 `Result<(), String>`，不携带 rebind decision。它需要变成 join-family 专用 decision：

```text
JoinContractDecision {
  CompatibleSafe,
  CompatibleSafeWithRebind(Vec<RebindColumn>),
  Incompatible(String),
}
```

调用方包括 join projection/filter 与 join aggregate refresh。这样 join key rename、dim-side group key rename、fact-side measure rename 都走同一条 rebind 路径。

### 6.4 Rebind rewrite 范围

重写 helper 要从“projection/filter only”升级为 SELECT AST 级别的 column-reference rewrite：

- projection expressions；
- WHERE；
- JOIN ON；
- GROUP BY expressions；
- HAVING；
- ORDER BY expressions；
- aggregate function arguments；
- CASE / CAST / binary op / unary op / nested expression / IN / BETWEEN / LIKE 等已有表达式递归。

重写规则：

1. 不重写 table name、catalog name、database name、alias name、string literal。
2. `Identifier`：
   - single-base SELECT 可以按 old column name 匹配；
   - join SELECT 如果存在同名字段歧义，不能盲改，必须依赖 qualifier 或返回清晰错误。
3. `CompoundIdentifier`：
   - 只重写最后一段 column name；
   - 前缀必须能匹配 base table name 或 SQL alias。
4. 输出 alias 不参与重写。例如 `SUM(f.amount) AS total_amount` 中只改 `f.amount`，不改 `total_amount`。

为了支持 join-family 安全重写，需要在 refresh 时构建 alias 到 base table 的映射。来源是 stored SELECT AST 的 `FROM` / `JOIN` table factors 与 `contract.bases`。如果无法把 qualifier 解析到唯一 base，rebind 应 fail fast，而不是按列名全局替换。

### 6.5 Refresh 接入点

所有 Iceberg target refresh 在进入 shape-specific planning 前先得到 effective definition：

```text
stored mv definition
  -> validate schema contract
  -> optional rebind stored select sql
  -> classify / plan / execute refresh
```

single-base aggregate 必须在 aggregate state-shaped rewrite 前完成 rebind。join aggregate 必须在 telescoping branch rewrite 前完成 rebind，确保 `DeltaL JOIN R0` 和 `L1 JOIN DeltaR` 两条 branch 都基于当前 base column name。

### 6.6 Base partition evolution

不要新增 base partition guard。需要新增的是 regression coverage 和必要的 bug fix：

- CREATE MV 时不记录 base partition spec。
- REFRESH 时不因为 base metadata 出现多个 partition spec 失败。
- change planning、snapshot pin、standalone query prep 要能读取同一个 snapshot range 内不同 spec 的 data files。
- target partition contract 仍只校验 target MV table 本身。

如果测试暴露某个 scan/change planner 仍隐式假设 single partition spec，应在该 planner 内修复，使它按 Iceberg data file 的 `partition_spec_id` 使用对应 spec。

## 7. 测试计划

### 7.1 Rust unit tests

在 `src/engine/mv/iceberg_refresh.rs` 或拆出的 rebind 模块中补充：

- rewrites GROUP BY referenced column rename；
- rewrites aggregate function argument rename；
- rewrites JOIN ON qualified column rename；
- rewrites HAVING / ORDER BY column references；
- does not rewrite output alias；
- does not rewrite string literal；
- fails on ambiguous unqualified join column rebind。

在 `src/engine/mv/schema_contract.rs` 中补充：

- single-base referenced nullability change is incompatible；
- join-family referenced rename returns rebind decision instead of incompatible；
- target partition spec drift remains incompatible。

### 7.2 SQL regression tests

新增或扩展 `sql-tests/iceberg-ivm/sql`：

1. `iceberg_ivm_aggregate_a11_base_rename_group_key.sql`
   - `GROUP BY region`；
   - rename `region` -> `area`；
   - insert/update/delete 后 refresh；
   - MV result 与 rewritten base query 对齐。
2. `iceberg_ivm_aggregate_a11_base_rename_measure.sql`
   - `SUM(amount)` / `COUNT(amount)`；
   - rename `amount` -> `gross_amount`；
   - delete-bearing refresh 验证 state merge。
3. `iceberg_ivm_join_aggregate_a11_base_rename_join_key.sql`
   - fact side join key rename；
   - fact insert/delete refresh；
   - join aggregate result 对齐。
4. `iceberg_ivm_join_aggregate_a11_base_rename_group_key.sql`
   - dim side group key rename；
   - dim update 导致 group move；
   - old group 与 new group 都正确。
5. `iceberg_ivm_aggregate_a11_base_nullability_change_referenced.sql`
   - referenced column nullability drift；
   - refresh fail fast。
6. `iceberg_ivm_aggregate_base_partition_evolution.sql`
   - base 从 unpartitioned 或旧 spec 演进到新 spec；
   - evolution 前后都有 data files；
   - refresh 后 aggregate MV 正确。
7. `iceberg_ivm_join_aggregate_base_partition_evolution.sql`
   - fact 或 dim 一侧发生 partition evolution；
   - refresh 后 join aggregate MV 正确。

已有 projection/filter A11 SQL cases 作为 baseline，不需要复制成同构矩阵；本任务只补充 aggregate / join aggregate 的新风险点。

## 8. 成功标准

1. Iceberg target single aggregate 和 join aggregate 在 referenced column rename 后能继续 incremental refresh，且结果与 base query 对齐。
2. Referenced drop/type/nullability drift 都 fail fast，不产生错误结果或 silent fallback。
3. Base partition evolution 不再作为 MV-level unsupported 条件；schema contract 兼容时 refresh 正确。
4. Target partition spec drift 仍 fail fast。
5. 所有新增 SQL cases 在 `iceberg-ivm` suite verify 模式通过。
6. 现有 A11 projection/filter schema evolution cases 不回退。

## 9. 风险与处理

1. **join SELECT 中 unqualified column 可能歧义**
   - 处理：multi-base rebind 只在 qualifier 可唯一绑定时自动改写；歧义场景 fail fast。
2. **AST string rewrite 覆盖不了所有表达式变体**
   - 处理：先覆盖当前 MV shape 允许的表达式；unsupported variant 保持 analyzer 报错，不做文本替换。
3. **base partition evolution 测试暴露 scan planner 隐式 single-spec 假设**
   - 处理：在 scan/change planner 按 data file `partition_spec_id` 取 spec；不把问题绕成 MV guard。
4. **nullability guard 变严格可能影响旧测试**
   - 处理：这是 intentional fail-fast；如有已有 case 依赖该行为，应更新期望。

## 10. 与后续任务关系

本任务只解决 correctness 与 contract/rebind 完整性。

Partition-aware + touched-group state lookup 是性能优化，应在本任务之后继续作为独立 P1 后续任务推进。它可以利用 target partition contract 做 old-state scan pruning，但不改变本任务定义的 base partition evolution 语义。
