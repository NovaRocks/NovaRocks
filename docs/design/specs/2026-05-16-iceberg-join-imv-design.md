# Iceberg two-table join IMV design

- 状态：待用户 review
- 日期：2026-05-16
- 范围：Iceberg-backed materialized view target、两表 inner equi-join、projection/filter、两侧 base 可在同一轮 refresh 中同时变化
- 依赖：
  - A1 delta pipeline：`__nr_ivm_delta`、`IcebergDeltaScan`、`IcebergMergeSink`
  - A2/A3 snapshot pin：任意 `[from, to]` change planning 与 `RefreshSnapshotPin`
  - A11 schema / field-id contract
  - 独立 `IcebergMvBackend` refresh lifecycle

## 1. 背景

当前 Iceberg-backed MV 已经支持单 base projection/filter 的 row-lineage IVM：

- base 必须是 Iceberg v3，并启用 `write.row-lineage=true`
- refresh 使用 `RefreshSnapshotPin` 固定本轮 target snapshot
- 增量 scan 通过 `__nr_ivm_delta('cat.ns.tbl', from, to)` 进入标准 analyzer / planner / lower / pipeline
- target apply 依赖隐藏 apply key，把 base `_row_id` 写入 target hidden column
- A11 contract 记录 base field-id lineage 与 target schema 约束

但目前 create / refresh 仍有单 base gate。A2/A3 已经为多 base pin 铺好基础设施，
A1 设计中也明确把 join IVM 作为后续工作。本设计打开第一阶段 join IMV，但只覆盖一个
可闭环的语义子集：两表 inner equi-join projection/filter。

## 2. 目标

1. 支持 Iceberg-backed MV target 上的两表 inner equi-join projection/filter IMV。
2. 同一轮 refresh 中两张 base table 都发生变化时，仍保证增量结果正确。
3. 用两侧 Iceberg row-lineage 生成 composite join apply key，避免 visible 输出重复时删错行。
4. 在 refresh 内新增 delta coalesce 层，合并同一 join row key 的 `+/-` 抵消。
5. 扩展 schema contract，记录两张 base 的 field-id lineage、join condition lineage、filter lineage 与 target hidden join key contract。
6. 保持现有单 base projection/filter MV 行为不变。

## 3. 非目标

- 不支持 managed-lake target MV。
- 不支持 aggregate、window、distinct、rollup、cube、grouping sets。
- 不支持 outer join、semi join、anti join、non-equi join。
- 不支持三表及以上 join。
- 不支持 CTE、subquery、union / intersect / except。
- 不把 full refresh 重新设计纳入本阶段；若当前 Iceberg-backed `REFRESH FULL` 仍禁用，本设计遵循现有错误策略。
- 不引入用户可见 primary-key table 或通用 upsert 语义。

## 4. 支持的 SQL 形态

首版只放行显式两表 inner join：

```sql
CREATE MATERIALIZED VIEW mv
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT <projection>
FROM ice.ns.left_table AS l
JOIN ice.ns.right_table AS r
  ON l.k1 = r.k1 [AND l.k2 = r.k2 ...]
WHERE <deterministic predicate>;
```

约束：

- `FROM` 必须只有一个 `TableWithJoins`，且正好一个 join。
- join type 必须是 `INNER JOIN`。
- join condition 必须是等值谓词的 `AND` 组合。
- 两侧 relation 必须都是 3-part Iceberg table name，可带 alias。
- projection、WHERE、ON 只能使用当前 projection/filter MV 已允许的 deterministic expression 子集。
- 两张 base table 都必须是 Iceberg v3 row-lineage table。
- target storage engine 必须是 `iceberg`。

首版不把 comma join 或 `CROSS JOIN ... WHERE l.k = r.k` 识别成 join IMV，以避免 shape 分类和 filter 下推边界不清。

## 5. 正确性模型

设上次 refresh 后 MV 对应：

```text
L0, R0
```

本次 refresh 开始时由 `RefreshSnapshotPin` 捕获：

```text
L1, R1
```

目标变化量：

```text
DeltaV = L1 join R1 - L0 join R0
```

如果两侧都变化，不能简单跑 `DeltaL join R1` 和 `L1 join DeltaR`，因为会重复计算
`DeltaL join DeltaR`。本设计使用 telescoping 分解：

```text
branch_left  = DeltaL join R0
branch_right = L1 join DeltaR

branch_left + branch_right
  = (L1 - L0) join R0 + L1 join (R1 - R0)
  = L1 join R0 - L0 join R0 + L1 join R1 - L1 join R0
  = L1 join R1 - L0 join R0
```

因此两张表同时变化时，`DeltaL join DeltaR` 只出现一次，不重不漏。

单侧变化是该公式的退化：

- 只有 left 变化：`DeltaL join R0`
- 只有 right 变化：`L1 join DeltaR`
- 都不变化：no-op metadata refresh

## 6. 架构

```text
CREATE MV
  -> analyze + classify JoinProjectionFilter
  -> validate two Iceberg v3 row-lineage bases
  -> create Iceberg target with hidden join apply key
  -> persist multi-base schema contract

REFRESH MV
  -> capture RefreshSnapshotPin { left: L1, right: R1 }
  -> read last_refresh_snapshots { left: L0, right: R0 }
  -> plan per-base IcebergChangeBatch
  -> build telescoping JoinDeltaBranchPlan list
  -> execute branch queries through standard SQL pipeline
  -> coalesce row-level branch deltas by composite join key
  -> write target inserts/deletes through Iceberg commit collector
  -> publish + finalize refresh metadata
```

Key design point：branch query 仍是标准 SQL AST，依赖现有 analyzer / planner / pipeline。
新逻辑只负责生成正确的 branch SQL、保留 hidden lineage 列、在 sink 前 coalesce。

## 7. Shape model

在 `src/connector/starrocks/managed/mv_shape.rs` 中扩展：

```rust
enum IncrementalMvShape {
    ProjectionFilter(ProjectionFilterMvShape),
    Aggregate(AggregateMvShape),
    JoinProjectionFilter(JoinProjectionFilterMvShape),
}

struct JoinProjectionFilterMvShape {
    left_table: ObjectName,
    left_alias: String,
    right_table: ObjectName,
    right_alias: String,
    join_keys: Vec<JoinKeyPairShape>,
}

struct JoinKeyPairShape {
    left_expr: Expr,
    right_expr: Expr,
}
```

首版 `JoinKeyPairShape` 的两边必须最终解析为单列引用。后续可以支持稳定表达式 join key，
但本阶段不引入表达式 key 的 hash / null 语义风险。

`IncrementalMvShape::base_table()` 当前返回单表引用，需要改成不适合 join 的旧 helper，
并新增：

```rust
fn base_tables(&self) -> Vec<&ObjectName>;
```

单 base 路径继续使用旧 helper 或 1-entry wrapper，join 路径使用 `base_tables()`。

## 8. Schema contract

A11 的 `MvSchemaContract` 当前是单 base。Join IMV 需要升级为 multi-base contract。

数据模型：

```text
MvSchemaContract {
  contract_version: 2,
  bases: Vec<BaseContract>,
  output: OutputContract,
  join: Option<JoinContract>,
  target: TargetContract,
}

BaseContract {
  table_fqn: String,
  table_uuid: String,
  alias_at_create: String,
  schema_id_at_create: i32,
  schema_at_create: BaseSchemaSnapshot,
}

JoinContract {
  kind: InnerEquiJoin,
  predicates: Vec<JoinPredicateLineage>,
}

JoinPredicateLineage {
  left: QualifiedFieldLineage,
  right: QualifiedFieldLineage,
}

QualifiedFieldLineage {
  table_fqn: String,
  qualifier_at_create: String,
  field_id: i32,
}

TargetContract {
  visible_columns: Vec<TargetVisibleColumn>,
  hidden_apply_key: HiddenApplyKeyContract,
}

HiddenApplyKeyContract {
  column_name: "__nova_join_row_key",
  target_field_id: i32,
  source: JoinRowKey,
}
```

Lineage 收集必须保留 qualifier。两表有同名列时，`l.id` 和 `r.id` 必须绑定到不同
`BaseContract`。未加 qualifier 且列名在两侧都存在时，CREATE 应 fail fast，而不是猜。

Contract validation：

- 两个 base uuid 都必须匹配。
- 两个 base 都必须仍为 Iceberg v3 row-lineage。
- join condition 引用的 field id 必须存在且 type_signature 不变。
- projection / filter 引用的 field id 必须存在且 type_signature 不变。
- 未引用字段的 add / drop / rename 不阻塞 refresh。
- target hidden apply key 必须存在、field id 匹配、类型稳定。

## 9. Composite join apply key

单 base MV 的 apply key 是 base `_row_id`。Join MV 的一行由两侧 base row 共同决定，
因此 target hidden key 必须是 composite row identity：

```text
__nova_join_row_key = stable_hash(
  left_table_uuid,
  left_row_id,
  right_table_uuid,
  right_row_id
)
```

要求：

- hash 输入包含 table uuid，避免不同 table 的 row id 碰撞。
- hash 算法和编码必须稳定，写入 contract 或以版本号固定在代码中。
- branch query 输出必须保留 `left._row_id` 和 `right._row_id` hidden lineage 列。
- target visible schema 不暴露 `_row_id` 或 `__nova_join_row_key`。

target hidden key 类型固定为 `VARCHAR`，内容为版本化编码：

```text
v1:<hex-128bit-hash>
```

首版优先可诊断性。后续如果需要减少存储，再通过明确 schema migration 迁移到
fixed binary；本阶段不引入双类型兼容。

## 10. Refresh branch planning

Refresh planner 输入：

```text
base_refs = [left, right]
last_refresh_snapshots = { left: L0, right: R0 }
pin = { left: L1, right: R1 }
change_batches = { left: DeltaL, right: DeltaR }
```

输出：

```text
Vec<JoinDeltaBranchPlan>
```

分支定义：

```text
JoinDeltaBranchPlan {
  delta_base: left | right,
  left_snapshot_mode: Delta(from=L0, to=L1) | Snapshot(L0 | L1),
  right_snapshot_mode: Delta(from=R0, to=R1) | Snapshot(R0 | R1),
}
```

规则：

- left changed, right unchanged:
  - `Delta(left L0->L1) join Snapshot(right R0)`
- left unchanged, right changed:
  - `Snapshot(left L1) join Delta(right R0->R1)`
- both changed:
  - `Delta(left L0->L1) join Snapshot(right R0)`
  - `Snapshot(left L1) join Delta(right R0->R1)`

注意第二个 both-changed branch 使用 `left L1`，这是 telescoping 正确性的核心。

每个 base 的 `plan_changes` 错误策略沿用现有 mapping：

- lineage broken / overwrite 等需要 full refresh 的情况：返回 full refresh policy。
- 如果 full refresh 在当前 Iceberg-backed MV 路径仍禁用，则向用户返回明确错误。
- schema evolution incompatible：fail fast。

## 11. Branch query AST rewrite

Branch query 基于 stored canonical MV SELECT AST 构造，不做 SQL 字符串拼接。

对 branch 中 delta side：

```sql
FROM __nr_ivm_delta('ice.ns.left', L0, L1) AS l
```

对 snapshot side：

```sql
JOIN ice.ns.right FOR VERSION AS OF <R0 or R1> AS r
```

同时追加 hidden projection：

```sql
__change_op,
l._row_id AS __nova_left_row_id,
r._row_id AS __nova_right_row_id
```

如果 hidden alias 与用户 visible 输出冲突，CREATE 阶段拒绝。

首版每个 branch 最多一个 `__nr_ivm_delta`。这使现有 `IcebergDeltaScan` 透明列保留逻辑更容易复用，也避免多个 delta source 的 `__change_op` 组合语义不清。

## 12. Delta coalesce

新增 refresh-scoped coalesce 层，固定放在 `src/engine/mv/iceberg_join_coalesce.rs`。
它不是普通查询算子，而是 Iceberg MV refresh 私有 sink：

```text
IcebergJoinCoalesceSinkFactory
  -> branch query sink
  -> shared JoinDeltaCoalescer
  -> flush_to_iceberg_commit_collector(...)
```

refresh driver 为每个 `JoinDeltaBranchPlan` 执行一次 branch query，并给每个 branch
挂同一个 `Arc<JoinDeltaCoalescer>`。branch query 不直接写 target table；它只把
row-level delta 累加到 coalescer。所有 branch 执行成功后，refresh driver 调用
`flush_to_iceberg_commit_collector`，再进入现有 commit / publish / finalize 流程。

职责：

1. 从 branch output chunk 读取 visible columns、`__change_op`、left row id、right row id。
2. 计算 `__nova_join_row_key`。
3. 按 key 累加 `net_change_op`。
4. 保存 net insert payload，用于最终写 target data file。
5. 校验同 key 的 visible payload 一致性。
6. 生成最终 apply stream：
   - `net = 0`：不写。
   - `net = +1`：写 insert/upsert row。
   - `net = -1`：写 delete by join row key。
   - `abs(net) > 1`：fail fast。

对 projection/filter join，正常情况下同一 key 在同一 refresh 内净值只应为 `-1, 0, +1`。
`abs(net) > 1` 表示 branch 规划、row identity 或输入 changelog 有 bug，不能容忍。

Coalesce 首版采用内存 `BTreeMap` / `HashMap`。这是首版两表 join 的显式约束；
如果 delta 规模超过内存预算，本阶段返回明确错误。后续需要大 delta 支持时，再引入
spill 或 partitioned coalesce。

## 13. Target apply and commit

target apply key source 扩展：

```text
ApplyKeySource::BaseRowId   -- 现有单 base projection/filter
ApplyKeySource::JoinRowKey  -- 新 join projection/filter
```

Join target table hidden column：

```text
__nova_join_row_key hidden required string
```

Commit 仍沿用 A7 branch-staged refresh transaction 与现有 Iceberg commit collector：

- insert rows 写 DataFile。
- delete rows 通过 join row key 定位 target rows，写 position deletes / row delta。
- commit unknown / publish / finalize 语义不改变。

`IcebergMergeSink` 的现有单 base 行为保持不变。Join IMV 使用
`IcebergJoinCoalesceSinkFactory`，在 coalesce 完成后复用 target apply / collector
helper，不把多 branch 合并逻辑塞进单 base merge sink。

## 14. Failure semantics

CREATE 阶段 fail fast：

- storage engine 不是 Iceberg。
- 不是两表 inner equi-join projection/filter。
- join key 不是列等值谓词。
- base table 不是 Iceberg v3 row-lineage。
- projection / filter / join condition 出现 ambiguous unqualified column。
- visible output 使用保留 hidden 列名。

REFRESH 阶段 fail fast：

- schema contract 不兼容。
- base table uuid 变化且无法按现有 policy 安全处理。
- coalesce 出现 `abs(net) > 1`。
- 同一 join row key 的 insert payload 不一致。
- target hidden key contract 被破坏。

Full refresh policy：

- 若 change planner 返回 full-refresh-required，而当前 Iceberg-backed full refresh 仍 disabled，则返回现有风格的明确错误。
- 本设计不恢复或重定义 `REFRESH MATERIALIZED VIEW ... FULL`。

## 15. 测试计划

### Rust 单测

`mv_shape`：

- 接受两表 inner equi-join。
- 拒绝 outer / semi / anti join。
- 拒绝 non-equi join。
- 拒绝三表 join。
- 拒绝 aggregate / window / subquery / CTE / union。

`mv_lineage` / contract：

- 两表同名列用 qualifier 正确绑定。
- ambiguous unqualified column 被拒绝。
- join condition field id 进入 contract。
- 未引用列 schema evolution 不阻塞。
- referenced field drop / type change 阻塞。

Branch planner：

- only-left changed -> `DeltaL join R0`。
- only-right changed -> `L1 join DeltaR`。
- both changed -> `DeltaL join R0` + `L1 join DeltaR`。
- no changed -> no-op。

Coalesce：

- `+1` 输出 insert。
- `-1` 输出 delete。
- `+1` 与 `-1` 抵消。
- payload mismatch fail。
- `abs(net) > 1` fail。

### SQL regression

放在 `sql-tests/iceberg-ivm`：

1. 初始 full refresh 后 join MV 与 base join 结果一致。
2. left INSERT 后 incremental refresh 正确。
3. left DELETE / UPDATE 后 incremental refresh 正确。
4. right INSERT 后 incremental refresh 正确。
5. right DELETE / UPDATE 后 incremental refresh 正确。
6. 两边同轮变化，覆盖 `DeltaL join DeltaR`，验证不重不漏。
7. visible output 有重复值，但 composite key 不冲突。
8. 两表同名列，projection / filter / join condition 都使用 alias。

负向 SQL：

- outer join CREATE 失败。
- non-equi join CREATE 失败。
- 三表 join CREATE 失败。
- 非 row-lineage Iceberg base CREATE 失败。

## 16. Implementation slices

建议按以下顺序实现：

1. Shape classifier：新增 `JoinProjectionFilter`，加单测。
2. Multi-base contract：扩展 contract 数据结构与 lineage 收集，保持单 base backward path。
3. CREATE path：Iceberg-backed MV 放行两表 join，创建 target hidden join key。
4. Branch planner：基于 pin / last refresh / change batch 生成 telescoping branches。
5. Branch AST rewrite：delta side 用 `__nr_ivm_delta`，snapshot side 注入 `FOR VERSION AS OF`，追加 hidden lineage projection。
6. Coalesce：新增 `iceberg_join_coalesce.rs`，先内存实现，单测覆盖抵消和异常。
7. Apply integration：新增 `JoinRowKey` apply source，coalescer flush 时接入 Iceberg commit collector。
8. SQL tests：补齐正向与负向 regression。

## 17. Open decisions resolved

- 首版选择支持两表同时变化，使用 telescoping branch，不做单 delta-only fallback。
- 首版只支持两表，不泛化 N 表。
- 首版只支持 Iceberg-backed MV target，不覆盖 managed-lake target。
- Join apply key 使用两侧 table uuid + row id 组合，不依赖 visible output 或用户 primary key。
- Coalesce 是 correctness 必需组件，不作为优化项延后。
