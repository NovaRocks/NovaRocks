# Iceberg IMV UNION ALL Delta Rewrite 设计

日期：2026-06-02

来源任务（NovaRocks Roadmap · Iceberg v3 Incremental MV Roadmap）：
- 任务 8：[UNION ALL delta rewrite](union-all-delta-rewrite.md) — aggregate over UNION ALL 多 base fan-in。
- 任务 9：[UNION ALL of aggregate branches](union-all-aggregate-branches.md) — 保留 branch 边界的 aggregate branch union。
- 关联：[UNION ALL multi-base aggregate IMV](union-all-multi-base.md)、
  umbrella `2026-05-28-iceberg-imv-rewrite-cutover-umbrella-design.md` 的阶段 6
  （UNION ALL family + contract hardening）。

本设计把任务 8、任务 9 合并为**一个 PR**，并在用户决策下**额外纳入 projection/filter UNION ALL**
（无聚合的多 base fan-in）。它对应 umbrella 阶段 6 的执行落地。

---

## 1. 目标

让以下三种 `UNION ALL` 形态的 Iceberg 增量 MV refresh 由 IMV logical rewrite 产出可执行 outcome，
并由现有 refresh executor 消费：

1. **aggregate over UNION ALL**（任务 8，A 族）：`Aggregate(Union(b₁..bₙ))`，同 group key 跨分支**合并**。
2. **UNION ALL of aggregate branches**（任务 9，B 族）：`Union(Aggregate(b₁)..Aggregate(bₙ))`，分支**独立**（bag semantics）。
3. **projection/filter UNION ALL**（扩展，B 族）：`Union(Proj/Filt(scan)..)`，多 base fan-in，分支**独立**。

增量 refresh 结果必须与 plain query（全量重算）等价。

## 2. 非目标

- 不支持 `UNION` / `INTERSECT` / `EXCEPT`（去重 / 交 / 差）增量 rewrite。
- 不支持 mixed-shape 分支（同一 union 内分支必须全 aggregate 或全 projection/filter）。
- 不支持分支内部嵌 join（v1 fail-fast，留作后续扩展）。
- 不支持隐式 cast；分支 arity / 类型 / nullability 不一致直接 fail-fast。
- 不支持 partitioned target MV（v1 仅 unpartitioned；partition 优化归任务 11）。
- 不改变普通 Iceberg table scan、FE-compatible backend mode 的语义。
- 不新增当前未支持的 aggregate function。
- 不对现存（非 union）MV 做任何 metadata 迁移。

## 3. 范围决策（已与用户确认）

| 决策点 | 选择 | 含义 |
| --- | --- | --- |
| Union 范围 | 任务 8 + 任务 9 + **projection/filter union** | 三种 shape 一个 PR |
| Partition 处理 | **仅 unpartitioned target** | partitioned target fail-fast；partition 优化留任务 11 |
| 任务 9 branch 身份 | **独立隐藏 `__branch_id__` 列 + 扩展 contract** | 显式列，非复用 `__row_id__` 复合编码 |

## 4. 当前代码上下文（复用基础）

IMV logical rewrite 位于 `src/sql/optimizer/rewrite/imv/`，pipeline 10 个 stage 顺序固定
（`pipeline.rs`）：logical-normalize → delta-marker → **join-delta** → **aggregate-state** →
**delta-pushdown** → scan-binding → action-propagation → apply-key → marker-cleanup → validation。

关键既有机制（本设计直接复用）：

- **join-delta 已经在内部造 UnionAll**：`RewriteJoinAggregateDeltaRule`（`join_delta.rs:117`）把
  `Delta(Aggregate(Join))` 改写成 `Delta(Aggregate(Union(join branches)))`，再交给 aggregate-state。
  其 `mark_delta_scan` / `mark_version_scan` / `normalize_branch_output` 是 union 分支处理的现成模板。
- **aggregate-state 能识别"输入 union 已带 marker"**：`signed_aggregate`（`aggregate_rewrite.rs:494`）用
  `plan_contains_imv_marker(&aggregate.input)` 判断，若 union 分支已被 delta/version 标记就保留 union、
  不再外包 `ImvDelta`。单测 `rewrite_aggregate_state_preserves_pre_expanded_join_delta_input`
  （`aggregate_rewrite.rs:1492`）锁定此行为。**这是 A 族能零成本复用 aggregate-state 的关键。**
- **union 当前全 fail-fast**：`PushDeltaThroughUnaryRule`（`delta_pushdown.rs:66`）、
  `PropagateActionColumnRule`（`action_propagation.rs:296`，报错文案 "scheduled for Phase 6"）、
  `ActionColumnValidationRule`（`action_column.rs:167`）都对 general union 报错；
  仅 `is_supported_join_delta_union`（`action_propagation.rs:306`）放行 join-delta union。
- **apply key 现有三种来源**：`ApplyKeySource::{BaseRowId(__nova_base_row_id), JoinRowKey(__nova_join_row_key),
  GroupRowId(__row_id__)}`（`mv_contract.rs:188`）。`HiddenApplyKeyContract` self-check 校验列名与 source
  匹配（`mv_contract.rs:408`）。
- **apply 侧泛化钩子**：`locate_target_rows_by_string_apply_key` 接受可配 `apply_key_column`
  （`iceberg_target_apply.rs:224`）；`IcebergMvTargetStateRowFilter::DeltaInputRowIds`
  把 target-state 读限定在 delta row ids；`IcebergMvTargetStatePartitionConstraint::Unpartitioned`
  已存在（`aggregate_rewrite.rs:106`）。
- **多 base snapshot pin**：`RefreshSnapshotPin`（`src/connector/starrocks/table/refresh_pin.rs`）已用
  `BTreeMap<fqn, snapshot/uuid>` 支持多 base（join 路径已在用）。
- **shape classifier**：`IncrementalMvShape::{ProjectionFilter, Aggregate, JoinProjectionFilter,
  JoinAggregate}`（`src/connector/starrocks/table/mv_shape.rs`）。各 `classify_*` 在 `SetExpr::Select`
  处一刀切拒绝 set op。
- **contract 多 base 已部分支持**：`MvSchemaContract { base, bases: Vec<BaseContract>, join, aggregate,
  target }`（`mv_contract.rs:10`）。

代码中目前**没有任何** `branch_id` / `__branch_id__` 概念，是全新引入。

## 5. 核心概念：两个 union family + `__branch_id__`

按 **"union 在 aggregate 之下还是之上"** 分族，这是整个设计的骨架。

| Family | 形态 | plan 形状 | 分支语义 | apply key |
| --- | --- | --- | --- | --- |
| **A（union 在下）** | 任务 8 | `Aggregate(Union(b₁..bₙ))` | 同 group key 跨分支**合并** | 现有 `group_row_id`（`__row_id__`），**不需要** branch 身份 |
| **B（union 在上）** | 任务 9 | `Union(Aggregate(b₁)..Aggregate(bₙ))` | 分支**独立**（bag） | `(__branch_id__, group_row_id)` |
| **B（union 在上）** | projection union | `Union(Proj/Filt(scan)..)` | 分支**独立** | `(__branch_id__, base_row_id)` |

### 5.1 `__branch_id__` 原理

`__branch_id__` 是给**顶层 `UNION ALL` 的每个分支**打的稳定编号（0,1,2…），唯一作用是在增量 refresh
时精确定位"目标表里这一行属于哪个分支"，避免**跨分支、同 group key（或同 base row id）的行互相覆盖**。
它解决 `UNION ALL` 的 bag semantics 与增量 refresh 的行定位（apply key）之间的冲突。

**为什么需要（任务 9 例子）**：

```sql
CREATE MATERIALIZED VIEW mv AS
  SELECT k, SUM(v) AS s FROM t1 GROUP BY k     -- 分支 L (branch_id=0)
  UNION ALL
  SELECT k, SUM(v) AS s FROM t2 GROUP BY k;    -- 分支 R (branch_id=1)
```

`UNION ALL` 不去重，结果可能出现两行同 k=1（一行来自 L、一行来自 R），它们绝不能合并。
聚合 MV 现有 apply key 是 `group_row_id = rid("k")`，只按 group key 编码：

```
L 的 k=1 行  →  apply key = rid("k=1")
R 的 k=1 行  →  apply key = rid("k=1")    ← 撞了！增量 refresh 无法区分 → 互相覆盖（bug）
```

引入 `__branch_id__` 后 apply key 变复合：

```
L 的 k=1 行  →  (0, rid("k=1"))
R 的 k=1 行  →  (1, rid("k=1"))           ← 隔离开
```

### 5.2 数据流转（一次增量 refresh）

初始物化后目标 Iceberg 表（`__branch_id__`、`__row_id__` 隐藏，`k`/`s` 可见，`state`/`cnt` 为聚合 detail-state，简化）：

```
__branch_id__ | __row_id__  | k | s   | state     | cnt
--------------+-------------+---+-----+-----------+----
      0       | rid("k=1")  | 1 | 30  | {sum:30}  | 2     ← L 的 k=1
      0       | rid("k=2")  | 2 | 5   | {sum:5}   | 1
      1       | rid("k=1")  | 1 | 100 | {sum:100} | 1     ← R 的 k=1
      1       | rid("k=3")  | 3 | 7   | {sum:7}   | 1
```

变更 `INSERT INTO t2 VALUES (1, 50);`（仅分支 R 的 base 变化）：

1. **抓 pin**：t1 snapshot 不变 → 分支 L 空 delta；t2 新 snapshot → 分支 R delta = 新增 (k=1,v=50)，action=+1。
2. **rewrite**：为分支 R 生成 branch-scoped `AggregateStateMerge`，delta 聚合得 `(__branch_id__=1, k=1, signed_state(+50))`，apply key `(1, rid("k=1"))`。
3. **target-state lookup（按 `__branch_id__=1` 限定）**：仅命中 `(1, rid("k=1"), s=100, cnt=1)`，**碰不到** L 的 `(0, rid("k=1"))`。← `__branch_id__` 在此发挥隔离作用。
4. **合并 state**：`{sum:100,cnt:1} + signed{+50} → {sum:150,cnt:2}`。
5. **写回**：按 `(1, rid("k=1"))` 替换该行 → s=150；L 的 k=1=30 完好。

最终 MV：`(1,30) (2,5) (1,150) (3,7)`，两行 k=1 各自正确。无 `__branch_id__` 时 Step 3 会同时命中两行 → 错误覆盖。

### 5.3 任务 8 为何不需要 `__branch_id__`

```sql
SELECT k, SUM(v) AS s FROM (SELECT k,v FROM t1 UNION ALL SELECT k,v FROM t2) GROUP BY k;
```

union 在内、group by 在外：所有 k=1 进**同一 group**，每个 k 只对应一行 → `group_row_id=rid("k")` 本身唯一，
不存在跨分支撞 key。分支只是喂给同一聚合的数据源。**A 族不引入 `__branch_id__`。**

一句话区分：**union 在聚合之上（结果分开）→ 要 `__branch_id__`；union 在聚合之下（结果合并）→ 不要。**

### 5.4 branch_id 稳定性

branch_id = 嵌套 `UNION ALL` flatten 后**左→右序号**（对应任务 9 "normalized UNION ALL tree path"）。
MV 定义 SQL 不变则分支顺序不变 → branch_id 稳定。写入 contract，保证 recreate 之外稳定、不 silent migrate。

## 6. Rewrite 设计

**Approach 选择**：新增**专用 rule** 镜像 join-delta 的成熟模式，而非把 union 逻辑塞进现有
pushdown / aggregate-state。理由：join-delta 已证明"专用 rule 在 structural 阶段消化多输入 →
后续 stage 复用 aggregate-state"可行；专用 rule match 条件清晰、失败定位精确、能直接复用现成 helper。

### 6.1 A 族 rule（任务 8）：`Delta(Aggregate(Union(children)))`

- 位置：structural 阶段，join-delta 之后、aggregate-state 之前（新 stage 或并入 join-delta stage）。
- match：`Delta[root](Aggregate(Union))` **且 `!plan_contains_imv_marker(union)`** —— 此 marker guard 把
  "源 union" 与 "join-delta 产出的（分支已带 marker 的）union" 区分开，防止重复处理。
- rewrite：对每个 union child 用 `mark_delta_scan(child, X)` 把 Delta 推到叶 scan，union output 追加共享
  action column X → `Delta[root,X](Aggregate(Union(Δb₁..Δbₙ)))`。公式 `Delta(UNION ALL)=UNION ALL(Delta(child))`，
  比 join 的 delta×version 笛卡尔展开简单。
- 之后 aggregate-state（已有）检测 union 已带 marker → 保留 union、生成 signed aggregate + `AggregateStateMerge`。
  group key 跨分支合并 = 现有行为。**零 apply-key 改动。**

### 6.2 B 族 rule（任务 9 + projection union）：`Delta(Union(branches))`

- 位置：structural 阶段，pushdown 之前（pushdown 对 `Delta(Union)` fail-fast）。
- match：`Delta[root](Union)` 且各分支同类（全 aggregate / 全 projection-filter）。
- 通用步骤：flatten 嵌套 union → N 分支；赋稳定 branch_id = 0..N-1；每分支注入常量隐藏列 `__branch_id__ = i`；
  N 个改写后的分支接成顶层 `Union`。
- **aggregate 分支（任务 9）**：每分支构造 **branch-scoped `AggregateStateMerge`**——把 `aggregate_rewrite.rs`
  的内部构造逻辑抽成参数化 helper（接收 branch_id）：target-state scan 按 `__branch_id__=i` AND delta row-ids
  限定；输出注入 `__branch_id__=i`；apply key `(__branch_id__, group_row_id)`。
- **projection/filter 分支**：每分支把 Delta 推到叶 scan（复用 pushdown），注入 `__branch_id__=i`，
  apply key `(__branch_id__, base_row_id)`。`(branch_id, base_row_id)` 组合唯一（即使同一 base 出现在多分支）。

### 6.3 rule 交互（已验证无冲突）

三个 top-level match 在原始 plan 上互斥：`Delta(Union)`（B 族）/ `Delta(Agg(Union))`（A 族）/
`Delta(Agg(Join))`（join-delta）。各自产物也不会被其它 rule 误匹配：

- A 族产物 `Delta(Agg(Union(marked)))`：B 族不匹配（input 是 Agg 非 Union）。
- B 族产物 `Union(AggregateStateMerge..)`：无 root Delta，三 rule 皆不匹配。
- join-delta 产物 `Delta(Agg(Union(marked)))`：A 族 marker guard 拒绝（union 已带 marker）。

### 6.4 pushdown 兜底

A/B 两族 rule 都在 pushdown stage 之前消化 union，到 pushdown 时 union 下只剩叶 scan marker。
任何"未被 union rule 处理就漏到 pushdown 的 union"仍 fail-fast。

## 7. 支撑层

### 7.1 shape classifier + dispatch（`mv_shape.rs`、`iceberg_refresh.rs`）

- **A 族**：扩展 `classify_aggregate_mv_query` 识别 "FROM (UNION ALL of base selects)" 派生表，归类为多 base
  aggregate；逻辑 plan `Aggregate(Union(...))`，走现有 aggregate refresh 路径，pin 多 base。
- **B 族**：新增 `classify_union_all_mv_query`：flatten 嵌套 union、逐分支分类、校验分支兼容，产出新 enum
  值 `IncrementalMvShape::UnionAll { branches, branch_kind }`。
- 分类期硬校验：仅 `UNION ALL`；分支 kind 一致；分支 arity/类型/nullability 严格一致（无隐式 cast）；
  分支内单 base（嵌 join fail-fast）。
- dispatch：A 族复用 aggregate 路径；B 族新增 union 路径，调用 IMV rewrite 后按分支 apply。
- CREATE MV 期同样走 classifier，非法 union 在 CREATE 即报错（对齐 `iceberg_ivm_join_reject_unsupported.sql` 约定）。

### 7.2 contract 记录 branch（`mv_contract.rs`）

给 `MvSchemaContract` 加**可选**字段（`#[serde(default)] = None`），仅 B 族 union MV 填充，
既有非 union MV contract 反序列化不变、**无需迁移现存 MV**：

```text
branch: Option<BranchUnionContract>

BranchUnionContract {
  branch_id_column: { column_name: "__branch_id__", target_field_id },
  branch_count: u32,
  inner_apply_key_source: ApplyKeySource,   // GroupRowId(任务9) / BaseRowId(projection union)
  branches: Vec<BranchEntry>,               // 每分支 base lineage + shape，供 validation/稳定性
}
```

- apply key 复合 `(__branch_id__, <inner_apply_key_source 列>)`；`HiddenApplyKeyContract.source` 继续描述内层 key，
  `branch.is_some()` 标记复合。
- 扩展 self-check（`mv_contract.rs:408`）：`branch` 存在时校验 `__branch_id__` 列存在、内层 key 列与
  `inner_apply_key_source` 匹配。
- `contract_version`：靠 `serde(default)` 向后兼容；可仅为新 union MV 选择性提升，老 MV 不受影响。

### 7.3 目标表创建：注入 `__branch_id__`

CREATE B 族 union MV 时目标 Iceberg schema 追加隐藏列 `__branch_id__`（`Int32`，required），分配 field id，
写入 contract。A 族不加（沿用现有聚合 target 布局）。

### 7.4 action 传播 / validation 放行（`action_propagation.rs`、`action_column.rs`）

把 union 识别从单一 `is_supported_join_delta_union` 泛化成一组 IMV 认可的 union 形状，其余仍 fail-fast：

1. join-delta union（已有，保持）；
2. **fan-in delta union**（A 族）：每分支是 delta-scan 上的 normalized projection，共享同一 action column；
3. **top branch union**（B 族）：分支是 `AggregateStateMerge`（任务 9）或带 `__branch_id__` 的 delta-projection（projection union）。

`PropagateActionColumnRule` 与 `ActionColumnValidationRule` 共用此 matcher。不支持时报错仍带 base FQN
（`first_delta_base_fqn` 已能穿过 Union）。

### 7.5 apply 侧（`iceberg_target_apply.rs`、`iceberg_aggregate_state.rs`）

- **复合 apply-key 定位**：扩展 `locate_target_rows_by_string_apply_key` 支持 branch 约束的复合定位
  （`__branch_id__=b AND <inner_key_col> IN (...)`）。
- **target-state scan 按 branch 限定**：给 `IcebergMvTargetStateRowFilter` 加可选 `branch_id`，分支 i 只读
  `__branch_id__=i` 行。`IcebergMvTargetStatePartitionConstraint::Unpartitioned` 已存在，A/B 族都走它。
- **per-branch merge**：每分支独立 `AggregateStateMerge` 且 target-state 已按 branch 限定，
  `merge_aggregate_target_state` 合并逻辑几乎不变（分支内合并与单 base aggregate 同形）。projection union
  分支复用 PF cutover row 定位，key 多 branch 前缀。

## 8. RefreshSnapshotPin / 多 base / empty delta

- 每分支 base 独立 pin（`RefreshSnapshotPin` 多 base map 已支持）。
- 某分支 base 无 delta → 该分支空 delta（**不**回退 current snapshot），其它分支继续。
- **全部**分支空 delta → metadata-only refresh，不产生 target commit。
- per-base UUID drift / previous snapshot 缺失 → 复用现有 fail-fast。

## 9. 错误处理 / fail-fast（无 silent fallback）

- 非 ALL set op、mixed-shape 分支、分支 arity/类型/nullability 不一致、partitioned target（v1） → CREATE/classify 期显式报错。
- per-base pin 缺失 / UUID drift → 复用现有 fail-fast。
- union 形状漏到 pushdown/validation → fail-fast（现有 tripwire 泛化后保留）。
- contract drift（`__branch_id__` 缺失 / branch_count 不符）→ fail-fast。
- B 族无 legacy path（对齐 umbrella 阶段表），不存在隐式 full refresh。

## 10. 测试矩阵

**Rust 单测**（各 rewrite/contract 模块）：
- A 族 rule 展开各分支 + marker guard 阻止误匹配 join-delta 产物。
- B 族 rule：`Union(Aggregate..)` → `Union(branch-scoped AggregateStateMerge)`；branch_id 稳定赋值；嵌套 flatten。
- B 族 projection：`Union(Project..)` → 带 `__branch_id__` 的 delta projection union。
- action 传播 / validation 接受三种新 union、拒绝其它。
- classifier：放行 `UNION ALL`、flatten、拒 `UNION`/`INTERSECT`/`EXCEPT`、拒 mixed shape、拒类型不一致。
- contract self-check（带 branch）；复合 apply-key 定位；target-state 按 branch 限定。

**SQL fixtures**（`sql-tests/iceberg-ivm/sql/`，遵循 `@sequential` / `@order_sensitive` / `@skip_result_check` /
`@explain_contains` / `@expect_error`，每 case 用"重算 base 查询"交叉校验）：
- `iceberg_ivm_union_all_aggregate_basic.sql` — 任务 8：2 分支 agg over union，INSERT+DELETE retract，== plain 重算。
- `iceberg_ivm_union_all_aggregate_three_branch.sql` — 任务 8：3 分支 + 嵌套 flatten。
- `iceberg_ivm_union_of_aggregates_basic.sql` — **任务 9 头号正确性 case：同 group key 跨 branch 不合并**；删某分支 group 不影响另一分支同 key 行。
- `iceberg_ivm_union_of_aggregates_branch_empty.sql` — 某分支空 delta，其它继续。
- `iceberg_ivm_union_projection_basic.sql` — projection union 多 base，`(branch_id, base_row_id)` 身份，INSERT/DELETE。
- `iceberg_ivm_union_reject_unsupported.sql` — 负向：UNION distinct / INTERSECT / EXCEPT / mixed shape / 类型不一致 / partitioned target，全 `@expect_error`。
- plan-shape golden：`@explain_contains` 锁 union 分支结构 + `AggregateStateMerge` + branch id。

## 11. 风险与缓解

| 风险 | 缓解 |
| --- | --- |
| 范围大（跨 classifier/optimizer/contract/apply/tests） | 内部分层（§12）+ 每层独立可测；A 族复用 aggregate-state，B 族复用 join-delta clone/normalize |
| union 分支 clone 串 column-id | branch-local + 共享 action column、output arity/类型校验、专门单测（照搬 join-delta 防护） |
| branch_id 稳定性 | 左→右 flatten 规范序 + 写 contract + recreate-only 稳定；嵌套 flatten 稳定性单测 |
| 跨分支错误合并 | 头号 same-key-cross-branch fixture + target-state 按 branch 限定 |
| contract 迁移 | `serde(default)` 兜底：union 是新 shape，不动现存 MV |
| target-state 退化全表扫 | unpartitioned + `DeltaInputRowIds` 行过滤把读限定 touched groups |

## 12. PR 内部分层（一个 PR，按此顺序实现）

1. **Classifier + contract + target 列脚手架**：放行并描述 union，先不切执行（对齐 umbrella "先验证 outcome"）。
2. **A 族 rewrite（任务 8）**：复用 aggregate-state，落地 union-delta 原语 + fan-in 传播，增量最小。
3. **B 族 rewrite + `__branch_id__` apply 侧（任务 9 + projection union）**：branch 身份机制。
4. **Fixtures + plan-shape golden**：三种 shape 全覆盖。

## 13. 验收标准

来自任务 8 / 9 文档：
- aggregate over UNION ALL refresh 与 plain query 结果一致。
- 多 base snapshot pin 每个 base 独立记录；某分支无 delta 不影响其它分支。
- 同 group key 跨 branch 不被错误合并；删除某分支 group 不影响其它分支同 key 行。
- branch id 在 MV recreate 之外稳定；schema/partition contract 指明 branch 归属。
- non-ALL set op、mixed shape、类型不一致、partitioned target 明确报错。
- `iceberg-ivm` 全量不回归，新增 union cases 全绿。

## 14. 后续计划入口

本 spec 通过后进入 implementation planning（writing-plans skill），按 §12 四层顺序拆 task。
完成后更新 NovaRocks Roadmap 中任务 8 / 9 的状态。
