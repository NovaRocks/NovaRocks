# IVM-A2 + A3 任意 snapshot range 与多基表 pin 基础设施设计

- 状态：草稿
- 日期：2026-05-15 立项
- 范围：connector iceberg snapshot-diff 原语、IVM change stream helper、MV refresh planner、SELECT AST 重写 pipeline
- 依赖：[IVM-A1](../../../docs/design/specs/2026-05-14-ivm-a1-delta-pipeline-design.md)（已落地，提供 `__nr_ivm_delta` table function + `IcebergDeltaScan` ExecNode + AST mutation 框架）
- 阻塞 / 后续：join IVM 算子、多基表聚合 IVM、chunked refresh、MV 损坏重放修复、FE 侧 `to_snapshot_id = pin[base]` 下推

---

## 1. 背景与问题

A1 已经把 iceberg-backed MV 增量刷新重构为"一次 ExecPlan、一条 pipeline、一个 sink"，base 表通过 AST mutation 成 `__nr_ivm_delta('cat.ns.tbl', from, to) AS <alias>` 表函数后由 analyzer 路由到 `IcebergDeltaScan` ExecNode（见 [iceberg_refresh.rs:2068 `mutate_query_for_ivm_delta_scan`](../../../src/engine/mv/iceberg_refresh.rs:2068)、[resolve_from.rs:610](../../../src/sql/analyzer/resolve_from.rs:610)）。但目前的 (from, to) **被锁死为 (previous_snapshot_id, current_snapshot_id)**：

- [iceberg_delta_scan.rs:90-115](../../../src/lower/node/iceberg_delta_scan.rs:90) 显式 guard "`to_snapshot_id != current_snapshot_id` → Err，pinning to historical is reserved for A2"。
- [changes.rs:517 `plan_changes`](../../../src/connector/iceberg/changes.rs:517) 签名是 `(table, previous_snapshot_id, pk)`，**内部重新调** `metadata.current_snapshot()` 取目标，不接受调用方传入的 to。
- [changes.rs:445 `classify_lineage`](../../../src/connector/iceberg/changes.rs:445) 同样硬绑 `metadata.current_snapshot()`。

由此带来的连锁缺陷：

1. **bookkeeping 与 delta 计算用的 snapshot 可能不一致**。`refresh_iceberg_mv` 入口处读了一次 `current_snapshot_id`，但 `plan_changes` 内部又读一次；如果中间 base 表 commit 了新 snapshot，写进 `last_refresh_snapshots` 的与 delta 算出来的就对不齐 —— 下一次 refresh 漏算 / 重算。
2. **多基表 join MV 根本无法做**。"一次 refresh 期间所有 base 表必须固定在一组 snapshot"这种跨表一致 freeze 没有任何代码支撑。base 表 commit 时序变化会改 refresh 结果，调试几乎不可能。
3. **chunked refresh / MV 重放无门**。"切出 `[A, A+k]` 子区间"或"从历史 from 重跑"都依赖任意 snapshot range 能力。

A2+A3 合一解决两件事：connector 层 `plan_changes` 接受**任意 `[from, to]`**（A2），refresh 层引入 **`RefreshSnapshotPin`** 作为整轮 refresh 唯一的 snapshot source-of-truth（A3）。

---

## 2. 目标 / 非目标

### 目标

- `plan_changes` / `classify_lineage` 接受显式 `to_snapshot_id`，不再内部读 `metadata.current_snapshot()`
- 引入 `RefreshSnapshotPin` 数据结构 + freeze 时刻 + 序列化到现有 `StoredMaterializedView.refresh_target_snapshots`
- 单基表 refresh 路径**端到端**用 pin 驱动：`plan_changes` 的 to_snap、`begin_mv_refresh_intent` 的 target_snapshots、`update_managed_mv_refresh_summary` 的 last_refresh_snapshots 全部从 pin 取
- 删除 [iceberg_delta_scan.rs:90-115](../../../src/lower/node/iceberg_delta_scan.rs:90) A1 留下的 "to ≠ current → Err" guard
- 为多基表预先铺好基础设施：`inject_pin_as_for_version_as_of` AST helper + `plan_change_batches_for_pin` 多 base helper，单测覆盖，等多基表 DDL gate 一开就立即生效
- iceberg / iceberg-rest SQL 测试 suite 保持全绿；新增一个 crate-internal 集成测试 deterministic 验证 pin freeze 不变量

### 非目标

- **多基表 MV DDL 创建**：[mv_ddl.rs:167](../../../src/connector/starrocks/managed/mv_ddl.rs:167) `if base_refs.len() != 1` gate 保留
- **多基表 refresh 入口**：[mv_refresh.rs:122](../../../src/connector/starrocks/managed/mv_refresh.rs:122)、[iceberg_refresh.rs:101](../../../src/engine/mv/iceberg_refresh.rs:101)、[iceberg_refresh.rs:590](../../../src/engine/mv/iceberg_refresh.rs:590) 三处 `let [base_ref] = ...` gate 保留
- **Join IVM / 多基表聚合 IVM**：独立工作
- **FE 侧下推非 current 的 `to_snapshot_id`**：BE 侧 A2 提供能力，FE 改造另开
- **chunked refresh、MV 损坏重放**：A2 是它们的前提，本设计只交付前提
- **`IcebergChangeBatch.previous_snapshot_id / current_snapshot_id` 字段重命名**：churn 太大，留给多基表 PR
- **`single_snapshot_map` / `single_table_uuid_map` / `validate_change_batch_current_snapshot` 等单基表 helper 彻底删除**：保留为 1-entry wrapper，多基表 PR 集中清理；`validate_change_batch_current_snapshot` 本设计直接删（A2 让它语义永真）

---

## 3. 决策汇总

| # | 决策点 | 选择 | 关键理由 |
|---|---|---|---|
| 1 | 范围边界 | B：基础设施 + 单基表端到端验证 | 多基表 DDL/refresh gate 暂留；pin 在单基表下也已有真实矫正价值（修复 bookkeeping/delta 不一致 bug） |
| 2 | API 形状 | α：低层 `plan_changes` 原语 + 高层 `plan_change_batches_for_pin` helper | helper 接受 pin、原语只接受 `(table, from, to)`；两者各司其职，delta-scan lowering 走原语，refresh 走 helper |
| 3 | Pin enforcement 机制 | b：AST 注入 `FOR VERSION AS OF` 子句 | NovaRocks 已有完整 `FOR VERSION AS OF` analyzer 支持（[iceberg_ref.rs:87](../../../src/sql/analyzer/iceberg_ref.rs:87)）；不写 InMemoryCatalog 合成表名、不引入 wrapper |
| 4 | `to_snap` 为 from 严格祖先 / 不在 metadata 时的错误 | 复用 `ChangeError::LineageBroken { previous_snapshot }` | A2 doc 明确要求"沿用现有 LineageBroken 雏形"；policy 映射 `FullRefresh` 不变 |
| 5 | `IcebergChangeBatch` 字段名 | 不重命名，更新注释 | 字段值改成反映实际 from/to，名字保留以减少 churn；多基表 PR 一起改 |
| 6 | `RefreshSnapshotPin` 容器类型 | `BTreeMap<IcebergTableRef, i64>` | 需要序列化进 `refresh_target_snapshots: BTreeMap<String, i64>`；BTreeMap 保证顺序稳定，便于 hash / 比较 / 日志 |
| 7 | 端到端验收测试位置 | I：crate-internal Rust 集成测试 | `#[cfg(test)] after_capture_hook` deterministic 触发并发 commit；SQL suite 加一个简单的 incremental refresh 走通回归 |
| 8 | scope B 阶段 `inject_pin_as_for_version_as_of` 是否实现 | i：实现 + 单测，production 不会触达 | 单基表 + delta-bearing skip → no-op；但函数与单测就位，多基表 gate 一开立即生效 |
| 9 | AST mutation 风格 | 全部 sqlparser AST 节点构造 / 字段赋值 | 与 A1 [build_nr_ivm_delta_table_factor](../../../src/engine/mv/iceberg_refresh.rs:2325) 风格统一；**禁止任何 SQL 字符串拼接 / sed-style replace** |
| 10 | `rewrite_time_travel_refs` 是否参与 refresh pipeline | 不参与 | A1 之后 MV refresh 走标准 catalog 路径，analyzer 原生处理 `FOR VERSION AS OF <int>`，不需要 InMemoryCatalog 合成表名机制 |

---

## 4. 架构分层

```
┌───────────────────────────────────────────────────────────────────┐
│ Layer 4. Refresh planner / dispatcher / bookkeeping               │
│  src/connector/starrocks/managed/mv_refresh.rs                    │
│  src/engine/mv/iceberg_refresh.rs                                 │
│  - refresh 入口：RefreshSnapshotPin::capture                       │
│  - dispatcher closure 入参由 i64 改为 &RefreshSnapshotPin           │
│  - begin_mv_refresh_intent / update_managed_mv_refresh_summary    │
│    全部用 pin.to_snapshot_map()                                    │
│  - 单基表三处 gate 保留                                             │
└─────────────────────┬─────────────────────────────────────────────┘
                      │
┌─────────────────────▼─────────────────────────────────────────────┐
│ Layer 3. Pin freeze + AST inject                                  │
│  src/connector/starrocks/managed/refresh_pin.rs (新建)             │
│  src/engine/mv/iceberg_refresh.rs (扩展)                           │
│  - RefreshSnapshotPin 数据结构                                      │
│  - capture(state, base_refs) → Pin                                │
│  - inject_pin_as_for_version_as_of(query, pin, delta_bearing_set) │
│    给非 delta-bearing base 注入 FOR VERSION AS OF <pin[base]>      │
│  - cfg(test) after_capture_hook                                   │
└─────────────────────┬─────────────────────────────────────────────┘
                      │
┌─────────────────────▼─────────────────────────────────────────────┐
│ Layer 2. Per-base change-batch helper                             │
│  src/connector/starrocks/managed/ivm_change_stream.rs             │
│  - plan_change_batches_for_pin(state, pin, last_refresh, pk_map)  │
│      → Vec<(IcebergTableRef, IcebergChangeBatch)>                 │
│  - 现有 plan_iceberg_change_batch_for_ivm 退化为 1-entry pin 薄包装 │
│  - 删除 validate_change_batch_current_snapshot                     │
└─────────────────────┬─────────────────────────────────────────────┘
                      │
┌─────────────────────▼─────────────────────────────────────────────┐
│ Layer 1. Connector primitive (A2)                                 │
│  src/connector/iceberg/changes.rs                                 │
│  - plan_changes(table, from, to: Option<i64>, pk)                 │
│  - classify_lineage(metadata, from, to)                           │
│  - ChangeError::LineageBroken 覆盖 from/to 不在 metadata 和         │
│    to 是 from 严格祖先三种场景                                       │
└───────────────────────────────────────────────────────────────────┘
```

A1 的 `IcebergDeltaScan` lowering ([lower/node/iceberg_delta_scan.rs](../../../src/lower/node/iceberg_delta_scan.rs)) 在 plan-time 直接调 Layer 1 `plan_changes`，不经过 Layer 2~4。它的 `to_snapshot_id` 由调用方（mutate_query_for_ivm_delta_scan 时塞进 `__nr_ivm_delta` 第三个参数）决定 —— 本设计让这个值变成 `pin[base]`。

---

## 5. Layer 1：connector 原语（A2 全部）

### 5.1 `classify_lineage` 签名改造

```rust
pub(crate) fn classify_lineage(
    metadata: &iceberg::spec::TableMetadata,
    from_snapshot_id: i64,
    to_snapshot_id: i64,
) -> Result<LineagePlan, ChangeError>
```

变化：
- cursor 从 `metadata.snapshot_by_id(to_snapshot_id)` 取，不再调 `metadata.current_snapshot()`
- 新增前置检查：`to_snapshot_id` 不在 metadata（被 expire / 不存在）→ `LineageBroken { previous_snapshot: from_snapshot_id }`
- `from == to` 短路逻辑保留
- "沿 parent 链回溯走到根仍没碰到 from" 兜底分支自动覆盖 "to 是 from 严格祖先" 新场景

### 5.2 `plan_changes` 签名改造

```rust
pub(crate) fn plan_changes(
    table: &iceberg::table::Table,
    from_snapshot_id: i64,
    to_snapshot_id: Option<i64>,
    pk_columns: &[String],
) -> Result<IcebergChangeBatch, ChangeError>
```

实现要点：
- `to_snapshot_id` 解析：`Some(id)` 直接用；`None` → `metadata.current_snapshot().snapshot_id()`，保留旧行为作为回归保护
- 调 `classify_lineage(metadata, from, resolved_to)`
- 返回的 `IcebergChangeBatch.previous_snapshot_id / current_snapshot_id` 值反映**实际请求**到的 `from / resolved_to`
- 字段名不重命名；只更新 doc comment 强调 "current 指 requested-to，不一定是 table 当前 current"

### 5.3 `ChangeError::LineageBroken` 复用

三种触发场景共用同一 variant：

1. `from` 不在 metadata（已有）
2. `to` 不在 metadata（新增）
3. 从 `to` 反走 parent 链到根仍没碰到 `from`（已有，新场景"to 是 from 严格祖先"自动落入）

`policy_signal_from_change_error` 把 `LineageBroken` 映射到 `FullRefresh`（[changes.rs:60](../../../src/connector/iceberg/changes.rs:60)），不改。

### 5.4 调用方收敛（5 处）

| 调用点 | 改动 |
|---|---|
| [iceberg_refresh.rs:2420 `incremental_refresh_iceberg_mv`](../../../src/engine/mv/iceberg_refresh.rs:2420)（legacy 路径） | 传 `Some(pin[base])` |
| [ivm_change_stream.rs:54 `plan_iceberg_change_batch_for_ivm`](../../../src/connector/starrocks/managed/ivm_change_stream.rs:54) | 退化为 1-entry pin 薄包装，内部仍传 `Some(expected_current)` |
| [lower/node/iceberg_delta_scan.rs:121](../../../src/lower/node/iceberg_delta_scan.rs:121) lowering | 改成 `Some(payload.to_snapshot_id)`，同时**删除 lines 90-115 的 A2 guard** |
| [engine/mod.rs:6295](../../../src/engine/mod.rs:6295) 测试 | 传 `None` |
| [changes.rs:2250 / 2370 / 2532](../../../src/connector/iceberg/changes.rs:2250) 内部单测 | 传 `None`（其中一条用作"`to=None` ≡ 旧行为"回归） |

### 5.5 A2 新增单测（5 个，全部 `changes.rs::tests`）

1. **回归**：`to=None` 行为按位等价于改造前。复用现有 [changes.rs:2250](../../../src/connector/iceberg/changes.rs:2250) fixture。
2. **等价**：`to=Some(current)` ≡ `to=None`。
3. **严格祖先**：lineage `s0→s1→s2`，调 `plan_changes(from=s2, to=Some(s0))` → `LineageBroken`。
4. **中间 ancestor 截断**：lineage `s0→s1(append A)→s2(append B)→s3(append C)`，`plan_changes(from=s0, to=Some(s2))` 返回的 `batch.inserts` 只含 A、B 文件，**不含** C 文件；`batch.current_snapshot_id == s2`。
5. **边界 expire**：构造一个 lineage，`expire_snapshot` 掉 `to`，`plan_changes` 返回 `LineageBroken`。

---

## 6. Layer 2：per-base helper

### 6.1 新 helper 函数

放在 [ivm_change_stream.rs](../../../src/connector/starrocks/managed/ivm_change_stream.rs)：

```rust
pub(crate) fn plan_change_batches_for_pin(
    state: &Arc<StandaloneState>,
    pin: &RefreshSnapshotPin,
    last_refresh: &BTreeMap<String, i64>,
    pk_columns_by_base: &HashMap<IcebergTableRef, Vec<String>>,
) -> Result<Vec<(IcebergTableRef, IcebergChangeBatch)>, ChangeError>;
```

行为：
- 对 `pin` 里每个 `(base, pinned_snap)`：
  1. 从 `last_refresh` 取该 base 的 `previous_snapshot_id`（缺失 → Err，表示该 base 之前没 refresh 过；scope B 单基表下永远命中，因为只有一个 base 且 last_refresh 必然有）
  2. `load_current_iceberg_base_table(state, base)`
  3. 调 Layer 1 `plan_changes(table, previous, Some(pinned_snap), pk_columns)`
  4. **断言** `batch.current_snapshot_id == pinned_snap`（pin 与 batch 一致性自检；理论上 A2 后语义永真，作为防御性 assert）
- 任意 base 返回 `ChangeError` → 整体 fail-fast 上抛

### 6.2 `plan_iceberg_change_batch_for_ivm` 退化

```rust
pub(crate) fn plan_iceberg_change_batch_for_ivm(
    base_table: &iceberg::table::Table,
    previous_snapshot_id: i64,
    expected_current_snapshot_id: i64,
    pk_columns: &[String],
) -> Result<IcebergChangeBatch, ChangeError> {
    plan_changes(
        base_table,
        previous_snapshot_id,
        Some(expected_current_snapshot_id),
        pk_columns,
    )
}
```

旧的 `validate_change_batch_current_snapshot` **直接删除** —— A2 后 `plan_changes` 内部已保证 `batch.current_snapshot_id == 入参`，二次校验语义永真。

### 6.3 单测

- `plan_change_batches_for_pin` 单基表与直接调 `plan_changes(..., Some(pin[base]))` 行为一致
- `pin` 含 base 但 `last_refresh` 不含该 base → Err
- `pin` 的 snapshot 在 metadata 已 expire → Err 从 Layer 1 透传，类型为 `LineageBroken`

---

## 7. Layer 3：Pin freeze + AST inject

### 7.1 新文件 `src/connector/starrocks/managed/refresh_pin.rs`

```rust
pub(crate) struct RefreshSnapshotPin {
    pinned: BTreeMap<IcebergTableRef, i64>,
    table_uuids: BTreeMap<IcebergTableRef, String>,
}

impl RefreshSnapshotPin {
    pub(crate) fn capture(
        state: &Arc<StandaloneState>,
        base_refs: &[IcebergTableRef],
    ) -> Result<Self, String>;

    pub(crate) fn get(&self, base: &IcebergTableRef) -> Option<i64>;
    pub(crate) fn uuid(&self, base: &IcebergTableRef) -> Option<&str>;
    pub(crate) fn iter(&self) -> impl Iterator<Item = (&IcebergTableRef, i64)>;
    pub(crate) fn len(&self) -> usize;
    pub(crate) fn is_empty(&self) -> bool;

    pub(crate) fn to_snapshot_map(&self) -> BTreeMap<String, i64>;
    pub(crate) fn to_table_uuid_map(&self) -> BTreeMap<String, String>;
}
```

`capture` 实现：
- 对每个 base 表调 `load_current_iceberg_base_table(state, base)`
- 读 `metadata.current_snapshot()`，**为 None → Err，refresh 整体 fail-fast**
- 读 `metadata.uuid().to_string()`，存入 table_uuids
- 不要求多基表 freeze 原子；每个 base 各自读一次 metadata 即可，refresh 期间外部 commit 不影响 —— 执行端会强制走 pin 里捕获的那个 snapshot

`#[cfg(test)] after_capture_hook` 机制（仿照 [internal_rpc_client.rs:260](../../../src/service/internal_rpc_client.rs:260) 风格）：

```rust
#[cfg(test)]
pub(crate) type AfterCaptureHook = std::sync::Arc<dyn Fn() + Send + Sync>;

#[cfg(test)]
fn after_capture_hook() -> &'static std::sync::Mutex<Option<AfterCaptureHook>> { ... }

#[cfg(test)]
pub(crate) fn set_after_capture_hook(f: AfterCaptureHook);
#[cfg(test)]
pub(crate) fn clear_after_capture_hook();

impl RefreshSnapshotPin {
    pub(crate) fn capture(...) -> Result<Self, String> {
        let pin = Self { ... };
        #[cfg(test)]
        if let Some(hook) = after_capture_hook().lock().unwrap().clone() {
            hook();
        }
        Ok(pin)
    }
}
```

### 7.2 AST inject helper

放在 [refresh_pin.rs](../../../src/connector/starrocks/managed/refresh_pin.rs) 或 `iceberg_refresh.rs`（看实现位置自然性）：

```rust
pub(crate) fn inject_pin_as_for_version_as_of(
    query: &mut sqlparser::ast::Query,
    pin: &RefreshSnapshotPin,
    delta_bearing: &HashSet<IcebergTableRef>,
) -> Result<usize, String>;
```

行为，对每个 `TableFactor::Table { name, version, .. }`：
- 已有 `version = Some(...)` → **Err**（refresh SELECT 不允许 user-side time travel 与 pin 共存）
- 解析 `name`（基于 current_catalog + current_database 补全三段式）为 `IcebergTableRef`
- 不在 pin 里 → 不动（非 MV base，可能是 CTE / 其它 catalog / 表别名引用）
- 在 pin 里且 ∈ delta_bearing → 不动（由 `mutate_query_for_ivm_delta_scan` 负责）
- 在 pin 里且 ∉ delta_bearing → set `version = Some(VersionAsOf(Expr::Value(Value::Number(pin[base].to_string(), false))))`

**AST 节点字段构造，禁止任何字符串拼接**。`Value::Number(s, false)` 是 sqlparser 的 number literal AST 形态（s 是 number 的字符串表示，第二个 bool 是 long suffix）。

返回值 `usize` = 注入数量；调用方根据需要判断是否合理。

遍历结构复用 [query_prep.rs `has_time_travel_in_set_expr`](../../../src/engine/query_prep.rs:115) / `_in_factor` 的 walker 形态（已经在生产路径上验证过覆盖 SELECT / CTE / join / subquery / SetOperation 所有形式）。

### 7.3 scope B 单基表场景下的行为

- 唯一 base 在 pin 里，且在 delta_bearing_set 里（A1 path 唯一支持的形态）→ inject 函数对它**跳过**
- 不在 pin 里的表保持不变
- 因此 inject 函数在 scope B production pipeline 里**实际是 no-op**

这是符合预期的：单基表 pin 通过 `__nr_ivm_delta` 的 `to_snapshot_id` 参数（由 A1 的 `mutate_query_for_ivm_delta_scan` 写入）落地，不通过 `FOR VERSION AS OF` 落地。

inject 函数的真正用武之地是多基表 future：delta-bearing base 走 delta scan，其它 base 走 time travel。

---

## 8. Layer 4：refresh planner / dispatcher / bookkeeping

### 8.1 Pin freeze 时机（唯一）

`refresh_iceberg_mv` 入口处，`parse_iceberg_table_refs` 之后**第一件事**：

```rust
let base_refs = parse_iceberg_table_refs(&mv_definition.base_table_refs)?;
let [base_ref] = base_refs.as_slice() else {
    return Err("incremental MV refresh requires a single Iceberg base table".to_string());
};

// === 新增：一次性 freeze pin ===
let pin = RefreshSnapshotPin::capture(state, &base_refs)?;
// 后续所有需要 snapshot id 的位置从 pin 取
```

同样模式应用于 [iceberg_refresh.rs:101, 590](../../../src/engine/mv/iceberg_refresh.rs:101) 三处入口。

### 8.2 dispatcher closure 入参形状变化

[dispatch_mv_refresh_strategy](../../../src/connector/starrocks/managed/mv_refresh.rs:174) 4 个 closure 的入参语义升级：

| closure | 旧入参 | 新入参 |
|---|---|---|
| `proj_full` | `()` | `()` |
| `agg_full` | `shape` | `shape` |
| `metadata_only` | `current_snapshot_id: i64` | `&RefreshSnapshotPin` |
| `incremental` | `(previous_snapshot_id: i64, current_snapshot_id: i64)` | `(&BTreeMap<String, i64> previous_snapshots, &RefreshSnapshotPin pin)` |

`incremental` closure 内部对单基表的旧逻辑：
```rust
let previous_snapshot_id = previous_snapshots.get(&base_ref.fqn()).copied().unwrap_or(...);
let current_snapshot_id = pin.get(base_ref).expect("pin must contain base");
```

`choose_snapshot_refresh_policy` 内部：
```rust
let previous = previous_snapshots.get(&base_ref.fqn()).copied();
let current = Some(pin.get(base_ref).expect("pin contains all base refs"));
let policy = choose_snapshot_refresh_policy(previous, current)?;
```

multi-base future：dispatcher 扩展为"每个 base 算 quadrant，全局 fail-fast"。本设计不实现，但 API 形状已 ready。

### 8.3 bookkeeping 语义变化（**关键正确性修复**）

**旧契约**：`update_managed_mv_refresh_summary` 写入的 `last_refresh_snapshots` = 从 `loaded.table.metadata().current_snapshot()` 在 refresh 入口读到的值。但 `plan_changes` 内部又读一次 current，两者可能不一致 → bookkeeping 错位。

**新契约**：

```rust
begin_mv_refresh_intent(state, table_id, pin.to_snapshot_map())?;
// ... 在 dispatcher 内：
plan_changes(..., from, to=Some(pin.get(base).unwrap()), ...)?;
// ... 在 commit 阶段：
update_managed_mv_refresh_summary(
    state, table_id, rows,
    pin.to_snapshot_map(),       // last_refresh_snapshots = pin
    pin.to_table_uuid_map(),     // last_refresh_table_uuids = pin 捕获时的 uuid
);
```

**pin 是整轮 refresh 唯一的 snapshot source-of-truth**。`last_refresh_snapshots[base] = pin[base]`，不是 "refresh 结束时刻的 current"。这一字面差别是 pin 的正确性核心 —— 下次 refresh 算 delta 从 `pin[base]` 开始，不漏不重。

### 8.4 失败模式

| 场景 | 处理 |
|---|---|
| `RefreshSnapshotPin::capture` 时某 base 表无 current snapshot | Err，refresh 整体失败，不进入 dispatcher |
| capture 后 base 表被 DROP | 后续 load_current_iceberg_base_table 会 Err；保留现有错误路径 |
| capture 后 base 表被 RECREATE（uuid 变化） | pin 已捕获旧 uuid，触发现有 `BaseTableRecreated` → `FullRefresh` 分支 |
| pin 里的 snapshot 在 plan_changes 时已被 expire | Layer 1 返回 `LineageBroken` → `policy_signal_from_change_error` → `FullRefresh`，旧 pin 失效，FullRefresh 路径重新 capture 新 pin（自然达成，FullRefresh 是独立 rebuild 流程） |
| `plan_changes` 其它 ChangeError（schema evolution 等） | 现有 policy 映射保留 |

### 8.5 三处单基表 gate 全部保留

scope B 明确不动：
- [mv_ddl.rs:167](../../../src/connector/starrocks/managed/mv_ddl.rs:167) `if base_refs.len() != 1`
- [mv_refresh.rs:122](../../../src/connector/starrocks/managed/mv_refresh.rs:122) `let [base_ref] = ...`
- [iceberg_refresh.rs:101, 590](../../../src/engine/mv/iceberg_refresh.rs:101) 同上

但 gate 内部 dispatcher / bookkeeping / helper 全部按 N-entry pin 形态写。多基表 PR 只需要解 gate，不需要改主流程。

---

## 9. 执行 SQL pipeline（订正版，A1 之后真实形态）

### 9.1 三个 SQL 的真实身份

| 称呼 | 实体 | 转换 |
|---|---|---|
| 定义 SQL | 用户 DDL 里 `AS` 之后的字符串 | 原文 |
| 元数据 SQL | `StoredMvDefinition.select_sql` | iceberg-backed 路径下被 [canonicalize_iceberg_mv_select_query](../../../src/connector/starrocks/managed/mv_ddl.rs:992) AST 改写为三段式全限定后 `to_string()` 持久化 |
| 执行 SQL | refresh 时实际跑的 AST | parse 元数据 SQL → 一连串 AST mutation → 直接送入 `execute_query_with_options` |

**三者全部 AST 路径，没有任何 sed-style 字符串替换**。中间会经过 `parse → mutate AST → AST.to_string() → 再 parse` 的序列化往返（因为有些下游函数收 `&str`），但每一步的变换都是 sqlparser AST 节点构造 / 字段赋值。

### 9.2 改造后的 refresh AST mutation pipeline

```
mv_definition.select_sql  (CREATE 时已 AST canonicalize 成 3-part 全限定)
   │
   │ ① parse_normalized_sql_raw → AST
   ▼
inject_pin_as_for_version_as_of(query, pin, delta_bearing)
   │ ② 给非 delta-bearing base 的 TableFactor::Table 设
   │   version = Some(VersionAsOf(Number(pin[base])))
   │   (scope B 单 base 场景：delta_bearing 含唯一 base, function no-op)
   ▼
mutate_query_for_ivm_delta_scan(query, base_ref, from=last_refresh[base], to=pin[base])
   │ ③ A1 已有：把 delta-bearing base 替换成
   │   __nr_ivm_delta('cat.ns.tbl', from, to) AS <alias>
   │   A2 的实质落点 = (from, to) 从 (prev, current) 改成 (last_refresh, pin)
   ▼
projection_select_with_change_op(...)
   │ ④ projection 末尾追加 __change_op 列
   ▼
strip_catalog_from_three_part_names(...)
   │ ⑤ 剩余非 __nr_ivm_delta 表（如 FOR VERSION AS OF 修饰的）3-part → 2-part
   ▼
execute_query_with_options(...)
   │ ⑥ analyzer 见 __nr_ivm_delta → IcebergDeltaScanNode → IcebergDeltaScan ExecNode
   │   analyzer 见 FOR VERSION AS OF <int> → iceberg_ref.rs 直接绑定 snapshot id
```

**`rewrite_time_travel_refs` 不在这条 pipeline 上**。A1 之后 MV refresh 走标准 catalog 路径，analyzer ([iceberg_ref.rs:87](../../../src/sql/analyzer/iceberg_ref.rs:87)) 原生支持 `FOR VERSION AS OF <integer>`，不需要 InMemoryCatalog 合成表名机制。

### 9.3 单基表场景（scope B production 路径）

定义 SQL：
```sql
SELECT k, v + 1 AS v1 FROM orders WHERE v > 0
```

元数据 SQL（CREATE 时 canonicalize）：
```sql
SELECT k, v + 1 AS v1 FROM ice.sales.orders WHERE v > 0
```

执行 SQL（scope B `inject_pin_as_for_version_as_of` no-op）：
```sql
SELECT k, v + 1 AS v1, __change_op
FROM __nr_ivm_delta('ice.sales.orders', <last_refresh[orders]>, <pin[orders]>) AS orders
WHERE v > 0
```

物理 plan：
```
Project(k, v1, __change_op)
└── IcebergDeltaScan(orders, from=last_refresh, to=pin[orders])
```

### 9.4 多基表 join 场景（future，scope B 不实现，列在这里说明 API 形状）

定义 SQL：
```sql
SELECT r.id, r.v, s.label FROM orders r JOIN dim s ON r.dim_id = s.id WHERE r.v > 0
```

元数据 SQL：
```sql
SELECT r.id, r.v, s.label
FROM ice.sales.orders AS r
JOIN ice.sales.dim    AS s ON r.dim_id = s.id
WHERE r.v > 0
```

执行 SQL（`orders` delta-bearing，`dim` snapshot-pinned）：
```sql
SELECT r.id, r.v, s.label, __change_op
FROM __nr_ivm_delta('ice.sales.orders', <S_o_prev>, <S_o_pin>) AS r
JOIN ice.sales.dim FOR VERSION AS OF <S_d_pin> AS s ON r.dim_id = s.id
WHERE r.v > 0
```

物理 plan：
```
HashJoin(r.dim_id = s.id)
├── IcebergDeltaScan(orders, from=S_o_prev, to=S_o_pin)
└── IcebergScan(dim @ S_d_pin)
```

`S_o_pin` 和 `S_d_pin` 来自同一次 `RefreshSnapshotPin::capture()`，refresh 期间不动。

### 9.5 AST mutation 实现规范

`inject_pin_as_for_version_as_of` 必须遵循与 [build_nr_ivm_delta_table_factor](../../../src/engine/mv/iceberg_refresh.rs:2325) 相同的实现风格：

- 全部 sqlparser AST 节点字段赋值 / 构造
- `Value::Number(snapshot_id.to_string(), false)` 字符串只是 sqlparser 内部 number AST 节点的存储形式，不是 SQL 拼接
- **禁止** `format!("FOR VERSION AS OF {}", id)` 这类 SQL 字符串拼接
- **禁止** `query.body.to_string().replace(...)` 这类 sed-style 替换
- 遍历 walker 形态参考 [query_prep.rs:115 `has_time_travel_in_set_expr`](../../../src/engine/query_prep.rs:115)

---

## 10. 测试与验收

### 10.1 单元测试矩阵

**Layer 1（A2，`changes.rs::tests`）**：见 5.5。

**Layer 2（helper，`ivm_change_stream.rs::tests`）**：
6. `plan_change_batches_for_pin` 单 base 与直接 `plan_changes(..., Some(pin[base]))` 等价
7. pin 含 base 但 last_refresh 不含 → Err
8. pin 的 snapshot expired → `LineageBroken` 透传

**Layer 3（pin + inject）**：
9. `RefreshSnapshotPin::capture` 多 base 各自读到自己 current snapshot
10. capture 某 base 无 current snapshot → Err
11. `inject_pin_as_for_version_as_of` 多 base SELECT 上为非 delta-bearing base 注入 `VersionAsOf` AST 节点，结构正确
12. inject 遇到已有 `FOR VERSION AS OF` 的表 → Err
13. inject 跳过 delta_bearing_set 里的 base
14. inject 跳过不在 pin 里的表（CTE / 其它 catalog / 别名引用）

**Layer 4（dispatcher / bookkeeping，扩展 `mv_refresh.rs::tests`）**：
15. `begin_mv_refresh_intent` 收到的 target_snapshots 等于 `pin.to_snapshot_map()`
16. `update_managed_mv_refresh_summary` 写入的 `last_refresh_snapshots` 等于 `pin.to_snapshot_map()`，**不是** refresh 结束时 base 表 current

### 10.2 端到端验收测试（crate-internal 集成测试，scope B 关键）

位置：`src/connector/starrocks/managed/mv_refresh.rs::tests`（或新模块 `mod refresh_pin_acceptance`）。

```text
test: pin_freeze_against_concurrent_commit

step 0: 设置 Iceberg memory catalog + 内存 warehouse
step 1: CREATE TABLE ice.db.t (id int, v int)
step 2: INSERT INTO t VALUES (1, 10), (2, 20)            // snapshot s1
step 3: CREATE MATERIALIZED VIEW mv AS
          SELECT id, v+1 AS v1 FROM ice.db.t
        REFRESH MATERIALIZED VIEW mv                      // full @ s1
step 4: INSERT INTO t VALUES (3, 30)                      // snapshot s2

step 5: refresh_pin::set_after_capture_hook(Arc::new(|| {
          // 在 pin 已经 freeze 在 s2 之后、plan_changes 之前
          INSERT INTO t VALUES (4, 40);                   // snapshot s3
        }))
        REFRESH MATERIALIZED VIEW mv

step 6: refresh_pin::clear_after_capture_hook()

断言:
- mv 表内容 = {(1,11), (2,21), (3,31)}
- mv 表 *不含* (4, 41)
- mv_definition.last_refresh_snapshots[t] == s2
- 再 REFRESH MATERIALIZED VIEW mv，mv 内容补上 (4, 41)
- mv_definition.last_refresh_snapshots[t] == s3
```

### 10.3 SQL suite 回归

- `iceberg` suite：保持全绿。
- `iceberg-rest` suite：保持全绿；新增一个 case "incremental refresh after external commit 正常推进"（不验证并发，验证基础走通）。

---

## 11. 风险

| 风险 | 缓解 |
|---|---|
| `plan_changes` 改签名后 caller 漏改 | Rust 编译器接住；强制 |
| `inject_pin_as_for_version_as_of` AST walker 漏一种 SetExpr 形式 | 复用现有 [query_prep.rs `has_time_travel_in_set_expr`](../../../src/engine/query_prep.rs:115) 的 walker；该 walker 已在生产路径验证过覆盖 |
| pin freeze 之后到 plan_changes 之前 base 表 snapshot 被 expire | A2 `LineageBroken` → `FullRefresh` 自然处理 |
| 端到端测试因时序而 flaky | `#[cfg(test)] after_capture_hook` 取代时序 (10.2) |
| 多基表未来 PR 解 gate 时漏 corner case | 单测 9-16 已覆盖多基表数据形状；future PR 只需解 gate |
| FE 侧不下推非 current 的 `to_snapshot_id`，A2 删的 lowering guard 不在生产路径生效 | scope B 不依赖 FE 改造；删 guard 是为 future FE PR 让路。当前 A1 delta-scan 调用 `to_snapshot_id` 都 == current，删 guard 不引入新行为 |
| `inject_pin_as_for_version_as_of` 函数本身在 scope B production 不被触达，回归覆盖不足 | 单测 11-14 完整覆盖；多基表 gate 一开自动生效 |

---

## 12. 兼容性 / 迁移

按 memory 指示 "NovaRocks 没有历史用户，不写兼容性代码"：

- `IcebergChangeBatch` 字段语义变化（current_snapshot_id 现在指 requested-to）**不加** alias、不写 migration。直接换。
- `plan_changes` 签名变化，所有 caller 同 PR 改完，不留 `plan_changes_v1` shim
- `RefreshSnapshotPin` 引入后，`Option<i64> current_snapshot_id` 局部变量直接删
- `validate_change_batch_current_snapshot` 直接删
- `last_refresh_snapshots[base] = pin[base]` 这一语义变化对存量已 refresh 过的 MV 影响：下一次 refresh 在 pin freeze 时读到的 current 就是 freeze 点，与旧路径"refresh 结束时刻的 current"在静态场景下完全等同；只有并发 commit 场景下行为变化，且变化方向是从"错误"到"正确"。无需 data migration

---

## 13. PR 内的实现顺序

一个 PR，按以下顺序提交（便于 review 分段思考）：

1. **A2 connector primitive** —— `classify_lineage` / `plan_changes` 签名 + 5 个新单测。所有 caller 暂传 `None` 保旧语义。这一步独立可编译可测。
2. **Layer 1 caller 切到 `Some(...)`**：
   - [iceberg_delta_scan.rs](../../../src/lower/node/iceberg_delta_scan.rs) 删 A2 guard，改 `Some(payload.to_snapshot_id)`
   - [iceberg_refresh.rs:2420](../../../src/engine/mv/iceberg_refresh.rs:2420) legacy 路径改 `Some(...)`
3. **Layer 2 helper + `RefreshSnapshotPin`**：新建 [refresh_pin.rs](../../../src/connector/starrocks/managed/refresh_pin.rs)、`plan_change_batches_for_pin`；`plan_iceberg_change_batch_for_ivm` 退化薄包装；删 `validate_change_batch_current_snapshot`；单测 6-10。
4. **Layer 3 AST inject helper**：`inject_pin_as_for_version_as_of` + 单测 11-14。
5. **Layer 4 refresh planner / dispatcher / bookkeeping**：`RefreshSnapshotPin::capture` 调用点、`begin_mv_refresh_intent` / `update_managed_mv_refresh_summary` 改用 pin、incremental closure 入参改型；单测 15-16。
6. **端到端 acceptance test**：after_capture_hook + crate-internal 集成测试；`iceberg-rest` suite 回归 case。

每一步完成后 `cargo fmt && cargo clippy && cargo build && cargo test` 全绿。

---

## 14. Open questions

暂无。多基表 DDL gate 解锁、FE 侧 `to_snapshot_id` 下推、join IVM 算法已明确为后续独立工作。
