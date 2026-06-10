# Iceberg MV Partition Umbrella 设计

- 日期:2026-06-10
- 状态:设计定稿,待实现(按 Phase 多次落地)
- 覆盖任务(NovaRocks Roadmap / Iceberg v3 Incremental MV Roadmap):
  - 任务 12:Partition contract under rewrite
  - 任务 15:Touched-group / affected-partition fallback thresholds
  - 任务 16:File/position 精确 state read
  - 任务 17:Join projection/filter MV 输出 partition 推导
  - 任务 22:Per-partition metadata state
  - 任务 23:`ALTER MATERIALIZED VIEW REPARTITION`
- 非目标:managed-lake target、非 Iceberg target、非 v3 base table、freshness-aware query
  rewrite(P8,远期)、partition-scoped commit(改变 refresh 原子性语义)、
  Iceberg multi-spec 存活数据(见决策 D3)。

---

## 1. 背景与现状

PR #142 / #145 / #160 建立了 partition contract、affected-partition planner 与
partition-pruned state lookup;其后 IMV rewrite cutover(PR #213 / #231 / #237 / #249)
把所有 shape 的 delta plan 统一到 logical rewrite,refresh dispatch 改为
capability-driven(PR #257 / #258),refresh 接入 shared operation lifecycle
(PR #259 / #262 / #263)。partition 相关任务是该 roadmap 最后一块未完成区域。

当前代码事实(2026-06-10 核实):

- partition 机制集中在 `src/engine/mv/partition/`:
  `key.rs`(`MvPartitionKey` / `TargetPartitionFilter`)、`mapping.rs`(file→key)、
  `planner.rs`(单 base PF manifest 推导)、`aggregate_delta.rs`
  (`derive_from_aggregate_delta`,delta-chunk 推导)。
- 两个平行结果类型:`AffectedMvPartitions`(Unpartitioned / Known{new,old} / Unknown,
  planner 路径)与 `AffectedAggregateTargetPartitions`(Unpartitioned / Known,
  aggregate 路径)。
- **生产 aggregate 剪枝路径(merge-sink cutover 后)**:plan 时
  `plan_aggregate_mv_affected_partitions`(manifest 路径,与 PF 共用
  `planner::plan_affected_partitions`)→ `RefreshPlan.affected_partitions` →
  codegen 时 `refresh_context::target_state_partition_allow_list` 对
  `IcebergMvTargetState` scan 做文件级剪枝;row 级精确过滤由
  `IcebergMvTargetStateRowFilter::DeltaInputRowIds` 运行时 row-filter 承担;
  locator 输入由 `load_target_apply_locator_inputs` 预载。
- **delta-chunk 推导链是死代码**(2026-06-10 核实):
  `apply_iceberg_aggregate_delta_chunks` / `build_aggregate_target_partition_filter` /
  `derive_from_aggregate_delta` 的 delta-chunk 求值 /
  `load_touched/current_aggregate_target_state` 在 PR #231 merge-sink cutover 后
  无生产调用方,仅单元测试引用;`iceberg_aggregate_mv.apply` 13 字段事件也只从
  死路径发出。其中 delta-chunk 求值逻辑是 P2+ 需要的资产,需先提取再删除死壳。
- logical rewrite 层无 partition 信息;`ImvPlanAnnotation` 仍是空壳占位
  (`src/sql/optimizer/rewrite/imv/annotation.rs`)。
- `IcebergMvTargetStateScan.partition_constraint`(Unpartitioned /
  AffectedPartitionAllowListRequired)已经为 derivation 留好消费接口,allow-list 在
  `refresh_context.rs::target_state_partition_allow_list` 填入。
- join projection/filter 与 UNION ALL 的 affected partitions 恒为
  `Unknown("... not implemented")`,无剪枝。
- target 行 locator(`locate_target_rows_by_*`,`src/engine/mv/iceberg_target_apply.rs`)
  已投影 `_file` / `_pos` 元列,但**不接受 partition filter**,扫全表后按 apply key 过滤。
- partitioned aggregate MV 的 affected partitions 为 `Unknown` 时,现状是
  `tracing::warn` + target-state 全表 scan(`refresh_context.rs::target_state_partition_allow_list`)
  ——静默性能回退,正确性不受影响;**没有** derivation 失败即 fail refresh 的行为。
  contract drift / transform 不匹配的 fail-fast 发生在
  `filter_target_state_files_by_partition` 的 mapping 校验,与 derivation 是两回事。
- 阈值 / fallback / per-partition state / REPARTITION 均不存在。

## 2. 决策记录(本设计前置澄清)

- **D1 范围**:六个任务做一份 umbrella 设计,实现按 Phase 分多次完成。
- **D2 fallback 策略**:阈值触发时采用 A(partition-only filter)+ B(full-load)组合
  矩阵;C(partition rebuild)等 per-partition state 落地后接入。所有 fallback 带
  `fallback_reason` tracing。
- **D3 REPARTITION v1 语义**:全量重建 + **单 active spec 不变量**——target table 存活
  data file 永远只属于 contract 的 `target_spec_id`;不做 Iceberg multi-spec 存活数据。
  剪枝 / mapping / state-load 全部保持单 spec 假设。
- **D4 per-partition state v1 边界**:观测优先。refresh 保持 staging branch 原子 commit;
  partition state 是 commit 成功后的派生元数据 + 失败诊断,不引入 partition-scoped
  commit,不做 partition 级局部 retry(留作后续 phase)。
- **D5 架构基线修正 + policy 后置(2026-06-10,写 P1 计划时核实)**:本 spec 初稿的
  §5 叙述部分基于 PR #231 之前的 apply 路径;实际生产路径见 §1 修正。由此:
  (a) P1 的「aggregate 迁移」对象改为真实的 plan-time planner / annotation /
  constraint 接线,并删除死的 pre-cutover apply 路径(先提取 delta-chunk 求值器);
  (b) v1 所有 shape 的 `PartitionPruningPolicy` 一律 `BestEffort`(保持现状的
  warn + 全扫回退),`Required` 枚举值落地但暂不启用,收紧到 Required 的决定
  推迟到 P2/P3(届时 NotDerivable 场景更少、证据更全)。

## 3. 方案对比

- **方案 1(选定):rewrite 时解析 derivation spec,运行时求值。**
  rewrite 层从 contract + plan lineage 解析出 `PartitionDerivationSpec`(能不能剪、
  怎么剪在 plan 时定),运行时对物化后的 delta chunks 机械求值。语义判断停在 logical
  rewrite 边界;join / union / aggregate 共享一个求值器。
- 方案 2(否决):全部推到 plan time(纯 manifest 推导)。group key / join 输出与 base
  物理分区无可证明映射(roadmap 红线:不能把 base partition key 当 MV partition key),
  通用场景不可行。
- 方案 3(否决):维持 apply 层、只统一类型。正是任务 12 要消灭的状态;每加 shape 手写
  一份 derivation 接线,「能否剪枝」要到执行中途才知道。

## 4. §1 统一 derivation 架构(任务 12 / 17 地基)

### 4.1 统一类型

```rust
// src/engine/mv/partition/derivation.rs(新)
pub(crate) enum AffectedTargetPartitions {
    Unpartitioned,
    Known { partitions: BTreeSet<MvPartitionKey> },
    NotDerived { reason: String },   // 显式记录不可推导,消费方按 policy 处理
}

pub(crate) struct PartitionDerivationSpec {
    pub target_spec_id: i32,
    pub fields: Vec<PartitionDerivationField>,
}

pub(crate) struct PartitionDerivationField {
    pub partition_field_name: String,
    pub source_target_field_id: i32,          // contract 级稳定标识
    pub output_index: usize,                  // 在 target.visible_columns 中的位置
    pub transform: iceberg::spec::Transform,  // 已验证支持(Void 拒绝)
}
// D5 修正:不用 plan ColumnId——求值发生在 chunk 上,绑定经由 layout/schema 名
// (apply 侧 binder);P2 join 路径再评估是否需要 plan 级绑定。
```

- `AffectedTargetPartitions` 替代并统一 `AffectedMvPartitions` 与
  `AffectedAggregateTargetPartitions`;`Known{new,old}` 的新旧区分只有观测价值,
  降级为 tracing 字段。
- `MvPartitionKey`(带 `spec_id`)不变;D3 的单 spec 不变量让所有匹配逻辑保持单 spec。
- 现有 `derive_from_aggregate_delta` 内部的三段结构(resolution / transformation /
  partitioning,`aggregate_delta.rs:126-262`)拆分:resolution 上移到 rewrite 规则;
  后两段泛化为通用求值器
  `evaluate_partition_spec(spec, chunks) -> Result<BTreeSet<MvPartitionKey>, _>`。

### 4.2 新 rewrite stage:`imv-partition-derivation`

- 位置:pipeline 第 9 阶段 `imv-apply-key` 之后、`imv-validation` 之前(此时 apply key、
  branch id、delta plan 形状均已定型)。
- 规则 `DerivePartitionSpecRule`:
  - contract 无 `target.partition` → 标注 `Unpartitioned`。
  - 对每个 partition field:沿 plan column lineage 验证 **pure-column**
    (`ExpressionKind::Column` 单一来源)+ transform 支持,解析出 `delta_output_column`。
  - UNION ALL:对每个 branch 独立解析 per-branch spec;v1 规则:任一 branch 不可证明 →
    整体 `NotDerivable`(不做半剪枝)。
  - 解析失败**不 fail rewrite**(剪枝是优化,roadmap 原则 5),产出
    `NotDerivable{reason}` + `RewriteTrace` 事件。
- 结果挂在 `ImvPlanAnnotation`(空壳首次启用):

```rust
pub(crate) struct ImvPlanAnnotation {
    pub partition: ImvPartitionAnnotation,
}

pub(crate) enum ImvPartitionAnnotation {
    Unpartitioned,
    // 非 union shape 恰有一个 spec;union families 每个 branch 一个。
    Derivable { specs: Vec<PartitionDerivationSpec> },
    NotDerivable { reason: String },
}
```

### 4.3 显式 policy 矩阵

「不可推导时怎么办」由 capability 层声明,新增到 `RefreshCapabilities`:

```rust
pub(crate) enum PartitionPruningPolicy {
    Required,    // NotDerivable ⇒ refresh fail-fast
    BestEffort,  // NotDerivable ⇒ 不剪枝 + tracing,refresh 继续
}
```

| shape | v1 policy | 依据 |
|---|---|---|
| 全部 shape(D5 修正) | `BestEffort` | 现状即如此:Unknown/NotDerivable → warn + 不剪枝,正确性不变 |

`Required` 作为枚举值落地但 v1 不启用;是否把 partitioned aggregate 收紧到
`Required`(NotDerivable ⇒ fail refresh,属于语义收紧而非保持现状)推迟到 P2/P3
再决定。优先级规则(届时生效):**contract 含 branch(union families)时一律
`BestEffort`**,即使带 aggregate state(A-family / B-family)。

执行点:rewrite 本身从不 fail(§4.2);policy 在 refresh dispatch 消费 annotation 时
执行——`Required` + `NotDerivable` ⇒ 在 apply 开始前 fail-fast,错误信息带 reason;
`BestEffort` + `NotDerivable` ⇒ 不剪枝 + tracing(现状的 warn 升级为结构化事件)。

**概念边界**:policy 管「语义上能否证明」;阈值 fallback(§5.4)管「可推导但量太大」,
是纯性能策略,两者正交,fallback 不违反 `Required`。

### 4.4 数据流

```text
plan_iceberg_mv_refresh
  └─ run_imv_rewrite ──► ImvPlanAnnotation.partition = Derivable(spec) | ...
        │                       (RewriteTrace: PartitionSpecResolved / NotDerivable)
        ▼
  执行 delta plan,物化 delta chunks
        │
        ▼
  evaluate_partition_spec(spec, delta_chunks) ──► AffectedTargetPartitions::Known
        │
        ├─► TargetPartitionFilter::AllowList ──► target-state 剪枝(P2 起接 live 路径;
        │      P1 的 live 剪枝输入仍是 plan-time manifest 推导,行为不变)
        └─► locator 剪枝(新):locate_target_rows_by_* 增加 partition filter 参数
```

- locator 剪枝补掉现状缺口:今天 locator 扫全表只按 apply key 过滤。
- 单 base PF 的 plan-time manifest 推导路径(`plan_affected_partitions`)**保留**,
  输出统一到 `AffectedTargetPartitions`,作为 delta-chunk 求值之外的第二来源:planning
  阶段即有结果,继续填 `RefreshPlan`,并供 per-partition state 使用。

## 5. §2 shape 接入 + 性能层(任务 12 / 17 / 15 / 16)

### 5.1 Aggregate / join aggregate:接入真实路径,不改行为(D5 修正)

- **求值器提取**:`derive_from_aggregate_delta` 的三段拆为
  `resolve_partition_derivation_spec(contract)`(contract 级,steps 1-2 + transform)
  + `bind_spec_to_aggregate_layout(spec, layout)`(steps 3-4,layout 依赖留在
  apply 侧 binder)+ `evaluate_partition_spec(bound, chunks)`(机械求值)。
  注:`PartitionDerivationField` 用 `source_target_field_id` + `output_index`
  标识列,不用 §4.1 初稿写的 plan `ColumnId`——求值发生在 chunk 上,绑定经由
  layout/schema 名,plan ColumnId 在 P1 没有消费方(P2 join 路径再评估)。
- **死代码删除**:提取完成后删除 pre-cutover apply 壳
  (`apply_iceberg_aggregate_delta_chunks`、`build_aggregate_target_partition_filter`、
  `load_touched/current_aggregate_target_state`、只为死路径服务的
  `iceberg_aggregate_mv.apply` 事件发射器);每个符号删除前必须核实无生产调用方。
- **annotation 接线**:`DerivePartitionSpecRule` 在 rewrite 时从 contract 解析 spec
  写入 annotation;P1 中 annotation 的消费方是观测(trace/log)与测试断言,
  live 剪枝输入仍是 plan-time manifest 路径(行为不变);P2 起 annotation spec
  成为 join PF / sink 侧剪枝的求值输入。
- `AffectedPartitionError` 全部 variant 保留,按时机分层:
  - plan 时报(进 annotation `NotDerivable`):`ContractMissing` /
    `TransformUnsupported` / `OutputLineageNotPureColumn`;
  - 运行时报(依赖实际 chunk):`GroupKeyColumnMissing` / `GroupKeyTypeMismatch` /
    `TransformFailed`。
- 验收:现有 `iceberg-ivm` partitioned aggregate SQL golden 全部不变;rewrite 阶段
  新增 `iceberg_mv.partition_derivation` tracing 事件。

### 5.2 Join projection/filter(任务 17)

- **可证明条件(v1)**:MV 的每个 partition 列必须 pure-column 映射到 join 某一侧的
  输出列;join 后才产生的计算列 → `NotDerivable`(BestEffort,不剪枝)。
- **求值输入**:join coalescer 输出的 delta chunks(含双侧 change op)。双侧 delta 都
  参与:dim 侧 partition 列变化使 fact 侧关联行的输出 partition 移动,coalescer 输出的
  签名行(old 行 −1 / new 行 +1)天然携带新旧两个 partition 值,对所有行求值即同时
  覆盖「移出」与「移入」,无需特判。
- **消费**:join PF 无 aggregate state,剪枝收益全在 locator——
  `locate_target_rows_by_string_apply_key`(`__nova_join_row_key`)增加
  `TargetPartitionFilter` 参数,按 allow-list 筛 `FileScanTask`。
- SQL fixture:左 / 右 / 双侧变化 + dim partition move。

### 5.3 UNION ALL families

- target partition fields 对所有 branch 相同(partition 是 target 层概念);spec 解析
  按 branch 各做一次(lineage 不同),annotation 存 per-branch spec。
- apply 时各 branch delta chunks 分别求值,**集合并集**为单一 target allow-list;
  branch-scoped state read / locator 在自身 branch 范围内用同一 allow-list
  (partition 与 branch 正交)。
- v1:任一 branch `NotDerivable` → 整体不剪枝。

### 5.4 阈值 fallback(任务 15)

配置跟随 `StandaloneServerConfig` 现有平铺模式(已有 `mv_refresh_scheduler_*` 先例):

```toml
[standalone_server]
mv_refresh_max_touched_groups = 100000      # row-id 精确过滤上限
mv_refresh_max_affected_partitions = 4096   # allow-list 剪枝上限
```

两阈值正交,组合为退化矩阵(全部 correctness 等价):

落点是 live merge-sink 路径(D5 修正):partition 剪枝 = target-state scan 的
allow-list 文件绑定(`target_state_partition_allow_list`);row-id 精确过滤 =
`DeltaInputRowIds` 运行时 row-filter。

| touched groups | affected partitions | 行为 | fallback_reason |
|---|---|---|---|
| ≤限 | ≤限 | 正常:allow-list 文件剪枝 + row-id 运行时过滤 | — |
| 超限 | ≤限 | **A**:保留 allow-list 剪枝,放弃 row-id 过滤 | `threshold_touched_groups` |
| ≤限 | 超限 | 放弃 allow-list 剪枝,保留 row-id 过滤 | `threshold_partitions` |
| 超限 | 超限 | **B**:全量 target-state scan(即现状 Unknown 回退路径,已知正确) | 两者并记 |

- 默认值保守,实现期参照任务 14 perf baseline 校准。
- fallback 不掩盖真正的 fail-fast 错误(contract drift / transform unsupported 照常报错)。

### 5.5 File/position 精确 state read(任务 16)

- 现状(live 路径,D5 修正):locator 输入预载(`load_target_apply_locator_inputs`)
  与 apply-key locator 扫描已投影 `_file` / `_pos`(`iceberg_target_apply.rs:719`),
  输出 (file, pos, apply_key);target-state scan 则按 allow-list 文件粒度绑定后
  整文件读、由 `DeltaInputRowIds` row-filter 在行级过滤——这是
  `O(touched partitions target rows)` 与终态 `O(touched groups)` 的差距。
- 设计方向:**locator 输出双用**——(a) 把 (file, pos) 集合转化为 target-state
  scan 的 position 级绑定 / 过滤,(b) 继续供 delete 阶段 `PositionDeleteGroup`;
  两者共享一次 `plan_files`。在 merge-sink 架构下的具体绑定形态(codegen 时
  文件绑定 vs 运行时 position 注入)是 P4 的首要 verify item。
- 过滤深度:`_file` 在 `FileScanTask` 粒度筛(等价 manifest 级);`_pos` 谓词能否进
  row-group 级是实现期 verify item(vendored iceberg-0.9.0),进不去则 batch 层过滤,
  仍优于读整 file。
- fallback 链:positions 路径失败 / 为空 → allow-list + row-id 路径(§5.4 矩阵)→
  全量 target-state scan;每层 correctness 等价,tracing 记录实际层。
- 语义守护:locator 输出(touched 旧行)与 state read 输入是同一批行;若未来 locator
  范围扩展,必须拆开(继承任务文档风险 2)。

## 6. §3 lifecycle + observability(任务 22 / 23)

### 6.1 Per-partition metadata state(任务 22,观测优先)

- **存储**:新 avsc family `mv.partition_state/0001.avsc` + repository CRUD,模式照抄
  `mv.refresh` family(`src/meta/avro/schemas/` + `src/meta/repository/mv.rs`)。
  记录字段:`mv_id`、`partition_key`(含 spec_id 的规范字符串编码)、
  `status (FRESH / REFRESHING / FAILED)`、`last_refresh_ms`、`base_snapshots`、
  `target_snapshot_id`、`last_refresh_id`、`failure_message`。
- **写入点**:staging branch publish 成功之后、finalize 阶段内(对齐 operation
  lifecycle 的 `Committed → Finalizing`)。refresh 原子性不变:
  - 成功 → 按 `AffectedTargetPartitions::Known` 集合写 FRESH;
  - 失败 → 整体 abort,affected 集合(若已知)记 FAILED + reason,**纯诊断**。
- **诚实性规则**:affected partitions 为 `NotDerived` 或 full refresh 时无法精确更新 →
  **清空该 MV 的 partition state 行**,并复位 MV 级 `partition_state_complete` 标记
  (存于 `StoredMvDefinition` 记录);不留假数据。
- **膨胀保护**:`mv_partition_state_max_entries`(默认 10000)/MV;超限 → 停止跟踪 +
  清空 + 标记。
- `SHOW MATERIALIZED VIEWS` 输出不变;partition state 不暴露用户层。partition-scoped
  retry / rebuild(D2 的选项 C)是后续 phase,接在本模型之上。

### 6.2 `ALTER MATERIALIZED VIEW <name> REPARTITION BY (...)`(任务 23)

- **Parser**:`AlterMaterializedViewAction::Repartition(...)` 新 variant(现有
  SetRefresh / PauseRefresh / ResumeRefresh 之外),复用
  `src/sql/parser/dialect/materialized_view.rs::parse_partition_by`。
- **执行器**(新 `src/engine/mv/repartition.rs`),全程走 operation lifecycle
  (新 `IcebergOperationKind::MvRepartition`):
  1. 取 refresh 同款锁 / intent(与并发 refresh、scheduler 互斥);pin base snapshots。
  2. 编译新 PARTITION BY → `UnboundPartitionSpec`(复用 CREATE 路径
     `src/connector/iceberg/partition_spec.rs`);target table 事务加 spec 并设默认
     (`TableUpdate::AddSpec` / `SetDefaultSpec`,vendored 0.9 可用性为 verify item,
     有 `vendor/iceberg-0.9.0/PATCH.md` 打补丁先例)。
  3. staging branch 上从 pinned snapshot **全量重算**,按新 spec 写;guarded publish
     以 overwrite 语义**替换全部 data files**——publish 即恢复单 spec 不变量(D3)。
  4. contract 更新:`target.partition` 整体替换(新 `target_spec_id` + fields);
     **旧 contract 存入 repartition operation record 作审计**,不引入完整 contract
     版本体系。
  5. 失败恢复:abort → 旧 spec + 旧数据完好;target table 上已加但未启用的 spec 是
     无害元数据,cleanup 留后续任务。
- base 在重建期间的变更自然成为下次增量 refresh 的 delta(pin 语义),无需 quiesce。
- 外部 ALTER target table partition spec 依旧 fail-fast(受控入口唯一)。
- partition 列校验与 CREATE 相同(必须是 MV 输出列,transform 受支持)。

### 6.3 Observability

- **RewriteTrace**:`PartitionSpecResolved` / `PartitionNotDerivable(reason)`;EXPLAIN
  集成搭现有 optimizer observability 后续车,非本设计阻塞项。
- **Tracing**:
  - `iceberg_aggregate_mv.apply` 13 字段保持 + 新 `fallback_reason: Option<&'static str>`;
  - join / union apply 事件补 `partition_filter` / `affected_partition_count` 字段;
  - rewrite 阶段新事件 `iceberg_mv.partition_derivation`(mv_id、target_fqn、outcome、
    reason);
  - `iceberg_aggregate_mv.partition_derivation_failed` 保留,语义收敛到 Required 路径。

### 6.4 测试策略

- Unit:spec resolution(各 shape)、求值器 golden(chunk → key set)、阈值矩阵
  4 格、partition state CRUD、repartition 状态机。
- SQL(`iceberg-ivm` 扩展):partitioned join PF(左 / 右 / 双侧 + dim partition
  move)、partitioned union families、阈值穿越 fixture(config 注入)、repartition
  e2e(建 partitioned MV → refresh → REPARTITION → refresh,断言新 spec 数据 + 旧
  数据可查)。
- 护栏:现有 partitioned aggregate golden 全程不变(P1 迁移的行为锁)。
- Perf:复用任务 14 已落地的 baseline harness,验证 P4 后 sparse fixture 上
  `scanned_target_row_count ≈ matched_target_row_count`。

## 7. Phase 切分(多次实现的边界)

| Phase | 内容 | 任务 | 预估 PR 数 | 依赖 |
|---|---|---|---|---|
| P1 | 统一类型 + 求值器抽取 + 死代码删除(pre-cutover apply 路径)+ `DerivePartitionSpecRule` + annotation / policy(全 BestEffort,行为不变) | 12 | 3 | — |
| P2 | locator partition filter 贯通 + join PF derivation + union 并集 | 12+17 | 2-3 | P1 |
| P3 | thresholds config + 退化矩阵 | 15 | 1-2 | P2 |
| P4 | locator 前移双用 + positions state read + perf 验证 | 16 | 2 | P2(与 P3 并行) |
| P5 | partition state avsc family + 写入点 + 清理语义 | 22 | 2 | P1 |
| P6 | REPARTITION parser + executor + e2e | 23 | 2-3 | P1(建议 P3 后) |

每个 Phase 独立可验收、可合入;P1 是唯一的全局前置。

## 8. 实现期 verify items

1. vendored iceberg-0.9.0 的 `_pos` 谓词下推深度(P4):能进 row-group 级最好,
   否则 batch 层过滤。
2. `TableUpdate::AddSpec` / `SetDefaultSpec` 在 vendored 0.9 的可用性(P6):缺失则
   按 PATCH.md 先例补可见性 / API patch。
3. join coalescer 输出 chunk 的列绑定形态(P2):`delta_output_column` 从 ColumnId 到
   chunk 列 index 的 binder 细节。

## 9. 风险

1. **P1 迁移引入行为漂移**:用现有 partitioned aggregate SQL golden + tracing 字段
   断言锁死;resolution 报错时机前移(plan 时)是预期变化,错误信息保持可定位。
2. **branch partition specs 不一致假象**:target fields 全 branch 相同,差异只在
   lineage;任一 branch 不可证明即整体放弃剪枝,不假装兼容。
3. **阈值默认值选错**:默认保守 + perf baseline 反推;阈值只影响性能不影响正确性。
4. **locator / state read 语义耦合**(P4):两者输入为同一批 touched 旧行,加注释 +
   debug assert 守护;未来 locator 扩展时必须拆开。
5. **REPARTITION 期间并发 refresh**:与 refresh 同锁互斥 + scheduler 感知;abort 路径
   保证旧 spec + 旧数据完好。
6. **partition state 膨胀**:entries 上限 + 清空语义;SQLite 不承载百万分区。

## 10. 继承红线(不变)

- 不把 base partition key 当成 MV partition key。
- apply key 永远是 correctness identity;partition 只做剪枝和粒度控制。
- 不在 target locator 读完整 target 后再做 partition 过滤(本设计反向收紧:locator
  也要吃 partition filter)。
- 不恢复旧 `REFRESH ... FULL`;rebuild 语义只通过受控入口(REPARTITION / 后续
  RebuildPartitions)。
- partition contract 必须存在于 NovaRocks repository,不只存在于 target Iceberg
  metadata。
- 不引入新的 silent fallback:所有退化路径必须带 `fallback_reason` 可观测。
