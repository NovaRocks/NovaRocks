# MvBackend lifecycle 与 Iceberg backend 解耦设计

## 背景

NovaRocks 现在有两条物化视图存储路径：

- managed-lake 物化视图：目标表由 managed-lake table/runtime 模型承载。
- Iceberg-backed 物化视图：目标表是普通 Iceberg table，NovaRocks 只维护
  MV 与 base table 的刷新关系元数据。

当前代码已经把大部分 Iceberg target table 和 refresh 逻辑移动到
`src/engine/mv/iceberg_refresh.rs`，MV 关系元数据也已经进入
`MvMetaRepository`。但 statement dispatch 仍然只注册了一个
`ManagedLakeMvBackend`，`src/engine/mv_flow.rs` 仍硬编码选择 `"managed"`
backend。managed backend 再在内部识别 Iceberg MV 并转发到
`engine::mv::iceberg_refresh`。

这让 `MvBackend::refresh_mv` 继续保持黑盒形态，也让 Iceberg-backed MV 在架构
上看起来仍是 managed-lake 的子路径。实际上它的 target table、commit 协议、
recovery 模型和 metadata 边界都已经不同。

## 目标

1. 把 refresh lifecycle 固化成 backend 接口，使后续 A1/A2/A3 能依赖它。
2. 注册独立的 `IcebergMvBackend`，不再通过 `ManagedLakeMvBackend` 转发
   Iceberg MV。
3. `mv_flow` 只负责 statement 级 routing 和通用 lifecycle 编排。
4. 保留 A7 branch-staged Iceberg refresh transaction 语义，包括 recovery 和
   commit-unknown 处理。
5. 保持现有 managed-lake MV 与 Iceberg-backed MV 的 SQL 行为稳定。

## 非目标

- 本次不实现 A1 pipeline sink rewrite。
- 本次不实现 A2 任意 snapshot range 规划。
- 本次不实现 A3 多 base table snapshot pinning，但接口必须为多 base 做好形态。
- 不引入一套与 `MvMetaRepository` 平行的 metadata transaction 系统。
- 不把 MV 关系元数据写进 Iceberg table properties 作为 correctness source。

## 选定方案

采用 **backend-owned lifecycle + shared typed refresh contracts**。

`MvBackend` 仍然是 DDL 和 refresh 语义的 backend 所有权边界，但 refresh 不再是
单个黑盒方法，而是拆成可见 lifecycle：

```rust
pub(crate) trait MvBackend: Send + Sync {
    fn name(&self) -> &'static str;

    fn create_mv(&self, req: CreateMvRequest) -> Result<(), String>;
    fn drop_mv(&self, req: DropMvRequest) -> Result<(), String>;
    fn list_mvs(&self, req: ListMvsRequest) -> Result<QueryResult, String>;

    fn plan_refresh(&self, req: RefreshRequest) -> Result<RefreshPlan, RefreshError>;
    fn execute_refresh(
        &self,
        plan: &RefreshPlan,
        ctx: &mut RefreshCtx,
    ) -> Result<RefreshOutcome, RefreshError>;
    fn commit_refresh(
        &self,
        outcome: RefreshOutcome,
        ctx: &mut RefreshCtx,
    ) -> Result<(), RefreshError>;
    fn rollback_refresh(
        &self,
        outcome: Option<RefreshOutcome>,
        ctx: &mut RefreshCtx,
    ) -> Result<(), RefreshError>;
}
```

请求 wrapper 的具体字段可以按实现细节微调，但 lifecycle 形态是设计约束。
`mv_flow` 必须按 `plan -> execute -> commit` 调用，失败时根据错误分类决定是否
调用 `rollback`。

## 路由边界

`mv_flow` 停止硬编码 `"managed"`。

路由规则：

- `CREATE MATERIALIZED VIEW`：根据 statement property 或配置默认值解析
  `storage_engine`，然后选择对应 backend。
- `REFRESH MATERIALIZED VIEW`：根据当前 catalog、database、statement name 解析
  target；读取 MV relationship metadata；Iceberg target 走 `IcebergMvBackend`，
  managed-lake target 走 `ManagedLakeMvBackend`。
- `DROP MATERIALIZED VIEW`：使用与 refresh 相同的 metadata-first routing。
- `SHOW MATERIALIZED VIEWS`：由 `mv_flow` 聚合所有注册 backend 的 `list_mvs`
  结果。每个 backend 只返回自己 storage engine 的行，最终列格式、过滤和排序
  通过共享 helper 保持现有行为。

`mv_flow` 不理解 Iceberg staging branch、managed-lake tablet、snapshot diff、
aggregate state file 等 backend 内部细节。

## Refresh contract

共享 lifecycle 模型放在 `src/engine/mv/lifecycle.rs`，避免
`src/connector/backend.rs` 成为 MV 状态模型的大杂烩。

核心结构：

```rust
pub(crate) struct RefreshRequest {
    pub target: MvTarget,
    pub current_catalog: Option<String>,
    pub current_database: String,
    pub statement: RefreshMaterializedViewStmt,
}

pub(crate) struct RefreshPlan {
    pub mv_id: Option<i64>,
    pub target: MvTarget,
    pub storage_engine: MvStorageEngine,
    pub mode: RefreshMode,
    pub base_refs: Vec<MvBaseRef>,
    pub snapshot_pins: BTreeMap<String, Option<i64>>,
    pub backend_plan: BackendRefreshPlan,
}

pub(crate) struct RefreshOutcome {
    pub mv_id: Option<i64>,
    pub target: MvTarget,
    pub rows: Option<i64>,
    pub base_snapshots: BTreeMap<String, i64>,
    pub base_table_uuids: BTreeMap<String, String>,
    pub target_snapshot_id: Option<i64>,
    pub backend_outcome: BackendRefreshOutcome,
}

pub(crate) struct RefreshCtx {
    pub refresh_id: Option<i64>,
    pub expected_target_snapshot_id: Option<i64>,
    pub recovery_required: bool,
}
```

`RefreshPlan` 必须显式保留 `base_refs` 和 `snapshot_pins` 集合。即使第一版仍只
支持单 base projection/filter Iceberg MV，也不能把公共接口写成单值，否则 A2
snapshot range 和 A3 多 base snapshot locking 会再次破坏接口。

`RefreshCtx` 不是 metadata transaction 层。它只是 lifecycle 调用栈中的内存上下文，
用于在 `mv_flow` 和 backend 之间传递 refresh id、expected target snapshot 和
recovery intent。持久化 intent、commit record、finalize record 与 recovery truth
仍以 `MvMetaRepository` 为事实来源。

Backend 私有 payload 使用强类型 enum：

```rust
pub(crate) enum BackendRefreshPlan {
    ManagedLake(ManagedLakeRefreshPlan),
    Iceberg(IcebergRefreshPlan),
}

pub(crate) enum BackendRefreshOutcome {
    ManagedLake(ManagedLakeRefreshOutcome),
    Iceberg(IcebergRefreshOutcome),
}
```

共享层只读取公共字段。每个 backend 负责验证自己收到的是对应 variant。

## 失败语义

Refresh error 需要机器可读分类，不能只有字符串，因为 rollback 行为取决于外部
commit 是否可能已经可见。

```rust
pub(crate) enum RefreshErrorKind {
    UserError,
    PreCommitFailed,
    CommitFailedKnownUncommitted,
    CommitFailedKnownCommitted,
    CommitUnknown,
    MetadataFinalizeFailed,
}

pub(crate) struct RefreshError {
    pub kind: RefreshErrorKind,
    pub message: String,
}
```

`mv_flow` 的处理规则：

- plan failure：直接返回错误，不调用 execute、commit、rollback。
- execute failure：如果有 partial outcome 就带上，没有则传 `None`；调用
  rollback，然后返回原始 execute 错误，并附带 rollback 错误信息。
- commit `UserError`、`PreCommitFailed`、`CommitFailedKnownUncommitted`：调用
  rollback，返回原始 commit 错误，并附带 rollback 错误信息。
- commit `CommitFailedKnownCommitted` 或 `MetadataFinalizeFailed`：不做破坏性
  rollback，触发或保留 recovery/finalize 状态。
- commit `CommitUnknown`：保留或标记 commit-unknown 状态，返回明确错误。后续
  startup 或 pre-refresh recovery 通过现有 marker/staging-branch 协议 reconcile。

Managed-lake 初期可以按实际写入边界把大部分非用户错误归类为
`PreCommitFailed` 或 `MetadataFinalizeFailed`。Iceberg 必须保留 A7 的语义，不能
把 commit-unknown 简化成 rollback。

## 模块拆分

预期模块所有权：

- `src/engine/mv/lifecycle.rs`
  定义共享 request、plan、outcome、context、target、storage engine、refresh mode
  和 error 类型。
- `src/engine/mv_flow.rs`
  解析 backend routing，并运行通用 lifecycle 状态机。
- `src/engine/mv/iceberg_backend.rs`
  定义并注册 `IcebergMvBackend`。它拥有 Iceberg MV lifecycle 实现，并委托给
  Iceberg refresh helper。
- `src/engine/mv/iceberg_refresh.rs`
  保留 Iceberg refresh 算法，但拆出 backend 内部可调用的 plan、execute、
  commit、rollback、recovery helper。
- `src/connector/starrocks/managed/backend.rs`
  保留 `ManagedLakeMvBackend`，并实现新的 lifecycle 接口。
- `src/connector/starrocks/managed/mv_ddl.rs`
  移除 Iceberg create/drop forwarding，只保留 managed-lake DDL 逻辑和真正共享的
  helper。
- `src/connector/starrocks/managed/mv_refresh.rs`
  移除 Iceberg refresh forwarding，只保留 managed-lake refresh 逻辑。
- `src/connector/mod.rs`
  同时注册 `ManagedLakeMvBackend` 和 `IcebergMvBackend`。

## 测试计划

单元测试：

- 增加用于 `mv_flow` lifecycle orchestration 的 mock backend。
- 验证 plan failure 不调用 execute 和 rollback。
- 验证 execute failure 调用 rollback，且不调用 commit。
- 验证 commit `PreCommitFailed` 调用 rollback。
- 验证 commit `CommitUnknown` 不做破坏性 rollback。
- 验证 rollback 失败时保留原始错误，并附带 rollback 错误信息。

路由测试：

- `CREATE MATERIALIZED VIEW ... PROPERTIES('storage_engine' = 'iceberg')`
  到达 `IcebergMvBackend`。
- `REFRESH` 和 `DROP` 通过 MV relationship metadata 路由，而不是探测
  managed-lake runtime。
- `SHOW MATERIALIZED VIEWS` 保持现有列、过滤和排序行为。

回归测试：

- 跑 `iceberg_refresh` 和 `meta_repository` 相关 focused Rust tests。
- 跑 `iceberg-ivm`，保护 Iceberg-backed MV target 行为。
- 跑 `mv-on-iceberg`，保护 managed-lake MV over Iceberg base 行为。
- 实现提交前跑 `cargo fmt` 和目标 `cargo test` 集合。

## 实现注意事项

实现应从当前集成目标拉分支。如果本地 checkout 有无关 dirty files，只 stage 属于
本改动的文件。

第一版实现要控制行为变化：

- 保留现有 SQL-visible error，除非 lifecycle split 需要更精确的内部 error class。
- 保留现有全局 MV refresh lock，除非后续设计明确改变并发语义。
- 保留 Iceberg staged-refresh recovery 作为正确性边界。
- 避免把 Iceberg table properties 用作 MV relationship truth。

## 验收标准

- `MvBackend` 暴露 refresh lifecycle 阶段，不再只有单个 refresh 黑盒。
- `IcebergMvBackend` 独立注册，Iceberg MV statement 不再通过
  `ManagedLakeMvBackend` 转发。
- `mv_flow` 执行通用 lifecycle orchestration 和 backend routing，不包含
  backend-specific refresh 语义。
- 选定验证范围内，现有 Iceberg-backed MV 和 managed-lake MV 回归测试继续通过。
- Iceberg refresh commit-unknown 仍可通过 A7 marker/staging-branch 协议恢复。
