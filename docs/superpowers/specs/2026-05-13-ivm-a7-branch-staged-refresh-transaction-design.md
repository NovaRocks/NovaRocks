# IVM-A7 Branch-Staged Refresh Transaction

**日期**：2026-05-13
**状态**：对话设计已确认；书面 spec 等待用户 review
**范围**：定义 Iceberg-backed materialized view 的长期 refresh transaction 协议，使 Iceberg 表元数据与 NovaRocks 内部 MV 元数据保持可恢复的一致性。

## 背景

NovaRocks 的 Iceberg-backed MV refresh 现在有两个独立 truth source：

- Iceberg target table metadata：table refs、snapshots、manifest lists、
  manifests、data files、delete files。
- NovaRocks MV metadata：MV 定义 SQL、base table refs、上次 refresh 的
  base snapshots、上次发布的 target snapshot、active refresh id、refresh
  transaction state。

这两个系统不能放进同一个物理原子事务中提交。当前
`src/engine/mv/iceberg_refresh.rs` 的 refresh 路径会先把 MV 输出写入 target
Iceberg table，然后再 finalize NovaRocks metadata。如果进程在 Iceberg commit
成功后、metadata finalize 前崩溃，Iceberg 可能已经暴露了新的 target snapshot，
而 NovaRocks 仍然认为 MV 停在旧 snapshot。

#124 已经引入了控制面基础：`MvMetaRepository`、`StoredMvDefinition`、
`StoredMvRefresh`、`refresh_in_progress`、`active_refresh_id`。A7 要把这些组件
推进成完整 publish protocol，而不是继续把它们当作 best-effort in-progress
标记。

## 问题

把 refresh 结果直接 commit 到 Iceberg target 的 `main` ref，会形成一个不适合
长期产品语义的顺序：

```text
Iceberg main 先变化
NovaRocks MV metadata 后补账
```

这个顺序只有在 NovaRocks 能证明每个 target snapshot 都来自某次特定 refresh
时才可恢复。否则服务重启时无法区分这些情况：

- 上一次 refresh 已经 commit 成功，只是 metadata finalize 没来得及执行；
- 外部 writer 修改了 target table；
- catalog 返回 commit-unknown，真实 commit 结果不明确；
- refresh 根本没有走到 Iceberg commit，只留下了过期的 in-progress metadata。

面向产品的 MV 系统里，Iceberg `main` 应该表示“已经正式发布，且 NovaRocks
metadata 能解释的 MV 版本”，而不是某次 half-finished refresh 写出的最新结果。

## 设计决策

A7 的目标架构采用 **branch-staged refresh transaction**。

每次 refresh 先把输出写到临时 Iceberg branch。只有 refresh 结果完成并通过校验
之后，才推进 target table 的 `main` ref。NovaRocks metadata 仍然是 intent、
recovery、audit state 的控制面。

目标协议如下：

```text
1. 写入 NovaRocks refresh intent。
2. 从当前 expected target main snapshot 创建 Iceberg staging branch。
3. 将 refresh 输出写入 staging branch。
4. 在 NovaRocks metadata 中记录 staging commit outcome。
5. 原子发布：将 main 从 expected snapshot 推进到 staging snapshot。
6. 记录 publish outcome，并 finalize NovaRocks metadata。
7. 删除 staging branch。
```

已有的 intent / external outcome / finalize 三段式能力仍然是控制面的组成部分，
但它不是一个单独的中间特性。A7 应该直接落成 branch-staged 目标协议。

## 产品语义

target Iceberg table 对 Spark、Trino 等外部系统来说仍然是一张普通 Iceberg 表。
它的 `main` ref 只有一个含义：

```text
main = 最后一次成功发布的 MV 版本
```

refresh 过程中的数据只存在于 NovaRocks 私有 staging branch：

```text
__nova_mv_refresh_<mv_id>_<refresh_id>
```

外部系统读取 `main` 时不会看到半完成 refresh。NovaRocks 如果在 publish 前崩溃，
`main` 仍停在旧 MV 版本。NovaRocks 如果在 publish 后、metadata finalize 前崩溃，
恢复流程可以证明 `main` 指向 active refresh 的 staging snapshot，并补齐 metadata
finalize。

Iceberg MV target 必须支持 branch/ref 操作。无法 create branch、commit to
branch、带 ref requirement 更新 `main`、drop staging branch 的 catalog/table
必须 fail fast。不能 fallback 到直接写 `main`，否则同一个 MV 功能会在不同环境
下呈现两套一致性语义。

## 非目标

- 不为 Iceberg MV refresh 保留 direct-main commit fallback。
- 不把“refresh 期间不要重启 server”作为 correctness 假设。
- 不用恢复时强制 full refresh 来掩盖不明确的事务状态。
- 不在 A7 中强制完成整个 `IcebergMvBackend` 路由重构。A7 应该兼容后续 A6 清理，
  但可以先落在当前 Iceberg MV refresh 模块内。
- 不把 NovaRocks 的 MV correctness state 只写进 Iceberg table properties。
  Iceberg target 仍然是普通外部表；MV 关系元数据由 NovaRocks 自己维护。

## 控制面数据模型

`StoredMvRefresh` 应成为 Iceberg MV refresh 的 durable transaction record。
具体 Rust 字段名可以在实现阶段调整，但持久化信息必须覆盖：

```text
refresh_id
mv_id
state
target_catalog
target_namespace
target_table
staging_branch
expected_main_snapshot_id
staging_snapshot_id
published_snapshot_id
base_snapshots
base_table_uuids
rows
commit_marker
```

`expected_main_snapshot_id` 是 refresh 开始前观察到的 target `main` snapshot。
publish 时必须用它作为 compare-and-swap guard。

`staging_snapshot_id` 是 refresh 输出写入 staging branch 后产生的 snapshot。

`published_snapshot_id` 是 publish 后 `main` 指向的 snapshot。正常情况下它等于
`staging_snapshot_id`。

`commit_marker` 用于让恢复流程证明某个 Iceberg snapshot 是 active NovaRocks
refresh 产生的。marker 至少应包含 `refresh_id`、`mv_id` 和一个稳定生成的 token。
它必须能从 target snapshot metadata 中读取，不能只存在于进程内存。

`StoredMvDefinition` 继续表示 durable MV definition 和最后一次成功 refresh 的摘要：

```text
last_refresh_snapshots
last_refresh_table_uuids
last_refreshed_iceberg_snapshot_id
last_refresh_rows
refresh_in_progress
active_refresh_id
```

refresh 处于非终态时，definition 指向 active refresh。进入终态后必须清空
`refresh_in_progress` 和 `active_refresh_id`。

## 状态机

refresh transaction 使用以下状态：

```text
IntentCreated
StagingCommitted
PublishCommitted
Finalized
Aborted
CommitUnknown
```

### IntentCreated

NovaRocks metadata 已经记录 refresh intent。记录中包含 target identity、
expected `main` snapshot、staging branch name、base snapshot pins。Iceberg 侧还
没有确认本次 refresh 的 staging snapshot。

恢复时如果看到该状态，且 staging branch 不存在、`main` 仍等于
`expected_main_snapshot_id`，可以将 refresh 标记为 `Aborted`。

### StagingCommitted

refresh 输出已经 commit 到 staging branch。staging branch 指向
`staging_snapshot_id`，且 snapshot marker 能证明它属于 `refresh_id`。此时 target
`main` 预期尚未变化。

恢复时如果看到该状态，且 `main` 仍等于 `expected_main_snapshot_id`，说明 refresh
尚未发布。恢复流程删除 staging branch，并将 refresh 标记为 `Aborted`。

### PublishCommitted

target `main` 已经从 `expected_main_snapshot_id` 推进到 `staging_snapshot_id`。
NovaRocks metadata 不一定已经更新 MV definition 的 last-successful 字段。

恢复时如果看到该状态，应该 finalize MV metadata 并删除 staging branch。

### Finalized

NovaRocks metadata 已经记录最终 base snapshots、target snapshot、row count，并
清空 active refresh pointer。staging branch 应该已经删除，或者可以安全地幂等
删除。

### Aborted

refresh 没有发布到 `main`。metadata 已进入终态，后续 refresh 不应再把该记录当作
active transaction。若上一次清理失败，可以幂等清理残留 staging branch。

### CommitUnknown

NovaRocks 无法证明 Iceberg commit 或 ref update 是否发生。该状态是自动 refresh
的安全终态。后续 refresh 必须 fail fast，直到人工或专门 repair 流程解决 mismatch。

## Publish 协议

publish 步骤是 ref-level compare-and-swap：

```text
assert main snapshot == expected_main_snapshot_id
set main snapshot = staging_snapshot_id
```

如果 publish 前 `main` 已经变化，NovaRocks 不能覆盖它。refresh 应进入
`CommitUnknown`，或者返回明确的 external modification 错误。

实现上应提供内部 Iceberg helper，而不是在 MV engine code 中直接拼
`TableUpdate` 序列。该 helper 负责：

- 加载最新 target table metadata；
- 校验 staging branch 存在且指向 `staging_snapshot_id`；
- 校验 staging snapshot marker 匹配 `refresh_id`；
- 用 expected-ref requirement 原子更新 `main`；
- 返回 published snapshot id；
- 将 commit-unknown outcome 与 definite failure 区分开。

删除 staging branch 属于 cleanup。finalize 后应尝试删除；恢复流程也应重试删除。
publish 已成功后，删除 staging branch 失败不能回滚已经发布的 refresh。

## 恢复协议

恢复流程在 server 启动时运行，位置应在 Iceberg catalogs restore 之后；同一个 MV
开始新 refresh 前也应先运行恢复。它扫描未完成的 Iceberg MV refresh records，并
用 target table 的当前 Iceberg metadata 做 reconcile。

自动恢复规则如下：

```text
IntentCreated:
  staging absent, main == expected
    -> mark Aborted
  staging present with matching marker, main == expected
    -> record StagingCommitted, drop staging, mark Aborted
  staging present with matching marker, main == staging
    -> record PublishCommitted, finalize
  otherwise
    -> CommitUnknown

StagingCommitted:
  staging present with matching marker, main == expected
    -> drop staging, mark Aborted
  staging present with matching marker, main == staging
    -> record PublishCommitted, finalize
  staging absent, main == expected
    -> mark Aborted
  otherwise
    -> CommitUnknown

PublishCommitted:
  main == published snapshot and marker matches refresh
    -> finalize and drop staging
  otherwise
    -> CommitUnknown
```

`Finalized` 和 `Aborted` 是终态。只要 refresh record 还记得 staging branch，恢复
流程可以为这两种状态重试 staging branch cleanup。

整体原则是保守恢复：只有当 target table state 能被证明属于当前 `refresh_id` 时，
才允许自动收敛。未知状态或外部修改必须 fail fast。

## 当前代码边界

A7 应通过聚焦改动接入现有模块。

`src/meta/repository/mv.rs`

- 扩展 refresh record payload 和状态机。
- 增加记录 staging commit、记录 publish commit、finalize、abort、mark
  commit-unknown 的 repository 方法。
- 继续把 active refresh rejection 集中在 repository 层。

`src/engine/mv/iceberg_refresh.rs`

- A7 阶段继续把 Iceberg MV lifecycle orchestration 放在这里。
- 将直接写 `"main"` 的 target commit 改成写 staging branch。
- 按协议顺序调用 publish/finalize helper。
- 增加 startup recovery 和 pre-refresh recovery 入口。

`src/connector/iceberg/commit`

- 确保 MV refresh 使用到的 commit actions 支持 non-`main` target refs。
- 提供 create staging branch、publish staging to main、drop staging branch 的
  helper。
- 保留 commit-unknown 作为独立错误分类。
- 支持 refresh identity 的 snapshot marker。

`src/engine/mod.rs`

- 在 metadata restore 阶段调用 Iceberg MV refresh recovery。调用点应在 Iceberg
  catalogs 可用之后、普通 SQL 流量可能 refresh 这些 MV 之前。

## 失败处理

publish 前的 definite failure 应 abort，并尽可能清理 staging state。

commit-unknown failure 不能被当作 definite failure 清理。refresh record 应进入
`CommitUnknown`，后续 refresh fail fast。诊断信息应包含 MV、refresh id、target
table、staging branch、expected main snapshot、observed main snapshot。

publish 后的 metadata commit failure 是可恢复的。因为 `main` 已经指向带 refresh
marker 的 snapshot，恢复流程应该 finalize metadata，而不是重试 data commit。

publish 前的 metadata commit failure 是可 abort 的。因为 `main` 仍停在 expected
snapshot，恢复流程可以删除 staging state 并将 refresh 标记为 aborted。

## 测试策略

### Repository Tests

- begin intent 持久化 staging branch、expected main snapshot、base snapshots、
  active refresh id。
- `IntentCreated -> StagingCommitted -> PublishCommitted -> Finalized` 清空
  `active_refresh_id`。
- `StagingCommitted -> Aborted` 清空 `active_refresh_id`。
- `CommitUnknown` 阻止同一 MV 的新 refresh。
- 已 finalized refresh 的重复 finalize 保持幂等。

### Iceberg Ref Tests

- 能从 expected `main` snapshot 创建 staging branch。
- commit 到 staging branch 不会推进 `main`。
- publish 校验 expected `main` 并将 `main` 推进到 staging snapshot。
- 如果 `main` 被外部修改，publish 失败。
- drop staging branch 幂等。
- 不支持 branch/ref 操作的 target fail fast。

### Engine and SQL Tests

使用可控 fault injection 模拟 refresh checkpoint 崩溃，不只依赖真实 `kill -9`
测试。每个 injection 必须留下与对应崩溃点等价的 durable metadata 和 Iceberg
state。

必须覆盖：

- intent 已写，staging branch 未创建：恢复后标记 aborted。
- staging branch 已 commit，`main` 未 publish：恢复后删除 staging，标记
  aborted，MV 仍读取旧版本。
- `main` 已 publish，metadata 未 finalize：恢复后自动 finalize，MV 读取新版本，
  下一次 refresh 不重复 append。
- active refresh 期间 `main` 被外部修改：恢复进入 `CommitUnknown`，后续 refresh
  fail fast。
- 成功 refresh 后没有 active refresh、没有 staging branch，metadata target
  snapshot 等于 Iceberg `main`。

## 验收标准

- Iceberg MV refresh 不会把未 finalize 的结果直接写入 `main`。
- publish 前 `main` 不变化。
- 已 publish 的 refresh 在重启后可以自动 finalize。
- 未 publish 的 staging commit 在重启后可以自动 abort。
- 未知或外部修改状态不会被猜测；必须 fail fast。
- 成功恢复后，NovaRocks metadata 与 Iceberg `main` snapshot 一致。
- 不支持 ref 能力的 target 给出明确错误，不静默 fallback 到 direct-main writes。
