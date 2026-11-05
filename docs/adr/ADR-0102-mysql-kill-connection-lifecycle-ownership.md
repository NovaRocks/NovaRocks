---
id: ADR-0102
title: "MySQL KILL connection lifecycle ownership"
domain: [distributed-query-lifecycle, frontend]
status: active
supersedes: [ADR-0010]
superseded-by: null
partially-superseded-by: [ADR-0213]
partially-superseded-scope:
  ADR-0213: "MySQL connection registry, connection-token control port and connection-addressed KILL are retired; application authorization, original cancellation identity, protocol/application ownership separation and actual-exit rules remain active."
date: 2026-08-23
provenance:
  - "discussion: 2026-08-22 MySQL KILL connection lifecycle semantics"
code-anchors:
  - "novarocks/frontend/src/client_connection.rs (ClientConnectionControlPort)"
  - "novarocks/frontend/src/query_control.rs (FrontendQueryControl)"
  - "novarocks/frontend/src/mysql/mod.rs (run_mysql_server_until_shutdown)"
---

## 问题

MySQL 的裸 `KILL <id>` 与 `KILL CONNECTION <id>` 如何在不让 frontend 持有 socket、protocol task 或 MySQL registry 的前提下，精确终止一个已认证连接，同时保持 `KILL QUERY <id>` 的 statement-only 语义？

## 背景与执行事实

ADR-0010 以 `KILL QUERY <connection_id>` 建立了 frontend session owner 到当前 statement cancellation source 的显式入口；它刻意不提供 connection kill。该限制不再满足 MySQL client contract：裸 `KILL` 等价于 `KILL CONNECTION`，必须在取消目标活动 statement 后关闭目标连接；`KILL QUERY` 则必须保留连接，目标 idle 时是 no-op OK。

frontend 已拥有认证后 session 的可见性、same-principal 判定和 statement cancellation source，却不拥有 listener、`AsyncMysqlIntermediary`、socket 或 connection task。MySQL protocol owner 可以观察 accept 到 task 退出的整个生命周期，但未认证连接不应成为 SQL KILL target。只按可见的 `u32` connection ID 定位又会在 ID 重用后产生 ABA：延迟的 kill 可能误中后继连接。

因此需要两个不同 owner 的 registry。frontend query-control registry 仅保存已认证 target 的 principal、statement state 和 protocol connection token；protocol registry 保存实际 task 的 termination capability，也覆盖尚未认证的连接。前者裁决 SQL target 与授权，后者裁决 exact connection resource 是否仍可终止。

## 考虑过的选项

1. 保留 ADR-0010 的 `KILL QUERY` 唯一 surface。它没有新的 lifecycle 机制，但裸 `KILL` 与 `KILL CONNECTION` 继续偏离 MySQL 语义，且无法关闭 idle connection。
2. 让 frontend 直接持有 MySQL registry、socket 或 task abort handle。它能缩短调用路径，却会让 application session owner 依赖 protocol resource，并绕过 ADR-0012 的 wire/application 分界；直接 abort 也无法把连接终止原因先交给活动 statement 的 first-wins cancellation source。
3. 让 MySQL protocol registry 自行按裸 ID 授权和执行 SQL KILL。它让 protocol 知道 principal、KILL kind 和 SQL errno，既把 session semantics 移出 frontend，也不能防止 ID 重用误杀。
4. frontend 通过 transport-neutral port 请求 protocol owner 终止 exact generation-fenced token。它保留两侧 owner，能在 protocol side 原子管理分配、注册、终止和注销；代价是必须维护两份具有不同职责的 registry，并显式处理 lookup 与 task exit 之间的 stale 竞态。

## 裁决

选择选项 4，并以本 ADR supersede ADR-0010。frontend 定义并只依赖 `ClientConnectionToken { connection_id, generation }`、`ClientConnectionControlPort`、typed termination reason 与 outcome；MySQL protocol owner 实现该 port，独占 connection registry、allocator、termination sender、intermediary/socket 和 task lifecycle。frontend 不获得 MySQL registry、socket、fd、`JoinSet` 或 task abort capability。

protocol registry 在一个临界区内分配非零 MySQL-visible connection ID、生成非零 generation 并注册 sender；它跳过 live ID，只允许 exact lease 注销后的 ID 重用，ID 或 generation 穷尽时 fail closed。terminate 与 unregister 都匹配完整 token，因此旧 connection 的 delayed terminate 或 Drop 不能影响同 ID 的新 connection。认证成功才把 token 交给 frontend session；未认证 connection 不在 SQL KILL target space。

frontend 对 `KILL QUERY` 先按 session epoch、target 存在性和 same-principal 授权，再只请求当前 statement 的既有 cancellation source。活动 statement 的 Requested 或 AlreadyRequested 返回 OK；目标 idle 也返回 OK、保持连接且不触碰 connection signal。裸 `KILL` 与 `KILL CONNECTION` 先做同一授权，再向 port 发送 exact-token termination request。unknown target 或 protocol lookup 后已 stale 的 token 返回 1094；cross-principal 返回 KILL 专属 1095，不为 `root`、角色或管理员身份设置例外。

每个 protocol connection task 消费协作式 first-wins termination signal。首次成功锁存 signal 是 KILL CONNECTION 的同步完成点：Requested 与 AlreadyTerminating 都成功，不等待 socket close、frontend session drop 或 BE resource teardown。收到 signal 后，task 先以 `ExplicitKillConnection` 原因请求已存在的 statement cancellation source，再退出 intermediary 并关闭 socket；self-kill 因关闭请求者自己的 socket，不保证客户端能够收到 OK 包。不得把 `AbortHandle`、直接 socket shutdown 或 Unix fd watcher 作为正常 KILL path。

server shutdown 先以既有 `ServerShutdown` cancellation 请求活动 statement，再向同一 registry 广播协作式 connection termination，随后停止 listener 并 drain；有界 `abort_all()` 保留为卡死 task 的最终兜底。ADR-0012 继续 active：它规定 frontend 拥有认证后 session admission、routing 和 SQL semantics，wire adapter 只拥有 protocol resource；本决策正是该边界在 connection lifecycle 上的延伸。

## 接受的妥协（诚实记录）

这个设计有意保留 frontend session registry 与 protocol connection registry 两份状态，并接受“frontend 授权成功后，protocol port 仍可能报告 Stale”的竞态。这样比共享一个 registry 或把 socket 交给 frontend 多了一次 lookup 和更多 lifecycle 测试；选择它是为了避免跨 owner 共享可变 protocol resource，而不是因为双 registry 更简单。

第一版仍只允许同一 authenticated principal 互相 KILL，不提供 `SHOW PROCESSLIST`、跨用户管理员权限、公开 distributed QueryId 或第二个 HTTP/gRPC control plane。这并不能满足完整运维管理需求，但在固定 listener user 与未成型 privilege service 的现状下，扩张权限模型会掩盖连接终止的核心语义。普通客户端断连的跨平台检测也不随本决策修复：KILL CONNECTION 和 shutdown signal 必须跨平台，而既有 Unix watcher/非 Unix 差异仍需独立处理。

KILL 成功不等待 BE 资源收敛，且 self-kill 不保证 OK 可见；这是以与 MySQL 客户端可观察行为相容的低延迟锁存点，换取 teardown 仍异步进行。生产验证必须观察最终 frontend/BE/connector 资源收敛，不能把该观察延迟塞进同步 SQL response。

## 何时重新评估

- 引入稳定的多用户认证、角色或 privilege service 时，单独扩展 authorization contract；不得让 protocol registry 推断 SQL 权限，也不得硬编码特殊用户名。
- 需要 process list、审计、跨进程管理面或公开 query identity 时，先裁决 target discovery、权限和可观察性，再复用 exact token termination，而不是把裸 socket/task capability 暴露出去。
- 当第二个正式 inbound protocol 需要相同 connection lifecycle 语义时，评估 neutral port 是否应迁入更低层的 protocol-neutral crate；在此之前不为假想复用移动 owner。
- 当需要一致处理普通 peer disconnect 时，单独设计 intermediary 的 EOF/read/write lifecycle，并证明它不会覆盖 KILL、deadline 或 shutdown 的 first-wins reason。
- 当 allocator/generation 接近实际耗尽，或 registry 锁成为经测量的 accept/control bottleneck 时，依据可复现负载重新裁决 allocator data structure；不得以重用裸 ID 或删除 generation fence 换取容量。
