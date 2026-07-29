---
id: ADR-0010
title: "Explicit query cancellation surface"
domain: [distributed-query-lifecycle]
status: active
supersedes: []
superseded-by: null
date: 2026-07-29
provenance:
  - "PR: #759"
  - "discussion: 2026-07-29 explicit query cancellation control plane"
code-anchors:
  - "novarocks/core/src/query_execution/control.rs (QueryControlPort)"
  - "novarocks/backend/src/fragment/service.rs (NativeFragmentService::cancel)"
---

## 问题

客户端如何可靠取消正在运行的分布式查询，同时不把 frontend session、distributed QueryId 与 backend 本地执行状态混为同一个控制面？

## 背景与执行事实

MySQL 连接断开不能保证 frontend 在结果尚未写出时立即可见；仅依赖连接断开无法成为可定位、可测试的取消入口。frontend coordinator 已拥有 distributed QueryId 和 fragment fanout bookkeeping，而 native backend 的 `QueryContextManager` 已是 pipeline、runtime filter 与 connector teardown 的本地取消事实源。两者不能共享进程内状态。

## 考虑过的选项

1. 公开 distributed QueryId 并以它作为客户端取消参数：可直接对应 coordinator，但要求额外公开 query list、编码与权限模型；一个 statement 也可能对应多个 execution。
2. 新建 HTTP 或 gRPC 管理接口：便于测试，但会增加认证、部署和第二套 client control surface。
3. 继续使用 socket disconnect：没有正式语义，且在无首包结果时不可可靠观察。
4. 使用 MySQL `KILL QUERY <connection_id>`，由 frontend 将 client locator 映射到当前 statement：复用已有客户端协议，并让 QueryId 维持内部身份。

## 裁决

选择 `KILL QUERY <connection_id>` 作为唯一新增 client-facing surface。`connection_id` 仅是 locator；当前 statement 的内部身份为 `(session_epoch, connection_id, generation)`，distributed QueryId 继续只由 frontend coordinator 持有。

每个 statement 创建一个 first-wins `QueryCancellationSource`，KILL、deadline、disconnect 与 server shutdown 共同请求它；所有执行路径只接收 read-only view。frontend query-control registry 拥有 session/statement state，coordinator 仍只负责既有 failure/cancel dispatch bookkeeping。BE 收到 cancel 后先写既有 `QueryContextManager`，再终止 request 与本地 query context 的 fragment handle 并集。

## 接受的妥协（诚实记录）

第一版只允许相同 authenticated principal，且不提供 `SHOW PROCESSLIST`、`KILL CONNECTION` 或公开 QueryId。这限制了管理权限模型和可观测性，但避免在取消语义尚未稳定时同时引入完整 process-list、特权与跨协议认证设计。timeout 后旧 worker 尚未退出时，同一连接的新 statement 会被拒绝而非并发执行；这是为了防止旧 generation 误伤新 query。

## 何时重新评估

- 引入多用户、角色或真正 privilege service 时，扩展 `PermissionDenied` 判定而不改变 statement state machine。
- 需要远程控制面或主动 cancel delivery 时，QLC-2 可以替换 FE 到 BE 的 transport，但不得新增第二个 client contract。
- 需要管理型 query list、跨 session 审计或运营 API 时，再单独裁决 QueryId 的可见性与权限模型。
