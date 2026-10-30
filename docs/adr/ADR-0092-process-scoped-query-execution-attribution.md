---
id: ADR-0092
title: "Process-scoped query execution attribution"
domain: [distributed-query-lifecycle]
status: active
supersedes: []
superseded-by: null
date: 2026-08-20
provenance:
  - "discussion: 2026-08-20 query execution process attribution"
code-anchors:
  - "novarocks/types/src/identity.rs (QueryIdAttribution)"
  - "novarocks/query-application/src/coordination/supervisor.rs (ProcessQueryIdAllocator)"
---

## 问题

独立 Frontend 进程生成的 native query execution identity 如何既保持 FE/BE 的固定 128-bit wire 形状，又能在日志、tombstone 与只读调试快照中可靠说明它属于哪个 Frontend process？

## 背景与执行事实

`QueryId` 已是两个 `i64` 组成的 128-bit value，而 `QueryExecutionId` 另外携带非零 `AttemptId`。它们经 Protocol 的 lifecycle codec 在 Frontend 和 Backend 之间无损传输；MySQL 的 `connection_id: u32` 则是 session admission 与 `KILL QUERY` 的独立公开 identity，不能与内部 query identity 混用。

旧 allocator 对每个 query 随机生成 high half，并以固定的大步长累加 low half。随机性足以降低碰撞概率，却不能回答同一 process 的连续请求是否属于同一个 namespace，也不能让收到 wire identity 的 Backend 或 Frontend inactive lookup 给出稳定、结构化的归属诊断。

## 考虑过的选项

1. **继续每次随机 high half，并只改善字符串日志。** 改动小，但“同一 process”仍不可观测，日志只能重复裸值，无法判定 local/foreign。
2. **引入 StateStore/global counter、leader 或 RPC allocator。** 可给 identity 增加跨进程协调，但把 query admission 热路径耦合到 durable membership/availability，也会提前引入 lease、fencing 与故障恢复语义。
3. **在 Frontend process 启动时冻结随机 namespace，再分配连续 local sequence，并将它们映射到既有两半 `QueryId`。** 接收方可从已有 wire value 派生 attribution；不新增协议字段或跨进程 authority。
4. **把内部 QueryId 暴露成 MySQL query id，或扩展 `KILL QUERY` 接受它。** 这会改变已经按 connection id 定义的 server protocol 与 session owner，不是诊断改进所必需。

## 裁决

选择选项 3。Frontend process 只在 allocator 构造时冻结一次高熵 `QueryProcessNamespace`；每个 admitted query 取得严格递增、非零的 `LocalQuerySequence`，并以这两个 typed value 构造现有 `QueryId`。序列耗尽必须失败，不能回绕、重用或静默降级。

`novarocks-types` 拥有 namespace、sequence、从 `QueryId` 派生 attribution 以及稳定 formatter 等纯 value semantics；它不得拥有随机源、atomic allocator、Frontend runtime 或 membership。Frontend 拥有 allocator、启动日志以及“本 namespace / foreign namespace” lookup 判断；Backend 仅从收到的 identity 派生并记录同一 attribution，不建立第二个 namespace authority。

Proto `UniqueId { hi, lo }`、`QueryExecutionId.attempt_id`、所有 lifecycle key/digest 与 raw `hi:lo:attempt` correlation token 均保持不变。MySQL handshake、`COM_QUERY`、error mapping 与 `KILL QUERY <connection_id>` 也保持不变；内部 `QueryId` 不进入 MySQL wire。

## 接受的妥协（诚实记录）

namespace 是 process-incarnation entropy，而不是 durable deployment identity；Frontend 重启后会得到新 namespace，两个真实 Frontend 的 namespace 也不由 StateStore 登记或验证。我们接受这点，是因为当前目标是无额外协调依赖的 collision-resistant attribution 与诊断，而不是 query takeover、membership fencing 或跨进程 session migration。用 `i64` carrier 映射无符号 namespace 的 bit pattern 也使面向人类的日志必须走显式 formatter，不能把 generic UUID display 误当作 ownership contract。

这不会使两个真实 Frontend 的 end-to-end orchestration 自动得到证明；单元测试可用注入 namespace 证明 allocator 语义，native 1FE+NBE 测试只证明同一 frozen identity 跨 FE/BE 边界保持一致。

## 何时重新评估

- Frontend deployment identity、process lease 或 durable membership 已有明确 owner，且需要把 process namespace 与它们绑定或验证时；
- 产品需要按内部 QueryId 公开查询、跨 FE `KILL QUERY`、session migration 或 coordinator takeover 时；
- namespace 熵、sequence 容量或日志表示不能满足实际查询率、保留期或运维关联需求时；
- lifecycle protocol 需要新增可互操作的 identity 字段而非从 `QueryId` 的固定 carrier 派生时。
