---
id: ADR-0028
title: "Frontend-owned MV refresh lifecycle"
domain: [frontend-mv, provider-spi]
status: active
supersedes: []
superseded-by: null
date: 2026-08-01
provenance:
  - "discussion: MVX-2 frontend refresh lifecycle"
code-anchors:
  - "novarocks/spi/src/connector/mutation.rs (ConnectorCommittedVersion)"
  - "novarocks/frontend/src/mv (refresh lifecycle)"
---

## 问题

如何让当前 materialized-view refresh 的 application owner、external commit truth 与 durable evidence 保持单一且可恢复？

## 背景与执行事实

旧 refresh 路径由 aggregate engine 串联 planning、staging、catalog publication、recovery state 和 repository 写入。这样既无法确认 commit outcome 的 application owner，也迫使 Frontend 解码 provider-private receipt 或从错误字符串猜测 truth。

## 考虑过的选项

1. 保持 aggregate engine 作为 lifecycle facade。实现成本较低，但长期保留双 owner 和错误的 crate 边界。
2. Frontend 直接解码 Iceberg receipt 并调用 catalog。能够快速取得 snapshot，但泄漏 provider runtime 和 codec。
3. Frontend 编排，SQL/provider/Execution 保持各自职责，并通过 bounded typed SPI fact 传递 commit truth。

## 裁决

采用选项 3。

- Frontend 拥有当前 refresh attempt、StateStore ledger、native query orchestration、outcome解释、cleanup与用户结果。
- SQL拥有无副作用 refresh preparation；provider拥有metadata observation、staging commit、guarded publication与authoritative reconciliation；Backend只执行和staging。
- provider 可返回 `ConnectorCommittedVersion`：它是带摘要校验的有界opaque payload和可选typed snapshot ID。Frontend只持久化并原样传回，不解码payload。
- mutation/write lease 必须从同一个retained control generation派生；planning后不得重新获取later current generation。
- 新attempt使用Frontend-owned v3 MV ledger；historical operation record只作为legacy recovery的只读兼容输入。
- `CommitUnknown`只在exact retained capability上reconcile一次；publication已知提交后禁止破坏性补偿。

## 接受的妥协（诚实记录）

SQLX crate 尚未物理化，因此SQL owner的实现短期仍在现有package。MVX-3前也不会自动收敛v3 unresolved record；这是为了先消除双owner和双journal，而非因为自动恢复已不重要。

## 何时重新评估

- SQL owner物理迁移到独立crate时；
- MVX-3实现跨incarnation recovery或multi-FE takeover时；
- 新provider不能以bounded committed version表达其publication truth时。
