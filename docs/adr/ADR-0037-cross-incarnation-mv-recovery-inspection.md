---
id: ADR-0037
title: "Cross-incarnation MV recovery uses lake inspection"
domain: [frontend-mv]
status: active
supersedes: []
superseded-by: null
date: 2026-08-02
provenance:
  - "discussion: 2026-08-02 historical materialized-view recovery"
code-anchors:
  - "novarocks/spi/src/connector/staged_publication_recovery.rs (ConnectorStagedPublicationRecovery)"
  - "novarocks/frontend/src/application.rs (FrontendApplicationHost)"
---

## 问题

Frontend 重启后如何在不伪造旧 Connector generation、不重放历史 write 或 publication 的前提下，收敛一个未完成的 materialized-view refresh？

## 背景与执行事实

当前 refresh 的 write 与 catalog mutation 都由 retained exact-generation lease 围栏。进程重启后原 incarnation 不再存在，把旧 evidence 交给当前 generation 的普通 reconcile 或再次 execute 会破坏该围栏。与此同时，lake 中的 ref、snapshot lineage、marker、provenance 和 snapshot summary 保留了可由 provider 验证的外部事实。

## 考虑过的选项

1. 让 Frontend 以当前 generation 重放旧 write、publication 或 reconcile。实现简单，但把当前 attempt 的 exact-generation 规则静默放宽为跨进程 replay。
2. 让 Frontend 直接读取 Iceberg metadata 并自行解释 ref、snapshot、manifest 与 provenance。能快速完成恢复，但泄漏 provider DTO 与 catalog ownership。
3. 由当前 provider generation提供只读历史 inspection，并仅在 inspection 的精确 proof 上执行 guarded staged-ref cleanup。

## 裁决

采用第三种方式。Frontend 拥有 candidate scan、fence、状态机、cleanup 决策和 StateStore convergence；provider 拥有 lake metadata interpretation、历史 committed version reconstruction 与 guarded cleanup。inspection 可以跨 incarnation 重复，普通 write/mutation execute 和同 lease reconcile 继续严格要求 original exact generation。`Staged` attempt 只清理并 abort，不在缺失用户 request 的 startup 中自动 publication。

## 接受的妥协（诚实记录）

这会增加一套窄 recovery SPI、v4 ledger 和 provider lineage 测试，而且完成但未发布的 staged work 必须重算。选择它是为了保住 commit fencing、provider ownership 和 fail-closed recovery，不是因为其实现成本更低。单 FE 的 StateStore fence 也不提供多 FE takeover；这是刻意留给后续协调机制的边界。

## 何时重新评估

- 多 FE lease、epoch 和 takeover 获得 durable owner source时；
- 需要在 StateStore ledger 缺失后从全湖发现历史 attempt时；
- provider 无法在有界 inspection 中证明 ref/lineage/provenance truth时；
- 产品要求在 restart 后继续原用户 request 的 publication时。
