---
id: ADR-0098
title: "Native Protocol Error Contract Ownership"
domain: [distributed-query-lifecycle, crate-boundary]
status: active
supersedes: []
superseded-by: null
date: 2026-08-18
provenance:
  - "discussion: 2026-08-18 native role data-plane ownership"
code-anchors:
  - "novarocks/proto/src/lib.rs (error)"
---

## 问题

FE/BE native wire 的字段路径和验证错误，应该由 Core 的通用模块保留，还是由 Protocol 与 schema、已验证 lifecycle value 一起拥有？

## 背景与执行事实

`FieldPath`、`ProtocolErrorKind` 与 `ProtocolError` 描述的是 native DTO 的结构性拒绝原因；它们由 Backend 的 fragment decoder 和 Core 的 query-options decoder 共同使用，并最终穿过 FE/BE 的 native gRPC 边界。此前 Core 同时携带 `ProtocolFamily` 以及未被生产路径使用的 StarRocks/transport decode 词表，使 native-only 协议错误看起来像通用多协议能力，也迫使消费者经 Core 门面引用协议值。

## 考虑过的选项

1. 继续由 Core 拥有全部错误词表。优点是既有 import 不变；代价是 protocol semantic 仍依赖 execution kernel 的目录位置，且无效的多协议概念继续存活。
2. 新建 lifecycle contract crate。优点是可以把错误与其他 lifecycle 值单独隔离；代价是为少量 native schema 验证值新增一条 crate 边和第二个契约入口。
3. 由 `novarocks-proto` 直接拥有 native error contract，并删除 family discriminator 与未使用 transport 词表。优点是 schema、validated value 与拒绝语义在同一 owner；代价是所有消费者必须直接依赖 Protocol。

## 裁决

选择选项 3。`novarocks-proto` 公开 `FieldPath`、`FieldPathSegment`、`ProtocolErrorKind` 和 `ProtocolError`；`ProtocolError` 固定表示 native 协议错误，构造函数不再接受 family。Core 仅保留使用 Protocol 值的 query-options 解码，Backend/Frontend 直接 import Protocol，不设置 Core compatibility re-export。

## 接受的妥协（诚实记录）

这不是因为 Protocol 天然适合承载所有错误。它只拥有 native DTO 验证错误；应用编排、传输故障和本地执行错误仍归各自 owner。直接迁移会一次性改动大量 Backend imports，短期 diff 较大；仍选择原子迁移，是为了避免以临时 facade 延长双 owner 和让无效的 StarRocks family 继续成为 API。

## 何时重新评估

当 NovaRocks 引入第二种由同一进程正式承载、且必须共享字段路径/错误 code 的稳定 inbound protocol 时，评估是否需要可参数化但不混淆 native wire 的公共错误层；当 native IDL 被拆分为独立发布包时，评估 error module 是否应随该包一并发布。仅因测试或 all-in-one 便利不得重新引入 family discriminator 或 Core facade。
