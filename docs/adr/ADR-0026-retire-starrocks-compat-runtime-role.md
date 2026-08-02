---
id: ADR-0026
title: "Retire StarRocks-compatible backend as a runtime role"
domain: [runtime-role]
status: active
supersedes: []
superseded-by: null
date: 2026-08-02
provenance:
  - "discussion: 2026-08-02 native runtime role and external Connector boundary"
code-anchors:
  - "novarocks-server/src/main.rs (top-level command dispatch)"
---

## 问题

NovaRocks 是否还应把自己作为可被 StarRocks FE 驱动的兼容 BE server，还是应只保留 native FE/BE 角色与 StarRocks 外部 Connector？

## 背景与执行事实

Compat server 曾同时维护 daemon 生命周期、C ABI/C++ shim、StarRocks thrift/brpc 协议、专用 thirdparty 工具链和测试集群。这些入口不是 Connector 的 control 或 execution binding，也不能与 native FE/BE 的进程、生命周期和 ownership 规则共享。

StarRocks 数据读取已由独立的 `novarocks-connector-starrocks` 承担。它作为 read-only external Connector，RPC 可覆盖全部拓扑，direct 永久仅适用于 shared-data；其协议事实和凭据不会进入 native carrier。native FE/BE 已经拥有 SQL admission、协调、BE-local execution 和 Connector host 装配边界。

## 考虑过的选项

保留 Compat feature、crate 与工具链可以维持旧部署表面，但会长期保留一套不再演进的 server 协议、CI 资产和安全更新责任。

把 Compat 命令临时改名为 native 命令但保留 shim 或 daemon，会让用户误以为旧协议仍受支持，并继续让两种 runtime ownership 共存。

完整删除 Compat server 产品面，同时保留外部 Connector，可让唯一生产路径是 native FE/BE，并把 StarRocks 集成限定为明确的 Connector 契约。

## 裁决

删除 StarRocks-compatible server runtime、daemon CLI、Compat ABI/shim、IDL、thirdparty 构建入口、专用 SQL runner/CI 和部署文档。顶层命令分发只接受 `novarocks standalone --role fe|be|all-in-one --config <path>`；命令解析必须先于任何运行时副作用。

StarRocks 只以 read-only external Connector 存在。native FE/BE host 装配其 control 与 execution binding；不恢复 inbound StarRocks server 协议，也不把 Connector 细节加入 native wire。

## 接受的妥协（诚实记录）

现有依赖 NovaRocks 充当 StarRocks BE 的用户必须迁移，不能获得 feature flag 或兼容期回退。这是为了终止两套 runtime 及其第三方工具链的持续维护成本，并非因为 native CLI 立刻覆盖旧 daemon 的全部运维体验。

Core 中没有非 Compat caller 的旧 StarRocks projection、lake、write 和 transaction kernel 会在后续独立清理；短期留下这些无调用实现会增加阅读噪音，但避免把 server 退役与内核重构混成一次不可审计的变更。

## 何时重新评估

- StarRocks external Connector 的公开 read contract 发生重大变化，必须重新确认 RPC 与 shared-data direct 的边界。
- native FE/BE 的运维 CLI 需要扩展时，应以 native role 生命周期另行裁决，而不是恢复 daemon/Compat 入口。
- 若产品重新要求接收 StarRocks FE 的 inbound 协议，必须提出新的 runtime-role ADR、独立安全与分布式验证计划，不能重新启用已删除代码。
