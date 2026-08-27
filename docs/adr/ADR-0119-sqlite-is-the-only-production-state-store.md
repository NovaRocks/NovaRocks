---
id: ADR-0119
title: "SQLite is the only production StateStore"
domain: [crate-boundary, configuration]
status: active
supersedes: [ADR-0093]
superseded-by: null
date: 2026-08-27
provenance:
  - "implementation: SQLite StateStore production baseline"
  - "discussion: 2026-08-27 StateStore contract and SQLite production boundary"
code-anchors:
  - "novarocks-server/src/state_store_config.rs (StateStoreConfig)"
  - "novarocks/state-store/sqlite/src/schema.rs (initialize)"
---

## 问题

当远程 StateStore 的 tenancy、复制、故障恢复和多 FE fencing 语义尚未裁决时，生产配置是否应继续把 MySQL、FoundationDB 与 SQLite 暴露为同等可选项？

## 背景与执行事实

StateStore SPI 只需要表达 transaction、range、change cursor、commit resolution、factory 与 lifecycle 的中立契约；它不拥有 FE 拓扑、active FE 数、deployment owner、path identity 或 incarnation。Frontend 是唯一的 consumer runtime owner，Server 是唯一的 TOML wire 与 concrete composition owner。

SQLite 已具有可验证的本地文件、相邻 owner lock、WAL/FULL、exact schema v2、cluster binding、bounded protocol history 与 fail-closed startup 语义。MySQL 与 FoundationDB 仍保留为独立 leaf crate，用来接受 SPI conformance 与研究其物理实现；它们没有被裁决为 NovaRocks production service，也没有 Server configuration、feature forwarding、readiness 或 native scenario 入口。

此前 ADR-0093 把三个 provider 的 Server selection 与 SQLite 的单 FE deployment check 作为阶段性构成。那会把尚未定义的远程服务形态和 FE topology 事实泄漏进稳定 SPI、Frontend host 与产品配置。

## 考虑过的选项

1. **保留三个 production provider 的 Server 枚举。** 配置表面看似更灵活，但会迫使项目为远程 credential、keyspace、HA、fencing、upgrade 和 failure semantics 作出未经验证的承诺。
2. **在 SPI 或 Frontend 保留 active-FE/access-mode gate。** 这能继续拒绝部分拓扑，却把 application deployment policy 伪装成 provider contract，并不能提供真正的多 FE takeover 证明。
3. **SQLite 是唯一 production provider；远程实现保留实验 leaf crate。** Server 只接受 SQLite typed config，SPI 保持 topology-free，远程实现只独立编译和测试。（采纳）

## 裁决

生产 Server 只解析 `[state_store] provider = "sqlite"`、`cluster_id`、`path` 和 `[state_store.history_retention]` 的五个 typed 字段。未知字段、远程 provider 名称及其 remote 参数都在 startup fail closed；BE 不创建 StateStore，configured SQLite 失败时没有 fallback。

SQLite schema v2 是 hard cut：可识别的 v1 或其他不支持版本返回 `UnsupportedFormat`，畸形 schema/meta 返回 `Corruption`，实现不迁移、重命名、删除或自动重建用户文件。identity 仅为 `store_id + cluster_id`；绝对 path 只用于 no-follow open 与 owner lock。history pruning 以 provider-private row counters、change floor 与 retired UUID envelope 保留有界但诚实的 change/commit-resolution 语义。

MySQL 与 FoundationDB 只保留为 experimental leaf crate；不得重新进入 Server TOML、production feature、native readiness 或 acceptance。未来远程 StateStore 需要新的 ADR，先定义 tenancy、credential、replication、fencing、upgrade 和 failure contract，再决定是否接入 composition。

## 接受的妥协（诚实记录）

**生产部署暂时没有远程 StateStore。** 这限制了使用共享远程控制面存储的部署选择；选择它是因为 SQLite 边界和失败语义已经可以验证，而远程服务的正确形态尚未想清楚，不是因为本地文件在所有规模下更优。

**SQLite 不解决多 FE 高可用。** 删除 topology gate 不等于宣称 active-active 安全；多 FE lease/fence/takeover 仍是未实现的独立架构问题。把一个常量 FE 数塞进 provider 只能制造错误的安全感。

**v1 文件不能就地升级。** 运维者必须保留备份并用匹配旧格式的 binary 读取旧文件；这是避免隐式 ALTER 或 reset 在控制面文件上造成不可逆损失的代价。

## 何时重新评估

1. 远程 provider 能提出并验证完整的 tenancy、credential rotation、keyspace isolation、replication、upgrade、outage 和多 FE fencing contract 时，写新的 ADR 后再考虑 Server composition。
2. SQLite 的 owner lock、single-file durability 或 bounded history 在真实生产负载中成为可量化瓶颈，且替代方案能保持同等 fail-closed 与 recovery proof 时。
3. 多 FE lease/takeover 已有独立的权威 source、fence 与 crash recovery 验证时；该工作不得通过恢复 SPI topology 字段偷渡。
