---
id: ADR-0103
title: "Central provider wire authority and homogeneous native build admission"
domain: [provider-spi, distributed-query-lifecycle, cluster-membership]
status: superseded
supersedes: [ADR-0047]
superseded-by: ADR-0105
date: 2026-08-24
provenance:
  - "discussion: 2026-08-21 central provider wire authority and homogeneous native build admission"
code-anchors:
  - "novarocks/proto/src/lib.rs (central IDL and generated DTO authority)"
  - "novarocks/frontend/src/topology.rs (ClusterBackendService::record_heartbeat_success)"
---

## 问题

内建 Connector 的跨 FE/BE 分发契约应由谁定义；如何既让 Provider-specific 事实以有类型的、可验证的 carrier 传输，又不把
Provider runtime、通用 SPI 或节点调度变成第二个 wire authority；并且在没有版本协商承诺时，如何在调度前拒绝不相同的
Native binary？

## 背景与执行事实

NovaRocks 的生产形态是独立的 FE 与多个 BE。当前 Iceberg 与 StarRocks 都是同仓、静态链接的内建 Provider；中央 IDL、
generated DTO、Provider 构型和 FE/BE 程序随同一次正式发布原子变化。`novarocks-proto` 已经集中编译仓库级 IDL、
导出 descriptor artifact，并拥有 generated message 的结构校验与 canonical digest；它是跨进程事实的自然规范位置。

一次 query 或 Provider 工作必须先冻结精确 Connector generation，再在所属 host 中经历 admission、纯 preparation、
带本地资源的 activation 和 runtime execution。Provider 的领域正确性、credential、client、reader/writer、外部 catalog
副作用和 durable lifecycle 都不因 DTO 集中而转移给 Protocol。相反，Frontend topology 拥有 Backend 的可调度资格；
durable membership 仍由 StateStore 拥有，heartbeat 的 liveness observation 不能成为第二份 durable catalog。

ADR-0047 正确地要求 exact generation 与单一 read admission，但把 native read 结论固定为 provider-neutral opaque
carrier。该做法会让 NovaRocks 自有的结构化 Provider 事实退化为 bytes，或诱使每个 Provider 自建 schema、decoder 和
registry。它不能再作为跨 FE/BE Provider contract 的长期表示裁决。

## 考虑过的选项

1. 继续以 provider-neutral opaque bytes 承载所有 Provider carrier。短期迁移最少，但字段语义、边界、验证和 digest
   不能由中央 Protocol 审计，且调用方容易重新引入私有 JSON、私有 protobuf 或 runtime registry。
2. 由每个 Provider crate 自行生成 Native schema，并以注册表或 generic envelope 接入。Provider 看似更独立，但会产生
   多个协议 authority、版本组合和动态装配问题；这不符合当前同仓内建、原子发布的产品边界。
3. 由仓库级 IDL 与 `novarocks-proto` 集中定义 carrier root；每个 carrier 使用 scoped `oneof` 容纳 Provider-specific
   variant，Protocol 负责 typed structural validation 与 canonical digest，真实 consumer 再按本 ADR 的 owner 边界原子切换。
   同时以单一 immutable Native build identity 在 heartbeat/topology 层做 exact-match admission。选择此方案。
4. 每个 Provider 单独上报 manifest、descriptor digest 或协商版本，再按部分能力接纳 Backend。它可服务将来的异构发布，
   但当前没有 mixed-version、rolling upgrade、外部 plugin 或独立 Provider release 承诺，且会把一次全局部署检查膨胀为
   多重 negotiation。

## 裁决

仓库级 IDL 与 `novarocks-proto` 是所有内建 Provider 跨 FE/BE wire DTO、generated surface、descriptor artifact、
typed structural validation 和 canonical digest 的唯一 authority。每个真实 carrier 在中央 root 下定义自身的 scoped `oneof`，
其 Provider variant 可以表达该 carrier 所需的具名、有界结构化事实；不得以 `payload`、`opaque`、`other` 或 generic envelope
重新包装 NovaRocks 自有语义。真正属于外部 authority 的 token、标准文档、Arrow IPC 或算法 artifact 仍可作为由具体字段
明确名称、上界、redaction 与 digest 语义的 binary content。

Protocol 只验证 wire 可见的结构与 canonical form。Host 先完成 first/exact/conflict admission；单一 Provider owner 再做
无副作用的领域 preparation，activation 才能绑定进程本地 credential、client、pool 与 runtime resource。generic scheduler
只消费明确提升的中立 view，不能解释 Provider variant；StateStore SPI、普通 catalog/metadata/runtime trait 也不取得
Provider transport 语义。第一个真实 wire-facing Connector carrier 才能使其必要的 SPI 模块依赖中央 Protocol DTO；不为
尚未迁移的 carrier 增加闲置依赖边或公共 wrapper。

每个 carrier 的 IDL variant、FE producer、wire-facing SPI、BE decode/host adapter、Provider preparation/activation、
canonical replay digest（如适用）和测试必须在同一变更中原子切换。旧 private carrier、raw constructor/accessor 与旧 decoder
在该 carrier 的切换中删除并按 protobuf 规则 reserve identity；禁止双读、双写、fallback decoder 或长期 bridge。本 ADR
只规定此方向，不能据此声称现有 opaque carrier 已经删除，也不提前创建没有真实 consumer 的 synthetic Provider schema。

Native 部署使用一个由正式构建来源产生的 immutable build identity。BE 只经既有 heartbeat identity 上报它；Frontend topology
只把与本进程完全相同的 Backend 标为 `Live`，不匹配者是有类型、可观察且不可调度的 admission failure，必须在 snapshot、
participant capture 与 Provider work 调度之前被排除。这里没有 Provider manifest、按能力部分准入、第二个 heartbeat field
或版本 negotiation；all-in-one 仍经同一 wire 路径，不能替代多 BE 验收。

本 ADR supersede ADR-0047：其“同一 exact Connector generation 内完成 read admission”的必要性保留，但原先的
provider-neutral opaque native carrier 被中央 typed carrier 规则取代。ADR-0006 仍保持 active：普通可替换 capability
仍是系统 SPI，只有 wire-facing 的真实 Connector carrier 可依赖 Protocol DTO。ADR-0023、ADR-0034、ADR-0050 与 ADR-0051
仍保持 active：它们关于 writer lifecycle、两级 scan lifecycle、sealed logical mutation effect 与 exact-generation write
activation 的 owner 裁决不因 representation 变化而失效。ADR-0079 仍是直接前例：IDL/Protocol 的 canonical authority
从 query lifecycle 扩展到同一 Native process boundary 上的 Provider carrier，而不是新建平行 authority。

## 接受的妥协（诚实记录）

选择中央 DTO 会让所有内建 Provider 的 wire 修改集中触及同一 IDL/build/generated surface，并要求 Provider author 在
carrier 设计时明确字段、上界、canonical digest 与测试；这不是因为它让 Provider 实现更少，而是因为当前产品只发布一个
同构 Native binary，多个私有 schema 的长期成本更高。我们不支持仓库外 plugin、Provider 独立协议发布、mixed-version 或
rolling upgrade；未来若需要这些能力，当前单一 gate 会成为刻意的阻碍，而不是兼容层。

完整 commit identity 足以代表当前正式发布的中央 IDL 与内建 Provider 构型，但不能区分同一 commit 上不同的 dirty 本地
构建，也没有额外计算 descriptor digest。正式 release 必须来自 clean commit；我们接受开发环境需要显式理解这一限制，
而不为尚未承诺的发布形态预建 manifest、schema lockfile 或部分能力 gate。

保留 Provider-specific binary field 也意味着 Protocol 无法通用解读所有外部格式或敏感内容。代价是每个字段都必须由其
owner 证明长度、redaction 与 digest 规则；这是为了不把真正外部 authority 的完整标准复制成 NovaRocks shadow schema。

## 何时重新评估

- 产品正式支持 role-specialized binary、mixed-version、rolling upgrade、source archive release、仓库外 plugin 或
  Provider 独立发布时，重新定义 build provenance、兼容范围和 admission granularity；
- 首个或后续真实 carrier 无法以 scoped `oneof`、具名 bounded field 和 Protocol validation 表达其稳定跨进程事实时；
- workload 证明单一 carrier 的 size、decode 或 canonical digest 成本超过其明确预算，且需要 chunk、manifest 或
  external artifact 的不同传输形态时；
- Provider preparation/activation 的 failure、replay 或 concurrency 语义需要越过现有 Host owner 边界时；
- durable membership、topology snapshot 或调度模型需要表达的兼容性不再是全局 exact match，而是有证据支持的产品级
  compatibility policy 时。
