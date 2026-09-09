---
id: ADR-0137
title: "Providers own private connector wire behind opaque Native envelopes"
domain: [provider-spi, distributed-query-lifecycle]
status: active
supersedes: []
superseded-by: null
date: 2026-09-08
provenance:
  - "discussion: 2026-09-08 multi-provider Connector wire and binding decision"
  - "PR: pending — backfill the number once the provider-owned wire migration merges"
code-anchors:
  - "novarocks/spi/src/connector/provider/definition.rs (ConnectorCodecDeclaration)"
  - "novarocks/proto-codec/src/connector_common.rs (ConnectorEncodedPayload codec)"
  - "novarocks-server/src/native_compatibility.rs (native_carrier_declarations)"
---

## 问题

当 Iceberg、Paimon 等 table-format provider 需要交换不同的表、列、读取视图、split、writer 和 commit 事实时，Native 协议应如何承载这些对象，才能让公共接口可扩展，又不让中央 IDL 成为所有 provider 私有语义的共同 owner？

## 背景与执行事实

旧的公共 `connector_read.proto` 与 `connector_write.proto` 直接定义 Iceberg table、manifest、Parquet file、delete、writer 和 commit 对象，并以 provider oneof 选择具体消息。新增 provider 因而只有两个选择：把自己的语义继续加入中央 schema，或把不同语义压成 Iceberg 形状。两者都会让 Iceberg 成为事实上的类型模板，并使公共 codec、FE、BE 和每个 provider 同时参与一次私有格式变更。

Native 真正需要的公共事实较少：准确的 provider identity、用途 category、非零 revision、payload 长度和 opaque bytes。某个 payload 内部如何表达 snapshot、merge group、delete 文件或 writer 产物，是该 provider 的 read/write correctness。`ConnectorCodecDeclaration` 将这一边界静态化：每个 provider 声明自己的 descriptor、format name 和 revision；Server 在启动时把所有已编译 provider 的私有 descriptor digest 纳入同一个 `NativeCompatibilityId`，与运行时是否配置该 catalog 无关。

公共 envelope 与 provider 私有 decoder 分两层拒绝非法输入。公共层在分配或 provider 调用前验证 header、category、revision 和总预算；provider 层再验证自己的原始 wire 结构、类型关系和 retained budget。FE 编码和 BE 解码必须命中同一个 exact provider generation，不能按名称猜测、回退到 Iceberg decoder，或在公共层复制 provider 私有对象。

## 考虑过的选项

**A. 继续扩展中央 provider oneof。** 单一生成入口看起来易于发现，跨 crate 调用也直接，但每加一个 provider 都要修改公共协议和所有公共消费者。中央 schema 会积累互不共享的字段，provider 无法独立修订私有格式，因此否决。

**B. 设计一个覆盖所有 table format 的统一 split/file 模型。** 公共优化器可以直接观察更多物理事实，但 Iceberg 的 delete/manifest 结构与 Paimon 的原子 merge group、KV system columns 和历史 schema 不是同一语义。取并集会成为带大量可选字段的中央超集，取交集又会丢失 correctness 所需事实，因此否决。

**C. 使用 protobuf `Any` 或运行时 descriptor discovery。** 它能装入任意消息，却把 provider 集合和兼容性判断推迟到运行时。NovaRocks 的 FE/BE binary 必须在开放 listener 前知道它们是否属于同一 compatibility island，不能依赖运行中协商或缺失 provider 时的降级，因此否决。

**D（选中）. 公共 opaque envelope + provider 自有 IDL/codec + 静态封闭登记。** SPI 只定义 provider-neutral domain、codec facet 和静态声明；公共 proto-codec 只拥有 envelope；每个 provider crate 生成和验证自己的私有 IDL。Server 从编译期 provider 集合派生角色 factory 与兼容材料。

## 裁决

公共 read/write IDL 只保留跨 provider 确实共享的 Native 事实，以及由 `ConnectorProviderId`、`ConnectorCodecCategory`、`ConnectorCodecRevision` 和 opaque bytes 组成的 `ConnectorEncodedPayload`。公共 IDL 不定义 Iceberg、Paimon 或未来 provider 的 table、column、view、split、writer、artifact 及其 oneof。

每个 provider crate 独占自己的私有 IDL、generated DTO、raw structural validation、domain conversion 和四向 codec。SPI 的 codec facet 只收发 provider-neutral值与 `ConnectorEncodedPayload`，不依赖 generated DTO、公共 proto-codec 或 `ProtocolError`；公共 proto-codec 在 Native 外层完成 DTO、字段路径、累计预算和 SPI 错误的转换。

Provider definition 是编译期封闭集合。它同时声明 capability、完整 FE/BE role factories、每个用途的 format/revision/descriptor。重复 identity、缺失 codec、能力与 codec 不一致或 descriptor 为空都在启动组合时失败。Server 的 `NativeCompatibilityId` 包含集合中每个私有 descriptor digest 与 revision；配置没有启用某个 catalog，不得从兼容材料中裁剪该 provider。

FE 只从 exact admitted catalog generation 取得 provider identity 和 encoder；BE 只从同一 frozen binding 取得 decoder 与 runtime factory。未知 provider、错误 catalog generation、错误 category/revision 或不匹配的私有 payload 都在 I/O 前失败，不提供 Iceberg fallback、legacy decoder 或双 registry。

## 接受的妥协（诚实记录）

**接受多个 IDL 与生成入口。** 每个 provider 都要维护 build script、descriptor、strict decoder 和兼容性测试，生成与审查成本高于一个中央 proto 文件。这是把私有语义交还正确 owner 的直接代价。

**公共层无法解释 provider 私有诊断。** 公共 envelope 只能报告 header、category、revision 和预算错误；更深的字段路径与损坏原因由 provider 返回。跨 provider 的统一诊断会少一些，但错误不会因统一展示而被错误接受。

**私有格式变化会改变整个 Native compatibility identity。** 即使某次部署没有配置相应 catalog，只要 binary 编译了该 provider，其 descriptor/revision 变化就要求 FE/BE 一致升级。这扩大了兼容变更的可见范围，换来 listener 前的确定拒绝和无协商数据面。

**静态登记暂不支持动态插件。** 当前二进制只接受编译期 provider 集合；新增 provider 需要构建和部署新 binary。这符合当前部署模型，但未来真正的插件系统需要另行设计可信加载、descriptor 身份和升级隔离。

## 何时重新评估

- 至少两个真实 provider 证明某组当前私有事实具有完全相同的语义、生命周期和演进权时，可以把该最小交集提升为公共 typed fact；仅字段相似不足以提升；
- NovaRocks 引入动态 provider 插件时，需要重新设计签名、加载、兼容 island 和 role factory sealing，不能直接把静态 registry 换成字符串查找；
- 私有 descriptor 变化导致不可接受的整 binary 升级频率时，可评估更细粒度 compatibility island，但必须继续在 admission 前 exact-fail，不能运行时协商降级；
- protobuf 不再适合某个 provider 的私有格式时，该 provider 可在保持公共 envelope 和声明契约不变的前提下更换内部 codec；公共层不因此取得其语义所有权。
