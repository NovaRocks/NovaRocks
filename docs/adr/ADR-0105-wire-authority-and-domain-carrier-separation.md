---
id: ADR-0105
title: "Wire authority and domain carrier separation"
domain: [provider-spi, distributed-query-lifecycle, cluster-membership]
status: active
supersedes: [ADR-0103]
superseded-by: null
date: 2026-08-25
provenance:
  - "discussion: 2026-08-25 connector execution binding carrier ownership"
code-anchors:
  - "novarocks/proto/src/provider/execution_binding.rs (connector_execution_binding_declaration_digest)"
  - "novarocks/spi/src/connector/execution_declaration.rs (ConnectorExecutionDeclaration)"
  - "novarocks/frontend/src/native/transport.rs (encode_connector_execution_declaration)"
  - "novarocks/backend/src/connector/binding_decode.rs (decode_connector_execution_declaration)"
---

## 问题

当一个内建 Connector 的跨 FE/BE carrier 同时需要稳定 wire DTO 与执行领域语义时，如何让 Protocol 保持唯一 wire authority，而不让 SPI 反向依赖 Protocol 或把 generated DTO 当作领域模型？

## 背景与执行事实

ADR-0103 正确地把仓库级 IDL、generated DTO 与 canonical digest 集中到 `novarocks-proto`，并把同构 native build admission 留在 Frontend topology。但是它允许第一个真实 carrier 把 Protocol declaration wrapper 直接暴露给 SPI；这会使 SPI、SQL、Execution 与 StateStore provider 经一条本不属于它们的依赖边获得 Protocol/prost/protoc，也会让 catalog admission 的 normalizing identity 与 wire ingress 的 strict canonical validation 混在同一 representation 内。

Connector execution binding 已有两个静态内建 variant（Iceberg 与 StarRocks）、一个真实 FE producer、一个真实 BE consumer，以及 Host 的 generation/digest 状态机。它不需要 runtime registry、opaque codec、第三个 adapter crate 或两份可变 authority。因而可以把“wire 的规范形式”与“进程内领域值”明确分开，同时保留单一 DTO digest 的跨进程身份。

## 考虑过的选项

1. SPI 继续 re-export Protocol 的 validated declaration wrapper。它省去两处转换代码，却把 transport crate 拉入所有 SPI consumer，并让领域 API 的生命周期、校验和 debug 形状受 generated DTO 支配。
2. 新建共享 adapter/domain crate，在其中保存另一份 declaration representation。它可以消除 FE/BE 局部重复，但会制造新的长期 crate owner，且仍要证明它不是第二个 validation 或 digest authority。
3. Protocol 只拥有 generated DTO、DTO structural surface、typed wire outcome validator 与 canonical digest；SPI 拥有闭合、私有字段、fallible constructor 的 transport-neutral declaration；FE 独占 domain-to-DTO encode，BE 独占 DTO-to-domain decode，并在 decode 前对原 DTO 计算 digest。选择此方案。
4. 让 Provider 自行编码/解码或通过动态 registry 传递 payload。它会重新引入多个 wire authority、未封闭 dispatch 和 fallback 风险，不符合当前静态内建 provider 与同构发布边界。

## 裁决

`novarocks-proto` 仍是跨进程 wire 的唯一 authority：IDL/generated DTO、typed Ensure/Retire outcome validator 与从原始 generated declaration 计算的 domain-separated canonical digest 均留在 Protocol。它不保存 Connector execution declaration 的领域 wrapper、ProviderKind、BindingKey 或重复 domain validation。

`novarocks-spi` 拥有 `ConnectorExecutionDeclaration`、`ConnectorExecutionBindingKey`、instance-id grammar 和闭合 provider variant。declaration 只有 Iceberg access binding 或 StarRocks local binding，字段私有，只能经 fallible constructor 产生；provider identity 只从 variant 派生，并必须和 control/write/rewrite binding descriptor 的 provider ID 交叉验证。catalog admission 保留 `ConnectorInstanceId::parse` 的 lowercase normalization；wire ingress 只用共享 grammar 的 `try_from_canonical`，因此 uppercase wire identity 必须失败而 SQL/catalog 既有大小写行为不回退。

Frontend 在其 native transport adapter 中把 SPI declaration 唯一编码为 generated DTO；Backend 在其 connector binding decoder 中先对该原 DTO 调用 Protocol digest helper，再严格解码为 SPI declaration，形成 `(digest, declaration)` 的 Backend-local admitted pair。Host 只接收这个 pair，绝不从 domain declaration 重算 wire digest；installer 只接收 domain declaration。两处 adapter 是刻意的本地 application owner，而非共享桥接层。

同构 Native build identity、Frontend topology admission、sealed installer set、Host 的 generation state machine 与 typed result wire contract 继续成立。Protocol crate 的 rename/split 不是本决策的一部分，必须另行设计与交付。

## 接受的妥协（诚实记录）

FE 与 BE 各自维护一次显式 variant mapping，短期确实比直接传一个 Protocol wrapper 多代码；我们选择它不是因为转换更少，而是为了避免 SPI 依赖图和领域 API 被 transport owner 反向控制。两处 mapping 必须由同一个真实 FE-encode/BE-decode contract test 覆盖，不能用 test-only copy 掩盖漂移。

SPI declaration 的 provider set 是 closed 的，新增 provider 需要原子修改 SPI variant、IDL oneof、两个 adapter、Host sealed composition 与生产验证。这牺牲了第三方动态插件的表面灵活性；当前产品没有外部 plugin、独立 provider release 或 mixed-version 承诺，预建 registry 只会制造第二 authority。

Protocol 仍保留 generated DTO 的 canonical digest helper，因而 BE 必须在翻译前保留原始 DTO。我们接受这一次短暂的 DTO/domain 双 representation；它们不构成双权威，因为只有 Protocol 定义 wire digest 和结构，只有 SPI 定义进程内 domain constructor，二者均不从另一侧反向派生可写 truth。

## 何时重新评估

- 产品支持仓库外 provider、独立发布、mixed-version 或 rolling upgrade，需要重新定义 closed variant、build admission 与 compatibility policy 时；
- 新 carrier 无法用明确有界的 generated DTO 和 SPI domain constructor 表达，且有真实 consumer 与兼容证据时；
- 多个 carrier 的 FE/BE adapter 重复已被度量为持续维护风险，并有方案能证明不新增 validation/digest authority 时；
- canonical digest 的 DTO 保留或跨进程 carrier 大小成为实测瓶颈，需要 chunk、manifest 或 external artifact 传输时；
- Protocol crate rename/split 被单独接受，并能证明不改变本 ADR 的 wire/domain authority 边界时。
