---
id: ADR-0086
title: "Frontend MV storage observation uses neutral SPI transport facts"
domain: [frontend-mv, provider-spi]
status: active
supersedes: [ADR-0060]
superseded-by: null
date: 2026-08-18
provenance:
  - "discussion: 2026-08-18 Frontend domain application owner cut"
code-anchors:
  - "novarocks/spi/src/connector/mv_storage_observation.rs (MvStorageObservationPort)"
  - "novarocks/frontend/src/mv/domain/storage_observation.rs (observe_refresh_base)"
  - "novarocks-server/src/composition.rs (IcebergMvStorageObservationAdapter)"
---

## 问题

MV refresh 的 storage observation 如何在 Frontend 持有 durable MV schema 与 descriptor 的前提下，仍让 provider 只投影
同一 retained exact metadata generation 的中立事实，而不让 SPI 依赖 Frontend、让 provider 依赖 Frontend，或在
Server 之外重复转换 authority？

## 背景与执行事实

MV target 创建、schema 验证、lake package、base pin、refresh target 与 maintenance observation 都从 retained
`ConnectorControlPlanningLease` 加载的 sealed `ConnectorTableMetadata` 投影。它们必须保持该 observation 的
identity、取消和 deadline 语义，不能重新解析 logical name、load latest metadata、重试或补充 provider runtime IO。

原 ADR-0060 将 `MvStorageObservationPort` 表述为 Core-owned consumer port，并因此拒绝把 base pin 做成 Connector SPI
capability。CLS-R3 把完整 MV application（包括 durable `MvDescriptorV1`、partition contract 和 refresh policy）迁入
Frontend 后，旧 port 的 Core DTO 已不再是合法 owner。与此同时，provider 仍只能命名 SPI，不能命名 Frontend durable
types；Server 已是同时装配 Iceberg provider 与 Frontend application 的唯一 composition root。

## 考虑过的选项

**A：把完整旧 port 和 durable DTO 留在 Frontend，令 provider 或 Server 依赖 Frontend。** 这样看似少一次映射，
却使 provider contract 反向依赖 application persistence，并把 composition 策略泄漏入 provider crate。

**B：把 durable descriptor、partition schema 与 refresh persistence 一并下沉 SPI。** 这会让 SPI 接管 Frontend 的
durable codec、validation 与错误分类；它不仅超出 observation transport，还把 application schema 演化承诺扩大为
所有 provider 的公共契约。

**C：保留 Core facade 或同时维护 Core/Frontend 两套 observation port。** 双 authority 会让同一 attempt 的转换和
failure precedence 随调用点漂移，且阻止完整 owner cut。

**D：SPI 只定义 sealed provider-neutral observation facts；Server 在一次 exact metadata observation 后转换 Iceberg
provider DTO；Frontend 将中立 facts 转换为自己的 durable MV contracts。**

## 裁决

采用 **D**，并 supersede ADR-0060 对 port owner 与位置的表述；其同版本、无额外 IO、fail-closed 的语义保持不变。

`novarocks-spi::connector::MvStorageObservationPort` 是唯一 provider-facing port。每个方法接收 retained exact lease、
由该 lease 产生的 sealed metadata 和 bounded request context，并返回有界的中立 observation value。未安装实现对
每个方法返回 typed `Unsupported`。SPI value 不携带 Frontend descriptor、durable partition contract、SQL AST、provider
handle、catalog client 或 native wire payload。

Iceberg provider inspector 只从 sealed metadata 解码 provider facts。Server composition 的
`IcebergMvStorageObservationAdapter` 是唯一 provider DTO 到 SPI observation 的转换点：它必须使用传入 metadata identity，
不得按 provider name 分支、re-load、retry 或查询 latest generation。Frontend 的 storage-observation helpers 是唯一
SPI observation 到 `MvDescriptorV1`、partition contract 与 refresh policy 的转换点；malformed durable descriptor 和
无法表示的 durable transform 均 fail closed，并保留 `CorruptData` 分类。

`observe_refresh_base` 继续从同一 metadata 版本投影非空 UUID 和 `Option<i64>` current snapshot。无 current snapshot
仍原样为 `None`；lease/metadata identity mismatch、opaque payload corruption、取消与 deadline 均不得降级为 fallback。
这个 port 是 provider-neutral SPI transport，不是新的 provider capability、runtime read path 或跨进程协议。

## 接受的妥协（诚实记录）

这项迁移引入两层看似机械的转换：Server 将 provider DTO 变为 SPI facts，Frontend 将 SPI facts 变为 durable contracts。
选择它不是因为映射更少或类型更漂亮，而是因为这是同时避免 provider→Frontend 反向依赖、SPI 持有 durable schema、和
Core/Frontend 双 port 的最小边界。代价是每次 observation 字段增加时，三个 owner 都需要审查并写对应测试。

SPI 的名称带有 MV，但它并不拥有 MV application policy；这是一个已批准的窄 transport vocabulary。若把它不断扩充为
任意 metadata dump，SPI 会变成 application schema 的替身，因此新字段必须证明其 exact-metadata 投影和跨 provider
必要性。

## 何时重新评估

- 非 MV consumer 需要相同 observation facts 时；应先判断是否可抽取更窄的 provider-neutral metadata value，不能
  直接把 Frontend DTO 或其 durable codec 暴露到 SPI。
- provider 无法从 sealed metadata 投影任一字段、必须进行额外 runtime IO 时；需要为该 IO 的 generation/failure
  semantics 作独立裁决，不能静默扩展本 port。
- 这些 facts 需要进入 FE/BE native wire 或 durable recovery record 时；必须定义 versioned bounded wire/durable
  evidence，而不是序列化本地 observation object。
- Frontend 的 durable descriptor 或 partition schema 演化到无法由中立 facts 无损表达时；应先裁决 codec owner 与
  compatibility，而不是把新 persistence type 直接搬到 SPI。
