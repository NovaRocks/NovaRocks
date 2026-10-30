---
id: ADR-0204
title: "Bounded upstream Iceberg backports and explicit provider suspension"
domain: [crate-boundary, provider-spi]
status: active
supersedes: []
superseded-by: null
partially-supersedes:
  - id: ADR-0138
    scope: "Paimon vendor retention while suspended; authorized read-host boundaries remain required for restoration"
    link: "ADR-0138-vendored-paimon-read-host-patch.md"
  - id: ADR-0139
    scope: "Manifest membership versus active capability: retain registered providers with explicitly suspended capabilities"
    link: "ADR-0139-sealed-active-provider-manifest.md"
  - id: ADR-0170
    scope: "Patch allowlist: remove paimon while its provider is suspended; no accounting forks and registry identity rules remain active"
    link: "ADR-0170-third-party-crates-are-bounded-by-public-configuration-not-forked.md"
  - id: ADR-0174
    scope: "Ruling 2 protocol-only divergence: additionally allow bounded exact upstream Iceberg dependency backports with provenance and exit conditions"
    link: "ADR-0174-vendored-iceberg-baseline-and-patch-policy.md"
date: 2026-10-28
provenance:
  - "discussion: 2026-10-27 accepted Arrow dependency convergence, upstream Iceberg backports and experimental provider suspension"
  - "discussion: 2026-10-28 accepted capability suspension contracts and resource model re-audit"
code-anchors:
  - "vendor/iceberg-0.11.0/PATCH.md (upstream backport provenance and exit conditions)"
  - "novarocks-server/library/src/provider_manifest.rs (ProviderBuilderDefinition, compose_pure_program_catalog)"
  - "novarocks/connector/paimon/src/role_binding.rs (control and execution role factories)"
---

## 问题

共享 Arrow 类型的依赖升级被实验性 Provider 的 SDK 阻塞时，何时可以移植上游 Iceberg 改动，何时应暂停能力，怎样保持注册、配置与实际能力的含义准确？

## 背景与执行事实

Arrow 批次跨第一方与 Provider SDK 的 Rust 类型边界；不同主版本不能直接交换这些类型。根 workspace 和 lock 拥有共同依赖图，不能通过调用方的配置隐藏版本冲突。

| 对象 | 保留的责任 | 能力边界 |
|---|---|---|
| Iceberg vendor 家族 | ADR-0174 的同源基线、可追溯分歧与消费方退出责任 | 本条增加有界的上游依赖移植，不增加资源计量补丁 |
| Server sealed manifest | Provider 合约、descriptor、carrier 与双角色 factory 的唯一组合 | 注册不单独证明当前读写能力；暂停状态必须显式表达 |
| Paimon control binding | catalog 配置验证、generation materialize 与投影 | 暂停期间 materialize 成功，目录与读取操作在第一个入口明确拒绝 |
| pure catalogue | 编译静态程序所需的完整 facet | 暂停 Provider 不登记空 read facet，由 Server 本地组合跳过 |
| Paimon execution factory | BE 角色的完整 factory 契约 | bind 明确拒绝，不生成空执行或本地回退 |

Paimon 的 metadata、planning、distribution 三个必备槽由统一拒绝实现承担，read/write 可选能力为空。目录入口覆盖 FE 实际调用的同步与异步方法；原先不支持的写入、DDL 与统计保持 SPI 的通用 Unsupported。`ConnectorErrorKind::Unsupported` 沿现有转换保留在 BE materialization 错误 cause 中，类别为 `InvalidDefinition`，重试为 `UntilDefinitionChanges`。FE 的既有共用错误投影不由本规则改写。

## 考虑过的选项

- **只等所有 SDK 同时发版：成本否决。** 能保持纯 registry 依赖，但一个实验性 Provider 的发布进度会阻塞所有共享 Arrow 的消费者；当上游支持的版本一致时应优先采用正式发布。
- **自行改造实验性 SDK 或新增 ORC vendor：设计否决。** 会为解除依赖阻塞增加没有上游来源的私有分歧，违背缩小 vendor 面的目标；不把依赖升级变成维护新 fork 的许可。
- **用两套 Arrow 和跨版本转换桥保留实验性能力：设计否决。** 为暂停能力另建类型与执行路径，增加依赖闭包和长期转换语义；本系统的 Arrow 交界选择同一主版本。
- **删除 Provider 注册或使 catalog materialize 失败：设计否决。** 前者改变配置与封闭身份，后者让确定的能力暂停进入不可用重试；两者都不能准确表达“catalog 存在、能力暂停”。
- **只保留已有协议补丁，禁止任何上游依赖移植：成本否决。** 来源最简单，但在上游已有小范围改动时仍须等待发版。若移植需要大规模重构，应恢复等待正式版重新基线的策略。
- **限定 Iceberg 上游移植，并显式暂停实验性 Provider：采用。** 保留关键读写能力、来源与退出责任；接受实验性读取暂时缺失。

## 裁决

1. **有界上游移植。** 仅 Iceberg vendor 家族可在上游发版前移植有界的依赖升级改动，内容原样来自上游 PR；优先已合入版本。未合入 PR 按自创补丁标准审查，不把“来自上游 PR”当作正确性证明。所需改动依赖上游大规模重构时停止移植，在下一正式版按 ADR-0174 重新基线。其他 vendor crate 不获得此许可。
2. **来源和退出同录。** 每包 `PATCH.md` 记录 PR 链接、状态、完整来源 commit、文件与依赖声明差异。上游合入后改写内容时同步合入版本；正式版包含改动时由依赖维护者协调重新基线并删除移植；上游关闭未合入时回到设计裁决，不能无限期把它保作“上游补丁”。既有补丁只做保持原合同的编译适配。
3. **暂停保留身份。** 实验性 Provider 因依赖版本线受阻时，保留注册、配置解析、合约、descriptor、carrier 和双角色 factory；移出不再使用的 SDK、vendor 副本与 patch 条目。Paimon 暂停期间不在 patch allowlist 中；恢复后仍须满足 ADR-0138 的授权读取与资源边界。
4. **在能力入口拒绝。** FE control materialize 成功，catalog 保持可投影的 Ready 状态；暂停前支持的目录与读取入口共用一个带原因和恢复条件的 typed Unsupported，错误信息容纳于现有 512 字节详情边界。BE bind 通过同一错误的既有映射拒绝，不重试、不返回空数据、不新增共享错误词汇。暂停前不存在的可选能力保持其原拒绝。
5. **pure 组合保持完整。** Server 的 pure builder 可选，组合时跳过暂停 Provider；不得登记与合约不一致的空 facet，也不为暂停修改 connector-contract。
6. **恢复有版本与行为条件。** Paimon 只有在 paimon-rust 正式发布支持同一 Arrow 60 版本线、其 orc-rust 与 paimon-mosaic-core 闭包也已升级且 zstd 为 0.14 线后才进入恢复工作；恢复须重新接回授权读取与真实分布式覆盖。ORC 只有在 orc-rust 正式支持 Arrow 60 与 zstd 0.14 后恢复物理 reader 和测试。版本满足是恢复前提，不是自动启用。

本条仅部分替代四份旧 ADR 的上述范围。ADR-0138 的授权读取边界、ADR-0139 的唯一 manifest 与退役配置拒绝、ADR-0170 的资源计量 fork 禁令及 registry/advisory 规则、ADR-0174 的来源闭包和消费方补丁退出责任继续有效。

## 接受的妥协（诚实记录）

Paimon 目录与读取、fs ORC 正向读取暂时缺失；保留配置与 catalog 可见性不能冒充数据读取可用。恢复要重新审计 SDK 与资源边界，并交付原生分布式行为证据，不能只从 Git 恢复源码后宣布支持。

未发布的上游 PR 可能变化或被否决，Iceberg 维护者承担持续复核与退出成本；path 包仍缺少 registry 公告覆盖，来源清单不能补成供应链安全证明。该许可优先保持上游原样，发现回归应向上游报告，不能借依赖升级在 vendor 自创兼容修补。

Server manifest 成员与实际可用能力不再等同，所有消费者必须读取真实 binding 与 pure catalogue。materialize 成功只证明 catalog generation 可以发布，操作错误和产品能力仍需分别判断。

## 何时重新评估

- 上游正式发布包含移植内容的版本时，依赖维护者按同源家族重新基线并删除移植；上游合入改写或关闭未合入时，按来源退出规则处理。
- 移植需要上游大规模重构或不断叠加私有分歧时，停止提前移植，重新评估等待正式版的成本；本条不授权扩大 vendor 面。
- Paimon 或 ORC 满足明确版本条件时，启动恢复设计与验证，保留授权读取、资源控制和 catalog 行为边界；不能仅凭 SDK 能编译恢复能力。
- 若产品需要在一个进程内并存不同 Arrow 主版本，须单独裁决类型隔离和转换语义，本条不提供桥接或 fallback。
