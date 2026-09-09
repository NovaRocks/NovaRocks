---
id: ADR-0139
title: "Server seals active providers and rejects retired configuration"
domain: [provider-spi, runtime-role]
status: active
supersedes: [ADR-0132]
superseded-by: null
date: 2026-09-09
provenance:
  - "discussion: 2026-09-09 Iceberg and Paimon sealed provider convergence"
  - "PR: pending — backfill the number once the provider manifest migration merges"
code-anchors:
  - "novarocks-server/src/provider_manifest.rs (PROVIDER_BUILDERS)"
  - "novarocks-server/src/app_config.rs (deserialize_connector_config)"
---

## 问题

当 Server 同时拥有 provider compatibility、FE/BE factory 投影和 role-local 配置时，怎样定义“当前可用 provider”，才能保留 provider-owned role factory，同时避免已废弃 connector 通过遗留配置形成集合之外的装配入口或虚假的产品能力？

## 背景与执行事实

ADR-0132 裁决 provider crate 拥有完整 role-binding factory，Server 只组合 role-local resource。其原则正确，但正文以当时的 StarRocks `local_binding`、HTTP client 和空 capability 为主要执行事实。后续多 provider 迁移把 provider contract、私有 descriptor、FE control factory builder 与 BE execution factory builder 收敛到同一个 `ServerProviderManifest`；Native compatibility 和两个角色投影都读取该 manifest。

当前 manifest 精确包含 Iceberg 与 Paimon。Iceberg 提供既有读写能力，Paimon 提供 Filesystem Catalog 上 append-only 与 `deduplicate` 主键表的只读能力。StarRocks connector 源码仍可作为历史参考，但未登记 contract/factory，没有 active read capability，也不再是 Server 依赖。旧 Server 配置仍曾接受 `[connector.starrocks].local_bindings`、构造 FE-local client，随后没有任何 manifest consumer 使用该资源；这会让运维方误以为 StarRocks 已启用，并让 secret/config 生命周期脱离实际能力集合。

## 考虑过的选项

**A. 为保留旧配置而把 StarRocks 重新加入 manifest。** 这样可以继续构造 local resource，但空 read capability 不能兑现查询能力，还会把废弃 provider 的 descriptor/factory 重新纳入所有 FE/BE compatibility identity，因此否决。

**B. 保留 `[connector.starrocks]` 解析，但忽略其结果。** 这对旧配置看似宽容，却会在启动成功后留下不可消费的 endpoint、credential 和 client，使配置成功不再表示对应能力存在，也无法区分拼错配置与有意废弃，因此否决。

**C. 删除所有 StarRocks 源码。** 能缩小仓库，但源码仍有历史实现和对照价值；是否物理删除不决定产品能力，且不应与 Server 装配权绑定，因此本次不要求。

**D（选中）. 单一封闭 manifest 定义 active provider，并显式拒绝退役配置。** 保留 ADR-0132 的 provider-owned factory 原则；Server 只从 manifest 组合每个 active provider 的角色资源与 factory。未登记 provider 不能由独立配置、registry 或 fallback 恢复。

## 裁决

`ServerProviderManifest` 是进程启动时 active provider 集合的唯一组合入口。每个 entry 必须同时提供 provider-owned contract、FE control factory builder 与 BE execution factory builder；Server 在开放 listener 前验证 identity 一致，并从同一个 immutable manifest 派生 `NativeCompatibilityId`、FE factory 集合和 BE factory 集合。缺 factory、重复 identity、descriptor/capability 不一致或角色投影漂移都使启动失败。

当前 active sealed set 精确为 Iceberg 和 Paimon。一个 provider 未出现在 manifest 中，就没有产品能力；crate 能编译、源码仍存在或 catalog type 字符串可解析，都不能替代 manifest admission。StarRocks 因而是废弃参考实现，没有 active read capability。Server 看到遗留 `[connector.starrocks]` 配置时必须返回固定、脱敏的明确错误，不能构造资源后闲置，也不能静默忽略。

Provider 继续拥有完整 role-binding factory 和私有 codec/领域解释；Server 只拥有启动配置解析、secret resolution、role-local 通用资源组合与封闭 manifest。恢复 StarRocks 或增加其他 provider 需要单独接受的能力设计，并原子加入 contract、两个角色 factory、private descriptor、compatibility 与真实分布式验证；不得只恢复 local-binding 配置。

## 接受的妥协（诚实记录）

**遗留 StarRocks 配置会在升级时硬失败。** 这会要求运维方删除不再生效的配置，无法做到无提示兼容。选择明确失败是因为启动成功必须准确表示可用能力，不能让废弃 secret 与 endpoint 继续被解析却无人消费。

**保留废弃源码会增加发现成本。** 读代码的人可能仍误以为 crate 等于产品能力，因此 README、部署指南和 AGENTS 必须同时说明 manifest 才是 authority。物理删除源码可另行评估，但不能以目录存在推导 active provider。

**静态集合要求统一升级 binary。** 即使部署只使用一个 catalog，Iceberg/Paimon descriptor 都进入 compatibility identity。增加或修改 provider 需要 FE/BE 同步部署；这是 admission 前确定失败换来的运维成本。

## 何时重新评估

- StarRocks 或其他 provider 获得单独接受的真实读写设计、完整双角色 factory、私有 descriptor 和 native `1FE+3BE` 证据时，重新评估 active set；不能仅凭旧 crate 或配置恢复。
- 引入可信动态插件系统时，重新设计签名、加载、兼容 island、角色资源与失败隔离；不得把 manifest 替换成字符串查找或运行时 fallback。
- 保留废弃 StarRocks 源码持续造成错误使用、依赖负担或安全维护成本时，评估物理删除 crate，并保持配置拒绝语义。
- 同一静态 binary 内完整 provider descriptor 导致不可接受的统一升级频率时，可评估更细 compatibility island，但所有参与角色仍必须在 I/O 前 exact-fail。
