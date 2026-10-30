---
id: ADR-0201
title: "Node-local credential sources keep renewable material with each signing role"
domain: [provider-spi, distributed-query-lifecycle]
status: active
supersedes: []
superseded-by: null
partially-supersedes:
  - id: ADR-0151
    scope: "NodeLocal references identify role-local acquisition definitions, not independently downscoped cloud principals; Static and Vended semantics and consumer-owned renewal remain unchanged"
    link: "ADR-0151-credential-renewal-is-driven-by-the-consumer.md"
date: 2026-10-28
provenance:
  - "discussion: 2026-10-28 explicit role-local ECS credential sources for OSS"
code-anchors:
  - "novarocks/spi/src/connector/credential.rs (NodeLocalCredentialReference)"
  - "novarocks/fs/src/aliyun_ecs_credentials.rs (AliyunEcsRamRoleConfig)"
  - "novarocks-server/library/src/catalog_credential_registry.rs"
  - "novarocks/fs/src/s3_signing.rs"
---

## 问题

部署环境提供可续期的节点身份时，Catalog 如何声明取得来源，而不把临时材料伪装成静态配置或远端降权能力？

## 背景与执行事实

Catalog 的凭证模式同时决定持久化、FE/BE 投影、运行时取得能力及签名者。临时 AccessKey、Secret 和 SessionToken 是一次原子材料；来源的名字和代际只标识取得定义，不能代替材料，也不能授予访问权。

| 模式 | 引用的事实 | 材料与续期的所有者 |
|---|---|---|
| Static | 原有静态材料定义 | 原静态接线，不增加自动取得来源 |
| Vended | Catalog 公告的降权取得路径 | 原角色本地 StorageAuthority |
| NodeLocal | 部署中具名、具代际、具 purpose 的本地取得定义 | 本地来源单次取得，原 StorageAuthority 管理缓存、单飞、续期与失效 |

NodeLocal 定义使用独立类型，进入 canonical identity、持久化与 protobuf oneof。配置按 metadata/data purpose 精确解析。FE 公告非秘密引用，BE 在自己的部署配置中重建来源，跨角色没有临时材料字段。

## 考虑过的选项

**启动时取得 STS 后填入 Static。设计否决。** 配置与 operator 会固定到一份有限寿命材料；轮换无法到达继续存活的签名者，还会把材料带入生成文件。

**把 ECS 身份当作 Vended。设计否决。** 节点身份不能证明 Catalog 已完成按请求降权，也不能证明角色换代撤销旧 token；这会误授予原 vended 能力合同。

**显式 NodeLocal 来源，复用原签名与 authority。采纳。** 使用上游凭证 SDK 取得完整材料和 Expiration；不增加第二套 Rust 刷新状态机或 OSS 原生对象 I/O。Java/Python fixture 各自使用官方 SDK 的原取得和刷新能力。

## 裁决

1. **定义与材料分离。** NodeLocal 配置仅保留来源种类、名字、代际、purpose、角色名与目录 scope；不持久化临时三元组。未知来源、错 purpose 或代际不符立即拒绝，不回退到 Static、Vended 或环境默认链。
2. **签名角色本地取得。** metadata 仅接 FE，data 仅接 BE；控制和 vending principal 不接此来源。ECS 仅沿明确角色的 IMDSv2 获取，不降级 IMDSv1。
3. **原 authority 拥有 Rust 续期。** SDK 适配器返回单次完整材料与寿命；operator 继续通过原 StorageAuthority 加载，不在创建时快照材料、不旁建 runtime 或刷新任务。
4. **每个对象请求核对 scope。** 来源定义限制实际签名和预签名请求，包括 list prefix、批量删除、copy source 与 multipart；非规范路径拒绝，不靠字符串公共前缀推导权利。
5. **引用隔离不等于云端降权。** metadata/data 使用各自的角色本地引用和 purpose。它们可以取得同一部署 RAM 角色的权限；进程内 scope 不能冒充独立 RAM policy，换机器或本地代际不能声称旧临时凭证已撤销。ADR-0151 的不同 principal 要求对原 Vended 路径继续有效。
6. **能力与部署验证分开。** OSS 使用现有 S3 I/O 与正确区域、虚拟主机端点；不支持的生产条件发布仍明确失败。benchmark 完成标记沿 ADR-0202 普通发布，不能据此删除 Hadoop metadata 的生产并发保证。

## 接受的妥协（诚实记录）

节点必须能访问自己的元数据服务，且每个角色的部署定义必须一致；故障不能靠 FE 传材料补救。同一 ECS 角色可能覆盖较大的测试 Bucket 前缀，本地 scope 检查不构成云端最小权限隔离。自然到期、云端吊销和重建机器是不同事件，不能用一次轮换模拟测试证明长期云环境验收。

## 何时重新评估

需要跨租户或角色云端权限隔离时，单独设计 RAM policy 或真正 vended 降权接线；不扩大 NodeLocal 的语义。新增其他云来源时沿同一显式来源与原 authority 接缝验证其 SDK、寿命、取消和错误分类。底层 S3 API 缺少生产所需原子语义时，评估对应 Catalog 能力支持，不在凭证层绕过提交合同。
