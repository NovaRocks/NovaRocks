---
id: ADR-0174
title: "Vendored Iceberg baselines are reproducible and protocol patches have explicit deletion owners"
domain: [crate-boundary]
status: active
supersedes: []
superseded-by: null
partially-superseded-by:
  - id: ADR-0204
    scope: "Ruling 2 protocol-only divergence: additionally allow bounded exact upstream Iceberg dependency backports with provenance and exit conditions"
    link: "ADR-0204-upstream-iceberg-backports-and-provider-suspension.md"
  - id: ADR-0179
    scope: "P2 terminal builder exit and U4 Hadoop directory-discovery handoff to IRU-6c; unrelated baseline and patch policies remain active"
    link: "ADR-0179-iceberg-publication-lifecycle-and-evidence.md"
  - id: ADR-0178
    scope: "Core U8 Arrow writer schema override retention and Variant read capability deferral; unrelated baseline and patch rules remain active"
    link: "ADR-0178-iceberg-write-type-boundary-and-variant-layering.md"
  - id: ADR-0175
    scope: "REST crate patch inventory (U1r, R1-R3, R5-R8, R10) and its deletion; ruling 4 temporary REST outcome narrowing; the compromise that authentication uses the SDK session and merged properties"
    link: "ADR-0175-iceberg-rest-session-authority-and-mutation-evidence.md"
  - id: ADR-0176
    scope: "Ruling 3 retention row for core U2, P8 and View API and its deletion condition: the patches are deleted; staged-create initialization, add-schema and view request vocabulary are owned by the Connector"
    link: "ADR-0176-iceberg-rest-request-vocabulary-and-supported-server-floor.md"
partially-supersedes:
  - id: ADR-0170
    scope: "Patch allowlist: add iceberg-property-macro; current Iceberg baseline and vendor code anchors"
date: 2026-10-13
provenance:
  - "discussion: 2026-10-13 coordinated Iceberg dependency baseline, protocol patch policy and conservative REST publication outcomes"
code-anchors:
  - "Cargo.toml ([patch.crates-io])"
  - "novarocks/connector/iceberg/src/resources.rs (IcebergCatalogRuntime::new)"
  - "novarocks/connector/iceberg/src/manifest.rs (load_manifest_list, load_manifest)"
---

## 问题

依赖上游 Iceberg SDK 的协议扩展时，怎样让源码基线可重建、补丁可退出，并让版本升级保持外部提交与产物清理的所有权？

## 背景与执行事实

NovaRocks 的 Iceberg Connector 使用 SDK 的元数据、格式与 catalog 接口；表读取、提交准备、外部结果判定和产物生命周期仍由 Connector 拥有。根 `[patch.crates-io]` 对整个 Cargo workspace 生效。目录中的版本号说明 Cargo package 版本，不单独证明正式发布或来源身份。

当前四个包共同来自 `apache/iceberg-rust` 的 tag `v0.11.0-rc.2`，commit 为 `859048563bc71a3167d0545cb66756d26c64e4ca`。源码经 Cargo 规范化，保留上游发布文件、依赖版本范围与 lint；上游 workspace 继承不直接进入排除于 NovaRocks workspace 之外的 vendor 目录。

| 包与来源清单 | 规范化 `.crate` SHA-256 | 本地分歧 |
|---|---|---|
| [iceberg](../../vendor/iceberg-0.11.0/PATCH.md) | `83bd584247a59b6ce2d2f6ce08188640977568aff9b7240c4a5e45a1c6214142` | 元数据/API 协议接缝 |
| [iceberg-catalog-rest](../../vendor/iceberg-catalog-rest-0.11.0/PATCH.md) | `fde7674c80f19dc54ae2b0b3fb95f0715a4407df615fa63fe42801bc3ec2d2d4` | 单页列举、view、stage/commit、委派与脱敏 |
| [iceberg-catalog-hms](../../vendor/iceberg-catalog-hms-0.11.0/PATCH.md) | `d934fc971b51044affb2b183a19f4fefcddaa513deb3350af6b361d6970010c0` | 缺表类型与实体列表分类 |
| [iceberg-property-macro](../../vendor/iceberg-property-macro-0.11.0/PATCH.md) | `23215cd7fcefd7d7c02f2590052cf1dc5311f7867b2e626d8b7b0b77692e4dd3` | 无源码补丁；与 core 同版本的构建依赖 |

生成命令在已核对的上游 checkout 中执行：

```sh
cargo +1.98.1 package --no-verify --exclude-lockfile --offline \
  -p iceberg-property-macro -p iceberg -p iceberg-catalog-rest -p iceberg-catalog-hms
```

`--exclude-lockfile` 只省略 package lock 的生成，避免为无关上游 workspace 开发包解析依赖；不改变发布源码或规范化 manifest。四个包先按原包解压并保留来源清单，再逐项移植协议补丁。`iceberg-property-macro` 保留 `0.11.0` 版本依赖，避免以无版本 path 代替依赖约束。它进入 patch allowlist 是同一 core 构建依赖的来源闭包，不授予新增运行时库或计量钩子的许可。

| Owner / 载体 | 拥有的事实 | 不从它推导的结论 |
|---|---|---|
| 每个包的 `PATCH.md` | 来源、用途、上游状态、生产消费者、删除条件与测试 | path 身份不能恢复 registry 安全公告覆盖 |
| Connector 的 catalog owner | 接受哪些外部证据、是否允许重试与清理 | 通用 SDK `ErrorKind` 不能独立证明所有 catalog 未提交 |
| `IcebergCatalogRuntime` | 由组合根注入句柄构造的 SDK Runtime | retry recipe 不携带 Runtime、FileIO 或请求资源 |
| `manifest::load_manifest_list` / `load_manifest` | 解析与旧继承字段语义 | 不调用 SDK reader 隐式解密或分配 `first_row_id` |
| 不可变 operation / attempt / artifact owner | 准备、冻结、单次派发、真实退出与清理责任（[ADR-0171](ADR-0171-commit-operation-model.md)） | SDK transaction/action 不是提交权威 |

## 考虑过的选项

**A（选中）：协调切换同源包，保持既有协议行为，并把补丁退出交给实际消费者 owner。** 保留 SDK 的新公开扩展点，同时避免把依赖升级、所有权重构和物理布局恢复揉成一次不可审阅变更。代价是短期继续维护协议补丁，并承担之后删除它们的迁移成本。

**B：等待全部补丁退出后才采用新的 SDK 基线。成本否决。** 可减少一次性移植，但会推迟需要新公开认证/会话扩展点的 Connector 改造，使后续代码在旧版本上先写、再迁移。上游提供完整替代 API 或补丁消费者迁移完成时，应重新评估这一成本。

**C：直接复制上游 workspace 源码，或只替换 core 而保留旧 catalog / macro 来源。设计否决。** 未展开的 workspace 继承不能独立解析；不同版本的接口与来源也不能组成可重建的同一依赖闭包。

**D：为保持旧功能或取得库内资源钩子，扩展升级时的补丁面。设计否决。** 升级不能成为扩大 vendor 的隐性授权。公开配置与库外准入原则仍由 [ADR-0170](ADR-0170-third-party-crates-are-bounded-by-public-configuration-not-forked.md) 约束；物理 Variant 功能需要由 Provider 的布局 owner 单独恢复，不改写 SDK 的不支持分支来绕过它。

## 裁决

1. **Coordinated source closure**：core、REST、HMS、property macro 使用同一核实过的上游基线。tag、commit、规范化生成命令与包 hash 必须可查；Cargo 目录版本与 RC 身份分别记录。
2. **Protocol-only divergence**：只原样迁移已接受的协议/API 补丁，必要结构适配不改变它们的对外合同。不恢复上游已吸收、生产不用或明确退出的旧分歧，不增加资源计量/退出钩子，不放宽上游 lint。
3. **Consumer-owned deletion**：删除补丁之前，由相应消费者交付等价接缝与覆盖；排进路线图不等于可删除。以下表给出当前完整清单与责任，具体源码/test 仍以各包 `PATCH.md` 为准。

| 保留项 | 当前用途 | 删除 owner 与条件 |
|---|---|---|
| core U4 | Hadoop 目录发现 | IRU-6c 的 Connector storage discovery owner 迁移后删除；IRU-6a 保留现协议 |
| core U2、P8、View API | REST 更新词汇、权威 stage 初始化、view carrier | Connector REST protocol owner 接管相应 wire/初始化/view 词汇并通过契约测试后删除 |
| core U8 | Arrow writer schema override | Provider Variant 物理布局/writer owner 提供正式布局接缝并移除该 override 消费后删除 |
| core P7a | 缺 `next-row-id` 的 v3 metadata 读取 | Connector 外部语义身份与兼容 owner 完成对应输入迁移并承接读取覆盖后删除 |
| core U3 | 完整 snapshot refs 观察 | Iceberg Connector 维护者采用等价上游访问器或迁移完整 ref-set 消费后删除 |
| REST U1r、R1、R2、R3、R5、R6、R7、R8、R10 | 分页/view/stage/commit、延迟委派、续租及脱敏 | Connector REST protocol/session owner 用共享认证会话和闭合响应/证据接管各端点及消费方，等价测试通过后逐项删除 |
| HMS H3 | `load_table` 以类型区分确知缺表与不可达 | Iceberg Connector 维护者采用保持 typed absence/transport 区别的上游 load API 并通过消费覆盖后删除 |
| HMS H4 | 实体参数分类；每批至多 100 个名字 | catalog listing/admission owner 接管实际 HMS 实体分类、缺失省略与批次验证，或采用等价上游行为后删除 |
| property macro | 同源构建依赖，无源码补丁 | 依赖维护者在 registry 具有等价可用来源、来源闭包可重建时评估移除 path；不按协议补丁退出处理 |

4. **Truth before cleanup**：REST 派发后，仅 `CatalogCommitConflicts`、`TableAlreadyExists`、`NamespaceAlreadyExists` 这三类收到的冲突错误参与确知未提交证明。404、400、解码无效的 200 和成功响应后的本地构造失败保留 Unknown，不授权清理。CTAS 最终提交把 SDK 的 `KnownNotDispatched` 与 `CommittedResponseInvalid` 也保守映射为 Unknown；stage 判定与 Hadoop 条件创建证明保持。该临时规则由 REST owner 的 `admits_uncommitted_proof` 选择，不全局收窄其它 catalog。
5. **Explicit runtime and owned artifacts**：SDK Runtime 只在 role-local 资源 owner 构造处从注入句柄取得一次，向 catalog/table builder 传递；请求/retry recipe 保持资源中立。在已有 `commit/operation` 与 `commit/staging` 写出路径中，manifest 产物先登记再打开 writer，取消/超时仍等实际退出。旧 direct `REWRITE MANIFESTS` 的 operation 接管与清理由 IRU-6a 承接，不属于本次已验证的登记/退出保证；本次保持其原有边界。读取保持既有继承规则。
6. **Capability honesty**：Variant 的类型构造迁移到上游 `Type::Variant`，只保证编译；旧写出转换/shredding 与特殊兼容捷径退出，SDK writer 的 `FeatureUnsupported` 保持。用户文档、删除/忽略测试与 SQL 跳过必须明确其尚待恢复的能力。

本 ADR 仅部分替代 ADR-0170 的 **patch allowlist**（增加 property macro）和当前 Iceberg **vendor 基线/代码锚点**。ADR-0170 仍保持 active；不为资源计量 fork 第三方库、registry/advisory 规则、外部 SDK 列表边界和现有序列化所有权原则继续适用。

旧 core 的 P3/P4/P5/U5/U9 仅由 SDK scan/ArrowReader 到达，NovaRocks 生产读取由自有 read stack 承担，故不再保留；P7b/U7 已由上游吸收。P6 的旧 PrimitiveType Variant 扩展退出。REST R4 的类型化错误由上游吸收；R9/R11/R12、无消费者的委派方法/类型与 token invalidate/regenerate helper 退出。HMS H1 的旧 Variant 匹配不再需要；H2 的写入补丁已退出，HMS 永久只读边界由 [ADR-0169](ADR-0169-read-only-hms-and-single-writer-admission.md) 保持。

## 接受的妥协（诚实记录）

- 四个 path 包不具有 registry 包的已知公告覆盖；源码身份清单和上游复核不能冒充 cargo-deny 对 registry 的安全证明。property macro 没有源码补丁，仍是维护成本明确的 vendor 依赖。
- 原样移植需要后续 owner 再删除一次。选择依据是避免阻塞依赖新公开接缝的改造，不是认为永久修改 SDK 更好。
- SDK 先缓冲整个 REST 响应体；提交在读取响应体失败时只能保留 PossiblyDispatched。临时结果收窄也会多保留未发布产物，不能反向把 Unknown 猜成未提交。外部年龄窗 GC 的边界继续见 [lake publication ADR](ADR-0110-lake-publication-crash-only-contract.md)。
- 当前认证仍使用 SDK 的 session/merged properties；本 ADR 不提前交付新的会话权威。默认 `none` 不因服务端下发 token 启用认证，已经选中的 OAuth2 会话仍按 SDK 合并属性处理 token。详细配置变化见 [Catalog 指南](../guides/iceberg-v3/catalog.md#rest-认证)。
- Variant 暂无 Iceberg 数据路径功能承诺；相关写出/SQL 覆盖暂缺，恢复 owner 必须重新交付数据正确性覆盖。开启 Parquet encryption feature 也不自动宣布 NovaRocks 已支持加密读取。

## 何时重新评估

- 上游正式发布新的同源包、补齐公开 protocol/view/listing/credential API 或暴露正式 writer layout 接缝：重新核对依赖闭包与消费覆盖，删除对应分歧；同时复核选项 B 的继续移植成本。
- Connector REST owner 完成共享认证会话、完整请求形态和闭合发布结果证据：逐项退出 REST/core 协议补丁；以等价测试证明替代后，删除临时 owner 谓词与委派模式的存在性 helper。不能仅因重构开始便恢复通用 SDK 错误的清理权限。
- Provider Variant layout/writer 能正确处理上游 Struct 表示、执行层逻辑载体与真实写出：恢复功能并重新运行被删/忽略/跳过的性质覆盖；不在基线升级内增加临时功能。
- path 依赖出现安全、许可或维护问题，或上游变更破坏现有接缝：独立审阅升级/替换策略。第三方计量与观测按当前测试治理另行评估，本来源策略不提供新的字节上界或性能结论。
