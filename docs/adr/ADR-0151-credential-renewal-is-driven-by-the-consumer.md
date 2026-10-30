---
id: ADR-0151
title: "Credential renewal is driven by the consumer that signs with it"
domain: [distributed-query-lifecycle, provider-spi]
status: active
supersedes: [ADR-0149]
superseded-by: null
partially-superseded-by:
  - id: ADR-0201
    scope: "NodeLocal references identify role-local acquisition definitions rather than independently downscoped cloud principals; Static, Vended and consumer-owned renewal remain effective"
    link: "ADR-0201-role-local-node-credential-sources.md"
  - id: ADR-0175
    scope: "Meaning of the announced acquisition paths: both carry the frozen public routing context, the execution node never opens its own catalog session, and a vended read is admitted only when execution-node first acquisition can be formed; consumer-driven renewal, role-local material and ratio windows remain effective"
    link: "ADR-0175-iceberg-rest-session-authority-and-mutation-evidence.md"
date: 2026-09-18
provenance:
  - "PR: https://github.com/NovaRocks/NovaRocks/pull/1063"
  - "discussion: 2026-09-16 CAD-1 caller-driven vended credential renewal design review"
code-anchors:
  - "novarocks/fs/src/storage_authority/mod.rs (StorageAuthority, AuthorityCapabilityPath, RefreshPolicy)"
  - "novarocks/fs/src/access.rs (AuthorityCredentialLoad, CredentialDenialLayer)"
  - "novarocks/connector/iceberg/src/execution_authority.rs (ExecutionNodeCredentialsEndpointRefresher)"
  - "novarocks/connector/iceberg/src/authority_source.rs (classify)"
  - "novarocks/spi/src/connector/credential_lease.rs (CredentialRenewalPath, CredentialLeaseDescriptor)"
---

## 问题

一份有寿命的存储凭据，由谁负责在它到期前换一份新的——是**下发它的那个节点**，还是
**用它签请求的那个节点**？

## 背景与执行事实

凭据材料不是一份可以搬运的数据，它是**存储客户端身份的一部分**。opendal 的 S3 客户端在每次请求
时调用 `load_credential()` 签名（`opendal-0.55.0/src/services/s3/core.rs`），provider pool 按
身份复用 operator。把材料焊进 operator 构造时，等于把「这个客户端是谁」钉死在第一份材料上：
后来下发的材料到不了它，第一份一过期它就停止工作。

| 实体 | 扛什么 | 按什么键 |
|---|---|---|
| `StorageAuthority` | 一个 scope 的材料及其取得能力 | `(CatalogHandle, prefix, AuthorityCapabilityPath)` |
| `AuthorityCapabilityPath` | **怎么**取得：服务端广告的地址 / load-table / 进程内已持有的 provider / 无续期权 | 其自身即键的一维 |
| `CredentialLeaseDescriptor` | 非秘密的**能力公告**：哪些 scope、沿哪条路径取 | 跨 FE/BE 进程边界 |
| `RefreshPolicy` | 预取窗口、有效性余量、退避、阻塞取得的努力窗口 | 全部为**比例**而非绝对值 |

三条容易踩的事实：

- **取得能力与材料寿命是两条独立的生命周期。** 材料 10:00 到期、查询 10:01 才需要下一次读取时，
  只要授权与取得能力仍有效，就应当重新取得。把两者绑在一起会让「材料没了」被读成「不能再取了」。
- **绝对值的窗口会在短寿命凭据上退化成循环。** 凭据整个生命都短于预取窗口时，authority 永远
  处在自己的窗口里，每个没有在途刷新的请求都发起一次刷新；catalog 流量随请求数放大而非随到期放大。
  有效性余量此前也踩过同一形态：固定余量会让短于余量的凭据一装上就被判为不可用。
- **一次操作的取得努力必须共享一个窗口。** 存储层会重试凭据加载失败的请求，每次重试各开一个
  取得预算时，合成开销是「重试次数 × 预算」，可以活得比它服务的那条查询还久——查询于是死于
  exchange 空闲超时，操作者被告知的是 exchange 而不是 catalog。

## 考虑过的选项

**A. 协调节点继续下发，并为消费者代为续期。** 机制：FE 侧轮换 owner 在到期前取得新材料并推给每个
BE。优势：BE 不需要 catalog 身份，部署面小。代价：材料必须跨进程传输（因此必须有加密传输准入门）；
一次轮换是集群级屏障，任一 BE 慢或不可达就拖住其余；材料的生命周期与使用它的对象的生命周期
在两个进程里各走各的。**设计否决**。

**B. 消费侧持有取得能力，协调节点只公告路径。** 机制：执行节点用自己的 catalog 身份直接取得；
协调节点公告「哪些 scope、沿哪条路径」，材料不过 wire。优势：材料与签名者同进程同生命周期；
没有集群级屏障；传输上不再有秘密，加密准入门随之失去被守护的对象。代价：执行节点到 catalog 的
网络可达性成为部署硬要求；每个外部 catalog 接入时都要确认其降权能力。**采纳**。

**C. remote signing（每请求由服务端签名）。** 规范侧未废弃且仍在扩展，但它是逐请求签名，社区自认
对大表会压垮服务端；Trino 未实现（trinodb/trino#21189 自 2024-03 开着）；iceberg-rust #506 已于
2025-10 关为 not_planned。**登记为不追求**，设计上只要求接缝不堵死它。

外部对照：Trino 把 catalog 凭据放在 worker 的节点本地配置里、由 worker 自行续期；Polaris 与
Lakekeeper 都按 principal 授权 `/credentials`，所以「给执行节点单独的 principal」不会换来更大的
授权面。Unity Catalog OSS 未实现 `/credentials`，凭据只能来自 `load_table`——这是能力协商必须有
两条路径而不是一条的原因。

## 裁决

选 B。固化成以下具名规则。

**开发规则**

- **材料属于身份**：凡是「哪个客户端」的一部分，就不得在客户端构造后由外部替换。新材料要么经
  authority 到达签名者，要么根本不要发。
- **公告非秘密，材料不旅行**：跨进程只传「哪些 scope、沿哪条路径取」。任何让材料重新上 wire 的
  改动，必须同时回答加密准入门由谁承担。
- **一个 principal 一条能力路径一个 authority**：`AuthorityCapabilityPath` 是缓存键的一维。协调
  节点与执行节点的 principal 必须不同（配置期拒绝同名同代际引用），单进程同时跑两个角色时靠这条
  区分，而不是靠进程边界。
- **所有时间窗口按比例，不按绝对值**：预取窗口、有效性余量都从凭据自身寿命派生并 clamp。绝对值
  大于凭据寿命就会退化——预取退化成循环，余量退化成一装上就不可用。
- **一次操作一个取得窗口**：第一个手里没材料的调用者开窗，其后所有调用者共用；窗口关闭时交回
  上一次取得**自己的原因**并退避到足以吞掉存储层剩余重试。不得让每个被重试的请求各开一个。
- **投影是白名单**：新增的 purpose / 能力枚举值必须同时检查每一个 `*_projection` 与
  `.filter(|x| x.purpose() == ...)`，它们默认丢弃不认识的值。

**调试规则**

- **四种取得失败必须可区分**：确认撤权（终结能力，不得当抖动重试）、catalog 不可达（部署问题）、
  网络抖动（可退避重试）、无续期权（部署形态，不是失败）。把前两者合并会让操作者去查权限策略，
  而事实是这个节点根本连不上 catalog。
- **错误文本必须带上原因链**：分类正确但原因丢在渲染处，等于没分类。

## ADR-0149 五条裁决的逐条处理

| ADR-0149 原裁决 | 本 ADR 的处理 |
|---|---|
| 1 放弃一轮不失败 attempt，并把在飞调用移交残留 owner | **部分保留**：保留「预取失败不等于操作失败」。**删除**移交机制——消费侧 authority 用代际栅栏在发布处丢弃迟到结果，不需要有人替它把调用养活。没有有效材料时的按需取得失败**仍可能失败操作** |
| 2 严格按 `not_after`、无余量判定失效 | **职责保留**：失效仍由取用处判断。**无余量规则被取代**：余量按剩余寿命的比例计入时钟偏差 |
| 3 以旧 lease 剩余寿命为退避上界，寿命耗尽即停止轮换 | **目的保留**（不得对刚失答的 provider 热循环）。**规则改写**：不得再用旧材料的寿命限制按需取得——材料到期不是取得能力到期 |
| 4 provider 调用须覆盖连接、读取与全部内部重试的总时限 | **保留并加强**：单次调用仍有硬 deadline，另加「一次操作共享一个取得窗口」，因为存储重试会把单次预算乘起来 |
| 5 轮换过程必须有生产可观测量 | **保留**，观测对象改为消费侧 authority（cache hit / prefetch / blocking wait / applied / failed / late discarded）。删除旧 owner 不等于删除这项要求 |

## 接受的妥协（诚实记录）

**执行节点到 catalog 的可达性成为部署硬要求。** 这是真实新增的运维约束，不是「本来就该这样」。
换来的是材料不再跨进程、轮换不再是集群级屏障。

**D1 是我们自己的架构选择，不以「比业界严」为依据。** 代价是接入每个外部 catalog 时都要确认
其按 principal 降权的能力。

**类型面今天仍是 S3 焊死的**：`ObjectStoreSecretMaterial{ak,sk,token}`、
`StorageCredentialScopePrefix` 强制 `scheme == "s3"`、SPI 的 `resolve_vended_s3`。接缝定义为
「给定 location，返回一个能授权该请求的 authority」，S3 provider 是它下面的第一个实现——但泛化
不在本轮范围内。

**阻塞取得的努力窗口上界是一个常量（30s），不是从查询存活预算推导的。** authority 看不到查询的
deadline：operator 是进程级的、跨查询复用的，没有 per-query 上下文能到达凭据加载器。常量选在
引擎最短存活边界（exchange 空闲超时）之内，但这是**选的**，不是**推出来的**。

## 何时重新评估

- 接入一个按 principal 无法降权的外部 catalog——那会让 D1 的授权面假设失效。
- 存储访问需要非 S3 provider——那会触发 `resolve_vended_s3` 一族的泛化。
- `remote-signing` 在上游获得可用实现且有真实需求——接缝已留门，但届时要重估服务端成本结论。
- 出现一种能把 per-query deadline 送达凭据加载器的机制——那时努力窗口的常量上界应当改为推导值。
