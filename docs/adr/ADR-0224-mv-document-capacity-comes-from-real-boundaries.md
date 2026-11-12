---
id: ADR-0224
title: "MV document capacity comes from real boundaries"
domain: [frontend-mv, provider-spi, frontend-durable-records, crate-boundary, sql-compiler]
status: active
supersedes: []
superseded-by: null
partially-superseded-by:
  - id: ADR-0227
    scope: "Background table retention of fixed candidate/thin-inventory/name/token limits; document, StateStore, identity and lifecycle rules remain active"
    link: "ADR-0227-metadata-listing-and-task-transport-use-real-capacity-boundaries.md"
partially-supersedes:
  - id: ADR-0180
    scope: "Ruling 1 document-version, aggregate-private-property and snapshot-record capacity quotas, and the first reevaluation trigger; inline storage, identity, request shapes, lifecycle, evidence and external rollback boundaries remain active"
    link: "ADR-0180-inline-keyed-documents-and-snapshot-records.md"
  - id: ADR-0175
    scope: "REST session and mutation-evidence ADR only: ruling 23 retention of requested property values and the definite-rejection set in ruling 16; authentication, routing, complete JSON validation, attribution and redaction remain active"
    link: "ADR-0175-iceberg-rest-session-authority-and-mutation-evidence.md"
date: 2026-11-08
updated: 2026-11-09
provenance:
  - "discussion: 2026-11-08 MV document capacity, backend-declared StateStore capabilities and request-derived REST evidence retention"
  - "discussion: 2026-11-09 ProviderRead aggregate version quota retirement and native compatibility isolation"
code-anchors:
  - "novarocks/state-store/api/src/limits.rs (StateStoreCapacity)"
  - "novarocks/connector/iceberg/src/document_storage/keyed/write.rs (create_properties, publication_updates)"
  - "novarocks/connector/iceberg/src/rest_protocol/evidence.rs (classify)"
  - "novarocks/mv-application/src/persistence/codec/wire.rs (preflight)"
  - "novarocks/connector-contract/src/read_facts.rs (ConnectorReadInputVersion)"
  - "novarocks/plan-codec/src/physical_provider_read_v2.rs (ProviderReadProjectionLimits, ProviderReadProjectionFacts)"
  - "novarocks/plan-codec/src/physical_package_v2/frozen_limits.rs (PackageEncodeLimits::FROZEN, PackageDecodeLimits::FROZEN)"
  - "novarocks/physical-plan/src/lib.rs (PLAN_CONTRACT_REVISION)"
  - "novarocks-server/library/src/native_compatibility.rs (resolve_native_compatibility_material)"
---

## 问题

MV 定义、发布记录、本地投影及执行所需读取命名空间的可接受规模应由哪些真实边界决定，怎样保证写入、核验、执行与恢复遵循同一合同？

## 背景与执行事实

MV Application 拥有计算定义 D、物化解释 L、发布 P 与配置 C；绑定 B 关联 D/L 与各来源 occurrence 的数据版本格式。
Iceberg Provider 将不可变版本、槽、根键与标记内联在表属性中，将发布记录附着在准确的快照 summary 上。
StateStore 中的整份投影仍是一个可从湖上来源重建的 Accelerator 值，不是文档权威、分片文档服务或恢复日志。

这些表示的长度不同：SQL 原文经过领域编码、引用和身份封套，再经过 base64url 与 JSON，不能把入口字符数当作最终字节数。
写方允许的文档若被另一层固定额度拒绝，就会出现成功保存后不能读取、核验或恢复的定义。固定估算也不等于实际资源授予。

| 边界与作者 | 实际负责的事实 |
|---|---|
| 客户端语句准入 | 可配置的语句长度上限；原文的上游入口约束不等于每一种派生表示的字节上限 |
| `persistence::codec::wire::preflight` 与领域验证 | 封闭 protobuf schema、规范编码、完整性、引用与身份；不推算解码工作集或用条目/深度配额拒绝有效文档 |
| Iceberg keyed writer | typed 文档、引用闭包、准确附着与原子提交；私有属性大小保留为诊断，不成为准入额度 |
| REST `TableViewRequest` 与 `AttributionExpectation` | 只为请求点名的属性保存足以比较的值；完整 JSON 校验和准确归属仍不可省略 |
| `StateStoreCapacity` 与具体 Provider | 声明本后端可用的键、值、事务字节与操作数边界；API 的中立字节类型不自带统一容量常量 |
| Accelerator 与候选盘点 | 整份投影的编码、装入与读回；盘点条目、薄快照、名字和续读令牌的既有界与定义大小分开 |
| `ConnectorReadInputVersion` | 单个 opaque 读取版本值的 1..4096 字节表示合同；不是整个读取命名空间的容量 |
| ProviderRead codec 与 package frozen policy | 独立读取身份、引用闭包与累计资源事实；读取定义数、allocation requests、request bytes、source/request coexist 和 work 检查继续由原编译 owner 执行 |

生产 SQLite 的键值能力从 bundled SQLite 的长度限制及真实 record 开销推导，并在打开连接时核对实际限制。
单键或单值声明不保证二者同时取各自最大值：同一行还包含另一列与 record header，组合超过 SQLite 的真实行边界仍由它拒绝。
SQLite 不另设事务字节或操作数额度；物理事务期限与应用运行政策仍有各自 owner。实验性 MySQL、FoundationDB 分别声明自己的边界。
这改变的是 StateStore 容量合同，不改变 [ADR-0122](ADR-0122-sqlite-is-the-only-production-state-store.md) 的生产 Provider 选择，
也不改变 [ADR-0140](ADR-0140-state-store-contract-and-testkit-crates.md) 的契约/testkit 物理分工；两条 ADR 均不被部分替代。

ProviderRead 是执行侧的读取引用命名空间，不是 MV 发布 P 的数据版本格式。不同 scan 的读取版本即使字节相等，
仍须保留其各自冻结的身份与引用；版本字节之和只反映一项累计事实，不等于整个 source、申请或并存占用。
曾有的 1024 字节累计额度既不是 wire 的单值表示边界，也没有独立测量依据，并与已有 source/request/coexist 检查重叠。
撤销它只改变这一项共享编译策略，不推广为撤销其他 package 资源门，也不为 MV 增加绕行编译的特殊入口。

## 考虑过的选项

1. **把文档额度调大。设计否决。** 改成更大的固定数字实现简单，却仍没有协议、实现或测量依据，
   只把不可读的边界推迟；不能把“有界”本身当作容量值的理由。
2. **按写入、读取、加速态分阶段放宽。设计否决。** 每个中间状态都可能允许一方产生另一方不能消费的文档。
   同一持久化合同的各作者与消费者必须一起演进。
3. **在 MV 侧分键保存投影。成本否决。** 分片可以适配小值后端，但需要拼装、一致性与事务安排，
   单值之外仍可能有事务边界。为兼容非生产后端而让 MV 自带第二套存储协议，当前没有收益依据；
   未来远程 StateStore 的大值表示应由该后端的完整设计处理。
4. **把文档写成表目录或仓库里的私有文件。设计否决。** 普通 Iceberg 引用图不保护这些文件，
   孤儿清理可能删除正确性所需字节；登记到可被其他引擎替换的统计槽也不能建立文档生命周期。
5. **为每个 MV 建伴生 Iceberg 表。成本否决。** 标准数据行能获得文件引用保护，但新增对象、名称生命周期、
   隔离与跨对象原子性。它是实测内联成本不再可接受时的候选，不是撤销无依据额度的前提。
6. **保留解码前的文档、条目、深度或工作集估算额度。设计否决。** 这些数字既不是封闭格式的结构要求，
   也没有接到实际容量授予，不能保证避免分配失败，却会拒绝有效输入。能改写 catalog 属性的主体本就能破坏表，
   不能用这一威胁假设替代有依据的进程资源治理。完整性扫描保留，资源预检配额删除。
7. **413 继续报告结果未知。成本否决。** 这是保守且安全的选择，但请求内容已被明确拒绝时仍进入观察流程，
   会延迟自有产物清理并掩盖准确错误。选择让该状态产生确定拒绝；其他状态仍按各自证据分类。
8. **保留或调大读取命名空间的独立版本合计额度。设计否决。** opaque 版本值合计便于计算，
   但不能替代完整 source、申请、并存和 work 的核算。单值合法、其余资源门允许的读取不应仅因版本合计超过任意常量被拒；
   改成更大数字或以无限值保留该 limits 字段，仍混淆累计事实与资源政策。
9. **真实边界声明、请求决定核验保留量、读写执行同时贯通。采用。** 保留内联格式、单值投影与共享 compiled 路径，
   删除没有真实依据的中间额度，后端执行自身限制，资源压力由原资源 owner 处理。

## 裁决

1. **MV 文档不自设容量额度。** D/L/B/C、发布记录、私有属性集合及 Accelerator 投影不设 NovaRocks 侧文档字节、
   条目、深度或推算工作集配额。原文沿用语句长度准入；外部后端的请求、数据库或存储限制由后端执行，
   特定后端的适配不能变成所有 catalog 的共同上限。保留格式与真实表示限制，例如 P 的 u16 occurrence 数、
   数据版本格式声明的最大值、合法 token、引用、键、完整帧和规范编码。取消容量门不取消语义验证。
2. **读方覆盖写方。** 同一受支持格式中，写方可产生的完整合法文档必须能经过 REST 核验、领域读取、
   Accelerator 装入、重开恢复与候选发现，不在后续阶段增加更窄的定义额度。改变这条合同必须一起修改相关作者与消费者。
   这不是无限资源或外部服务永远接受的承诺：实际后端拒绝、磁盘/运行期资源失败仍保留原错误与 owner。
3. **StateStore 容量归具体后端。** `Key`、`Value` 等 API 类型不再携带公共容量上限；
   Provider 用 `StateStoreCapacity` 声明自身真实边界，打开时校验请求的 limits 不超过声明。
   SQLite 按真实长度/record 布局解释能力，不为实验性远程后端保留公共小值额度；投影继续保存为一个值。
   将来远程 Store 进入生产前，须在自己的 ADR 中解决大值、事务、隔离、故障及恢复合同，不把分键责任推给 MV。
4. **核验由请求决定，拒绝由证据决定。** 被点名属性按其期望值的 UTF8 字节长度多保留一个字节，
   可区别完全相等与更长的值；被核对的键集合就是请求点名集合，不另设属性值 64KiB 或 32 键额度。
   保留量不随回应中的快照/schema 数增长，完整 JSON、通用键/ref 边界、重复字段与身份检查继续生效。
   REST 413 映射为 `Rejected(ResourceExhausted)`，同次 attempt 自有产物沿既有确定拒绝清理；
   用户错误保留拒绝类别与状态码并遵循原脱敏规则。400、404 可能发生在发布之后，仍为 Unknown；415 等其他状态分类不变。

5. **ProviderRead 保留累计事实，退役独立合计额度。** 删除 `ProviderReadProjectionLimits::max_input_version_bytes`
   及 encode/decode 两侧的累计额度比较，不以新数字或 `usize::MAX` 替代。
   `ProviderReadProjectionFacts::input_version_bytes`、checked sum、布局与算术溢出检查、work 计量及引用校验保留。
   其余五项资源门——读取定义数、allocation requests、request bytes、source/request coexist 与 work——保持原作者和原值；
   单个 `ConnectorReadInputVersion` 的 1..4096 表示合同不变。较大的累计合法版本仍可能因这些保留的资源检查被拒，
   不能把累计额度退役解释为任意规模读取均可执行。
6. **冻结策略通过既有原生岛隔离。** 共享编译政策的变化使用既有 `PLAN_CONTRACT_REVISION` 从 21 提至 22，
   进入 `NativeCompatibilityId` 及 package required contract 的准确匹配。旧、新冻结策略不得混用同一原生岛，
   也不因某个小查询恰能互通而协商放行；不能依赖源码 hash 自动改变兼容身份。
   Protobuf 字段、package wire 版本与 provider 私有格式不变，不另起兼容入口或 fallback。
   [ADR-0185](ADR-0185-compiled-packages-are-the-sole-static-plan-path.md) 的唯一 compiled 路径、原 ingress 分工与 exact-island 规则继续有效，
   本条不重新定义它们。

本条仅部分替代 [ADR-0180](ADR-0180-inline-keyed-documents-and-snapshot-records.md) 裁决 1 的额度表及其派生检查，
并以实测成本/后端拒绝取代“何时重新评估”第一条的固定大小触发条件；内联存放、身份、写入形状、回收、
证据与外部回滚边界继续有效。[REST 会话版 ADR-0175](ADR-0175-iceberg-rest-session-authority-and-mutation-evidence.md)
仅替代裁决 23 中请求属性的固定保留量表述，以及裁决 16 的确定拒绝状态集；其余认证、路由、完整校验、归属与脱敏规则保留。
同号的 [测试能力版 ADR-0175](ADR-0175-test-capabilities-and-finite-role-authorization.md) 不在替代范围。

## 接受的妥协（诚实记录）

- **提交成本真实存在。** Iceberg 每次提交仍重写整份 metadata.json；大定义随属性进入每个元数据文件，
  发布时还要重写引用闭包，请求和服务端保存成本随之增加。去掉额度没有消除这些复制与 I/O。
- **历史由生命周期承担。** 普通刷新复用 B，不为每个快照复制稳定版本；建表、定义变化、重分区或重绑定产生新版本时，
  被保留快照引用的旧闭包仍存在，直到真实生命周期 owner 回收。不能把总历史增长转换成新定义的固定 DDL 拒绝。
- **加载与本地文件会变大。** 编解码、封套、完整读取和 Accelerator 装入有随定义增长的临时占用，SQLite 文件也随投影增大。
  没有额外文档额度意味着这些成本会到达原运行期资源边界；本裁决不声称 FE 的全部分配都已取得统一事前预算，
  不承诺避免物理分配失败，也不另造 Connector/MV 专属信号量或资源 owner。
- **编译兼容有升级成本。** 独立版本合计额度退役后，更多合法读取可到达其余编译资源门；累计计数仍不授予容量。
  plan contract revision 的变化会分隔旧、新 FE/BE，即使 wire 字段没变也需要同岛程序一起更新，不提供混合策略的宽松窗口。
- **后端可以拒绝。** REST 服务/代理或后端数据库仍可能限制请求或值。NovaRocks 如实报告其原分类，
  不对每个部署猜测一个全局预检值。未来远程 StateStore 的大值支持有额外实现成本，须由其自身架构承担。
- **回退有单向边界。** 格式、身份与键名不变，已有小文档无需迁移；新写入的大定义可能无法由仍执行旧额度的程序读取。
  不提供降级截断、双格式或悄悄缩短原文；回退前须处理这类定义，开发环境可清理相应对象后回退。

## 何时重新评估

1. 实测提交请求、metadata 历史、完整加载或 Accelerator 装入成本成为可量化瓶颈，或受支持部署出现后端拒绝时，
   重新比较伴生表与内联方案，必须同时解决对象生命周期、隔离与跨对象原子性；不因任意文档大小数字触发迁移。
2. 远程 StateStore 具备可验证的生产 tenancy、复制、fencing、故障和恢复设计，且其真实单值/事务边界要求分片时，
   在该后端的 ADR 中评估分键的大值表示与成本。保持 MV 逻辑投影合同，不恢复公共小值上限。
3. 受支持 REST 后端的真实证据表明某个状态码不能提供当前结论，或提供了更准确的确定拒绝合同，
   由该后端适配重新核对状态集与清理资格；不能把本文对 413 的判定泛化为其他 4xx 的清理授权。
4. 真实资源测量证明需要新的治理接线时，沿原进程、查询与工作资源 owner 处理；任何新增硬限制须有协议、
   实现或测量依据，并明确配置、计量单位、失败语义与读写一致性，不能仅以“有界更安全”为由重建文档额度。
