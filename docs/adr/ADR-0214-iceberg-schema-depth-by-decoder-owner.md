---
id: ADR-0214
title: "Iceberg schema depth follows the owning decoder's supported boundary"
domain: [provider-spi]
status: active
supersedes: []
superseded-by: null
partially-supersedes: [ADR-0213]
date: 2026-11-03
provenance:
  - "discussion: 2026-11-03 accepted schema nesting boundaries by decoder owner"
  - "discussion: 2026-11-03 keep finite support and withdraw maximum-depth expansion"
code-anchors:
  - "novarocks/connector/iceberg/src/metadata_json.rs (complete provider document decoding)"
  - "novarocks/connector/iceberg/src/rest_protocol/json.rs (iterative JSON scanner)"
  - "novarocks/connector/iceberg/src/schema_capability.rs (DDL admission and manifest preflight)"
  - "novarocks/connector/iceberg/src/manifest.rs (decode_manifest)"
  - "tests/sql/fixtures/iceberg-read-policy/fixture.scala (MetadataHistoryFixture)"
---

## 问题

当逻辑类型深度与格式容器深度不一致，且完整表元数据和 manifest 由不同作者解码时，Iceberg Connector 应如何保证能够写出的 schema 也能读回，并准确表达上游能力缺口？

## 背景与执行事实

| 边界 | 作者与事实 |
|---|---|
| 逻辑类型 | 类型合同的逻辑上限为 64，不等同于当前 Iceberg 端到端读写保证；顶层列开始计数，容器与标量叶分别占一层 |
| Iceberg schema JSON | schema 根对象、`fields` 数组和顶层 field 对象构成外壳；每层 STRUCT 再加类型对象、`fields` 数组与 field 对象，共三层；LIST/MAP 类型每层只加一个类型对象 |
| Provider 完整文档 | REST 完整响应、提交响应、Hadoop metadata 与内部 schema 再解析，由 Connector 在物化原 SDK 对象之前迭代检查完整语法与有限物理深度，然后一次解码 |
| SDK manifest | iceberg-rust 0.11 的 `Manifest::parse_avro` 从标准 Avro 头的 `schema` 元数据再次构造 SDK schema，使用 serde_json 默认递归上限；最多接受 127 个嵌套 JSON 容器 |
| DDL 与外部数据 | 新建与演进 schema 可在写入前用 SDK 序列化器检查；外部写入的 manifest 必须检查它实际携带的 schema，不能只检查当前表 schema |

纯 STRUCT 链的顶层列嵌套 k 层时，schema JSON 的深度为 `3k+3`，因此 41 层可解、42 层超过 SDK 边界。63 层 ARRAY 加 INT 的逻辑深度同样为 64，schema JSON 只有 66 层。边界来自实际解码路径，不是 Iceberg 格式对合法数据的限制。

嵌套 STRUCT 默认值只允许 `{}` 或 null；LIST/MAP 默认值与类型并列，每级至多两个容器，不能超过全 STRUCT 链的最坏物理深度。Provider 完整文档以实际文档外壳中顶层 field 对象的容器深度 `envelope` 推导 `envelope + 3 × (64−1)`，不使用独立的经验倍数和预留常数。

## 考虑过的选项

- **全局降低逻辑深度：设计否决。** 会拒绝 SDK 已能正确读写的 ARRAY/MAP 或混合类型，也把一个解码作者的限制扩散到其他载体。
- **全局关闭递归限制而不预检：设计否决。** 无法界定解码、Debug、Drop 与后续遍历的实际输入，不能以合法输入为由忽略运行栈。
- **Provider 按文法预检再解码原 SDK 类型：采用。** 保留完整 metadata、历史 schema、原授权与控制；扫描器只验证语法和几何，不另造 Iceberg 语义解析器。
- **自建 manifest 条目解码或移植功能性上游 PR：设计否决。** 会取得第二套统计、partition、继承等格式语义责任，或突破 ADR-0204 的依赖升级移植边界。
- **等待官方公开入口后恢复深 STRUCT：采用。** 当前对上游不能解码的合法 schema 返回 Unsupported；通过准确拒绝与恢复条件避免写出不能读回的表。

## 裁决

1. **双坐标规则。** 逻辑深度 64 与 JSON 物理容器深度分别检查。仅替代 ADR-0213 裁决 5 中 `4 × MAX_VALUE_TYPE_DEPTH + 32` 的经验推导；该条的完整预扫描、一次解析、历史与授权责任继续有效。
2. **文法上限规则。** 每种实际解码文档声明自己的 envelope，按最坏合法类型与默认值文法推导有限深度。预扫描在 SDK 对象物化之前进行；超出支持深度返回类型化 Unsupported，不把合法大 schema 报为 DataInvalid。错误 JSON 仍按原语法错误处理。
3. **先准入后发布规则。** CREATE、ALTER（含嵌套字段）、CTAS 与 MV 存储表建表，在任何 metadata 发布前，对将写入 manifest 的准确 SDK schema 序列化并扫描；超过 127 个容器时拒绝。不同表名、存在性探测或发布形式不能绕过同一 Provider 判据。
4. **实际 manifest 规则。** 唯一 manifest 读取入口先用标准 Avro reader 读取头部元数据，仅扫描其实际 `schema` JSON；超过 SDK 边界时类型化 Unsupported。条目、统计值与字段继承仍完全交给原 SDK，不以当前 schema 代替内嵌 schema。
5. **可恢复拒绝规则。** 两处拒绝保留同一原因与恢复条件：iceberg-rust 正式版提供公开的按字节和表 metadata 解码 manifest 的入口，且不依赖 `schema-id` 是否存在；依 ADR-0174 重新基线后才移除判定。保留既有安全错误投影与细节长度边界。
6. **有限支持规则。** 当前仅保留已有解析与拒绝能力，不以 metadata 可解证明相同深度的 Native scan、Writer 或值路径可用。用户已撤回本次最大深度读写扩展与最大深度验收；现有 SDK 契约、解码及 dev/release 栈材料只证明其实际局部覆盖，不作为完整 logical64 保证。两条 depth64 原生用例暂时 ignore，按现有精确 case 处置保留原因、owner、恢复条件与原失败，不计为 PASS。

## 接受的妥协（诚实记录）

合法但内嵌 schema JSON 超过 127 个容器的表暂不支持；纯 STRUCT 链约从 42 层开始拒绝。LIST/MAP 或混合深类型仍受现有 scan、Writer 与值路径能力约束，不承诺完整逻辑深度64。这个限制来自当前 SDK 对 manifest 内嵌 schema 的重新解析，不是格式规范要求，也不是资源配额。

DDL 增加一次准确 schema 的序列化与扫描；manifest 在原解码前额外读取一次标准容器头。它们不解释条目语义、不取得额外 I/O 权限，也不承诺 footer、page-index 或任意第三方解码的资源上界。

## 何时重新评估

升级 iceberg-rust 时运行 SDK 边界契约测试。若边界或解码行为改变，重新检查拒绝策略与错误投影，不能只调整常数使测试通过。正式版提供不依赖内嵌 schema、接受字节与表 metadata、无 `schema-id` 前提的公开入口时，按 ADR-0174 重新基线、评估删除两处 SDK 边界判定；最大深度读写与忽略用例的恢复另行明确范围并验证，不能仅因 SDK 升级自动宣称可用。

Iceberg 文档文法、默认值规则或响应外壳变化时，重新推导每种文档深度并验证真实运行栈。ADR-0213 的读取解释政策、历史只读与当前写权限仍独立有效。
