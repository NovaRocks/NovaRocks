---
id: ADR-0176
title: "Recursive execution semantics are separate from layout, encoding and source ownership"
domain: [sql-compiler, memory-governance, low-cardinality]
status: active
supersedes: []
superseded-by: null
date: 2026-10-14
provenance:
  - "discussion: 2026-10 recursive execution types and independent schema construction responsibility"
code-anchors:
  - "novarocks/type-contract/src/value_type.rs (ValueTypeRef, ValueField, ValueKind)"
  - "novarocks/type-contract/src/value_layout.rs (ValueLayout)"
  - "novarocks/type-contract/src/value_schema.rs (TypedValueBinding, ValueSchema)"
  - "novarocks/type-contract/src/value_schema_creation.rs (ValueSchemaCreationScope, ValueSchemaCreationFactory)"
  - "novarocks/plan-codec/src/physical_type_v2.rs (DecodedSourceFieldTable)"
  - "novarocks/execution/src/exec/chunk/chunk_impl.rs (Chunk)"
---

## 问题

查询执行如何共享完整的递归类型语义，同时让物理布局、每批编码、外部字段事实和独立引用的构造责任各归其真正的拥有者？

## 背景与执行事实

执行语义必须在 SQL 作者、冻结计划、函数、Connector、Chunk、exchange 和结果出口之间保持一致。Arrow 的同一载体可以表示不同领域；例如 Binary 可以承载普通字节、bitmap 或序列化聚合状态。嵌套字段的 NULL 保证和领域同样是语义，不能只验证根节点或根据载体猜测。

| 实体 | 拥有的事实 | 使用约束 |
|---|---|---|
| `ValueTypeRef`、`ValueField` | 递归语义、NULL、领域、命名结构的名字及顺序 | 与分配身份无关，独立等义对象可以绑定 |
| `ValueLayout` | 去除运行时编码包装后的准确 Arrow 值布局 | 保留 offset 宽度和真实布局参数，不授予编码消费能力 |
| `TypedValueBinding`、`FunctionValueType` | 完整语义与准确值布局的组合 | 逻辑签名比较只消费语义；物理绑定比较同时消费布局 |
| `ValueSchema` | 有序语义字段与完整 `ValueTypeRef` | 重复列 occurrence 仍是独立位置，不能按名字去重；本体不包含物理布局 |
| `ChunkSchema`、`ChunkSlotSchema`、`StaticLayout` | 批或程序的有序输出及其准确 typed binding | 由实际组合作者保留准确值布局，不能仅由 `ValueSchema` 推出 |
| `DecodedSourceFieldTable` | 外部来源的字段 metadata 与来源投影 | ID、default、字典属性等不能成为执行类型的第二作者 |
| Arrow 数组 | 本批实际编码、buffer、offset、NULL | 按具体消费边界校验并适配，不能反推领域 |
| construction scope 与 storage lifetime | 分配前的实际创建责任、最后引用的保活 | 责任保活不授予下一次分配权；不等于物理 allocator 总账 |

`ValueLayout` 使用同一 normalizer 的纯入口与 creator 入口。`ValueSchemaCreationFactory` 开启独立实体或临时操作的责任；`ValueSchemaCreationScope` 在作者分配之前观察实际 Layout 或有根据的库内结构上界。小型 `ValueTypeStorageLifetime` 可以随独立 child、field、layout、schema 或投影存活。

本文记录已接受的架构合同。实现的构建、功能和分布式验收由所属变更的验证材料证明，不能由 ADR 状态代替。

## 考虑过的选项

**以 Arrow metadata 或同一 schema 指针作为执行真相：设计否决。** 这能少改调用面，但把外部属性、名义领域和分配来源混在一起；等义独立对象被拒绝，领域不同的同载体又可能被接受。指针共享只能加速已经完整验证的对象。

**只保留载体或根节点领域：设计否决。** 这种类型不能表达嵌套 JSON、命名 Struct/Union、子节点 NULL 或聚合状态合同，无法承担冻结边界的精确校验。

**由 FE 冻结每个 BE 算子的编码消费能力：设计否决。** 计划语义必须跨部署稳定，而编码快路径属于 BE 本地实现。某算子缺少字典快路径不意味着值类型不合法。

**让全部类型引用持有 query、factory 或整个构造主账：设计否决。** 独立小引用会延长无关对象的责任和生命周期；而在 query 终止时先释放主账，又会遗漏真实晚到引用。独立引用必须保留自己所依赖的有限结构责任。

**进程全局类型 interning：待评估。** 它可能减少重复结构，但同时引入全局缓存寿命、竞争、淘汰和责任归属问题。当前使用 owner 内不可变共享与借用比较；类型等义不依赖全局 registry。

## 裁决

1. **递归语义唯一作者。** List 与 Map 的子节点按 item/key/value 角色匿名；Struct 与 Union 的字段名字、顺序、NULL 和领域属于语义。不能把匿名角色的来源名字带入执行签名，也不能丢弃命名结构的名字。
2. **语义、布局、编码分开。** String/Binary 的逻辑身份不受 offset 宽度影响，准确物理绑定仍保留该宽度。Dictionary 等运行时包装属于实际数组；全引擎合法编码和当前消费者的直接消费能力分别验证。缺少直接能力时由真实消费 owner 在获得原资源许可后适配，不能无条件重标数组。
3. **执行 metadata 为空。** 新建的执行 schema 与递归 Field 使用零容量 metadata；来源属性保留在来源 owner。Connector 写入的 field ID/default/lineage 使用真实 target/source 事实，不从执行 Arrow metadata 恢复。
4. **FE 纯构造，BE 真正创建。** FE 保持结构限额及取消/超时控制，不增加内存配额、MEM admission 或成功 no-op scope。BE 的实际作者在分配前取得原 owner 的创建责任，原 Host/Control/资源原因跨边界保持，不用错误文本重分类。
5. **独立引用独立保活。** 不可变结构可以 cheap clone；新创建的结构由自己的小型 hold 负责。临时 expected 类型只持有临时 scope，准备产物不能保留这份临时主账。最后实际引用退出才释放相应责任，取消和 terminal 不能代替退出。
6. **保留原合同的严格校验。** FE 冻结完整声明，BE 用同一个原类型作者验证。SQL 的公开 NULL 声明反映实际表达式与错误策略保证；root 保持精确校验。不能靠第二 resolver、从 selected 结果反签 expected、改为 Unsupported 或放宽 oracle 通过迁移。

类型深度、单层编码语法与类型遍历工作区由 [ADR-0279](ADR-0279-one-logical-type-depth-and-schema-free-constants.md) 细化。裁决 4 的原构造与资源职责继续适用。

## 接受的妥协（诚实记录）

这要求所有真实构造、投影、writer、exchange 和 root 边界显式接线，改造量大于继续使用 metadata。结构存在共享头与有限 hold 成本；已经验证的不可变对象优先共享，仅用于比较的类型优先借用原完整声明，避免创建随后丢弃的拥有对象。

实际请求责任、库内结构上界、allocator 归属、cgroup 和 RSS 是不同观测，不声称相互相等。此合同不扩展支持的载体或函数，不改变原 credit、window、容量政策或 Worker 实际退出合同，也不取代 ADR-0167 的分配归属模型。性能成本需要独立测量，功能通过不能证明成本已经满足目标。

## 何时重新评估

- owner 内重复类型创建成为经测量确认的主要成本时，评估更广的不可变共享或全局 interning；先明确缓存寿命、并发和淘汰责任，不凭类型 hash 推出生命周期安全。
- 新载体或编码进入支持集合时，补齐实际形状、消费能力、域/布局校验及原资源许可；不能仅因 Arrow 库支持就宣布全引擎支持。
- 新 provider 引入 ID/default/其他来源字段事实时，扩展来源合同及 owner 投影，不恢复执行 metadata 作者。
- 独立引用出现跨 task、缓存或不同 Application 生命周期的持有时，重新验证有限责任的最后引用退出，不用 query 终止替代回收。
