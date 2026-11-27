---
id: ADR-0278
title: "One logical type depth and schema-free constants"
domain: [sql-compiler, memory-governance, provider-spi]
status: active
supersedes: []
superseded-by: null
date: 2026-11-27
provenance:
  - "7B3F F9 design v3 and approved plan v4"
code-anchors:
  - "novarocks/type-contract/src/depth_profile.rs (logical and raw Arrow paths)"
  - "novarocks/plan-codec/src/physical_package_v2/decode.rs (compiled package admission)"
  - "novarocks/connector/iceberg/src/wire/read.rs (private column-identity wire grammar)"
  - "novarocks/spi/src/connector/row_mutation (MOR preparation identity)"
---

# ADR-0278 统一逻辑类型深度与常量记录

- 状态：已接受设计，实施验证中
- 日期：2026-11-27
- 关联：[ADR-0176](ADR-0176-recursive-execution-types-and-independent-storage-ownership.md)、[ADR-0214](ADR-0214-iceberg-schema-depth-by-decoder-owner.md)、[ADR-0221](ADR-0221-schema-free-online-units-and-prepared-layout-recipes.md)、[ADR-0233](ADR-0233-iceberg-mor-only-and-provider-owned-write-validation.md)、[ADR-0276](ADR-0276-plan-freezing-uses-frontend-planning-memory.md)

## 问题

怎样让 SQL、冻结包、Arrow 和 Iceberg 在各自入口核对同一个逻辑类型承诺，同时保留真实载体、资源和变更签发的边界？

## 背景

SQL 逻辑嵌套、Arrow 物理结构、Iceberg 私有 protobuf 的位置和 Parquet 文件格式使用不同坐标。原来的类型节点数、Writer 字段与元数据长度门，以及常量附带的 Arrow Schema，可能让合法的宽值或深值在不同边界得到不同判定。MOR 原来的 preparation 摘要由不可变对象按自己的内容重算，无法区分同一操作下内容相同的两次独立签发。

## 考虑过的选项

- 用 Arrow 物理深度作为 SQL 限额会让同一种逻辑值随 Map 或运行时编码的选择得到不同判定。
- 保留独立的结构宽度、字段大小与类型遍历工作区配额，会让可支付真实数据和元数据费用的宽值提前受拒。
- 仅在消费时剥离重复编码，已物化的 Arrow `DataType` 仍会在比较、哈希和析构时经过重复包装。
- 用 `operation_id` 或 preparation 对自身数据的重算摘要作为匹配依据，会让同一操作的不同签发互相替代。

## 裁决

1. **逻辑准入。** `novarocks-type-contract::MAX_VALUE_TYPE_DEPTH` 唯一定义 D=64，根与叶都计入。SQL 在声明和派生结果类型诞生处检查 D，超过 D 返回具名 `NOT_SUPPORTED`，先于创建 Task。Provider 由原作者检查声明，冻结包先检查扁平类型图、引用闭合及环，再物化递归 Arrow 对象。RF 继续沿现有标量准入集合。
2. **编码形状。** 一个逻辑节点可带一层 Dictionary 或 RunEndEncoded。连续的同节点包装由原生产者归一化，保留值和 NULL。引擎入口在递归物化或比较前检查包装、Map entries 与辅助整数载体。规范 Map 路径至多 `2D−1` 个 DataType 节点，raw 路径至多 `3D−1` 个，独立 Field/Carrier 的包图游标至多 `2(3D−1)` 槽。
3. **宽度与资源。** 类型遍历每层保留当前节点和下一个子项，定长游标直接随调用持有。宽类型按字段和实际内容推进。共享 backing 的实际分配计一次，重复 occurrence 仍逐次访问。类型遍历的工作区不占用构造配额，值的数组、批、缓冲和真实元数据分配沿原作者及最后别名寿命保管。本条调整 ADR-0176 裁决 4 中的类型结构限额。
4. **常量线路。** 从计划包修订 48 起，每个 `IpcConstantPool.arrow_ipc` 是一条未压缩的 V5 Arrow RecordBatch。continuation 和有符号 i32 metadata 长度在 FlatBuffer 头之前。body 包含对齐，尾随任意字节或 EOS 都拒绝。包先检查准确修订，再用原 `value_type_id` 和 `source_field_id` 恢复内存中的精确 Schema。旧 revision 46 和 47 的载荷在解释前拒绝。Native 兼容材料包含准确计划修订，生产 epoch 继续为 18。
5. **Parquet 解释与写入。** FS 的 `FileFormat::Parquet(policy)` 显式指定解释策略，prepared footer 与缓存沿同一策略。Iceberg 依据实际 footer 注解、原字段 ID 和 Provider 绑定读取。iceberg-rust 0.11 的 writer 继续写出 `ARROW:schema`，引擎读路径采用 Skip 或 Derived。允许的 V/W/T、requiredness、FixedSize 位宽和实际 Writer 保留费用沿现有作者。
6. **私有位置。** Iceberg ColumnIdentity 的逻辑深度以原列身份根计，私有校验以列和递归身份计 D+1 个位置。外层 split、predicate 和 change-window 消息是独立的有限文法。把这些信封计入列身份会让同一个深类型只在某些读路径失败。Provider 私有 codec 按此在 Prost 解码前核对形状。SDK manifest 边界继续按 ADR-0214 的原文档和解码作者检查。
7. **MOR 身份。** 每次成功签发 preparation 后生成不透明进程内身份。克隆保留同一身份，相同 `operation_id` 的独立签发得到不同身份。读写按原表、快照和签发身份核对，再沿原 Session 及发布校验。源 schema 与重新读取的当前表以及 temporal source 的内容比较继续使用显式字段编码。

## 栈验证

在 dev 和 release 中用每种生产线程允许的最小配置验证。D 深的值走原生准入和实际消费。进程隔离的 2D 样本由白盒探查交给实际线程和适用的库入口，覆盖 List、Struct、Map、支持的 Dictionary 叶和最坏混合形状。记录实际栈容量、类型及操作、构建身份、返回和真正退出。形状在 2D 上通过给出所测路径的余量下界。更早生效的格式能力拒绝和真实栈溢出分别取证。

目前开发预探查通过了 FE bounded worker 和 Iceberg catalog bridge 的 D/2D 样本。Darwin 报告的实际栈容量分别为 16,789,504 和 2,109,440 字节。探查调用了 Arrow 形状构造、比较、哈希、Debug、Clone 与析构，以及 LIST schema 的原 SDK 序列化和反序列化。材料见任务证据目录的 `20261127T091802Z-P09-owner-preflight.GY9jGyCo`。最终两种构建的线程覆盖、配置下限与原生通过范围待补齐。

## 接受的妥协

iceberg-rust 0.11 公开 writer 无法透传 Arrow writer options。外部使用默认 Arrow 提示的读取器可能在较深提示上达到自己的 FlatBuffer 边界。SDK manifest 依赖其实际内嵌 schema，达到 ADR-0214 已定义边界时继续返回明确 Unsupported。

## 何时重新评估

- Arrow、Parquet、Prost、Serde 或 Iceberg SDK 改变递归形状时，按实际作者重新推导位置并复跑最小生产栈验证。
- SDK 公开 writer options 后，核对实际互操作结果，重新评估是否省写 Arrow 提示。
- SDK 提供满足 ADR-0214 恢复条件的 manifest 解码入口后，单独复验 Type D 和真实发布路径。
