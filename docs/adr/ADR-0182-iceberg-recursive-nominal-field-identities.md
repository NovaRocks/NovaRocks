---
id: ADR-0182
title: "Iceberg nominal domains persist by stable recursive source field IDs"
domain: [provider-spi]
status: active
supersedes: []
superseded-by: null
date: 2026-10-17
provenance:
  - "discussion: 2026-10 accepted Provider-owned recursive nominal declarations, stable field identity and exact historical validation"
code-anchors:
  - "novarocks/connector/iceberg/src/schema_mapping/nominal_declaration.rs (from_metadata, from_columns, append_definition, validate_frozen_schema)"
  - "novarocks/connector/iceberg/src/catalog_control/catalog_mutation.rs (prepare_table_creation, scalar_integer_schema_updates)"
  - "novarocks/connector/iceberg/src/catalog_control/type_mapping.rs (schema_fields)"
  - "novarocks/connector/iceberg/src/schema_mapping/typed_value.rs (from_field)"
  - "novarocks/connector/iceberg/src/typed_read/codec/program_fields/typed_compare.rs (value)"
---

## 问题

Iceberg 原生类型不能区分 NovaRocks 的 HLL、Bitmap 和 LARGEINT 时，持久化的名义领域由谁声明，如何在嵌套投影、字段改名及历史读取之间保持同一身份？

## 背景与执行事实

[ADR-0176](ADR-0176-recursive-execution-types-and-independent-storage-ownership.md) 分离执行语义、值布局与来源事实；[ADR-0178](ADR-0178-iceberg-write-type-boundary-and-variant-layering.md) 规定 Iceberg schema 是存储权威。本条记录该分层下的 Provider 持久声明，不改变 Arrow 字节或执行类型所有权。

| 事实 | 作者与表示 |
| --- | --- |
| HLL、Bitmap 存储 | Iceberg Binary；仅显式 Provider 声明赋予对应领域 |
| LARGEINT 存储 | Iceberg Fixed(16)；普通同宽 FIXED 不自动成为 LARGEINT |
| 嵌套身份 | Iceberg 正整数 field ID，包括 Struct 字段、List element、Map key/value |
| 持久声明 | 表属性 `novarocks.nominal-types.v1`，内容为 `{"version":1,"fields":[{"id":3,"domain":"largeint"}]}` |
| 旧顶层声明 | `novarocks.logical_type.<column>`；解析为同一当前 schema 的 field ID，只作兼容输入 |
| 冻结与运行时 | Provider 私有 recipe 的精确声明和所选 schema；执行字段 metadata 不成为权威 |

领域词汇封闭为小写 `hll`、`bitmap`、`largeint`。文档拒绝未知字段、重复或非正 ID、未知领域和版本；现有硬上限为 1 MiB、16,384 个声明，递归定义沿统一类型深度上限。持久文档的 v1 与 Provider 私有 codec revision 是独立版本；后者因冻结语义变化升级为 8，不将旧冻结内容自动猜测为新合同。

## 考虑过的选项

- **从 Binary/FIXED 字节、宽度或 Arrow annotation 推断领域：设计否决。** 同载体有不同合法语义，来源 annotation 也不能代替已加载 metadata 的声明。
- **按嵌套名字或文本路径持久化：设计否决。** 字段改名、删除后同名新增和匿名容器角色不能保持准确历史身份。
- **仅维护顶层旧别名：设计否决。** 无法表达递归执行类型所需的子领域；保留它作为兼容输入，不作为新的完整表示。
- **Provider 以稳定 field ID 声明并在完整 schema 上校验：采用。** 持久化与冻结都由来源 owner 负责，消费端不另建类型权威。

## 裁决

1. **定义作者拥有声明。** CREATE / ALTER ADD 从已准入的完整列定义递归生成声明，按实际发布 schema 的字段 ID 绑定；ID 分配与当前 SDK 的实际递归编号保持一致。普通属性 SET/UNSET 不得写入、删除或伪造保留的 versioned namespace；旧顶层名义别名也不能绕过 schema mutation 改变领域。
2. **稳定 ID 不随名字迁移。** rename 保持 field ID；顶层兼容别名同步改名。drop 移除旧名字别名，ID 声明继续服务保留的历史 schema；同名新增使用新 ID，不继承旧领域。MODIFY 不得在未重写存储值时改换已有名义领域。
3. **兼容必须一致。** 旧顶层别名在同一加载到的当前 schema 中按名字准确解析，拒绝大小写歧义、缺失或存储类型冲突；与 v1 对同一 ID 的声明必须相同。没有显式声明的普通 Binary/Fixed(16) 保持普通存储语义。
4. **历史与选择都严格验证。** 声明 ID 在每份包含该 ID 的保留 schema 中必须匹配原存储类型，且至少存在于一份保留 schema。冻结所选 schema 的准确 ID 子集；解码拒绝声明指向不存在的源字段。Struct/List/Map 投影和运行时类型验证消费同一来源声明，不能因未投影其他列而为选中字段猜测领域。
5. **Provider 私有闭合。** 外部 Arrow annotation 只是准确声明的向外投影；冲突或无声明的名义 annotation 拒绝，不能反向签发领域。Native 只承载原 Provider opaque recipe；不扩展中立 SPI 的存储类型，也不恢复执行 metadata 作者。所有结构创建和晚到引用继续遵循 ADR-0176 与 [ADR-0167](ADR-0167-size-banded-allocation-attribution.md) 的真实 owner、控制和最后引用退出合同。

## 接受的妥协（诚实记录）

- 标准 Iceberg 引擎看到 Binary/Fixed(16) 存储，不自动理解 NovaRocks 名义领域；本条不承诺 HLL/Bitmap 计算或 LARGEINT 名义解释的跨引擎互操作。
- 外部工具若破坏保留声明、复用 ID 或更换同 ID 存储类型，读取明确拒绝。缺失历史字段声明不会从现存字节修复；孤立 ID 的自动清理不属于本实现。
- 保留旧顶层别名增加两种表示的一致性校验；新递归声明仍只有一份稳定 ID 真相。

## 何时重新评估

- Iceberg 提供这些名义领域的正式存储类型或标准扩展，并有独立实现互读证据时，评估迁移表示；迁移不得靠载体宽度推断旧领域。
- 历史 schema 清理需要回收声明或外部工具需安全改写属性时，先定义准确历史闭包、迁移与冲突规则。
- 新名义领域或 SDK 字段编号规则变化时，显式升级/扩展 Provider 词汇并补齐 CREATE/ADD、rename/drop/name reuse、所选及历史 schema 与冻结重放验证。
