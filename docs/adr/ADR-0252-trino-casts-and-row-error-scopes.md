---
id: ADR-0252
title: "Trino casts, coercion, and row error scopes"
domain: [sql-compiler, execution]
status: active
supersedes: []
superseded-by: null
date: 2026-11-22
provenance:
  - "TRC-6b spec and plan v2, approved 2026-11-21"
  - "Trino 484 terminal-page comparisons with Memory and Iceberg REST"
code-anchors:
  - "novarocks/type-contract/src/trino_coercion.rs"
  - "novarocks/expression/src/typing.rs (type_cast)"
  - "novarocks/functions/src/cast/trino.rs"
  - "novarocks/expression/src/evaluator (selected row errors)"
  - "novarocks/sql/src/analyzer/write_assignment.rs"
---

## 问题

SQL 表达式、函数绑定、写入和持久视图都需要转换值，转换的准入关系、失败范围和可观察结果取决于语境。如何保留同一个源表达式、原值类型以及选中行的错误，并让 FE 折叠和 BE 执行得到一致结果？

## 裁决

1. **冻结转换语境。** 显式 CAST、TRY_CAST、表达式的隐式提升和写入赋值各有固定语境。编译输入保有完整源类型、目标类型及原语句的位置。候选函数按签名定向匹配。比较和共同输出类型复用同一纯关系。CHAR 与有界 VARCHAR 按 Unicode 码点定长、截取或填充，声明与 UTF-8 物理载体分别冻结。
2. **由原内核运行实际转换。** 已选转换按原批次的选中行产生值、成功 NULL 或具名数据错误。TRY_CAST 消费本次转换错误，源表达式仍按原求值规则报告自己的错误。TRY 在自己的守卫作用域消费可捕获的具名行错误，保留成功邻行、不可捕获原因和原控制事实。数组的可恢复叶错误作用于所在数组的整行值。
3. **折叠与必需常量复用同一表达式单元。** 普通折叠遇到未捕获的行错误保留原式。必需常量沿原语句的控制及类型要求执行。原单元、已选目录身份、常量来源及宿主的资源回收按 [ADR-0242](ADR-0242-constant-folding-compiles-expression-units.md) 保管。
4. **写入在完整源之后定型。** VALUES、SELECT、UNION ALL 先依通常查询规则产生源列。外层按写入目标检查类型关系和实际可达值。Iceberg 的 V/W/T、原 FIXED 字节、required 和发布义务由 [ADR-0233](ADR-0233-iceberg-mor-only-and-provider-owned-write-validation.md) 的原作者保管。
5. **以准确元数据识别退役类型。** 存量 Iceberg 字段仍依 [ADR-0182](ADR-0182-iceberg-recursive-nominal-field-identities.md) 的字段 ID 和声明读取退役数值身份。普通 Iceberg FIXED 由 Provider 保管实际定长存储，标准 SQL 投影为 VARBINARY。Iceberg VIEW 按已发布的标准 schema 重新载入有界 VARCHAR 的无界公开声明，含 CHAR 的发布按 Provider 能力拒绝。

## 升级与定位

CAST 的 wire 保留旧字段编号 3、4、5，字段 6 要求有效语境。完整类型及控制域的变化将 Native 计划修订推进至 36，生产 epoch 推进至 18。部署与回退使用同一候选的 FE 和 BE。仍有独立算术消费者的设置及语义字段随 TRC-6a 交接。

查询报错时按原语句及选中行检查 Kernel 原因码、捕获范围和 HTTP 错误名。检查字符值时同时保存输出签名及原始 JSON 空格。检查写入时同时记录源列类型、目标回读和快照。遇到旧 Iceberg 名义字段时，以实际 table、schema 与 field ID 检查声明，按数据转换需要显式迁移。
